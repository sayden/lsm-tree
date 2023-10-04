const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const UUID = @import("./pkg/zig-uuid/uuid.zig").UUID;

const RecordNS = @import("./record.zig");
const HeaderNs = @import("./header.zig");
const SstNs = @import("./sst.zig");
const OpNs = @import("./ops.zig");
const StringsNs = @import("./strings.zig");
const WalNs = @import("./wal.zig");
const DiskManagerNs = @import("./disk_manager.zig");
const WalHandlerNs = @import("./wal_handler.zig");

const DebugNs = @import("./debug.zig");

const Header = HeaderNs.Header;
const Pointer = RecordNS.Pointer;
const Sst = SstNs.Sst;
const Op = OpNs.Op;
const Record = RecordNS.Record;
const Math = std.math;
const Order = Math.Order;
const DiskManager = DiskManagerNs.DiskManager;
const WalHandler = WalHandlerNs.WalHandler;
const Iterator = @import("./iterator.zig").Iterator;
const FileData = DiskManagerNs.FileData;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const SstIndex = @import("./sst_index.zig").SstIndex;

const strcmp = StringsNs.strcmp;
const sliceEqual = std.mem.eql;

usingnamespace DebugNs;

const Error = error{ IdNotFound, EmptyFile, NoSstFilesFound };

const logns = std.log.scoped(.SstManagerNS);

pub const SstManager = struct {
    const Self = @This();
    const log = std.log.scoped(.SstManager);

    // total_files: usize = 0,

    wh: *WalHandler,
    disk_manager: *DiskManager,

    // Those 2 probably needs to be null to support the use case where the
    // sstmanager do not have any index yet (so no first and last pointer)
    first_key: ?[]const u8,
    last_key: ?[]const u8,

    indices: ArrayList(*SstIndex),

    alloc: Allocator,

    pub fn init(wh: *WalHandler, dm: *DiskManager, alloc: Allocator) !*Self {
        try Recoverer.start(dm, alloc);

        const file_entries = try dm.getFilenames("sst", alloc);
        defer {
            for (file_entries) |entry| {
                alloc.free(entry);
            }
            alloc.free(file_entries);
        }

        log.debug("Found {} SST Files", .{file_entries.len});

        var mng: *Self = try alloc.create(Self);
        mng.* = .{
            .first_key = null,
            .last_key = null,
            .alloc = alloc,
            .disk_manager = dm,
            .wh = wh,
            .indices = try ArrayList(*SstIndex).initCapacity(alloc, file_entries.len),
        };

        errdefer mng.deinit();

        for (0..file_entries.len) |i| {
            const idx = SstIndex.init(file_entries[i], alloc) catch |err| {
                switch (err) {
                    Error.EmptyFile => {
                        std.fs.deleteFileAbsolute(file_entries[i]) catch |delete_error| {
                            log.warn("Found empty file '{s}' but there was an error attempting to delete it: {}", .{ file_entries[i], delete_error });
                        };
                        continue;
                    },
                    else => return err,
                }
            };
            errdefer idx.deinit();

            try mng.addNewIndex(idx);
        }

        const newfiles = try Compacter.attemptCompaction(&mng.indices, wh, alloc);
        defer newfiles.deinit();

        for (newfiles.items) |filedata| {
            try mng.notifyNewIndexFileCreated(filedata);
        }

        return mng;
    }

    pub fn getIndicesCount(self: *Self) usize {
        return self.indices.items.len;
    }

    pub fn deinit(self: *Self) void {
        for (0..self.getIndicesCount()) |i| {
            self.indices.items[i].deinit();
        }

        if (self.first_key) |key| {
            self.alloc.free(key);
        }
        if (self.last_key) |key| {
            self.alloc.free(key);
        }

        self.indices.deinit();
        self.alloc.destroy(self);
    }

    fn addNewIndex(self: *Self, idx: *SstIndex) !void {
        try self.indices.append(idx);
        // self.total_files += 1;
        try self.updateFirstAndLastPointer(idx);
    }

    fn updateFirstAndLastPointer(self: *Self, idx: *SstIndex) !void {
        if (self.first_key) |f_key| {
            if (strcmp(idx.firstKey(), f_key).compare(Math.CompareOperator.lte)) {
                self.alloc.free(self.first_key.?);
                self.first_key = try self.alloc.dupe(u8, idx.firstKey());
            }

            if (strcmp(idx.lastKey(), self.last_key.?).compare(Math.CompareOperator.gte)) {
                self.alloc.free(self.last_key.?);
                self.last_key = try self.alloc.dupe(u8, idx.lastKey());
            }
        } else {
            self.first_key = try self.alloc.dupe(u8, idx.firstKey());
            self.last_key = try self.alloc.dupe(u8, idx.lastKey());
        }
    }

    fn notifyNewIndexFilenameCreated(self: *Self, filename: []const u8) !void {
        const idx: *SstIndex = try SstIndex.init(filename, self.alloc);
        errdefer idx.deinit();

        self.addNewIndex(idx);
    }

    /// Consumes filedata, giving ownership to the newly created index.
    /// Deinitialization happens when deinitializing self
    fn notifyNewIndexFileCreated(self: *Self, filedata: FileData) !void {
        const idx: *SstIndex = try SstIndex.initFile(filedata, self.alloc);
        errdefer idx.deinit();

        try self.addNewIndex(idx);
    }

    pub fn append(self: *Self, r: *Record) !void {
        return if (try self.wh.append(r, self.alloc)) |file_data| {
            try self.notifyNewIndexFileCreated(file_data);
        };
    }

    pub fn appendOwn(self: *Self, r: *Record) !void {
        return if (try self.wh.appendOwn(r, self.alloc)) |file_data| {
            try self.notifyNewIndexFileCreated(file_data);
        };
    }

    // Looks for the key in the WAL, if not present, checks in the indices
    pub fn find(self: *Self, key: []const u8, alloc: Allocator) !?*Record {
        // Check in wal first
        if (try self.wh.find(key, alloc)) |record| {
            return record;
        }

        // if it does, retrieve the index (file) that contains the record
        const idx = self.findIndexForKey(key);
        if (idx) |index| {
            return index.get(key, alloc);
        }

        return null;
    }

    pub fn totalRecords(self: *Self) usize {
        var total: usize = 0;

        var iter = self.getIterator();
        while (iter.next()) |index| {
            total += index.header.total_records;
        }

        return total + self.wh.totalRecords();
    }

    const IndexIterator = Iterator(*SstIndex);
    fn getIterator(self: *Self) IndexIterator {
        return IndexIterator.init(self.indices.items);
    }

    pub fn persist(self: *Self, alloc: Allocator) !?FileData {
        return self.wh.persistCurrent(alloc);
    }

    // checks if key is in the range of keys
    pub fn isBetween(self: *Self, key: []const u8) bool {
        if (self.first_key) |first_key| {
            if (self.last_key) |last_key| {
                return strcmp(key, first_key).compare(std.math.CompareOperator.gte) and strcmp(key, last_key).compare(std.math.CompareOperator.lte);
            }
        }

        return false;
    }

    fn indicesHaveOverlappingKeys(level: u8, idx1: *SstIndex, idx2: *SstIndex) bool {
        var id1: [36]u8 = undefined;
        var id2: [36]u8 = undefined;
        _ = idx1.header.getId(&id1);
        _ = idx2.header.getId(&id2);

        if (idx1.header.level == level and idx2.header.level == level) {
            if (strcmp(idx1.lastKey(), idx2.firstKey()) == Order.gt) {
                log.debug("\nFound overlapping keys in index '{s}' and '{s}'\n", .{ id1, id2 });
                return true;
            }

            if (strcmp(idx2.lastKey(), idx1.firstKey()) == Order.gt) {
                log.debug("\nFound overlapping keys in index '{s}' and '{s}'\n", .{ id1, id2 });
                return true;
            }
        }

        return false;
    }

    fn lastKey(self: *Self) []const u8 {
        return self.last_pointer.key;
    }

    fn setLastKey(self: *Self, new: []const u8) void {
        self.last_pointer.key = new;
    }

    fn firstKey(self: *Self) []const u8 {
        return self.first_pointer.key;
    }

    fn setFirstKey(self: *Self, new: []const u8) void {
        self.first_pointer.key = new;
    }

    fn findIndexForKey(self: *Self, key: []const u8) ?*SstIndex {
        for (self.indices.items) |index| {
            if (index.isBetween(key)) {
                return index;
            }
        }

        return null;
    }
};

const Compacter = struct {
    const log = std.log.scoped(.Compacter);

    pub fn attemptCompaction(indices: *ArrayList(*SstIndex), wh: *WalHandler, alloc: Allocator) !ArrayList(FileData) {
        var newfiles = ArrayList(FileData).init(alloc);

        inline for (1..6) |level| {
            blk: {
                const items = indices.items;
                const total_items = items.len;

                for (0..total_items) |i| {
                    const idx1 = items[i];
                    //break if it is the last item
                    if (i == total_items) {
                        break;
                    }

                    //early continue if this file is not the expected level
                    if (idx1.header.level != level) {
                        continue;
                    }

                    for (i + 1..total_items) |j| {
                        const idx2 = items[j];
                        //avoid any kind of accidental self-merging, just to ensure more consistency in case of a regression
                        if (std.mem.eql(u8, &idx1.header.id, &idx2.header.id)) {
                            continue;
                        }

                        if (SstManager.indicesHaveOverlappingKeys(@truncate(level), idx1, idx2)) {
                            const new_file = try compactAndUpdate(indices, wh, idx1, idx2, alloc);
                            try newfiles.append(new_file);
                            break :blk;
                        }
                    }
                }
            }
        }

        return newfiles;
    }

    fn compactAndUpdate(indices: *ArrayList(*SstIndex), wh: *WalHandler, idx1: *SstIndex, idx2: *SstIndex, alloc: Allocator) !FileData {
        const wal = try compact2Indices(idx1, idx2, alloc);
        defer wal.deinit();

        const filedata = try wh.persist(wal, alloc);
        errdefer filedata.?.deinit();

        try removeIndex(indices, &idx1.header.id);
        try removeIndex(indices, &idx2.header.id);

        return filedata.?;
    }

    // TODO Does not take into account the operation (records are never deleted)
    pub fn compact2Indices(idx1: *SstIndex, idx2: *SstIndex, alloc: Allocator) !WalNs.Mem.Type {
        log.debug("Compacting lvl {} indices '{s}' and '{s}' with {} and {} records respectively", .{ idx1.header.level, idx1.header.id, idx2.header.id, idx1.header.total_records, idx2.header.total_records });

        const combined_size = idx1.size() + idx2.size();
        const level = idx1.header.level + 1;
        var wal = try WalNs.Mem.init(combined_size, null, alloc);
        errdefer wal.deinit();

        var ws = ReaderWriterSeeker.initFile(idx1.filedata.file);
        var idx1_iterator = idx1.getPointersIterator();
        while (idx1_iterator.next()) |pointer| {
            const r = try pointer.readValue(&ws, alloc);
            errdefer r.deinit();
            try wal.appendOwn(r);
        }

        var ws2 = ReaderWriterSeeker.initFile(idx2.filedata.file);
        var idx2_iterator = idx2.getPointersIterator();
        while (idx2_iterator.next()) |pointer| {
            const r = try pointer.readValue(&ws2, alloc);
            errdefer r.deinit();
            try wal.appendOwn(r);
        }

        wal.ctx.header.level = level;

        return wal;
    }

    fn removeIndex(indices: *ArrayList(*SstIndex), id: []const u8) !void {
        //Find index current position
        var pos: usize = 0;
        var found: bool = false;

        log.debug("Deleting index {s}", .{id});
        for (indices.items, 0..) |idx, i| {
            if (std.mem.eql(u8, &idx.header.id, id)) {
                pos = i;
                found = true;
                break;
            }
        }

        if (!found) {
            log.debug("Index {s} not found", .{id});
            return Error.IdNotFound;
        }

        var removed = indices.swapRemove(pos);
        defer removed.deinit();

        log.debug("Deleting file {s}", .{removed.filedata.filename});
        try std.fs.deleteFileAbsolute(removed.filedata.filename);
    }
};

const Recoverer = struct {
    /// startRecover attempts to recover a wal written on a file in a post crash scenario. If a
    /// WAL is found it will write a SstIndex file with it, regardless of its current size.
    /// Compaction at a later stage should deal with a scenario where the WAL is "too small"
    fn start(dm: *DiskManager, alloc: Allocator) !void {
        const file_entries = try dm.getFilenames("wal", alloc);
        defer {
            for (file_entries) |entry| {
                alloc.free(entry);
            }
            alloc.free(file_entries);
        }

        for (file_entries) |filename| {
            var file = try std.fs.openFileAbsolute(filename, std.fs.File.OpenFlags{});
            const stat = try file.stat();
            if (stat.size == 0) {
                continue;
            }
            logns.debug("Trying to recover file {s}", .{filename});

            var rs = ReaderWriterSeeker.initFile(file);
            const mem_wal = try WalNs.readWalIntoMem(&rs, alloc);
            defer mem_wal.deinit();

            var dest_file = try dm.getNewFile("sst", alloc);
            defer dest_file.deinit();

            var dest_writer = ReaderWriterSeeker.initFile(dest_file.file);
            _ = try WalNs.persistG(mem_wal.ctx.mem, &mem_wal.ctx.header, &dest_writer);

            //finally delete the unfinished wal file
            try std.fs.deleteFileAbsolute(filename);
        }
    }
};

const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const allocPrint = std.fmt.allocPrint;

const testObj = struct {
    dm: *DiskManager,
    wh: *WalHandler,
    s: *SstManager,

    alloc: Allocator,

    fn setup(path: []const u8, alloc: Allocator) !testObj {
        var dm = try DiskManager.init(path, alloc);
        errdefer dm.deinit();

        var wh = try WalHandler.init(dm, 512, alloc);
        errdefer wh.deinit();

        var s = try SstManager.init(wh, dm, alloc);
        errdefer s.deinit();

        return testObj{
            .dm = dm,
            .wh = wh,
            .s = s,
            .alloc = alloc,
        };
    }

    fn teardown(self: *testObj) void {
        defer self.s.deinit();
        defer self.wh.deinit();
        defer self.dm.deinit();
    }
};

test "sstmanager_init" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    const abs_path = try createSandbox("sstmanager_init", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};

    var dm = try DiskManager.init(abs_path, alloc);
    defer dm.deinit();

    var wh = try WalHandler.init(dm, 512, alloc);
    defer wh.deinit();

    var sstmanager = try SstManager.init(wh, dm, alloc);
    defer sstmanager.deinit();

    try expectEqual(@as(usize, 1), sstmanager.getIndicesCount());

    try std.fs.deleteFileAbsolute(sstmanager.indices.getLast().filedata.filename);
}

test "sstmanager_find" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    var abs_path = try createSandbox("sstmanager_find", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};
    var t = try testObj.setup(abs_path, alloc);
    defer t.teardown();

    try expectEqualStrings("hello0", t.s.first_key.?);
    try expectEqualStrings("hello6", t.s.last_key.?);

    const maybe_record = try t.s.find("hello6", alloc);
    defer maybe_record.?.deinit();

    try expectEqualStrings("hello6", maybe_record.?.getKey());
    try expectEqualStrings("world6", maybe_record.?.value);

    try expectEqual(@as(usize, 14), t.s.totalRecords());

    // this line appends an already existing key to the wal
    var record = try Record.init("hello6", "new_world", Op.Upsert, alloc);
    defer record.deinit();

    _ = try t.s.append(record);
    try expectEqual(@as(usize, 15), t.s.totalRecords());

    const maybe_record2 = try t.s.find("hello6", alloc);
    defer maybe_record2.?.deinit();

    try expectEqualStrings("new_world", maybe_record2.?.value);
}

test "sstmanager_retrieve_record" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    var abs_path = try createSandbox("sstmanager_retrieve_record", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};
    var t = try testObj.setup(abs_path, alloc);
    defer t.teardown();

    const r = try t.s.find("hello5", alloc);
    defer r.?.deinit();

    try expectEqualStrings("world5", r.?.value);
}

test "sstmanager_isBetween" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    var abs_path = try createSandbox("sstmanager_isBetween", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};
    var t = try testObj.setup(abs_path, alloc);
    defer t.teardown();

    var data = try alloc.alloc(u8, 10);
    defer alloc.free(data);

    const case = struct {
        result: bool,
        case: []const u8,
    };

    var cases = [_]case{
        .{ .result = false, .case = "hello" },
        .{ .result = false, .case = "abc" },
        .{ .result = false, .case = "zzz" },
        .{ .result = true, .case = "hello11" },
        .{ .result = true, .case = "hello10" },
        .{ .result = false, .case = "hellz12" },
        .{ .result = true, .case = "hello40" },
    };

    for (cases) |_case| {
        std.mem.copyForwards(u8, data, _case.case);
        try expectEqual(_case.result, t.s.isBetween(data[0.._case.case.len]));
    }
}

test "sstmanager_notify_new_index" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    var abs_path = try createSandbox("sstmanager_notify_new_index", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};
    var t = try testObj.setup(abs_path, alloc);
    defer t.teardown();

    const r1 = try Record.init("mario", "caster", Op.Delete, alloc);
    defer r1.deinit();

    try t.s.append(r1);

    const maybe_filedata = try t.s.persist(alloc);
    if (maybe_filedata) |filedata| {
        const file_abs_path = try std.fs.cwd().realpathAlloc(alloc, filedata.filename);
        defer alloc.free(file_abs_path);
        defer std.fs.deleteFileAbsolute(file_abs_path) catch |err| {
            logns.err("Error deleting file {s}: {}", .{ file_abs_path, err });
        };

        try t.s.notifyNewIndexFileCreated(filedata);
    } else {
        try std.testing.expect(false);
    }

    try expectEqual(@as(usize, 2), t.s.getIndicesCount());
    try expectEqual(@as(usize, 15), t.s.totalRecords());

    const maybe_record = try t.s.find("mario", alloc);
    if (maybe_record) |record| {
        defer record.deinit();
        try expectEqualStrings("mario", record.getKey());
    } else {
        try std.testing.expect(false);
    }
}

test "sst_manager_are_overlapping" {
    var alloc = std.testing.allocator;

    var idx1 = try SstIndex.init("testing/example.sst", alloc);
    var idx2 = try SstIndex.init("testing/example2.sst", alloc);
    defer idx1.deinit();
    defer idx2.deinit();

    var key1 = try alloc.dupe(u8, "hello50");
    var key2 = try alloc.dupe(u8, "hello0");
    var key3 = try alloc.dupe(u8, "hello100");
    var key4 = try alloc.dupe(u8, "hello25");

    alloc.free(idx1.lastKey());
    alloc.free(idx1.firstKey());
    idx1.setLastKey(key1);
    idx1.setFirstKey(key2);

    alloc.free(idx2.lastKey());
    alloc.free(idx2.firstKey());
    idx2.setLastKey(key3);
    idx2.setFirstKey(key4);

    try std.testing.expect(SstManager.indicesHaveOverlappingKeys(1, idx1, idx2));
}

test "compacter_attemptCompaction" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    var abs_path = try createSandbox("compacter_attemptCompaction", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};
    var t = try testObj.setup(abs_path, alloc);
    defer t.teardown();

    try expectEqual(@as(usize, 1), t.s.getIndicesCount());

    const total_expected_records: usize = 14;

    const idx3 = t.s.indices.getLast();

    try expectEqual(total_expected_records, idx3.pointers.len);

    defer deleteFile(idx3.filedata.filename);
}

test "sst_manager_start_recover" {
    // std.testing.log_level = .debug;

    var alloc = std.testing.allocator;
    const abs_path = try createSandbox("sst_manager_start_recover", alloc);
    defer alloc.free(abs_path);
    defer std.fs.deleteTreeAbsolute(abs_path) catch {};

    const dm = try DiskManager.init(abs_path, alloc);
    defer dm.deinit();

    var wal = try WalNs.File.init(512, dm, alloc);
    defer wal.deinit();

    // 0 SST file must be in the provided folder
    var files = try dm.getFilenames("sst", alloc);
    try expectEqual(@as(usize, 2), files.len);
    freeSliceData(files, alloc);

    try wal.appendOwn(try Record.init("broken0", "file0", Op.Upsert, alloc));
    try wal.appendOwn(try Record.init("broken1", "file1", Op.Upsert, alloc));
    try wal.appendOwn(try Record.init("broken2", "file2", Op.Upsert, alloc));

    var t = try testObj.setup(abs_path, alloc);
    defer t.teardown();

    // a new sst file must have been created, 2 indices must be present, one with merged "hello" keys
    // and one with "broken" keys
    try expectEqual(@as(usize, 2), t.s.getIndicesCount());

    // inserted values must be searchable
    var r4 = try t.s.find("broken0", alloc);
    defer r4.?.deinit();

    try expectEqualStrings(r4.?.getVal(), "file0");

    // no more WAL files must be present in the folder
    var files2 = try dm.getFilenames("wal", alloc);
    try expectEqual(@as(usize, 0), files2.len);
    freeSliceData(files2, alloc);

    // 1 SST file must have been created, plus the 2 in a previous line in this test
    var files3 = try dm.getFilenames("sst", alloc);
    try expectEqual(@as(usize, 2), files3.len);
    freeSliceData(files3, alloc);
}

fn freeSliceData(s: [][]const u8, alloc: Allocator) void {
    for (s) |item| {
        alloc.free(item);
    }
    alloc.free(s);
}

fn deleteFile(filename: []const u8) void {
    std.fs.deleteFileAbsolute(filename) catch |err| {
        std.debug.print("File {s} could not be deleted: {}\n", .{ filename, err });
    };
}

fn createSandbox(testname: []const u8, alloc: Allocator) ![]const u8 {
    // var dir = std.testing.tmpIterableDir(std.fs.Dir.OpenDirOptions{ .access_sub_paths = true });

    var abs_folder = try std.fmt.allocPrint(alloc, "/tmp/{s}", .{testname});
    try std.fs.makeDirAbsolute(abs_folder);

    const file1 = try copyFileToFolder("testing/example.sst", "example.sst", abs_folder, alloc);
    defer file1.close();
    const file2 = try copyFileToFolder("testing/example2.sst", "example2.sst", abs_folder, alloc);
    defer file2.close();

    return abs_folder;
}

fn copyFileToFolder(filepath: []const u8, dest_name: []const u8, folder: []const u8, alloc: Allocator) !std.fs.File {
    var abs_filepath = try std.fs.realpathAlloc(alloc, filepath);
    defer alloc.free(abs_filepath);

    var origin_file = try std.fs.openFileAbsolute(abs_filepath, std.fs.File.OpenFlags{});
    defer origin_file.close();

    var abs_folderpath = try std.fs.realpathAlloc(alloc, folder);
    defer alloc.free(abs_folderpath);

    var abs_dest_file = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ abs_folderpath, dest_name });
    defer alloc.free(abs_dest_file);

    var new_file = try std.fs.createFileAbsolute(abs_dest_file, std.fs.File.CreateFlags{});
    const stat = try origin_file.stat();
    _ = try origin_file.copyRangeAll(0, new_file, 0, stat.size);

    try new_file.seekTo(0);

    return new_file;
}
