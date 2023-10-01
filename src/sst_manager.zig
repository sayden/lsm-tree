const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const RecordPkg = @import("./record.zig");
const HeaderNs = @import("./header.zig");
const PointerNs = @import("./pointer.zig");
const SstNs = @import("./sst.zig");
const OpNs = @import("./ops.zig");
const StringsNs = @import("./strings.zig");
const Wal = @import("./wal.zig");
const DiskManagerNs = @import("./disk_manager.zig");
const WalHandlerNs = @import("./wal_handler.zig");

const DebugNs = @import("./debug.zig");

const Header = HeaderNs.Header;
const Pointer = PointerNs.Pointer;
const Sst = SstNs.Sst;
const Op = OpNs.Op;
const Record = RecordPkg.Record;
const Math = std.math;
const Order = Math.Order;
const DiskManager = DiskManagerNs.DiskManager;
const WalHandler = WalHandlerNs.WalHandler;
const Iterator = @import("./iterator.zig").Iterator;
const FileData = DiskManagerNs.FileData;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

const strcmp = StringsNs.strcmp;
const sliceEqual = std.mem.eql;

usingnamespace DebugNs;

const Error = error{IdNotFound};

const logns = std.log.scoped(.SstManagerNS);

pub const SstIndex = struct {
    const log = std.log.scoped(.SstIndex);

    header: Header,

    first_pointer: *Pointer,
    last_pointer: *Pointer,

    file: std.fs.File,
    pointers: []*Pointer,

    allocator: Allocator,

    pub fn init(relative: []const u8, alloc: Allocator) !*SstIndex {
        var path = try std.fs.cwd().realpathAlloc(alloc, relative);
        defer alloc.free(path);

        log.debug("Opening file {s}", .{path});
        var file = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});

        var ws = ReaderWriterSeeker.initFile(file);
        // read header
        const header = try Header.read(&ws);

        // read pointers
        var pointers = try alloc.alloc(*Pointer, header.total_records);
        errdefer alloc.free(pointers);

        for (0..header.total_records) |i| {
            var p = try Pointer.read(&ws, alloc);
            errdefer p.deinit();
            pointers[i] = p;
        }

        const first_pointer = pointers[0];
        const last_pointer = pointers[pointers.len - 1];

        var s = try alloc.create(SstIndex);
        s.* = SstIndex{
            .header = header,
            .first_pointer = first_pointer,
            .last_pointer = last_pointer,
            .file = file,
            .allocator = alloc,
            .pointers = pointers,
        };

        return s;
    }

    pub fn deinit(self: *SstIndex) void {
        for (self.pointers) |p| {
            p.deinit();
        }

        self.allocator.free(self.pointers);
        self.file.close();
        self.allocator.destroy(self);
    }

    pub fn size(self: *SstIndex) usize {
        // TODO To use HeaderNs.headerSize() is not backwards compatible with headers that contain less information
        return self.header.pointers_size + self.header.records_size + HeaderNs.headerSize();
    }

    pub fn getPointer(pointers: []*Pointer, index: usize) ?*Pointer {
        if (index >= pointers.len) {
            return null;
        }

        return pointers[index];
    }

    pub fn load(s: *SstIndex) !*Sst {
        return Sst.initWithIndex(s, s.allocator);
    }

    pub fn binarySearchFn(_: void, key: []const u8, mid_item: *Pointer) std.math.Order {
        return strcmp(key, mid_item.key);
    }

    pub fn find(idx: *SstIndex, key: []const u8) ?*Pointer {
        if (!idx.isBetween(key)) {
            return null;
        }

        //TODO Fix this binary search that seems to not work
        const i = std.sort.binarySearch(*Pointer, key, idx.pointers, {}, binarySearchFn);
        if (i) |index| {
            return idx.pointers[index];
        }

        return null;
    }

    pub fn get(idx: *SstIndex, key: []const u8, alloc: Allocator) !?*Record {
        const p = idx.find(key);
        if (p) |pointer| {
            return idx.retrieveRecordFromFile(pointer, alloc);
        }

        return null;
    }

    const PointerIterator = Iterator(*Pointer);
    pub fn getPointersIterator(self: *SstIndex) PointerIterator {
        return PointerIterator.init(self.pointers);
    }

    fn retrieveRecordFromFile(idx: *SstIndex, p: *Pointer, alloc: Allocator) !*Record {
        try idx.file.seekTo(try p.getOffset());
        var ws = ReaderWriterSeeker.initFile(idx.file);
        return try p.readValue(&ws, alloc);
    }

    // checks if key is in the range of keys of this sst
    pub fn isBetween(self: *SstIndex, key: []const u8) bool {
        return strcmp(key, self.first_pointer.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
    }

    pub fn debug(self: *SstIndex) void {
        self.log.debug("\nFirst key\t{first_key}\nLast key\t{last_key}", self);
    }
};

pub fn SstManager(comptime WalType: type) type {
    return struct {
        const Self = @This();
        const log = std.log.scoped(.SstManager);

        total_files: usize = 0,

        wh: *WalHandler(WalType),
        disk_manager: *DiskManager,
        first_pointer: *Pointer,
        last_pointer: *Pointer,

        indices: ArrayList(*SstIndex),

        alloc: Allocator,

        pub fn init(wh: *WalHandler(WalType), dm: *DiskManager, alloc: Allocator) !*Self {
            const file_entries = try dm.getFilenames("sst", alloc);
            defer {
                for (file_entries) |entry| {
                    alloc.free(entry);
                }
                alloc.free(file_entries);
            }

            var first_pointer: ?*Pointer = null;
            var last_pointer: ?*Pointer = null;

            var indices = try ArrayList(*SstIndex).initCapacity(alloc, file_entries.len);
            errdefer indices.deinit();
            // var indices = try alloc.alloc(*SstIndex, file_entries.len + 10);
            // errdefer alloc.free(indices);

            var total_files: usize = 0;
            for (0..file_entries.len) |i| {
                var idx = try SstIndex.init(file_entries[i], alloc);
                errdefer idx.deinit();

                if (first_pointer == null) {
                    first_pointer = idx.first_pointer;
                    last_pointer = idx.last_pointer;
                } else {
                    if (strcmp(idx.first_pointer.key, first_pointer.?.key) == Order.lt) {
                        first_pointer = idx.first_pointer;
                    }

                    if (strcmp(idx.last_pointer.key, last_pointer.?.key) == Order.gt) {
                        last_pointer = idx.last_pointer;
                    }
                }
                try indices.append(idx);
                // indices[i] = idx;
                total_files += 1;
            }

            var mng: *Self = try alloc.create(Self);

            mng.* = .{
                .indices = indices,
                .alloc = alloc,
                .disk_manager = dm,
                .wh = wh,
                .first_pointer = first_pointer.?,
                .last_pointer = last_pointer.?,
                .total_files = total_files,
            };

            return mng;
        }

        pub fn deinit(self: *Self) void {
            for (0..self.total_files) |i| {
                self.indices.items[i].deinit();
            }
            self.indices.deinit();
            // self.alloc.free(self.indices);
            self.alloc.destroy(self);
        }

        fn notifyNewIndexFileCreated(self: *Self, filename: []const u8) !void {
            var idx: *SstIndex = try SstIndex.init(filename, self.alloc);
            errdefer idx.deinit();

            try self.indices.append(idx);
            self.total_files += 1;

            // Check if there is still space in the indices array
            // if (self.total_files < self.indices.len) {
            //     self.indices[self.total_files] = idx;
            //     self.total_files += 1;
            //     return;
            // }

            // if no space is available, resize or rewrite the index table
            // const current_size: usize = self.total_files;
            // const is_resized: bool = self.alloc.resize(self.indices, current_size + 10);
            // if (is_resized) {
            //     self.indices.len = current_size + 10;
            //     self.indices[current_size] = idx;
            //     self.total_files += 1;
            // } else {
            // var indices = try self.alloc.alloc(*SstIndex, current_size + 10);
            // errdefer self.alloc.free(indices);

            // for (0..current_size) |i| {
            //     indices[i] = self.indices[i];
            // }

            // indices[current_size] = idx;
            // self.total_files += 1;

            // self.alloc.free(self.indices);
            // self.indices = indices;
            // }
        }

        pub fn append(self: *Self, r: *Record) !void {
            return if (try self.wh.append(r, self.alloc)) |file_data| {
                defer file_data.deinit();
                try self.notifyNewIndexFileCreated(file_data.filename);
            };
        }

        // Looks for the key in the WAL, if not present, checks in the indices
        pub fn find(self: *Self, key: []const u8, alloc: Allocator) !?*Record {
            // Check in wal first
            if (try self.wh.find(key, alloc)) |record| {
                return record;
            }

            // check if it exists at all on the files
            if (!self.isBetween(key)) {
                return null;
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

        pub fn persist(self: *Self, alloc: ?Allocator) !?[]const u8 {
            return self.wh.persistCurrent(alloc);
        }

        // checks if key is in the range of keys
        pub fn isBetween(self: *Self, key: []const u8) bool {
            return strcmp(key, self.first_pointer.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
        }

        pub fn attemptCompaction(self: *Self) !void {
            var newfiles = ArrayList(FileData).init(self.alloc);
            defer newfiles.deinit();

            for (1..6) |level| {
                blk: {
                    const items = self.indices.items;
                    const total_items = self.total_files - 1;

                    for (0..total_items) |i| {
                        for (1..total_items) |j| {
                            if (indicesHaveOverlappingKeys(level, items[i], items[j])) {
                                const wal = try self.compact2Indices(items[i], items[j], self.alloc);
                                const filedata = try self.wh.persist(wal);
                                errdefer filedata.deinit();

                                try self.removeIndex(i);
                                try self.removeIndex(j);

                                try newfiles.append(filedata);
                                break :blk;
                            }
                        }
                    }
                }
            }

            for (newfiles.items) |new_index| {
                try self.notifyNewIndexFileCreated(new_index.filename);
                new_index.deinit();
            }
        }

        pub fn compact2Indices(_: *Self, idx1: *SstIndex, idx2: *SstIndex, alloc: Allocator) !Wal.Mem.Type {
            const combined_size = idx1.size() + idx2.size();
            const level = idx1.header.level + 1;
            var wal = try Wal.Mem.init(combined_size, alloc);
            errdefer wal.deinit();

            var ws = ReaderWriterSeeker.initFile(idx1.file);
            var idx1_iterator = idx1.getPointersIterator();
            while (idx1_iterator.next()) |pointer| {
                const r = try pointer.readValue(&ws, alloc);
                errdefer r.deinit();
                try wal.appendOwn(r);
            }

            var ws2 = ReaderWriterSeeker.initFile(idx2.file);
            var idx2_iterator = idx2.getPointersIterator();
            while (idx2_iterator.next()) |pointer| {
                const r = try pointer.readValue(&ws2, alloc);
                errdefer r.deinit();
                try wal.appendOwn(r);
            }

            wal.ctx.header.level = level;

            return wal;
        }

        fn removeIndex(self: *Self, id: []const u8) !void {
            //Find index current position
            var pos: usize = 0;
            var found: bool = false;
            for (self.indices.items, 0..) |idx, i| {
                if (std.mem.eql(u8, &idx.header.id, id)) {
                    pos = i;
                    found = true;
                    break;
                }
            }

            if (!found) {
                return Error.IdNotFound;
            }

            var removed = self.indices.swapRemove(pos);
            if (!builtin.is_test) {
                try std.fs.deleteFileAbsolute(removed.file);
            }
            removed.deinit();
            self.total_files -= 1;
        }

        fn indicesHaveOverlappingKeys(level: u8, id1: *SstIndex, id2: *SstIndex) bool {
            if (id1.header.level == level and id2.header.level == level) {
                if (strcmp(id1.last_pointer.key, id2.first_pointer.key) == Order.gt) {
                    return true;
                }

                if (strcmp(id2.last_pointer.key, id1.first_pointer.key) == Order.gt) {
                    return true;
                }
            }

            return false;
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
}

const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const allocPrint = std.fmt.allocPrint;

const testObj = struct {
    const WalType = Wal.Mem;
    const WalHandlerType = WalHandler(WalType);
    const SstManagerType = SstManager(WalType);

    dm: *DiskManager,
    wh: *WalHandlerType,
    s: *SstManagerType,

    alloc: Allocator,

    fn setup(alloc: Allocator) !testObj {
        var dm = try DiskManager.init("./testing", alloc);
        errdefer dm.deinit();

        var wh = try WalHandlerType.init(dm, alloc);
        errdefer wh.deinit();

        var s = try SstManagerType.init(wh, dm, alloc);
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

test "sstindex_binary_search" {
    var alloc = std.testing.allocator;

    var idx = try SstIndex.init("./testing/example.sst", alloc);
    defer idx.deinit();

    var maybe_record = idx.find("hello6");

    try expectEqualStrings(idx.pointers[6].key, maybe_record.?.key);
}

test "sstmanager_init" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    try expectEqualStrings("hello0", t.s.first_pointer.key);
    try expectEqualStrings("hello6", t.s.last_pointer.key);

    const maybe_record = try t.s.find("hello6", alloc);
    defer maybe_record.?.deinit();

    try expectEqualStrings("hello6", maybe_record.?.getKey());
    try expectEqualStrings("world6", maybe_record.?.value);

    try expectEqual(@as(usize, 14), t.s.totalRecords());

    // this line appends an already existing key to the wal
    var record = try Record.init("hello6", "new_world", Op.Create, alloc);
    defer record.deinit();

    _ = try t.s.append(record);
    try expectEqual(@as(usize, 15), t.s.totalRecords());

    const maybe_record2 = try t.s.find("hello6", alloc);
    defer maybe_record2.?.deinit();

    try expectEqualStrings("new_world", maybe_record2.?.value);
}

test "sstmanager_retrieve_record" {
    var alloc = std.testing.allocator;

    var t = try testObj.setup(alloc);
    defer t.teardown();

    const r = try t.s.find("hello5", alloc);
    defer r.?.deinit();

    try expectEqualStrings("world5", r.?.value);
}

test "sstmanager_isBetween" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
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

test "sstmanager_persist" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    const res = try t.s.persist(null);
    // no records, so a null result (no error, no file written) must be returned
    try std.testing.expect(res == null);

    const record = try Record.init("hello", "world", Op.Delete, alloc);
    defer record.deinit();

    try t.s.append(record);

    const maybe_filename = try t.s.persist(alloc);
    if (maybe_filename) |filename| {
        defer alloc.free(filename);
        try std.fs.deleteFileAbsolute(filename);
    }
}

test "sstmanager_notify_new_index" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    const r1 = try Record.init("mario", "caster", Op.Delete, alloc);
    defer r1.deinit();

    try t.s.append(r1);

    try expectEqual(@as(usize, 2), t.s.total_files);

    const maybe_filename = try t.s.persist(alloc);
    if (maybe_filename) |filename| {
        defer alloc.free(filename);

        const abs_path = try std.fs.cwd().realpathAlloc(alloc, filename);
        defer alloc.free(abs_path);
        defer std.fs.deleteFileAbsolute(abs_path) catch |err| {
            logns.err("Error deleting file {s}: {}", .{ abs_path, err });
        };

        try t.s.notifyNewIndexFileCreated(filename);
    }
    try expectEqual(@as(usize, 3), t.s.total_files);

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

    var t1 = try testObj.setup(alloc);
    defer t1.teardown();

    var t2 = try testObj.setup(alloc);
    defer t2.teardown();

    var idx1 = t1.s.indices.items[0];
    var idx2 = t2.s.indices.items[0];

    var key1 = try alloc.dupe(u8, "hello50");
    var key2 = try alloc.dupe(u8, "hello0");
    var key3 = try alloc.dupe(u8, "hello100");
    var key4 = try alloc.dupe(u8, "hello25");

    alloc.free(idx1.last_pointer.key);
    alloc.free(idx1.first_pointer.key);
    idx1.last_pointer.key = key1;
    idx1.first_pointer.key = key2;

    alloc.free(idx2.last_pointer.key);
    alloc.free(idx2.first_pointer.key);
    idx2.last_pointer.key = key3;
    idx2.first_pointer.key = key4;

    try std.testing.expect(SstManager(Wal.Mem).indicesHaveOverlappingKeys(1, idx1, idx2));
}

test "sstmanager_compact_indices" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    const idx1 = t.s.indices.items[0];
    const idx2 = t.s.indices.items[1];
    const total_expected_records: usize = idx1.header.total_records + idx2.header.total_records;

    try expectEqual(@as(usize, 2), t.s.total_files);

    var wal = try t.s.compact2Indices(idx1, idx2, alloc);
    defer wal.deinit();

    try t.s.removeIndex(&idx1.header.id);
    try t.s.removeIndex(&idx2.header.id);

    const filedata = try t.wh.persist(wal);
    defer filedata.deinit();
    defer deleteFile(filedata.filename);

    try t.s.notifyNewIndexFileCreated(filedata.filename);

    try expectEqual(@as(usize, 1), t.s.total_files);
    try expectEqual(@as(usize, total_expected_records), t.s.indices.items[0].header.total_records);
}

fn deleteFile(filename: []const u8) void {
    std.fs.deleteFileAbsolute(filename) catch |err| {
        std.debug.print("File {s} could not be deleted: {}\n", .{ filename, err });
    };
}
