const std = @import("std");

const RecordPkg = @import("./record.zig");
const HeaderNs = @import("./header.zig");
const PointerNs = @import("./pointer.zig");
const SstNs = @import("./sst.zig");
const OpNs = @import("./ops.zig");
const StringsNs = @import("./strings.zig");
const MemoryWalNs = @import("./memory_wal.zig");
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
const WalAppendResult = WalHandlerNs.AppendResult;
const MemoryWal = MemoryWalNs.MemoryWal;
const WalError = MemoryWalNs.Error;
const Iterator = @import("./iterator.zig").Iterator;
const FileData = DiskManagerNs.FileData;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

const strcmp = StringsNs.strcmp;
const sliceEqual = std.mem.eql;

usingnamespace DebugNs;

const Error = error{ResizeAttemptFailed};

const logns = std.log.scoped(.SstManagerNS);

pub const SstIndex = struct {
    const log = std.log.scoped(.SstIndex);

    header: Header,

    first_pointer: *Pointer,
    last_pointer: *Pointer,

    file: std.fs.File,
    pointers: []*Pointer,

    allocator: std.mem.Allocator,

    pub fn init(relative: []const u8, alloc: std.mem.Allocator) !*SstIndex {
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
        return self.header.pointers_size + self.header.records_size + self.header.header_size;
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

    pub fn get(idx: *SstIndex, key: []const u8, alloc: std.mem.Allocator) !?*Record {
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

    fn retrieveRecordFromFile(idx: *SstIndex, p: *Pointer, alloc: std.mem.Allocator) !*Record {
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

pub fn SstManager(comptime WalHandlerType: type) type {
    return struct {
        const Self = @This();
        const log = std.log.scoped(.SstManager);

        total_files: usize = 0,

        wh: *WalHandlerType,
        disk_manager: *DiskManager,
        first_pointer: *Pointer,
        last_pointer: *Pointer,

        indices: []*SstIndex,

        alloc: std.mem.Allocator,

        pub fn init(wh: *WalHandlerType, dm: *DiskManager, alloc: std.mem.Allocator) !*Self {
            const file_entries = try dm.getFilenames(alloc);
            defer {
                for (file_entries) |entry| {
                    alloc.free(entry);
                }
                alloc.free(file_entries);
            }

            var first_pointer: ?*Pointer = null;
            var last_pointer: ?*Pointer = null;

            var indices = try alloc.alloc(*SstIndex, file_entries.len + 10);
            errdefer alloc.free(indices);

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
                indices[i] = idx;
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
                self.indices[i].deinit();
            }
            self.alloc.free(self.indices);
            self.alloc.destroy(self);
        }

        fn notifyNewIndexCreated(self: *Self, filename: []const u8) !void {
            var idx: *SstIndex = try SstIndex.init(filename, self.alloc);
            errdefer idx.deinit();

            // Check if there is still space in the indices array
            if (self.total_files < self.indices.len) {
                self.indices[self.total_files] = idx;
                self.total_files += 1;
                return;
            }

            // if no space is available, resize or rewrite the index table
            const current_size: usize = self.total_files;
            const is_resized: bool = self.alloc.resize(self.indices, current_size + 10);
            if (is_resized) {
                self.indices.len = current_size + 10;
                self.indices[current_size] = idx;
                self.total_files += 1;
            } else {
                var indices = try self.alloc.alloc(*SstIndex, current_size + 10);
                errdefer self.alloc.free(indices);

                for (0..current_size) |i| {
                    indices[i] = self.indices[i];
                }

                indices[current_size] = idx;
                self.total_files += 1;

                self.alloc.free(self.indices);
                self.indices = indices;
            }
        }

        pub fn append(self: *Self, r: *Record) !void {
            return if (try self.wh.append(r, self.alloc)) |file_data| {
                defer file_data.deinit();
                try self.notifyNewIndexCreated(file_data.filename);
            };
        }

        //TODO
        fn getSstIndexFromWal(self: *Self, wal: *MemoryWal) !*SstIndex {
            var s: *SstIndex = try self.alloc.create(SstIndex);
            s.allocator = self.alloc;
            // s.file
            s.first_pointer = wal.pointers[0];
            s.last_pointer = wal.pointers[wal.header.total_records - 1];
            s.header = wal.header;
        }

        // Looks for the key in the WAL, if not present, checks in the indices
        pub fn find(self: *Self, key: []const u8, alloc: std.mem.Allocator) !?*Record {
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
            var iter: Iterator(*SstIndex) = Iterator(*SstIndex).init(self.indices[0..self.total_files]);
            while (iter.next()) |index| {
                total += index.header.total_records;
            }

            return total + self.wh.totalRecords();
        }

        const IndexIterator = Iterator(*SstIndex);
        fn getIterator(self: *Self) IndexIterator {
            return IndexIterator.init(self.indices);
        }

        pub fn compactIndices(self: *Self, idx1: *SstIndex, idx2: *SstIndex, alloc: std.mem.Allocator) !FileData {
            //TODO fix this 2048
            var wal = try MemoryWal(2048).init(alloc);
            defer wal.deinit();

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

            const file_data = try self.wh.persist(wal);
            try self.notifyNewIndexCreated(file_data.filename);
            return file_data;
        }

        pub fn persist(self: *Self, alloc: ?std.mem.Allocator) !?[]const u8 {
            return self.wh.persistCurrent(alloc);
        }

        // checks if key is in the range of keys
        pub fn isBetween(self: *Self, key: []const u8) bool {
            return strcmp(key, self.first_pointer.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
        }

        fn findIndexForKey(self: *Self, key: []const u8) ?*SstIndex {
            for (self.indices) |index| {
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
    const WalType = MemoryWal(2048);
    const WalHandlerType = WalHandler(WalType);
    const SstManagerType = SstManager(WalHandlerType);

    dm: *DiskManager,
    wh: *WalHandlerType,
    s: *SstManagerType,

    alloc: std.mem.Allocator,

    fn setup(alloc: std.mem.Allocator) !testObj {
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
        self.s.deinit();
        self.wh.deinit();
        self.dm.deinit();
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

        try t.s.notifyNewIndexCreated(filename);
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

test "sstmanager_compact_indices" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    const idx1 = t.s.indices[0];
    const idx2 = t.s.indices[1];

    try expectEqual(@as(usize, 2), t.s.total_files);

    var file_data = try t.s.compactIndices(idx1, idx2, alloc);
    defer file_data.deinit();
    defer deleteFile(file_data.filename);

    try expectEqual(@as(usize, 3), t.s.total_files);
}

fn deleteFile(filename: []const u8) void {
    std.fs.deleteFileAbsolute(filename) catch |err| {
        std.debug.print("File {s} could not be deleted: {}\n", .{ filename, err });
    };
}
