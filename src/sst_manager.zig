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
const WalResult = WalHandlerNs.Result;
const MemoryWal = MemoryWalNs.MemoryWal;
const WalError = MemoryWalNs.Error;

const strcmp = StringsNs.strcmp;
const sliceEqual = std.mem.eql;

usingnamespace DebugNs;

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

        log.debug("Opening file {s}\n", .{path});
        var file = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});

        // read header
        const header = try Header.read(&file);

        // read pointers
        var pointers = try alloc.alloc(*Pointer, header.total_records);
        for (0..header.total_records) |i| {
            var p = try Pointer.read(&file, alloc);
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

    fn retrieveRecordFromFile(idx: *SstIndex, p: *Pointer, alloc: std.mem.Allocator) !*Record {
        try idx.file.seekTo(try p.getOffset());
        return try p.readValue(&idx.file, alloc);
    }

    // checks if key is in the range of keys of this sst
    pub fn isBetween(self: *SstIndex, key: []const u8) bool {
        return strcmp(key, self.first_pointer.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
    }

    pub fn debug(self: *SstIndex) void {
        self.log.debug("\nFirst key\t{first_key}\nLast key\t{last_key}\n", self);
    }
};

pub fn SstManager(comptime WalHandlerType: type) type {
    return struct {
        const Self = @This();
        const log = std.log.scoped(.SstManager);

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

            var indices = try alloc.alloc(*SstIndex, file_entries.len);
            errdefer alloc.free(indices);

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
            }

            var mng: *Self = try alloc.create(Self);

            mng.* = .{
                .indices = indices,
                .alloc = alloc,
                .disk_manager = dm,
                .wh = wh,
                .first_pointer = first_pointer.?,
                .last_pointer = last_pointer.?,
            };

            return mng;
        }

        pub fn deinit(self: *Self) void {
            for (self.indices) |i| {
                i.deinit();
            }
            self.alloc.free(self.indices);
            self.alloc.destroy(self);
        }

        pub fn append(self: *Self, r: *Record) !void {
            return switch (try self.wh.append(r)) {
                WalResult.Ok => return,
                WalResult.WalSwitched => {},
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
        var wh = try WalHandlerType.init(dm, alloc);

        var s = try SstManagerType.init(wh, dm, alloc);

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

    // this line appends an already existing key to the wal
    var record = try Record.init("hello6", "new_world", Op.Create, alloc);
    defer record.deinit();

    _ = try t.s.append(record);

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
