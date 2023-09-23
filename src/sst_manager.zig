const std = @import("std");
const Header = @import("./header.zig").Header;
const Strings = @import("strings").String;

const Pointer = @import("./pointer.zig").Pointer;
const Sst = @import("./sst.zig").Sst;
const RecordPkg = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = RecordPkg.Record;
const strcmp = @import("./strings.zig").strcmp;
const stringEqual = std.mem.eql;
const Math = std.math;
const Order = Math.Order;
const DiskManager = @import("./disk_manager.zig").DiskManager;
const WalHandler = @import("./wal_handler.zig").WalHandler;
const WalResult = @import("./wal_handler.zig").Result;
const MemoryWal = @import("./memory_wal.zig").MemoryWal;

const Debug = @import("./debug.zig");
const println = Debug.println;
const prints = Debug.prints;
const print = std.debug.print;

pub const SstIndex = struct {
    header: Header,

    first_pointer: *Pointer,
    last_pointer: *Pointer,

    file: std.fs.File,
    pointers: []*Pointer,

    allocator: std.mem.Allocator,

    pub fn init(relative: []const u8, alloc: std.mem.Allocator) !*SstIndex {
        var path = try std.fs.cwd().realpathAlloc(alloc, relative);
        defer alloc.free(path);

        std.debug.print("Opening file in {s}\n", .{path});
        var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});

        var reader = f.reader();

        // read header
        const header = try Header.fromReader(reader);

        // read pointers
        var pointers = try alloc.alloc(*Pointer, header.total_records);
        for (0..header.total_records) |i| {
            var p = try Pointer.read(reader, alloc);
            pointers[i] = p;
        }

        const first_pointer = pointers[0];
        const last_pointer = pointers[pointers.len - 1];

        var s = try alloc.create(SstIndex);

        s.header = header;
        s.first_pointer = first_pointer;
        s.last_pointer = last_pointer;
        s.file = f;
        s.allocator = alloc;
        s.pointers = pointers;

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
        return Sst.tinitWithIndex(s, s.allocator);
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
            var new_pointer = try pointer.clone(alloc);
            return idx.retrieveRecordFromFile(new_pointer, alloc) catch |err| {
                new_pointer.deinit();
                return err;
            };
        }

        return null;
    }

    fn retrieveRecordFromFile(idx: *SstIndex, p: *Pointer, alloc: std.mem.Allocator) !*Record {
        try idx.file.seekTo(p.offset);
        var r = try p.readRecord(idx.file.reader(), alloc);
        r.pointer = p;

        return r;
    }

    // checks if key is in the range of keys of this sst
    pub fn isBetween(self: *SstIndex, key: []const u8) bool {
        return strcmp(key, self.first_pointer.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
    }

    pub fn debug(self: *SstIndex) void {
        std.debug.print("\nFirst key\t{first_key}\nLast key\t{last_key}\n", self);
    }
};

pub fn SstManager(comptime WalHandlerType: type) type {
    return struct {
        const Self = @This();

        wh: *WalHandlerType,
        disk_manager: *DiskManager,

        indices: []*SstIndex,
        first_pointer: ?*Pointer = null,
        last_pointer: *Pointer,
        alloc: std.mem.Allocator,

        pub fn init(relative: []const u8, wh: *WalHandlerType, dm: *DiskManager, alloc: std.mem.Allocator) !*Self {
            _ = relative;

            const file_entries = try dm.getFilenames(alloc);
            defer {
                for (file_entries) |item| {
                    alloc.free(item);
                }
                alloc.free(file_entries);
            }

            var indices = try alloc.alloc(*SstIndex, file_entries.len);
            var mng: *Self = try alloc.create(Self);
            mng.indices = indices;
            mng.alloc = alloc;
            mng.first_pointer = null;
            mng.disk_manager = dm;
            mng.wh = wh;
            mng.disk_manager = dm;

            if (file_entries.len == 0) {
                return mng;
            }

            for (0..file_entries.len) |i| {
                var idx = try SstIndex.init(file_entries[i], alloc);
                if (mng.first_pointer == null) {
                    mng.first_pointer = idx.first_pointer;
                    mng.last_pointer = idx.last_pointer;
                } else {
                    if (strcmp(idx.first_pointer.key, mng.first_pointer.?.key) == Order.lt) {
                        mng.first_pointer = idx.first_pointer;
                    }

                    if (strcmp(idx.last_pointer.key, mng.last_pointer.key) == Order.gt) {
                        mng.last_pointer = idx.last_pointer;
                    }
                }
                mng.indices[i] = idx;
            }

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

        fn getSstIndexFromWal(self: *Self, wal: *MemoryWal) !*SstIndex {
            var s: *SstIndex = try self.alloc.create(SstIndex);
            s.allocator = self.alloc;
            // s.file
            s.first_pointer = wal.pointers[0];
            s.last_pointer = wal.pointers[wal.header.total_records - 1];
            s.header = wal.header;
        }

        pub fn find(self: *Self, key: []const u8, alloc: std.mem.Allocator) !?*Record {
            // Check in wal first
            if (self.wh.find(key)) |record| {
                return record;
            }

            if (!self.isBetween(key)) {
                return null;
            }

            const idx = self.findIndexForKey(key);
            if (idx) |index| {
                return index.get(key, alloc);
            }

            return null;
        }

        // checks if key is in the range of keys
        pub fn isBetween(self: *Self, key: []const u8) bool {
            return strcmp(key, self.first_pointer.?.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
        }

        fn findIndexForKey(self: *Self, key: []const u8) ?*SstIndex {
            for (self.indices) |index| {
                if (index.isBetween(key)) {
                    return index;
                }
            }

            print("Index not found\n", .{});
            return null;
        }
    };
}

const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const allocPrint = std.fmt.allocPrint;

fn testCreateIndex(comptime pattern: []const u8, alloc: std.mem.Allocator) !*SstIndex {
    var idx: *SstIndex = try alloc.create(SstIndex);

    idx.allocator = alloc;
    idx.pointers = try alloc.alloc(*Pointer, 30);
    idx.file = try std.fs.openFileAbsolute("/dev/null", std.fs.File.OpenFlags{});

    var first: ?*Pointer = null;
    var last: ?*Pointer = null;

    for (0..30) |i| {
        var p: *Pointer = try alloc.create(Pointer);
        p.key = try allocPrint(alloc, pattern, .{i});
        p.allocator = alloc;
        if (i == 0) first = p;
        if (i == 29) last = p;
        idx.pointers[i] = p;
    }

    idx.first_pointer = first.?;
    idx.last_pointer = last.?;

    return idx;
}

test "sstmanager_retrieve_record" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    const r = try t.s.find("hello23", alloc);
    defer r.?.deinit();

    try expectEqualStrings("world23", r.?.value);
}

test "sstindex_binary_search" {
    var alloc = std.testing.allocator;
    var idx = try testCreateIndex("Hello{}", alloc);
    defer idx.deinit();

    var maybe_record = idx.find("Hello15");

    try expectEqualStrings(idx.pointers[15].key, maybe_record.?.key);
}

const testObj = struct {
    const WalType = MemoryWal(2048);
    const WalHandlerType = WalHandler(WalType);
    const SstManagerType = SstManager(WalHandlerType);

    dm: *DiskManager,
    wh: *WalHandlerType,
    s: *SstManagerType,

    fn setup(alloc: std.mem.Allocator) !testObj {
        var dm = try DiskManager.init("./testing", alloc);
        var wh = try WalHandlerType.init(dm, alloc);
        var s = try SstManagerType.init("./testing", wh, dm, alloc);

        return testObj{
            .dm = dm,
            .wh = wh,
            .s = s,
        };
    }

    fn teardown(self: *testObj) void {
        self.s.deinit();
        self.wh.deinit();
        self.dm.deinit();
    }
};

test "sstmanager_init" {
    var alloc = std.testing.allocator;
    var t = try testObj.setup(alloc);
    defer t.teardown();

    try expectEqualStrings("hello0", t.s.first_pointer.?.key);
    try expectEqualStrings("hello9", t.s.last_pointer.key);

    const maybe_record = try t.s.find("hello10", alloc);
    defer maybe_record.?.deinit();

    try expectEqualStrings("hello10", maybe_record.?.getKey());
    try expectEqualStrings("world10", maybe_record.?.value);

    // _ = try t.wh.append(try Record.init("hello10", "new_world", Op.Create, alloc));
    // const maybe_record2 = try t.s.find("hello10", alloc);
    // defer maybe_record2.?.deinit();

    // try expectEqualStrings("new_world", maybe_record2.?.value);
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
