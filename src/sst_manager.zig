const std = @import("std");
const Header = @import("./header.zig").Header;
const Strings = @import("strings").String;

const Pointer = @import("./pointer.zig").Pointer;
const Sst = @import("./sst.zig").Sst;
const RecordPkg = @import("./record.zig");
const Record = RecordPkg.Record;
const strcmp = @import("./strings.zig").strcmp;
const stringEqual = std.mem.eql;
const Math = std.math;
const Order = Math.Order;
const DiskManager = @import("./disk_manager.zig").DiskManager;

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

    pub fn init(relative: []u8, alloc: std.mem.Allocator) !*SstIndex {
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

    pub fn binarySearchFn(_: void, key: []u8, mid_item: *Pointer) std.math.Order {
        return strcmp(key, mid_item.key);
    }

    pub fn find(idx: *SstIndex, key: []u8) ?*Pointer {
        if (!idx.isBetween(key)) return null;

        const i = std.sort.binarySearch(*Pointer, key, idx.pointers, {}, binarySearchFn);
        if (i) |index| {
            return idx.pointers[index];
        }

        return null;
    }

    pub fn get(idx: *SstIndex, key: []u8, alloc: std.mem.Allocator) !?*Record {
        if (!idx.isBetween(key)) return null;
        const p = idx.find(key);
        if (p) |pointer| {
            return idx.retrieveRecordFromFile(pointer, alloc);
        }
        return null;
    }

    fn retrieveRecordFromFile(idx: *SstIndex, p: *Pointer, alloc: std.mem.Allocator) !*Record {
        try idx.file.seekTo(p.offset);
        var r = try p.readRecord(idx.file.reader(), alloc);
        r.pointer = try p.*.clone(alloc);

        return r;
    }

    // checks if key is in the range of keys of this sst
    pub fn isBetween(self: *SstIndex, key: []u8) bool {
        return strcmp(key, self.first_pointer.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
    }

    pub fn debug(self: *SstIndex) void {
        std.debug.print("\nFirst key\t{first_key}\nLast key\t{last_key}\n", self);
    }
};

pub const SstManager = struct {
    indices: []*SstIndex,
    first_pointer: ?*Pointer = null,
    last_pointer: *Pointer,
    alloc: std.mem.Allocator,
    disk_manager: *DiskManager,

    pub fn init(relative: []u8, alloc: std.mem.Allocator) !*SstManager {
        const dm = try DiskManager.init(relative, alloc);

        const file_entries = try dm.getFilenames(alloc);
        defer {
            for (file_entries) |item| {
                alloc.free(item);
            }
            alloc.free(file_entries);
        }

        var indices = try alloc.alloc(*SstIndex, file_entries.len);
        var mng: *SstManager = try alloc.create(SstManager);
        mng.indices = indices;
        mng.alloc = alloc;
        mng.first_pointer = null;
        mng.disk_manager = dm;

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

    pub fn deinit(self: *SstManager) void {
        for (self.indices) |i| {
            i.deinit();
        }
        self.alloc.free(self.indices);
        self.disk_manager.deinit();
        // self.alloc.destroy(self.disk_manager);
        self.alloc.destroy(self);
    }

    pub fn find(self: *SstManager, key: []u8, alloc: std.mem.Allocator) !?*Record {
        if (!self.isBetween(key)) return null;

        const idx = self.findIndexForKey(key);
        if (idx) |index| {
            return index.get(key, alloc);
        }

        return null;
    }

    // checks if key is in the range of keys
    pub fn isBetween(self: *SstManager, key: []u8) bool {
        return strcmp(key, self.first_pointer.?.key).compare(std.math.CompareOperator.gte) and strcmp(key, self.last_pointer.key).compare(std.math.CompareOperator.lte);
    }

    fn findIndexForKey(self: *SstManager, key: []u8) ?*SstIndex {
        for (self.indices) |index| {
            if (index.isBetween(key)) {
                return index;
            }
        }

        return null;
    }
};

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

test "sstmanager_init" {
    var alloc = std.testing.allocator;

    const folder_path = try alloc.dupe(u8, "./testing");
    defer alloc.free(folder_path);

    var mng = try SstManager.init(folder_path, alloc);
    defer mng.deinit();

    var s = try alloc.dupe(u8, "Hello10");
    defer alloc.free(s);

    const maybe_record = try mng.find(s, alloc);
    if (maybe_record) |record| {
        defer record.deinit();
        try expectEqualStrings("SDFADFS", maybe_record.?.getKey());
    }
}

test "sstindex_retrieve_record" {
    var alloc = std.testing.allocator;

    var path = try alloc.dupe(u8, "./testing/example.sst");
    defer alloc.free(path);

    var s = try SstIndex.init(path, alloc);
    defer s.deinit();

    var key = try alloc.dupe(u8, "hello23");
    defer alloc.free(key);

    const p = s.find(key);
    // defer p.?.deinit();

    const r = try s.retrieveRecordFromFile(p.?, alloc);
    defer r.deinit();

    try expectEqualStrings("world23", r.value);
}

test "sstindex_binary_search" {
    var alloc = std.testing.allocator;
    var idx = try testCreateIndex("Hello{}", alloc);
    defer idx.deinit();

    var key = try alloc.dupe(u8, "Hello15");
    defer alloc.free(key);

    var maybe_record = idx.find(key);

    try expectEqualStrings(idx.pointers[15].key, maybe_record.?.key);
}

test "sstindex_init" {
    var alloc = std.testing.allocator;
    var path = try alloc.dupe(u8, "./testing/example.sst");
    defer alloc.free(path);

    var s = try SstIndex.init(path, alloc);
    defer s.deinit();

    try expectEqualStrings("hello0", s.first_pointer.key);
    try expectEqualStrings("hello49", s.last_pointer.key);

    var hell1 = try std.fmt.allocPrint(alloc, "hello10", .{});
    defer alloc.free(hell1);

    try std.testing.expect(s.isBetween(hell1));
}

test "sstindex_between" {
    var alloc = std.testing.allocator;
    var path = try alloc.dupe(u8, "./testing/example.sst");
    defer alloc.free(path);

    var ssti = try SstIndex.init(path, alloc);
    defer ssti.deinit();

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
        .{ .result = false, .case = "hellz12" },
        .{ .result = true, .case = "hello40" },
    };

    for (cases) |_case| {
        std.mem.copyForwards(u8, data, _case.case);
        try expectEqual(_case.result, ssti.isBetween(data[0.._case.case.len]));
    }
}
