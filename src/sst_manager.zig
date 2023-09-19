const std = @import("std");
const Header = @import("./header.zig").Header;
const Strings = @import("strings").String;

const Pointer = @import("./pointer.zig").Pointer;
const Sst = @import("./sst.zig").Sst;
const RecordPkg = @import("./record.zig");
const Record = RecordPkg.Record;
const strcmp = @import("./strings.zig").strcmp;

const Debug = @import("./debug.zig");
const println = Debug.println;
const prints = Debug.prints;
const print = std.debug.print;

pub const SstIndex = struct {
    header: Header,

    first_key: []u8,
    last_key: []u8,

    f: std.fs.File,
    pointers: []*Pointer,

    allocator: std.mem.Allocator,

    pub fn init(path: []const u8, allocator: std.mem.Allocator) !*SstIndex {
        var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});
        var s = try allocator.create(SstIndex);
        s.f = f;

        s.allocator = allocator;
        var reader = f.reader();

        // read header
        s.header = try Header.fromReader(reader);

        // read pointers
        s.pointers = try allocator.alloc(*Pointer, s.header.total_records);
        for (0..s.header.total_records) |i| {
            var p = try Pointer.read(reader, s.allocator);
            s.pointers[i] = p;
        }

        //first and last key
        s.first_key = try allocator.dupe(u8, s.getPointer(0).?.key);
        s.last_key = try allocator.dupe(u8, s.getPointer(s.header.total_records - 1).?.key);

        return s;
    }

    pub fn deinit(self: *SstIndex) void {
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
        for (self.pointers) |p| {
            p.deinit();
        }
        self.allocator.free(self.pointers);
        self.f.close();
        self.allocator.destroy(self);
    }

    pub fn getPointer(s: *SstIndex, index: usize) ?*Pointer {
        if (index >= s.header.total_records) {
            return null;
        }

        return s.pointers[index];
    }

    pub fn load(s: *SstIndex) !*Sst {
        return Sst.tinitWithIndex(s, s.allocator);
    }

    pub fn find(idx: *SstIndex, key: []u8) !?Record {
        if (!idx.IsBetween(key)) return null;

        var p: *Pointer = undefined;
        _ = p;
    }

    // checks if key is in the range of keys of this sst
    pub fn IsBetween(self: *SstIndex, key: []u8) bool {
        var res = strcmp(key, self.first_key);

        if (res < 0) {
            return false;
        }

        res = strcmp(key, self.last_key);
        if (res > 0) {
            return false;
        }

        return true;
    }

    pub fn debug(self: *SstIndex) void {
        std.debug.print("\nFirst key\t{first_key}\nLast key\t{last_key}\n", self);
    }
};

pub const SstManager = struct {
    pub fn init(file_entries: []std.fs.IterableDir.Entry) void {
        for (file_entries) |entry| {
            _ = entry;
        }
    }
};

test "SstIndex_init" {
    var allocator = std.testing.allocator;

    var buf = try allocator.alloc(u8, 50);
    defer allocator.free(buf);
    var path = try std.fs.cwd().realpath("./testing/example.sst", buf);

    var s = try SstIndex.init(path, allocator);
    defer s.deinit();

    try std.testing.expectEqualStrings("hello", s.first_key);
    try std.testing.expectEqualStrings("hello", s.last_key);

    var hell1 = try std.fmt.allocPrint(allocator, "hell1", .{});
    defer allocator.free(hell1);

    try std.testing.expect(s.IsBetween(hell1));
}

test "SstIndex_between" {
    var allocator = std.testing.allocator;

    var buf = try allocator.alloc(u8, 50);
    defer allocator.free(buf);
    var path = try std.fs.cwd().realpath("./testing/example.sst", buf);

    var ssti = try SstIndex.init(path, allocator);
    defer ssti.deinit();

    var data = try allocator.alloc(u8, 10);
    defer allocator.free(data);

    const case = struct {
        result: bool,
        case: []const u8,
    };

    var cases = [_]case{
        .{ .result = false, .case = "agg" },
        .{ .result = false, .case = "abc" },
        .{ .result = false, .case = "zzz" },
        .{ .result = true, .case = "hello" },
        .{ .result = true, .case = "hellz" },
        .{ .result = false, .case = "hel" },
    };

    for (cases) |_case| {
        std.mem.copyForwards(u8, data, _case.case);
        try std.testing.expectEqual(_case.result, ssti.IsBetween(data[0.._case.case.len]));
    }
}
