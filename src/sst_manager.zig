const std = @import("std");
const Header = @import("./header.zig").Header;
const Strings = @import("strings").String;
const println = @import("./debug.zig").println;
const Pointer = @import("./pointer.zig").Pointer;

pub const SstIndex = struct {
    header: Header,
    path: []const u8,
    first_key: []u8,
    last_key: []u8,
    f: std.fs.File,
    allocator: std.mem.Allocator,

    pub fn init(path: []const u8, allocator: std.mem.Allocator) !*SstIndex {
        var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});

        var s = try allocator.create(SstIndex);
        s.f = f;
        s.allocator = allocator;
        s.header = try Header.fromReader(f.reader());
        s.path = path;

        try f.seekTo(s.header.first_pointer_offset);
        var p = try Pointer.fromBytesReader(allocator, f.reader());
        defer p.deinit();
        s.first_key = try allocator.dupe(u8, p.key);

        try f.seekTo(s.header.last_pointer_offset);
        var p1 = try Pointer.fromBytesReader(allocator, f.reader());
        defer p1.deinit();
        s.last_key = try allocator.dupe(u8, p1.key);

        return s;
    }

    fn deinit(self: *SstIndex) void {
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
        self.allocator.free(self.path);
        self.f.close();
        self.allocator.destroy(self);
    }

    // checks if key is in the range of keys of this sst
    pub fn isIn(self: *SstIndex, key: []u8) bool {
        var isInside: bool = false;
        _ = isInside;

        var isHigher = false;
        for (self.first_key, 0..) |lower, i| {
            if (key[i] >= lower) {
                isHigher = true;
                break;
            }
        }
        if (!isHigher) return false;

        var isLower = false;
        for (self.last_key, 0..) |lower, i| {
            if (key[i] <= lower) {
                isLower = true;
                break;
            }
        }

        return isLower;
    }

    pub fn debug(self: *SstIndex) void {
        std.debug.print("Path\t{s}\nFirst key\t{s}\nLast key\t{s}\n", .{ self.path, self.first_key, self.last_key });
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
    // defer allocator.free(buf);
    var path = try std.fs.cwd().realpath("./testing/example.sst", buf);

    var s = try SstIndex.init(path, allocator);
    defer s.deinit();

    try std.testing.expectEqualStrings("hell0", s.first_key);
    try std.testing.expectEqualStrings("hell2", s.last_key);

    var hell1 = try std.fmt.allocPrint(allocator, "hell1", .{});
    defer allocator.free(hell1);

    try std.testing.expect(s.isIn(hell1));
}

test "SstIndex_contains" {
    var allocator = std.testing.allocator;

    var buf = try allocator.alloc(u8, 50);
    // defer allocator.free(buf);
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
        .{ .result = true, .case = "agg" },
        .{ .result = true, .case = "abc" },
        .{ .result = false, .case = "zzz" },
        .{ .result = true, .case = "hello" },
    };

    for (cases) |_case| {
        std.mem.copyForwards(u8, data, _case.case);
        try std.testing.expectEqual(_case.result, ssti.isIn(data));
    }
}
