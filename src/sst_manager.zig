const std = @import("std");
const Header = @import("./header.zig").Header;
const Strings = @import("strings").String;
const println = @import("./debug.zig").println;
const Pointer = @import("./pointer.zig").Pointer;

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
        s.first_key = s.getPointer(0).?.key;
        s.last_key = s.getPointer(s.header.total_records - 1).?.key;

        return s;
    }

    fn getPointer(s: *SstIndex, index: usize) ?*Pointer {
        if (index >= s.header.total_records) {
            return null;
        }

        return s.pointers[index];
    }

    fn deinit(self: *SstIndex) void {
        self.allocator.free(self.first_key);
        self.allocator.free(self.last_key);
        for (self.pointers) |p| {
            p.deinit();
        }
        self.allocator.free(self.pointers);
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

    try std.testing.expect(s.isIn(hell1));
}

test "SstIndex_contains" {
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
