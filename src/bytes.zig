const std = @import("std");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Allocator = std.mem.Allocator;

pub fn StringReader(comptime String: type) type {
    return struct {
        const Self = @This();

        rw: *ReaderWriterSeeker,

        pub fn init(rw: *ReaderWriterSeeker) Self {
            return Self{
                .rw = rw,
            };
        }

        pub fn read(s: *Self, alloc: Allocator) ![]u8 {
            const length = try s.rw.readIntLittle(String);
            const str: []u8 = try alloc.alloc(u8, length);
            errdefer alloc.free(str);

            _ = try s.rw.readAtLeast(str, length);
            return str;
        }

        pub fn seekTo(s: *Self, pos: usize) !void {
            return s.rw.seekTo(pos);
        }
    };
}

test "StringReader.read" {
    var alloc = std.testing.allocator;

    const buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);
    var rw = ReaderWriterSeeker.initBuf(buf);

    var sr = StringReader(u16).init(&rw);
    try rw.writeIntLittle(u16, 5);
    try rw.writeAll("hello");

    try rw.seekTo(0);
    const hello = try sr.read(alloc);
    defer alloc.free(hello);

    try std.testing.expectEqualStrings("hello", hello);
}

pub fn StringWriter(comptime String: type) type {
    return struct {
        const Self = @This();

        rw: *ReaderWriterSeeker,

        pub fn init(rw: *ReaderWriterSeeker) Self {
            return Self{
                .rw = rw,
            };
        }

        pub fn write(s: *Self, str: []const u8) !void {
            try s.rw.writeIntLittle(String, @as(String, @truncate(str.len)));
            return s.rw.writeAll(str);
        }

        pub fn seekTo(s: *Self, pos: usize) !void {
            return s.rw.seekTo(pos);
        }
    };
}

pub fn StringReaderWriter(comptime String: type) type {
    return struct {
        const Self = @This();

        rw: *ReaderWriterSeeker,

        pub fn init(rw: *ReaderWriterSeeker) Self {
            return Self{
                .rw = rw,
            };
        }

        // returned []u8 must be freed by the caller
        pub fn read(s: *Self, alloc: Allocator) ![]u8 {
            const length = try s.rw.readIntLittle(String);
            const str: []u8 = try alloc.alloc(u8, length);
            _ = try s.rw.readAtLeast(str, length);
            return str;
        }

        pub fn write(s: *Self, str: []const u8) !void {
            try s.rw.writeIntLittle(String, @as(String, @truncate(str.len)));
            return s.rw.writeAll(str);
        }

        pub fn seekTo(s: *Self, pos: usize) !void {
            return s.rw.seekTo(pos);
        }
    };
}

test "string_reader_writer" {
    var alloc = std.testing.allocator;

    const buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);
    var rw = ReaderWriterSeeker.initBuf(buf);

    var srw = StringReaderWriter(u16).init(&rw);
    try srw.write("hello");

    try srw.seekTo(0);
    const hello = try srw.read(alloc);
    defer alloc.free(hello);

    try std.testing.expectEqualStrings("hello", hello);
}
