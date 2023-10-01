const std = @import("std");

const Output = union(enum) {
    file: std.fs.File,
    fixed: []const u8,
};

pub const ReaderWriterSeeker = union(enum) {
    const Self = @This();

    file: std.fs.File,
    buf: std.io.FixedBufferStream([]u8),

    pub fn initBuf(buf: []u8) Self {
        var fixed: std.io.FixedBufferStream([]u8) = std.io.fixedBufferStream(buf);
        return Self{ .buf = fixed };
    }

    pub fn initFile(f: std.fs.File) Self {
        return Self{ .file = f };
    }

    pub fn write(self: *ReaderWriterSeeker, bytes: []u8) anyerror!usize {
        return switch (self.*) {
            inline else => |*case| case.write(bytes),
        };
    }

    pub fn writeAll(self: *ReaderWriterSeeker, bytes: []u8) anyerror!void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                return writer.writeAll(bytes);
            },
        };
    }

    pub fn writeIntLittle(self: *ReaderWriterSeeker, comptime T: type, value: T) anyerror!void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                return writer.writeIntLittle(T, value);
            },
        };
    }

    pub fn readIntLittle(self: *ReaderWriterSeeker, comptime T: type) !T {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();
                return reader.readIntLittle(T);
            },
        };
    }

    pub fn readAtLeast(self: *ReaderWriterSeeker, buffer: []u8, len: usize) anyerror!usize {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();
                return reader.readAtLeast(buffer, len);
            },
        };
    }

    pub fn writeByte(self: *ReaderWriterSeeker, byte: u8) anyerror!void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                return writer.writeByte(byte);
            },
        };
    }

    pub fn read(self: *ReaderWriterSeeker, buffer: []u8) anyerror!usize {
        return switch (self.*) {
            inline else => |*case| case.read(buffer),
        };
    }

    pub fn readByte(self: *ReaderWriterSeeker) (anyerror || anyerror)!u8 {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();
                return reader.readByte();
            },
        };
    }

    pub fn seekTo(self: *ReaderWriterSeeker, pos: usize) anyerror!void {
        return switch (self.*) {
            inline else => |*case| case.seekTo(pos),
        };
    }

    pub fn getPos(self: *ReaderWriterSeeker) usize {
        return switch (self.*) {
            inline else => |case| case.getPos(),
        };
    }
};

test "asdfasdf" {
    var alloc = std.testing.allocator;

    // Create a temp file
    var tmp_dir = std.testing.tmpDir(std.fs.Dir.OpenDirOptions{});
    defer tmp_dir.cleanup();

    var file = try tmp_dir.dir.createFile("test.sst", std.fs.File.CreateFlags{ .read = true });
    defer file.close();

    var buf = try alloc.alloc(u8, 100);
    defer alloc.free(buf);

    // var fseek = f.seekableStream();
    // _ = fseek;
    var fwriter = file.writer();
    _ = fwriter;

    var ws = ReaderWriterSeeker.initBuf(buf);
    var wss = &ws;
    var hello = try alloc.dupe(u8, "hello");
    defer alloc.free(hello);

    _ = try wss.seekTo(0);
    _ = try wss.write(hello);
}
