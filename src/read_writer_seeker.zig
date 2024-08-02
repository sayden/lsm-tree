const std = @import("std");
const native_endian = @import("builtin").cpu.arch.endian();
const File = std.fs.File;

pub const ReaderWriterSeeker = union(enum) {
    const Self = @This();

    file: std.fs.File,
    buf: std.io.FixedBufferStream([]u8),
    mmapfile: std.io.FixedBufferStream([]align(std.mem.page_size) u8),

    pub fn initBuf(buf: []u8) Self {
        const fixed: std.io.FixedBufferStream([]u8) = std.io.fixedBufferStream(buf);
        return Self{ .buf = fixed };
    }

    pub fn initReadMmap(mmap: []align(std.mem.page_size) u8) Self {
        const fixed: std.io.FixedBufferStream([]align(std.mem.page_size) u8) = std.io.fixedBufferStream(mmap);
        return Self{ .mmapfile = fixed };
    }

    pub fn initReadFileMmap(file: File) !Self {
        const stat = try file.stat();
        const handle = file.handle;
        const addr = try std.posix.mmap(null, stat.size, std.posix.PROT.READ | std.posix.PROT.WRITE, std.posix.MAP{ .TYPE = .SHARED }, handle, 0);
        const fixed = std.io.fixedBufferStream(addr);
        return Self{ .mmapfile = fixed };
    }

    pub fn initFile(f: std.fs.File) Self {
        return Self{ .file = f };
    }

    pub fn write(self: *ReaderWriterSeeker, bytes: []const u8) anyerror!usize {
        return switch (self.*) {
            inline else => |*case| case.write(bytes),
        };
    }

    pub fn writeAll(self: *ReaderWriterSeeker, bytes: []const u8) anyerror!void {
        return switch (self.*) {
            .file => |*file| file.writeAll(bytes),
            inline else => |*fbs| fbs.writer().writeAll(bytes),
        };
    }

    pub fn writeIntLittle(self: *ReaderWriterSeeker, comptime T: type, value: T) anyerror!void {
        return switch (self.*) {
            inline else => |*case| return case.writer().writeInt(T, value, std.builtin.Endian.little),
        };
    }

    pub fn writeIntNative(self: *ReaderWriterSeeker, comptime T: type, value: T) anyerror!void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                return writer.writeInt(T, value, native_endian);
            },
        };
    }

    pub fn writeFloat(self: *ReaderWriterSeeker, comptime T: type, v: T) !void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                var buf: [53]u8 = undefined;
                const f = try std.fmt.formatFloat(&buf, v, .{ .mode = .decimal, .precision = 6 });
                _ = try writer.write(f);
                try writer.writeByte('!');
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

    pub fn readFloat(self: *ReaderWriterSeeker, comptime T: type) !T {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();

                var buf: [64]u8 = undefined;
                var fbs = std.io.fixedBufferStream(&buf);
                const writer = fbs.writer();

                try reader.streamUntilDelimiter(writer, '!', 64);

                const written = fbs.getWritten();
                const value = try std.fmt.parseFloat(T, written);

                return value;
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

    pub fn read(self: *ReaderWriterSeeker, buffer: []u8) anyerror!usize {
        return switch (self.*) {
            inline else => |*case| case.read(buffer),
        };
    }

    pub fn readIntLittle(self: *ReaderWriterSeeker, comptime T: type) !T {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();
                return reader.readInt(T, std.builtin.Endian.little);
            },
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

    pub fn seekBy(self: *Self, amt: i64) anyerror!void {
        return switch (self.*) {
            inline else => |*case| case.seekBy(amt),
        };
    }

    pub fn getPos(self: *ReaderWriterSeeker) !usize {
        return switch (self.*) {
            inline else => |*case| case.getPos(),
        };
    }
};

// test "ReaderWriterSeeker_write" {
//     var buf: [8]u8 = undefined;
//
//     var wss = ReaderWriterSeeker2([]u8).init(&buf);
//     const bytes_written = try wss.write("hello");
//     try std.testing.expectEqual(@as(usize, 5), bytes_written);
//     try std.testing.expectEqualSlices(u8, "hello", buf[0..5]);
// }

// test "ReaderWriterSeeker_read_write_float" {
//     // Create a temp file

//     var buf: [128]u8 = undefined;

//     var rws = ReaderWriterSeeker.initBuf(&buf);
//     try rws.writeFloat(f64, 23.4);
//     try rws.seekTo(0);

//     const val = try rws.readFloat(f64);
//     try std.testing.expectEqual(@as(f64, 23.4), val);
// }
