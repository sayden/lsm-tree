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
            inline else => |*case| return case.writer().writeIntLittle(T, value),
        };
    }

    pub fn writeIntNative(self: *ReaderWriterSeeker, comptime T: type, value: T) anyerror!void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                return writer.writeIntNative(T, value);
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

    pub fn readIntNative(self: *ReaderWriterSeeker, comptime T: type) !T {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();
                return reader.readIntNative(T);
            },
        };
    }

    pub fn readFloat(self: *ReaderWriterSeeker, comptime T: type) !T {
        return switch (self.*) {
            inline else => |*case| {
                var reader = case.reader();

                var buf: [64]u8 = undefined;
                var fbs = std.io.fixedBufferStream(&buf);
                var writer = fbs.writer();

                try reader.streamUntilDelimiter(writer, '!', 64);

                var written = fbs.getWritten();
                const value = try std.fmt.parseFloat(T, written);

                return value;
            },
        };
    }

    pub fn writeFloat(self: *ReaderWriterSeeker, comptime T: type, v: T) !void {
        return switch (self.*) {
            inline else => |*case| {
                var writer = case.writer();
                try std.fmt.formatFloatDecimal(v, .{}, writer);
                try writer.writeByte('!');
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

pub fn ReaderWriterSeeker2(comptime SourceT: type) type {
    const SourceTT: type = switch (SourceT) {
        std.fs.File => std.fs.File,
        inline else => std.io.FixedBufferStream([]u8),
    };

    return struct {
        const Self = @This();

        source: SourceTT,
        writer: SourceTT.Writer,
        reader: SourceTT.Reader,

        pub fn init(source: SourceT) Self {
            return switch (SourceT) {
                std.fs.File => return Self{ .source = source, .writer = source.writer(), .reader = source.reader() },
                inline else => {
                    var fbs = std.io.fixedBufferStream(source);
                    var writer = fbs.writer();
                    var reader = fbs.reader();
                    var s = Self{ .source = fbs, .writer = writer, .reader = reader };
                    std.debug.print("{},{},{*}\n", .{ s.source.buffer.len, s.source.pos, s.source.buffer });
                    return s;
                },
            };
        }

        pub fn write(self: *Self, bytes: []const u8) !usize {
            return self.writer.write(bytes);
        }

        pub fn writeAll(self: Self, bytes: []const u8) anyerror!void {
            return self.writer.writeAll(bytes);
        }

        pub fn writeIntLittle(self: Self, comptime T: type, value: T) anyerror!void {
            return self.writer.writeIntLittle(T, value);
        }

        pub fn writeIntNative(self: Self, comptime T: type, value: T) anyerror!void {
            return self.writer.writeIntNative(T, value);
        }

        pub fn readIntLittle(self: Self, comptime T: type) !T {
            return self.reader.readIntLittle(T);
        }

        pub fn readIntNative(self: Self, comptime T: type) !T {
            return self.reader.readIntNative(T);
        }

        pub fn readFloat(self: Self, comptime T: type) !T {
            var buf: [64]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&buf);
            var _writer = fbs.writer();

            try self.reader.streamUntilDelimiter(_writer, '!', 64);

            var written = fbs.getWritten();
            const value = try std.fmt.parseFloat(T, written);

            return value;
        }

        pub fn writeFloat(self: Self, comptime T: type, v: T) !void {
            try std.fmt.formatFloatDecimal(v, .{}, self.writer);
            try self.writer.writeByte('!');
        }

        pub fn readAtLeast(self: Self, buffer: []u8, len: usize) anyerror!usize {
            return self.reader.readAtLeast(buffer, len);
        }

        pub fn writeByte(self: Self, byte: u8) anyerror!void {
            return self.writer.writeByte(byte);
        }

        pub fn read(self: Self, buffer: []u8) anyerror!usize {
            return self.reader.read(buffer);
        }

        pub fn readByte(self: Self) (anyerror || anyerror)!u8 {
            return self.reader.readByte();
        }

        pub fn seekTo(self: *Self, pos: usize) anyerror!void {
            return self.source.seekTo(pos);
        }

        pub fn seekBy(self: Self, amt: i64) anyerror!void {
            return self.source.seekBy(amt);
        }

        pub fn getPos(self: Self) !usize {
            return self.source.getPos();
        }
    };
}

test "ReaderWriterSeeker_write" {
    var buf: [8]u8 = undefined;

    var wss = ReaderWriterSeeker2([]u8).init(&buf);
    const bytes_written = try wss.write("hello");
    try std.testing.expectEqual(@as(usize, 5), bytes_written);
    try std.testing.expectEqualSlices(u8, "hello", buf[0..5]);
}

// test "ReaderWriterSeeker_read_write_float" {
//     // Create a temp file

//     var buf: [128]u8 = undefined;

//     var rws = ReaderWriterSeeker.initBuf(&buf);
//     try rws.writeFloat(f64, 23.4);
//     try rws.seekTo(0);

//     const val = try rws.readFloat(f64);
//     try std.testing.expectEqual(@as(f64, 23.4), val);
// }
