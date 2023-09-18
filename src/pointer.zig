const std = @import("std");
const Record = @import("./record.zig").Record;
const KeyLengthType = @import("./record.zig").KeyLengthType;
const Op = @import("ops.zig").Op;
const Error = error{ArrayTooSmall};

// A pointer contains an Operation, a key and a offset to find the Value of the record.
// The pointer is stored as follows:
// 1 byte: Operation
// 2 bytes: Key size
// X bytes: Key
// 8 bytes: Offset in the data
pub const Pointer = struct {
    op: Op,
    key: []u8,
    byte_offset: usize = 0,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.key);
        self.allocator.destroy(self);
    }

    pub fn bytesLen(self: *Self) usize {
        return 1 + @sizeOf(KeyLengthType) + self.key.len + @sizeOf(@TypeOf(self.byte_offset));
    }

    // Get a byte array representation of a pointer using the provided allocator.
    // FREE the returned array using the provided allocator. The array look like this:
    // 1 byte: Operation
    // 2 bytes: Key size
    // X bytes: Key
    // 8 bytes: Offset in the data
    pub fn toBytesAlloc(self: *Pointer, allocator: *std.mem.Allocator) ![]u8 {
        var buf = try allocator.alloc(u8, self.bytesLen());
        _ = try self.toBytes(buf);

        return buf;
    }

    pub fn toBytes(pointer: *Pointer, buf: []u8) !usize {
        if (pointer.bytesLen() > buf.len) {
            return Error.ArrayTooSmall;
        }

        var writerType = std.io.fixedBufferStream(buf);
        var writer = writerType.writer();
        return pointer.toBytesWriter(writer);
    }

    // Get a byte array representation of a pointer using the provided array.
    // It must be at least of the size of Pointer.bytesLen(). The array look like this:
    // 1 byte: Operation
    // 2 bytes: Key size
    // X bytes: Key
    // 8 bytes: Offset in the data
    pub fn toBytesWriter(self: *Pointer, writer: anytype) !usize {
        // Op
        const op = [1]u8{@intFromEnum(self.op)};
        _ = try writer.write(&op);

        // key length
        try writer.writeIntLittle(KeyLengthType, @as(u16, @truncate(self.key.len)));

        // key
        _ = try writer.write(self.key);

        //offset
        try writer.writeIntLittle(usize, @as(usize, self.byte_offset));

        return self.bytesLen();
    }

    pub fn fromBytesReader(allocator: std.mem.Allocator, reader: anytype) !*Pointer {
        var p = try allocator.create(Pointer);

        //Op
        p.op = @as(Op, @enumFromInt(try reader.readByte()));

        //Key length
        const key_length = try reader.readIntLittle(KeyLengthType);
        p.key = try allocator.alloc(u8, key_length);

        // Key
        _ = try reader.readAtLeast(p.key, key_length);

        // Offset
        p.byte_offset = try reader.readIntLittle(usize);
        p.allocator = allocator;

        return p;
    }

    // Reads the provided array and return a Pointer from the contents. If the contents of the array
    // are not correct, it will return a corrupted Pointer.
    // The size of this array is expected to be X + 11 being X the key length
    pub fn fromBytes(allocator: std.mem.Allocator, bytes: []u8) !*Pointer {
        var fixedReader = std.io.fixedBufferStream(bytes);
        var reader = fixedReader.reader();
        return Pointer.fromBytesReader(allocator, reader);
    }
};

test "pointer_bytesLen" {
    var allocator = std.testing.allocator;
    var hello = try allocator.alloc(u8, 5);
    @memcpy(hello, "hello");

    var p = Pointer{
        .op = Op.Create,
        .key = hello,
        .byte_offset = 100,
        .allocator = allocator,
    };
    defer allocator.free(p.key);

    var len = p.bytesLen();
    try std.testing.expectEqual(@as(usize, 16), len);
}

test "pointer_toBytes" {
    var allocator = std.testing.allocator;
    var hello = try allocator.alloc(u8, 5);
    @memcpy(hello, "hello");

    var p = Pointer{
        .op = Op.Create,
        .key = hello,
        .byte_offset = 100, // char d
        .allocator = allocator,
    };
    defer allocator.free(p.key);

    var buf = try allocator.alloc(u8, p.bytesLen());
    defer allocator.free(buf);

    var writerType = std.io.fixedBufferStream(buf);
    var writer = writerType.writer();
    _ = try p.toBytesWriter(writer);

    // char d at the end of "hello" is the byte_offset 100 written above in the example
    try std.testing.expect(!std.mem.eql(u8, buf, "hellod"));
    try std.testing.expectEqual(@as(usize, 16), p.bytesLen());
    try std.testing.expectEqual(buf[0], @intFromEnum(Op.Create));
}

test "pointer_fromBytes" {
    var allocator = std.testing.allocator;

    var key = try allocator.alloc(u8, 5);
    @memcpy(key, "hello");

    var p = Pointer{
        .op = Op.Create,
        .key = key,
        .allocator = allocator,
        .byte_offset = 100,
    };
    defer allocator.free(p.key);

    var buf = try p.toBytesAlloc(&allocator);
    defer allocator.free(buf);

    var p2 = try Pointer.fromBytes(allocator, buf);
    defer p2.deinit();

    const eq = std.testing.expectEqual;
    try eq(@as(usize, 5), p.key.len);
    try eq(@as(usize, 100), p.byte_offset);

    try std.testing.expectEqualSlices(u8, "hello", p.key);

    try eq(buf.len, p.bytesLen());
}
