const std = @import("std");
const record = @import("./record.zig");
const KeyLengthType = record.KeyLengthType;
const Op = @import("ops.zig").Op;

pub const Pointer = struct {
    const key_size: usize = @sizeOf(KeyLengthType);

    key: []const u8,
    byte_offset: usize = 0,
    op: Op,

    const Self = @This();

    pub fn bytesAlloc(self: *Self, file_offset: usize, allocator: *std.mem.Allocator) ![]u8 {
        const returned_bytes_length = @sizeOf(KeyLengthType) + self.key.len + @sizeOf(usize);
        var buf = try allocator.alloc(u8, returned_bytes_length);
        _ = self.bytes(&buf, file_offset);
        return buf;
    }

    pub fn bytes(self: *Self, buf: []u8) usize {
        var offset: usize = 0;

        // key length
        std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + key_size], self.key.len);
        offset += key_size;

        // key
        std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
        offset += self.key.len;

        //offset
        std.mem.writeIntSliceLittle(usize, buf[offset .. offset + key_size], offset);
        offset += @sizeOf(usize);

        return offset;
    }

    pub fn bytesLength(self: *const Self) usize {
        return Self.key_size + self.key.len + @sizeOf(@TypeOf(self.byte_offset));
    }
};

pub fn readPointer(bytes: []u8) Pointer {
    //Op
    var op = @intToEnum(Op, bytes[0]);
    var offset: usize = 1;

    //Key length
    var key_length = std.mem.readIntSliceLittle(KeyLengthType, bytes[offset .. offset + Pointer.key_size]);
    offset += Pointer.key_size;

    // Key
    var key = bytes[offset .. offset + key_length];
    offset += key_length;

    // Offset
    var byte_offset = std.mem.readIntSliceLittle(usize, bytes[offset .. offset + @sizeOf(usize)]);

    return Pointer{
        .key = key,
        .byte_offset = byte_offset,
        .op = op,
    };
}

test "pointer.read" {
    var buf: [16]u8 = undefined;
    buf[0] = 0;
    std.mem.writeIntSliceLittle(u16, buf[1..3], 5);
    std.mem.copy(u8, buf[3..8], "hello");
    std.mem.writeIntSliceLittle(usize, buf[8..16], 99);

    const p = readPointer(&buf);
    const eq = std.testing.expectEqual;
    try eq(@as(usize, 5), p.key.len);
    try eq(@as(usize, 99), p.byte_offset);

    try std.testing.expectEqualSlices(u8, "hello", p.key);

    try eq(@as(usize, 15), p.bytesLength());
}
