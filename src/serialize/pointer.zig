const std = @import("std");

const record = @import("../record.zig").Record;
const Record = record.Record;
const KeyLengthType = record.KeyLengthType;
const Pointer = @import("main").Pointer;
const Op = @import("../ops.zig").Op;

pub fn fromRecord(p: Record, buf: []u8, file_offset: usize) usize {
    // op
    buf[0] = @enumToInt(p.op);
    var offset: usize = 1;

    // key length
    std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + @sizeOf(KeyLengthType)], @intCast(KeyLengthType, p.key.len));
    offset += @sizeOf(KeyLengthType);

    // key
    std.mem.copy(u8, buf[offset .. offset + p.key.len], p.key);
    offset += p.key.len;

    //offset
    std.mem.writeIntSliceLittle(usize, buf[offset .. offset + @sizeOf(@TypeOf(file_offset))], file_offset);
    offset += @sizeOf(@TypeOf(file_offset));

    return offset;
}

pub fn toBytesAlloc(self: Pointer, file_offset: usize, allocator: *std.mem.Allocator) ![]u8 {
    const returned_bytes_length = @sizeOf(KeyLengthType) + self.key.len + @sizeOf(usize);
    var buf = try allocator.alloc(u8, returned_bytes_length);
    _ = self.bytes(&buf, file_offset);
    return buf;
}

pub fn toBytes(self: Pointer, buf: []u8) usize {
    var offset: usize = 0;

    // key length
    std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + Pointer.key_size], self.key.len);
    offset += Pointer.key_size;

    // key
    std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
    offset += self.key.len;

    //offset
    std.mem.writeIntSliceLittle(usize, buf[offset .. offset + Pointer.key_size], offset);
    offset += @sizeOf(usize);

    return offset;
}

pub fn fromBytes(bytes: []u8) Pointer {
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

    const p = fromBytes(&buf);
    const eq = std.testing.expectEqual;
    try eq(@as(usize, 5), p.key.len);
    try eq(@as(usize, 99), p.byte_offset);

    try std.testing.expectEqualSlices(u8, "hello", p.key);

    try eq(@as(usize, 15), p.bytesLen());
}
