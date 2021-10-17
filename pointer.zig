const std = @import("std");

pub fn Pointer(comptime KeyLengthType: type) type {
    return struct {
        const key_size: usize = @sizeOf(KeyLengthType);

        key: []const u8,
        byte_offset: usize = 0,

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
}

pub fn readPointer(comptime KeyLengthType: type, bytes: []u8) Pointer(KeyLengthType) {
    const T = Pointer(KeyLengthType);

    var key_length = std.mem.readIntSliceLittle(KeyLengthType, bytes[0..T.key_size]);
    var byte_offset = std.mem.readIntSliceLittle(usize, bytes[T.key_size + key_length .. T.key_size + key_length + 8]);
    return Pointer(KeyLengthType){
        .key = bytes[T.key_size .. T.key_size + key_length],
        .byte_offset = byte_offset,
    };
}

test "pointer.read" {
    var buf: [17]u8 = undefined;
    std.mem.writeIntSliceLittle(u32, buf[0..4], 5);
    std.mem.copy(u8, buf[4..9], "hello");
    std.mem.writeIntSliceLittle(usize, buf[9..], 99);

    const p = readPointer(u32, &buf);
    const eq = std.testing.expectEqual;
    try eq(@as(usize, 5), p.key.len);
    try eq(@as(usize, 99), p.byte_offset);

    try std.testing.expectEqualSlices(u8, "hello", p.key);

    try eq(@as(usize, 17), p.bytesLength());
}
