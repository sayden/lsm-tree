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
    };
}

// pub fn readPointer(bytes: []u8) *Pointer {}

// pub fn toPointerAlloc(r: *Record, allocator: *std.mem.Allocator) *Pointer {}
