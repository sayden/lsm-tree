const std = @import("std");

pub fn Pointer(comptime KeyLengthType: type) type {
    return struct {
        const key_size: usize = @sizeOf(KeyLengthType);

        key: []u8,
        byte_offset: usize = 0,

        const Self = @This();
        pub fn toPointer(comptime RecordType: type, r: *RecordType) Self {
            return Self{
                .key = r.read_key(),
            };
        }

        pub fn bytes(self: *Self, offset: usize, buf: []u8) usize {
            var offset: usize = 0;

            // key length
            std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + self.key_size], self.key.len);
            offset += self.key_size;

            // key
            std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
            offset += self.key.len;

            //offset
            std.mem.writeIntSliceLittle(usize, buf[offset .. offset + self.key_size], offset);
            offset += @sizeOf(usize);

            return offset;
        }
    };
}

pub fn readPointer(bytes: []u8) *Pointer {}

pub fn toPointerAlloc(r: *Record, allocator: *std.mem.Allocator) *Pointer {}
