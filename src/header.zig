const std = @import("std");

const Error = error{NoLastKeyOffsetFound};

/// 1 byte of magic number
/// 8 bytes with the offset of the first key in the "keys" chunk.
/// 8 bytes with the offset of the last key in the "keys" chunk. This actually ocupes 16 bytes in memory because it's an optional
/// 8 bytes with the offset of the beginning of the "keys" chunk.
/// 8 bytes of total records
pub const Header = struct {
    //magic number
    magic_number: u8 = 1,

    //header data
    total_records: usize,

    //keys offsets
    reserved: [128]u8 = undefined,
    first_key_offset: usize,
    last_key_offset: usize,

    pub fn init(comptime T: type, wal: *T) Header {
        //pointers starts after header + all records
        const first_key_offset = headerSize() + wal.current_size;

        // last key cannot be computed yet
        return Header{
            .reserved = undefined,
            .first_key_offset = first_key_offset,
            .total_records = wal.total_records,
            .last_key_offset = 0,
        };
    }
};

pub fn headerSize() usize {
    // magic number + usize*3 + 128 for the reserved space
    return @sizeOf(u8) + 128 + (@sizeOf(usize) * 3);
}

test "Header.size" {
    const size = @sizeOf(Header);
    try std.testing.expectEqual(160, size);
    try std.testing.expectEqual(@as(usize, 153), headerSize());
}
