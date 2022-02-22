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
    pointers_byte_offset: usize,
    first_key_offset: usize,
    last_key_offset: usize,

    pub fn init(comptime T: type, wal: *T) Header {
        //pointers starts after header + all records
        const pointer_byte_offset = headerSize() + wal.current_size;

        //first key starts atm exactly where pointers start so...
        const first_key_offset = pointer_byte_offset;

        // last key cannot be computed yet
        return Header{
            .pointers_byte_offset = pointer_byte_offset,
            .first_key_offset = first_key_offset,
            .total_records = wal.total_records,
            .last_key_offset = 0,
        };
    }
};

pub fn headerSize() usize {
    // magic number + usize*4
    return @sizeOf(u8) + (@sizeOf(usize) * 4);
}

test "Header.size" {
    const size = @sizeOf(Header);
    try std.testing.expectEqual(40, size);

    try std.testing.expectEqual(@as(usize, 33), headerSize());
}
