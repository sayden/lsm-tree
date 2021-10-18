const std = @import("std");

/// 1 byte of magic number
/// 8 bytes with the offset of the first key in the "keys" chunk.
/// 8 bytes with the offset of the last key in the "keys" chunk.
/// 8 bytes with the offset of the beginning of the "keys" chunk.
/// 8 bytes of total records
pub const Header = struct {
    const magic_number: u8 = 1;
    //header data
    total_records: usize,

    //data offsets

    //keys offsets
    pointers_byte_offset: usize,
    first_key_offset: usize,
    last_key_offset: ?usize = null,
    

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
        };
    }
};

pub fn headerSize() usize {
    return @sizeOf(@TypeOf(Header.magic_number)) + (@sizeOf(usize) * 4);
}

pub fn toBytes(h: *Header, buf: []u8) !void {
    if (h.last_key_offset == null) {
        return Error.NoLastKeyOffsetFound;
    }

    const magic_number_size = @sizeOf(@TypeOf(Header.magic_number));

    std.mem.writeIntLittle(@TypeOf(Header.magic_number), buf[0..magic_number_size], Header.magic_number);
    std.mem.writeIntLittle(usize, buf[magic_number_size .. magic_number_size + 8], h.first_key_offset);
    std.mem.writeIntLittle(usize, buf[magic_number_size + 8 .. magic_number_size + 16], h.last_key_offset.?);
    std.mem.writeIntLittle(usize, buf[magic_number_size + 16 .. magic_number_size + 24], h.pointers_byte_offset);
    std.mem.writeIntLittle(usize, buf[magic_number_size + 24 .. magic_number_size + 32], h.total_records);
}

const Error = error{NoLastKeyOffsetFound};
