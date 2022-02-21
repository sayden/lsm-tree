const std = @import("std");

const Error = error{ArrayTooSmall};

pub fn toBytes(h: *Header, buf: []u8) !usize {
    if (h.last_key_offset == null) {
        return Error.NoLastKeyOffsetFound;
    }

    const magic_number_size = @sizeOf(@TypeOf(Header.magic_number));

    std.mem.writeIntLittle(@TypeOf(Header.magic_number), buf[0..magic_number_size], Header.magic_number);
    std.mem.writeIntLittle(usize, buf[magic_number_size .. magic_number_size + 8], h.first_key_offset);
    std.mem.writeIntLittle(usize, buf[magic_number_size + 8 .. magic_number_size + 16], h.last_key_offset.?);
    std.mem.writeIntLittle(usize, buf[magic_number_size + 16 .. magic_number_size + 24], h.pointers_byte_offset);
    std.mem.writeIntLittle(usize, buf[magic_number_size + 24 .. magic_number_size + 32], h.total_records);

    return 40;
}

pub fn fromBytes(buf: []u8) !Header {
    if (bytes.len < 12) {
        return Error.ArrayTooSmall;
    }

    //Op
    var op = @intToEnum(Op, bytes[0]);
    var offset: usize = 1;

    // offset of the first key in the "keys" chunk.
    var first_key = std.mem.readIntSliceLittle(usize, bytes[offset .. offset + @sizeOf(usize)]);
    offset += @sizeOf(usize);

    // offset of the last key in the "keys" chunk.
    var last_key = std.mem.readIntSliceLittle(usize, bytes[offset .. offset + @sizeOf(usize)]);
    offset += @sizeOf(usize);

    // offset of the beginning of the "keys" chunk.
    var keys_offset = std.mem.readIntSliceLittle(usize, bytes[offset .. offset + @sizeOf(usize)]);
    offset += @sizeOf(usize);

    // total records
    var total_records = std.mem.readIntSliceLittle(usize, bytes[offset .. offset + @sizeOf(usize)]);

    var header = Header {
        .total_records = total_records,
        .pointers_byte_offset = keys_offset,
        .last_key_offset = last_key,
        .first_key_offset = first_key,
    };

    return header;
}

test "header.fromBytes" {
    var header = Header {
        .total_records = 10,
        .pointers_byte_offset = 32,
        .last_key_offset = 48,
        .first_key_offset = 40,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 100);

    try toBytes(&header, buf);

    var h = try fromBytes(buf);
    try std.testing.expectEqual(h.total_records, 10);
}