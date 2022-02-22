const std = @import("std");

const Header = @import("lsmtree").Header;
const expectEqual = std.testing.expectEqual;

const Error = error{ ArrayTooSmall, NoLastKeyOffsetFound };

pub fn toBytes(h: *Header, buf: []u8) !usize {
    if (buf.len != 33) {
        return Error.ArrayTooSmall;
    }

    var offset: usize = 0;

    std.mem.writeIntSliceLittle(@TypeOf(h.magic_number), buf[offset .. offset + @sizeOf(@TypeOf(h.magic_number))], h.magic_number);
    offset += @sizeOf(@TypeOf(h.magic_number));

    std.mem.writeIntSliceLittle(@TypeOf(h.first_key_offset), buf[offset .. offset + @sizeOf(@TypeOf(h.first_key_offset))], h.first_key_offset);
    offset += @sizeOf(@TypeOf(h.first_key_offset));

    std.mem.writeIntSliceLittle(@TypeOf(h.last_key_offset.?), buf[offset .. offset + @sizeOf(@TypeOf(h.last_key_offset.?))], h.last_key_offset.?);
    offset += @sizeOf(@TypeOf(h.last_key_offset.?));

    std.mem.writeIntSliceLittle(@TypeOf(h.pointers_byte_offset), buf[offset .. offset + @sizeOf(@TypeOf(h.pointers_byte_offset))], h.pointers_byte_offset);
    offset += @sizeOf(@TypeOf(h.pointers_byte_offset));

    std.mem.writeIntSliceLittle(@TypeOf(h.total_records), buf[offset .. offset + @sizeOf(@TypeOf(h.total_records))], h.total_records);
    offset += @sizeOf(@TypeOf(h.total_records));

    return offset;
}

pub fn fromBytes(buf: []u8) !Header {
    if (buf.len < 12) {
        return Error.ArrayTooSmall;
    }

    //Magic number
    var magic = buf[0];
    var offset: usize = 1;

    // offset of the first key in the "keys" chunk.
    var first_key = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);
    offset += @sizeOf(usize);

    // offset of the last key in the "keys" chunk.
    var last_key = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);
    offset += @sizeOf(usize);

    // offset of the beginning of the "keys" chunk.
    var keys_offset = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);
    offset += @sizeOf(usize);

    // total records
    var total_records = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);

    var header = Header{
        .total_records = total_records,
        .pointers_byte_offset = keys_offset,
        .last_key_offset = last_key,
        .first_key_offset = first_key,
        .magic_number = magic,
    };

    return header;
}

test "header.fromBytes" {
    var header = Header{
        .total_records = 10,
        .pointers_byte_offset = 32,
        .last_key_offset = 48,
        .first_key_offset = 40,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 100);
    defer alloc.free(buf);

    _ = try toBytes(&header, buf);

    var new_h = try fromBytes(buf);
    try expectEqual(new_h.magic_number, header.magic_number);
    try expectEqual(header.total_records, 10);
    try expectEqual(new_h.total_records, header.total_records);
    try expectEqual(new_h.pointers_byte_offset, header.pointers_byte_offset);
    try expectEqual(new_h.last_key_offset, header.last_key_offset);
    try expectEqual(new_h.first_key_offset, header.first_key_offset);
}

test "header.toBytes" {
    var header = Header{
        .total_records = 10,
        .pointers_byte_offset = 32,
        .last_key_offset = 48,
        .first_key_offset = 40,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 100);
    defer alloc.free(buf);

    var total_bytes = try toBytes(&header, buf);
    try expectEqual(@as(usize, 33), total_bytes);

    // magic number
    try expectEqual(@as(u8, 1), buf[0]);

    //first key
    try expectEqual(@as(u8, 40), buf[1]);

    // last key
    try expectEqual(@as(u8, 48), buf[9]);

    // beginning of the keys chunk
    try expectEqual(@as(u8, 32), buf[17]);

    // total records
    try expectEqual(@as(u8, 10), buf[25]);
}
