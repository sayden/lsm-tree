const std = @import("std");

const Header = @import("lsmtree").Header;
const headerSize = @import("lsmtree").headerSize;
const expectEqual = std.testing.expectEqual;

const Error = error{ InputArrayTooSmall, OutputArrayTooSmall, NoLastKeyOffsetFound };

pub fn toBytes(h: *Header, buf: []u8) !usize {
    if (buf.len < headerSize()) {
        return Error.OutputArrayTooSmall;
    }

    var offset: usize = 0;

    std.mem.writeIntSliceLittle(@TypeOf(h.magic_number), buf[offset .. offset + @sizeOf(@TypeOf(h.magic_number))], h.magic_number);
    offset += @sizeOf(@TypeOf(h.magic_number));

    std.mem.writeIntSliceLittle(@TypeOf(h.first_key_offset), buf[offset .. offset + @sizeOf(@TypeOf(h.first_key_offset))], h.first_key_offset);
    offset += @sizeOf(@TypeOf(h.first_key_offset));

    std.mem.writeIntSliceLittle(@TypeOf(h.last_key_offset), buf[offset .. offset + @sizeOf(@TypeOf(h.last_key_offset))], h.last_key_offset);
    offset += @sizeOf(@TypeOf(h.last_key_offset));

    std.mem.copy(u8, buf[offset .. offset + @sizeOf(@TypeOf(h.reserved))], h.reserved[0..]);
    offset += @sizeOf(@TypeOf(h.reserved));

    std.mem.writeIntSliceLittle(@TypeOf(h.total_records), buf[offset .. offset + @sizeOf(@TypeOf(h.total_records))], h.total_records);
    offset += @sizeOf(@TypeOf(h.total_records));

    return offset;
}

pub fn fromBytes(buf: []u8) !Header {
    if (buf.len < headerSize()) {
        return Error.InputArrayTooSmall;
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

    var header = Header{
        .total_records = 0,
        .reserved = undefined,
        .last_key_offset = last_key,
        .first_key_offset = first_key,
        .magic_number = magic,
    };

    // reserved space
    std.mem.copy(u8, header.reserved[0..], buf[offset .. offset + @sizeOf(@TypeOf(header.reserved))]);
    offset += @sizeOf(@TypeOf(header.reserved));

    // total records
    var total_records = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);

    header.total_records = total_records;

    return header;
}

test "header.fromBytes" {
    var header = Header{
        .total_records = 10,
        .last_key_offset = 48,
        .first_key_offset = 40,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    _ = try toBytes(&header, buf);

    var new_h = try fromBytes(buf);
    try expectEqual(new_h.magic_number, header.magic_number);
    try expectEqual(header.total_records, 10);
    try expectEqual(new_h.total_records, header.total_records);
    try expectEqual(new_h.reserved, header.reserved);
    try expectEqual(new_h.last_key_offset, header.last_key_offset);
    try expectEqual(new_h.first_key_offset, header.first_key_offset);
}

test "header.toBytes" {
    var header = Header{
        .total_records = 10,
        .last_key_offset = 48,
        .first_key_offset = 40,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    var total_bytes = try toBytes(&header, buf);
    try expectEqual(@as(usize, 153), total_bytes);

    // magic number
    try expectEqual(@as(u8, 1), buf[0]);

    //first key
    try expectEqual(@as(u8, 40), buf[1]);

    // last key
    try expectEqual(@as(u8, 48), buf[9]);

    // reserved data
    try expectEqual(@as(u8, 0), buf[17]);

    // total records
    try expectEqual(@as(u8, 10), buf[145]);
}
