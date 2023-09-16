const std = @import("std");
const expectEqual = std.testing.expectEqual;

const Error = error{ InputArrayTooSmall, OutputArrayTooSmall, NoLastKeyOffsetFound };

/// 1 byte of magic number
/// 8 bytes with the offset of the first key in the "keys" chunk.
/// 8 bytes with the offset of the last key in the "keys" chunk. This actually ocupes 16 bytes in memory because it's an optional
/// 8 bytes with the offset of the beginning of the "keys" chunk.
/// 8 bytes of total records
/// 8 bytes of total records size
pub const Header = struct {
    //magic number
    magic_number: u8 = 1,

    //header data
    total_records: usize = 0,

    //keys offsets
    reserved: [128]u8 = undefined,
    first_pointer_offset: usize,
    last_pointer_offset: usize,
    records_size: usize,

    pub fn init() Header {
        // last key cannot be computed yet
        return Header{
            .reserved = undefined,
            .first_pointer_offset = headerSize(),
            .records_size = 0,
            .last_pointer_offset = 0,
        };
    }

    pub fn toBytes(h: *Header, buf: []u8) !usize {
        if (buf.len < headerSize()) {
            return Error.OutputArrayTooSmall;
        }

        var offset: usize = 0;

        std.mem.writeIntSliceLittle(@TypeOf(h.magic_number), buf[offset .. offset + @sizeOf(@TypeOf(h.magic_number))], h.magic_number);
        offset += @sizeOf(@TypeOf(h.magic_number));

        std.mem.writeIntSliceLittle(@TypeOf(h.first_pointer_offset), buf[offset .. offset + @sizeOf(@TypeOf(h.first_pointer_offset))], h.first_pointer_offset);
        offset += @sizeOf(@TypeOf(h.first_pointer_offset));

        std.mem.writeIntSliceLittle(@TypeOf(h.last_pointer_offset), buf[offset .. offset + @sizeOf(@TypeOf(h.last_pointer_offset))], h.last_pointer_offset);
        offset += @sizeOf(@TypeOf(h.last_pointer_offset));

        std.mem.copy(u8, buf[offset .. offset + @sizeOf(@TypeOf(h.reserved))], h.reserved[0..]);
        offset += @sizeOf(@TypeOf(h.reserved));

        std.mem.writeIntSliceLittle(@TypeOf(h.total_records), buf[offset .. offset + @sizeOf(@TypeOf(h.total_records))], h.total_records);
        offset += @sizeOf(@TypeOf(h.total_records));

        std.mem.writeIntSliceLittle(@TypeOf(h.records_size), buf[offset .. offset + @sizeOf(@TypeOf(h.records_size))], h.records_size);
        offset += @sizeOf(@TypeOf(h.records_size));

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
            .last_pointer_offset = last_key,
            .first_pointer_offset = first_key,
            .magic_number = magic,
            .records_size = 0,
        };

        // reserved space
        std.mem.copy(u8, header.reserved[0..], buf[offset .. offset + @sizeOf(@TypeOf(header.reserved))]);
        offset += @sizeOf(@TypeOf(header.reserved));

        // total records
        var total_records = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);
        offset += @sizeOf(usize);
        header.total_records = total_records;

        // Size of the records store, only records without header or pointers
        var records_size = std.mem.readIntSliceLittle(usize, buf[offset .. offset + @sizeOf(usize)]);
        header.records_size = records_size;

        offset += @sizeOf(usize);

        return header;
    }
};

pub fn headerSize() usize {
    // magic number + usize*3 + 128 for the reserved space
    return @sizeOf(u8) + 128 + (@sizeOf(usize) * 4);
}

test "Header.size" {
    const size = @sizeOf(Header);
    try std.testing.expectEqual(168, size);
    try std.testing.expectEqual(@as(usize, 161), headerSize());
}

test "header.fromBytes" {
    var header = Header{
        .total_records = 10,
        .last_pointer_offset = 48,
        .first_pointer_offset = 40,
        .records_size = 99,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    _ = try Header.toBytes(&header, buf);

    var new_h = try Header.fromBytes(buf);
    try expectEqual(new_h.magic_number, header.magic_number);
    try expectEqual(header.total_records, new_h.total_records);
    try expectEqual(header.records_size, new_h.records_size);
    try expectEqual(new_h.reserved, header.reserved);
    try expectEqual(new_h.last_pointer_offset, header.last_pointer_offset);
    try expectEqual(new_h.first_pointer_offset, header.first_pointer_offset);
}

test "header.toBytes" {
    var header = Header{
        .records_size = 99,
        .total_records = 10,
        .last_pointer_offset = 48,
        .first_pointer_offset = 40,
    };

    var alloc = std.testing.allocator;
    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    var total_bytes = try Header.toBytes(&header, buf);
    try expectEqual(@as(usize, 161), total_bytes);

    // magic number
    try expectEqual(header.magic_number, buf[0]);

    //first key
    try expectEqual(header.first_pointer_offset, buf[1]);

    // last key
    try expectEqual(header.last_pointer_offset, buf[9]);

    // total records
    try expectEqual(header.total_records, buf[145]);

    // record size
    try expectEqual(header.records_size, buf[153]);
}
