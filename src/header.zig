const std = @import("std");
const expectEqual = std.testing.expectEqual;
const Pointer = @import("./pointer.zig").Pointer;

const Error = error{ InputArrayTooSmall, OutputArrayTooSmall, NoLastKeyOffsetFound };

/// 1 byte of magic number
/// 8 bytes with the offset of the first pointers in the "pointers" chunk.
/// 8 bytes with the offset of the last pointers in the "pointers" chunk. This actually ocupes 16 bytes in memory because it's an optional
/// 8 bytes of total records
/// 8 bytes of total records size
/// 128 bytes reserved
pub const Header = struct {
    //magic number
    magic_number: u8 = 1,

    //header data
    total_records: usize = 0,

    //keys offsets
    reserved: [128]u8 = undefined,
    last_pointer_offset: usize = 0,
    first_pointer_offset: usize = 0,
    records_size: usize = 0,

    // Pointer size must be pre-calculated before persisting because the header is the first
    // thing written on files. Changing header to EOF would fix this problem
    pointers_size: usize = 0,

    pub fn init() Header {
        // last key cannot be computed yet
        return Header{
            .first_pointer_offset = headerSize(),
        };
    }

    pub fn write(h: *Header, file: *std.fs.File) !usize {
        var writer = file.writer();
        try writer.writeIntLittle(u8, h.magic_number);
        try writer.writeIntLittle(usize, headerSize());
        try writer.writeIntLittle(usize, h.last_pointer_offset);
        _ = try writer.write(&h.reserved);
        try writer.writeIntLittle(usize, h.total_records);
        try writer.writeIntLittle(usize, h.records_size);
        try writer.writeIntLittle(usize, h.pointers_size);

        return headerSize();
    }

    pub fn read(file: *std.fs.File) !Header {
        var reader = file.reader();

        //Magic number
        var magic = try reader.readByte();

        var first_key = try reader.readIntLittle(usize);
        var last_key = try reader.readIntLittle(usize);

        var header = Header{
            .total_records = 0,
            .reserved = undefined,
            .last_pointer_offset = last_key,
            .first_pointer_offset = first_key,
            .magic_number = magic,
            .records_size = 0,
            .pointers_size = 0,
        };

        // reserved space
        _ = try reader.readAtLeast(&header.reserved, @sizeOf(@TypeOf(header.reserved)));

        // total records
        var total_records = try reader.readIntLittle(usize);
        header.total_records = total_records;

        // Size of the records store, only records without header or pointers
        var records_size = try reader.readIntLittle(usize);
        header.records_size = records_size;

        // Size of the pointers space
        var pointers_size = try reader.readIntLittle(usize);
        header.pointers_size = pointers_size;

        return header;
    }

    pub fn debug(h: *const Header) void {
        std.debug.print("\n------\nHeader\n------\n", .{});
        std.debug.print("Magic number:\t\t{}\nTotal records:\t\t{}\nFirst pointer offset:\t{}\n", .{ h.magic_number, h.total_records, h.first_pointer_offset });
        std.debug.print("Last pointer offset:\t{}\nRecords size:\t\t{}\nPointers size:\t\t{}\n", .{ h.last_pointer_offset, h.records_size, h.pointers_size });
        std.debug.print("Reserved: {s}\n\n", .{h.reserved});
    }
};

pub fn headerSize() usize {
    return @sizeOf(u8) + 128 + (@sizeOf(usize) * 5);
}

test "Header.size" {
    const size = @sizeOf(Header);
    try std.testing.expectEqual(176, size);
    try std.testing.expectEqual(@as(usize, 169), headerSize());
}

test "header_write_read" {
    var header = Header{
        .total_records = 10,
        .last_pointer_offset = 48,
        .first_pointer_offset = headerSize(),
        .records_size = 99,
    };

    // Create a temp file
    var tmp_dir = std.testing.tmpDir(std.fs.Dir.OpenDirOptions{});
    defer tmp_dir.cleanup();

    var file = try tmp_dir.dir.createFile("test.sst", std.fs.File.CreateFlags{ .read = true });
    defer file.close();

    _ = try header.write(&file);
    try file.seekTo(0);

    var new_h = try Header.read(&file);
    try expectEqual(new_h.magic_number, header.magic_number);
    try expectEqual(header.total_records, new_h.total_records);
    try expectEqual(header.records_size, new_h.records_size);
    try expectEqual(new_h.reserved, header.reserved);
    try expectEqual(new_h.last_pointer_offset, header.last_pointer_offset);
    try expectEqual(new_h.first_pointer_offset, header.first_pointer_offset);
}
