const std = @import("std");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const strings = @import("./strings.zig");
const bytes = @import("./bytes.zig");

test "ASDFASDF" {
    var alloc = std.testing.allocator;

    var file = try std.fs.openFileAbsolute("/home/mcastro/software/prometheus-2.47.1.linux-amd64/data/chunks_head/000002", .{});
    defer file.close();

    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();
    var tmpfile = try tmpdir.dir.createFile("test", std.fs.File.CreateFlags{ .read = true });
    defer tmpfile.close();

    var filereader = file.reader();
    var filewriter = tmpfile.writer();
    _ = filewriter;
    var dec = std.compress.zstd.decompressStream(alloc, filereader);
    defer dec.deinit();
    // try tmpfile.seekTo(0);
    // var reader = tmpfile.reader();
    var reader = dec.reader();

    // var magic: [4]u8 = undefined;
    // _ = try reader.readAtLeast(&magic, 4);
    var magic = try reader.readIntNative(u32);

    const version = try reader.readByte();

    var padding: [3]u8 = undefined;
    _ = try reader.readAtLeast(&padding, 3);

    var series_ref: [8]u8 = undefined;
    _ = try reader.readAtLeast(&series_ref, 8);
    const mint_t = try reader.readIntNative(usize);
    std.debug.print("\n---------------------'{}', '{}', '{d}' '{d}', {}\n", .{ magic, version, padding, series_ref, mint_t });

    // const max_t = try reader.readIntLittle(usize);
    // const enconding = try reader.readByte();
    // const len = try reader.readIntLittle(usize);
    // var data = try alloc.alloc(u8, len);
    // defer alloc.free(data);

    // _ = try reader.readAtLeast(data, len);

    // var crc = try reader.readIntLittle(u32);

    // std.debug.print("{}, {}, {}, {}, {s}, {s}", .{ mint_t, max_t, enconding, crc, series_ref, data });
}
