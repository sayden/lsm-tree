const std = @import("std");
const Allocator = std.mem.Allocator;

const UUID = @import("./pkg/zig-uuid/uuid.zig").UUID;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Column = @import("./columnar.zig").Column;
const Op = @import("./ops.zig").Op;
const Data = @import("./data.zig").Data;

pub const Metadata = struct {
    const log = std.log.scoped(.Metadata);

    const MAX_SIZE = std.mem.page_size * 32;

    id: [36]u8,
    firstkey: Data,
    lastkey: Data,
    magicnumber: u16 = 0,
    count: usize = 0,

    pub fn init(firstkey: Data, lastkey: Data) Metadata {
        const uuid = UUID.init();
        var m = Metadata{
            .firstkey = firstkey,
            .lastkey = lastkey,
            .id = undefined,
        };
        uuid.to_string(&m.id);

        return m;
    }

    pub fn initDefault(comptime T: type) Metadata {
        return init(T.default(), T.default());
    }

    pub fn deinit(self: Metadata) void {
        self.firstkey.deinit();
        self.lastkey.deinit();
    }

    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !Metadata {
        const magicn = try reader.readIntNative(u16);
        var m = Metadata{ .magicnumber = magicn, .firstkey = undefined, .lastkey = undefined, .id = undefined };
        _ = try reader.readAtLeast(&m.id, 36);
        m.count = try reader.readIntNative(usize);
        m.firstkey = try T.readIndexingValue(reader, alloc);
        m.lastkey = try T.readIndexingValue(reader, alloc);

        return m;
    }

    pub fn write(self: Metadata, writer: *ReaderWriterSeeker) !void {
        try writer.writeIntNative(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeAll(&self.id);
        try writer.writeIntNative(@TypeOf(self.count), self.count);
        try self.firstkey.writeIndexingValue(writer);
        try self.lastkey.writeIndexingValue(writer);
    }

    pub fn debug(m: Metadata) void {
        log.debug("--------", .{});
        log.debug("Metadata", .{});
        log.debug("--------", .{});
        log.debug("Id:\t{s}", .{m.id});
        log.debug("FistKey:", .{});
        m.firstkey.debug(log);
        log.debug("LastKey:", .{});
        m.lastkey.debug(log);
        log.debug("MagicNumber:\t{}", .{
            m.magicnumber,
        });
        log.debug("Count:\t\t{}", .{m.count});
        log.debug("--------", .{});
    }
};

test "Metadata" {
    const col1 = Column.new(99999, 123.2, Op.Upsert);
    const data = Data.new(col1);

    var meta = Metadata{
        .id = undefined,
        .firstkey = data,
        .lastkey = data,
        .magicnumber = 123,
        .count = 456,
    };
    const uuid = UUID.init();
    uuid.to_string(&meta.id);
    defer meta.deinit();

    var buf: [128]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);

    try meta.write(&rws);
    try rws.seekTo(0);

    var alloc = std.testing.allocator;
    const meta2 = try Metadata.read(&rws, Column, alloc);
    defer meta2.deinit();

    try std.testing.expectEqual(meta.magicnumber, meta2.magicnumber);
    try std.testing.expectEqual(meta.count, meta2.count);
    try std.testing.expectEqual(meta.firstkey.col.ts, meta2.firstkey.col.ts);
    try std.testing.expectEqual(meta.lastkey.col.ts, meta2.lastkey.col.ts);
}
