const std = @import("std");
const Allocator = std.mem.Allocator;

const UUID = @import("./pkg/zig-uuid/uuid.zig").UUID;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Column = @import("./columnar.zig").Column;
const Op = @import("./ops.zig").Op;
const Data = @import("./data.zig").Data;

const log = std.log.scoped(.Metadata);

pub const Metadata = struct {
    const MAX_SIZE = std.mem.page_size * 32;
    pub const Kind = enum {
        Chunk,
        Wal,
        Index,
    };

    id: [36]u8,
    kind: Kind,
    firstkey: Data,
    lastkey: Data,
    magicnumber: u16 = 0,
    count: usize = 0,

    pub fn init(kind: Kind, firstkey: Data, lastkey: Data) Metadata {
        const uuid = UUID.init();
        var m = Metadata{
            .firstkey = firstkey,
            .lastkey = lastkey,
            .id = undefined,
            .kind = kind,
        };
        uuid.to_string(&m.id);

        return m;
    }

    pub fn initDefault(kind: Kind, comptime T: type) Metadata {
        return init(kind, T.defaultFirstKey(), T.defaultLastKey());
    }

    pub fn deinit(self: Metadata) void {
        self.firstkey.deinit();
        self.lastkey.deinit();
    }

    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !Metadata {
        const magicn = try reader.readIntNative(u16);
        const kindbyte = try reader.readByte();
        const kind = @as(Metadata.Kind, @enumFromInt(kindbyte));
        var m = Metadata{ .kind = Metadata.Kind.Chunk, .magicnumber = magicn, .firstkey = undefined, .lastkey = undefined, .id = undefined };
        _ = try reader.readAtLeast(&m.id, 36);
        m.count = try reader.readIntNative(usize);
        m.firstkey = try T.readIndexingValue(reader, alloc);
        m.lastkey = try T.readIndexingValue(reader, alloc);
        m.kind = kind;

        return m;
    }

    pub fn write(self: Metadata, writer: *ReaderWriterSeeker) !void {
        try writer.writeIntNative(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeByte(@intFromEnum(self.kind));
        try writer.writeAll(&self.id);
        try writer.writeIntNative(@TypeOf(self.count), self.count);
        try self.firstkey.writeIndexingValue(writer);
        try self.lastkey.writeIndexingValue(writer);
    }

    pub fn updateSelfFirstAndLastKey(self: *Metadata, k: Data) void {
        if (k.compare(self.firstkey)) {
            self.firstkey = k;
            return;
        }

        if (self.lastkey.compare(k)) {
            self.lastkey = k;
        }
    }

    pub fn updateFirstAndLastKey(original_first: *?Data, original_last: *?Data, meta: Metadata) void {
        updateKey(original_first, meta.firstkey);
        updateKey(original_last, meta.lastkey);
    }

    pub fn updateKey(original: *?Data, key: Data) void {
        if (original.*) |original_key| {
            if (key.compare(original_key)) {
                original.* = key;
            }
        } else {
            original.* = key;
        }
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

test "Metadata_key_updates" {
    var firstkey = Data{ .col = Column{ .ts = 100, .val = 0, .op = Op.Upsert } };
    var new_later_key = Data{ .col = Column{ .ts = 120, .val = 0, .op = Op.Upsert } };
    var initialNullData: ?Data = null;
    Metadata.updateKey(&initialNullData, firstkey);
    try std.testing.expect(initialNullData != null);

    var meta = Metadata.initDefault(Metadata.Kind.Chunk, Column);

    meta.updateSelfFirstAndLastKey(firstkey);
    try std.testing.expectEqualDeep(firstkey, meta.firstkey);
    meta.updateSelfFirstAndLastKey(new_later_key);
    try std.testing.expectEqualDeep(firstkey, meta.firstkey);
    try std.testing.expectEqualDeep(new_later_key, meta.lastkey);
    const new_earlier = Data{ .col = Column{ .ts = 50, .val = 0, .op = Op.Upsert } };
    meta.updateSelfFirstAndLastKey(new_earlier);
    try std.testing.expectEqualDeep(new_earlier, meta.firstkey);
}

test "Metadata" {
    const col1 = Column.new(99999, 123.2, Op.Upsert);
    const data = Data.new(col1);

    var meta = Metadata{
        .kind = Metadata.Kind.Chunk,
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
    try std.testing.expectEqual(meta.kind, meta2.kind);
}
