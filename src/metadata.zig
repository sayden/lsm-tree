const std = @import("std");
const Allocator = std.mem.Allocator;

const UUID = @import("./pkg/zig-uuid/uuid.zig").UUID;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Op = @import("./ops.zig").Op;
const KV = @import("./kv.zig").Kv;

const log = std.log.scoped(.Metadata);

// Metadata is a struct that holds the metadata of a chunk, wal, or index.
// id: a unique identifier for the metadata.
// kind: the kind of metadata, can be Chunk, Wal, or Index.
// firstkey: the first key of the metadata.
// lastkey: the last key of the metadata.
// magicnumber: a magic number to identify the metadata.
// count: the number of elements in the metadata.
pub const Metadata = struct {
    const MAX_SIZE = std.mem.page_size * 32;
    pub const Kind = enum {
        Chunk,
        Wal,
        Index,
    };

    id: [36]u8,
    kind: Kind,
    firstkey: KV,
    lastkey: KV,
    magicnumber: u16 = 0,
    count: usize = 0,

    pub fn init(kind: Kind, firstkey: KV, lastkey: KV) Metadata {
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

    pub fn updateFirstAndLastKey(self: *Metadata, k: KV) void {
        if (k.compare(self.firstkey)) {
            self.firstkey = k;
            return;
        }

        if (self.lastkey.compare(k)) {
            self.lastkey = k;
        }
    }

    fn maybeUpdateFirstAndLastKey(original_first: *?KV, original_last: *?KV, meta: Metadata) void {
        updateKey(original_first, meta.firstkey);
        updateKey(original_last, meta.lastkey);
    }

    fn updateKey(original: *?KV, key: KV) void {
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
    std.testing.log_level = .debug;
    const firstkey = KV{ .ts = 100, .key = "hello", .val = "world", .op = Op.Upsert };
    const new_later_key = KV{ .ts = 101, .key = "mario", .val = "caster", .op = Op.Upsert };

    var meta = Metadata.initDefault(Metadata.Kind.Chunk, KV);
    meta.updateFirstAndLastKey(firstkey);
    meta.updateFirstAndLastKey(new_later_key);
    meta.debug();
    try std.testing.expectEqualDeep(firstkey, meta.firstkey);
    try std.testing.expectEqualDeep(new_later_key, meta.lastkey);

    const new_earlier = KV{ .ts = 99, .val = "world", .key = "hello", .op = Op.Upsert };
    try std.testing.expectEqualDeep(new_earlier, meta.firstkey);

    try std.testing.expectEqualDeep(new_later_key, meta.lastkey);
    try std.testing.expectEqualDeep(new_earlier, meta.firstkey);
}

test "Metadata" {
    const firstkey = KV{ .ts = 100, .key = "hello", .val = "world", .op = Op.Upsert };

    var meta = Metadata{
        .kind = Metadata.Kind.Chunk,
        .id = undefined,
        .firstkey = firstkey,
        .lastkey = firstkey,
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

    const alloc = std.testing.allocator;
    const meta2 = try Metadata.read(&rws, KV, alloc);
    defer meta2.deinit();

    try std.testing.expectEqual(meta.magicnumber, meta2.magicnumber);
    try std.testing.expectEqual(meta.count, meta2.count);
    try std.testing.expectEqual(meta.firstkey.ts, meta2.firstkey.ts);
    try std.testing.expectEqual(meta.lastkey.ts, meta2.lastkey.ts);
    try std.testing.expectEqual(meta.kind, meta2.kind);
}
