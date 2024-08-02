const std = @import("std");
const Allocator = std.mem.Allocator;

const UUID = @import("./pkg/zig-uuid/uuid.zig").UUID;
const strings = @import("./strings.zig");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Op = @import("./ops.zig").Op;
const KVNs = @import("./kv.zig");
const KV = KVNs.Kv;

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
        Wal,
        Index,
    };

    // size of the metadata header only,
    size: ?usize = null,

    // unique identifier of the metadata.
    id: [36]u8,

    // kind of metadata. Different kinds of metadata may have different structures.
    kind: Kind,

    // first key of the file
    firstkey: []const u8,

    // offset of the first key in the file
    firstkeyoffset: ?usize = null,

    // last key of the file
    lastkey: []const u8,

    // offset of the last key in the file
    lastkeyoffset: ?usize = null,

    // magic number
    magicnumber: u16 = 0,

    // number of elements of data
    count: usize = 0,

    pub fn init(kind: Kind, firstkey: KV, lastkey: KV) Metadata {
        const uuid = UUID.init();
        var uuid_string: [36]u8 = undefined;
        uuid.to_string(&uuid_string);

        var m = Metadata{
            .firstkey = firstkey.key,
            .lastkey = lastkey.key,
            .id = uuid_string,
            .kind = kind,
        };
        uuid.to_string(&m.id);

        return m;
    }

    pub fn initDefault(kind: Kind) Metadata {
        return init(kind, KV.defaultFirstKey(), KV.defaultLastKey());
    }

    pub fn read(reader: *ReaderWriterSeeker, alloc: Allocator) !Metadata {
        const size = try reader.readIntLittle(usize);

        const magicn = try reader.readIntLittle(u16);

        const kindbyte = try reader.readByte();
        const kind = @as(Metadata.Kind, @enumFromInt(kindbyte));

        var id: [36]u8 = undefined;
        _ = try reader.readAtLeast(&id, 36);

        const count = try reader.readIntLittle(usize);

        const firstkey = try KV.readKey(reader, alloc);
        const fistKeyOffset = try reader.readIntLittle(usize);
        const lastkey = try KV.readKey(reader, alloc);
        const lastKeyOffset = try reader.readIntLittle(usize);

        return Metadata{
            .kind = kind,
            .magicnumber = magicn,
            .firstkey = firstkey.key,
            .lastkey = lastkey.key,
            .id = id,
            .count = count,
            .size = size,
            .firstkeyoffset = fistKeyOffset,
            .lastkeyoffset = lastKeyOffset,
        };
    }

    pub fn sizeInBytes(self: *Metadata) usize {
        return @sizeOf(usize) + // size of the metadata
            @sizeOf(@TypeOf(self.magicnumber)) +
            @sizeOf(@TypeOf(self.kind)) +
            self.id.len +
            @sizeOf(@TypeOf(self.count)) +
            @sizeOf(KVNs.KeyLength) + self.firstkey.len +
            @sizeOf(KVNs.KeyLength) + self.lastkey.len +
            @sizeOf(usize);
    }

    pub fn write(self: *Metadata, writer: *ReaderWriterSeeker) !usize {
        var written: usize = 0;

        self.size = self.sizeInBytes();

        // size of the metadata
        try writer.writeIntLittle(usize, self.size.?);
        written += @sizeOf(usize);

        // magic number
        try writer.writeIntLittle(@TypeOf(self.magicnumber), self.magicnumber);
        written += @sizeOf(@TypeOf(self.magicnumber));

        // kind
        try writer.writeByte(@intFromEnum(self.kind));
        written += @sizeOf(@TypeOf(self.kind));

        // id
        try writer.writeAll(&self.id);
        written += self.id.len;

        // number of elements
        try writer.writeIntLittle(@TypeOf(self.count), self.count);
        written += @sizeOf(@TypeOf(self.count));

        // first key
        try writer.writeIntLittle(KVNs.KeyLength, @as(KVNs.KeyLength, @truncate(self.firstkey.len)));
        written += @sizeOf(KVNs.KeyLength);
        try writer.writeAll(self.firstkey);
        written += self.firstkey.len;
        try writer.writeIntLittle(usize, self.firstkeyoffset.?);
        written += @sizeOf(usize);

        // last key
        try writer.writeIntLittle(KVNs.KeyLength, @as(KVNs.KeyLength, @truncate(self.lastkey.len)));
        written += @sizeOf(KVNs.KeyLength);
        try writer.writeAll(self.lastkey);
        written += self.lastkey.len;
        try writer.writeIntLittle(usize, self.lastkeyoffset.?); // unknown until the data has been filled up later
        written += @sizeOf(usize);

        return written;
    }

    pub fn updateFirstAndLastKey(self: *Metadata, newkey: KV) void {
        var order = strings.strcmp(newkey.key, self.firstkey);
        if (order == std.math.Order.lt or order == std.math.Order.eq or self.firstkey.len == 0) {
            self.firstkey = newkey.key;
        }

        order = strings.strcmp(self.lastkey, newkey.key);
        if (order == std.math.Order.lt or order == std.math.Order.eq or self.lastkey.len == 0) {
            self.lastkey = newkey.key;
        }
    }

    pub fn debug(m: Metadata) void {
        log.debug("----------------------------------------", .{});
        log.debug("--------------- Metadata ---------------", .{});
        log.debug("----------------------------------------", .{});
        log.debug("Id:\t\t\t{s}", .{m.id});
        log.debug("FistKey:\t\t{s}", .{m.firstkey});
        log.debug("LastKey:\t\t{s}", .{m.lastkey});
        log.debug("MagicNumber:\t{}", .{m.magicnumber});
        log.debug("Count:\t\t{}", .{m.count});
        log.debug("----------------------------------------", .{});
    }
};

test "Metadata_key_updates" {
    var firstkey = KV{ .ts = 100, .key = "hello", .val = "world", .op = Op.Upsert, .size = 0 };
    firstkey.size = firstkey.sizeInBytes();
    var new_later_key = KV{ .ts = 101, .key = "mario", .val = "caster", .op = Op.Upsert, .size = 0 };
    new_later_key.size = new_later_key.sizeInBytes();

    var meta = Metadata.initDefault(Metadata.Kind.Wal);
    meta.updateFirstAndLastKey(firstkey);
    meta.updateFirstAndLastKey(new_later_key);
    try std.testing.expectEqualDeep(firstkey.key, meta.firstkey);
    try std.testing.expectEqualDeep(new_later_key.key, meta.lastkey);

    var new_earlier = KV{ .ts = 99, .val = "world", .key = "hallo", .op = Op.Upsert, .size = 0 };
    new_earlier.size = new_earlier.sizeInBytes();
    meta.updateFirstAndLastKey(new_earlier);
    try std.testing.expectEqualDeep(new_earlier.key, meta.firstkey);
    try std.testing.expectEqualDeep(new_later_key.key, meta.lastkey);
}

test "Metadata_gp" {
    std.testing.log_level = .debug;
    const firstkey = KV.new("hello", "world", Op.Upsert);

    var meta = Metadata.initDefault(Metadata.Kind.Wal);
    meta.count = 1;
    meta.magicnumber = 87;

    meta.updateFirstAndLastKey(firstkey);

    var buf: [256]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);

    _ = try meta.write(&rws);
    try rws.seekTo(0);

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    const alloc = arena.allocator();
    defer arena.deinit();

    const meta2 = try Metadata.read(&rws, alloc);

    try std.testing.expectEqual(meta.magicnumber, meta2.magicnumber);
    try std.testing.expectEqual(meta.count, meta2.count);
    try std.testing.expectEqualSlices(u8, meta.firstkey, meta2.firstkey);
    try std.testing.expectEqualSlices(u8, meta.lastkey, meta2.lastkey);
    try std.testing.expectEqual(meta.kind, meta2.kind);
}

pub const SSTableMetadata = struct { Metadata };
pub const TempSSTableMetadata = struct { Metadata };
