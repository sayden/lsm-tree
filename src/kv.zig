const std = @import("std");
const os = std.os;
const system = std.os.system;
const fs = std.fs;
const math = std.math;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const Op = @import("./ops.zig").Op;
const strings = @import("./strings.zig");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const bytes = @import("./bytes.zig");
const StringReader = bytes.StringReader;

pub const KeyLength = u16;
pub const ValueLength = u16;

pub const Kv = struct {
    op: Op,
    ts: i128,
    size: usize,

    key: []const u8,
    val: []const u8,

    pub fn new(k: []const u8, v: []const u8, op: Op) Kv {
        var kv = Kv{
            .ts = std.time.nanoTimestamp(),
            .op = op,
            .key = k,
            .val = v,
            .size = 0,
        };
        kv.size = kv.sizeInBytes();
        return kv;
    }

    pub fn read(reader: *ReaderWriterSeeker, alloc: Allocator) !Kv {
        // Kv size
        const size = try reader.readIntLittle(usize);

        // Op
        const opbyte = try reader.readByte();
        const op: Op = @enumFromInt(opbyte);

        const timestamp = try reader.readIntLittle(i128);

        var sr = StringReader(KeyLength).init(reader);
        const key = try sr.read(alloc);
        const value = try sr.read(alloc);

        return Kv{
            .op = op,
            .ts = timestamp,
            .key = key,
            .val = value,
            .size = size,
        };
    }

    /// Size of the key in bytes: key length (KeyLength) + key
    pub fn keySize(self: Kv) usize {
        return @sizeOf(KeyLength) + self.key.len;
    }

    /// Size of the kv in bytes: op + timestamp + key + value. It doesn't include any checksum or padding
    pub fn sizeInBytes(self: Kv) usize {
        return 1 + @sizeOf(i128) + @sizeOf(usize) + self.keySize() + @sizeOf(ValueLength) + self.val.len;
    }

    /// Serializes the kv into a buffer. It does not include checksum or padding
    /// The format is: size + op + timestamp + key + value
    /// Be aware that this format is not the same format that in the WAL file. WAL uses:
    /// size + checksum + op + timestamp + key + value
    pub fn serialize(self: Kv, buf: []u8) !usize {
        var fb = std.io.fixedBufferStream(buf);
        const writer = fb.writer();
        var written: usize = 0;

        // Kv size
        try writer.writeInt(usize, self.sizeInBytes(), std.builtin.Endian.little);
        written += @sizeOf(usize);

        // Op
        try writer.writeByte(@intFromEnum(self.op));
        written += 1;

        // Timestamp
        try writer.writeInt(@TypeOf(self.ts), self.ts, std.builtin.Endian.little);
        written += @sizeOf(@TypeOf(self.ts));

        // Key
        try writer.writeInt(KeyLength, @as(KeyLength, @truncate(self.key.len)), std.builtin.Endian.little);
        try writer.writeAll(self.key);

        written += @sizeOf(KeyLength) + self.key.len;

        // Value
        try writer.writeInt(ValueLength, @as(ValueLength, @truncate(self.val.len)), std.builtin.Endian.little);
        try writer.writeAll(self.val);
        written += @sizeOf(ValueLength) + self.val.len;

        return written;
    }

    /// Compares two keys and returns true if lhs is less than rhs
    pub fn compare(self: Kv, other: Kv) bool {
        return lexicographical_compare({}, self, other);
    }

    pub fn sortFn(_: void, lhs: Kv, rhs: Kv) bool {
        return lexicographical_compare({}, lhs, rhs);
    }

    pub fn readKey(reader: *ReaderWriterSeeker, alloc: Allocator) !Kv {
        var sr = StringReader(KeyLength).init(reader);
        const firstkey = try sr.read(alloc);
        var kv = Kv{ .key = firstkey, .op = Op.Upsert, .ts = 0, .val = undefined, .size = 0 };
        kv.size = kv.sizeInBytes();
        return kv;
    }

    pub fn checksum(self: Kv) u32 {
        // TODO: Is there any way to not rely in a fixed value here?
        var buf: [1024]u8 = undefined;
        const written = try self.serialize(&buf);
        return std.hash.Crc32.hash(buf[0..written]);
    }

    pub fn clone(self: Kv, alloc: Allocator) !Kv {
        return Kv{ .kv = Kv{
            .op = self.op,
            .ts = self.ts,
            .key = try alloc.dupe(u8, self.key),
            .val = try alloc.dupe(u8, self.val),
            .alloc = alloc,
        } };
    }

    pub fn cloneTo(self: Kv, other: *Kv, alloc: Allocator) !void {
        other.kv.op = self.op;
        other.kv.ts = self.ts;
        other.kv.key = try alloc.dupe(u8, self.key);
        other.kv.val = try alloc.dupe(u8, self.val);
        other.kv.alloc = alloc;
    }

    pub fn equals(self: Kv, other: Kv) bool {
        return std.mem.eql(u8, self.key, other.kv.key);
    }

    pub fn debug(self: Kv, log: anytype) void {
        log.debug("\t\t[Row] Key: {s}, Ts: {}, Val: {s}, Op: {}\n", .{ self.key, self.ts, self.val, self.op });
    }

    pub fn defaultFirstKey() Kv {
        var kv = Kv{
            .op = Op.Skip,
            .ts = std.math.minInt(i128),
            .key = undefined,
            .val = undefined,
            .size = 0,
        };
        kv.size = kv.sizeInBytes();
        return kv;
    }

    pub fn defaultLastKey() Kv {
        var kv = Kv{
            .op = Op.Skip,
            .ts = std.math.minInt(i128),
            .key = undefined,
            .val = undefined,
            .size = 0,
        };
        kv.size = kv.sizeInBytes();
        return kv;
    }
};
pub fn lexicographical_compare_bytes(_: void, lhs: []const u8, rhs: []const u8) bool {
    const res = strings.strcmp(lhs, rhs);

    // If keys and ops are the same, return the lowest string
    if (res == math.Order.eq) {
        return res.compare(math.CompareOperator.lte);
    }

    return res.compare(math.CompareOperator.lte);
}

/// Compares two keys and returns true if lhs is less than rhs
fn lexicographical_compare(_: void, lhs: Kv, rhs: Kv) bool {
    const res = strings.strcmp(lhs.key, rhs.key);

    // If keys and ops are the same, return the lowest string
    if (res == math.Order.eq and lhs.op == rhs.op) {
        return res.compare(math.CompareOperator.lte);
    } else if (res == math.Order.eq) {
        return @intFromEnum(lhs.op) < @intFromEnum(rhs.op);
    }

    return res.compare(math.CompareOperator.lte);
}

test "Row" {
    const alloc = std.testing.allocator;

    const row = Kv.new("key", "val", Op.Upsert);
    var buf: [64]u8 = undefined;

    const bytes_written = try row.serialize(&buf);
    try std.testing.expectEqual(row.sizeInBytes(), bytes_written);
    try std.testing.expectEqual(@as(usize, 35), bytes_written);

    var rws = ReaderWriterSeeker.initBuf(&buf);
    try rws.seekTo(0);

    const row2 = try Kv.read(&rws, alloc);
    defer alloc.free(row2.key);
    defer alloc.free(row2.val);

    try std.testing.expectEqualStrings(row.key, row2.key);
    try std.testing.expectEqualStrings(row.val, row2.val);
    try std.testing.expectEqual(row.ts, row2.ts);
    try std.testing.expectEqual(row.op, row2.op);
}

test "kv.size_in_bytes" {
    const row = Kv.new("key", "val", Op.Upsert);
    var buf: [1024]u8 = undefined;
    const written = try row.serialize(&buf);
    try std.testing.expectEqual(row.sizeInBytes(), written);
}

test "kv.checksum" {
    const row = Kv.new("key", "val", Op.Upsert);
    var buf: [1024]u8 = undefined;
    const written = try row.serialize(&buf);
    const checksum = std.hash.Crc32.hash(buf[0..written]);
    const again = std.hash.Crc32.hash(buf[0..written]);

    try std.testing.expectEqual(checksum, again);
}
