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
const Data = @import("./data.zig").Data;
const StringReader = bytes.StringReader;
const StringWriter = bytes.StringWriter;

const KeyLength = u16;
const ValueLength = u16;

pub const Kv = struct {
    op: Op,
    ts: i128,

    key: []const u8,
    val: []const u8,

    alloc: ?Allocator = null,

    pub fn new(k: []const u8, v: []const u8, op: Op) Kv {
        return Kv{
            .ts = std.time.nanoTimestamp(),
            .op = op,
            .key = k,
            .val = v,
        };
    }

    pub fn deinit(self: Kv) void {
        if (self.alloc) |alloc| {
            alloc.free(self.key);
            alloc.free(self.val);
        }
    }

    pub fn read(reader: *ReaderWriterSeeker, alloc: Allocator) !Kv {
        // Op
        const opbyte = try reader.readByte();
        const op: Op = @enumFromInt(opbyte);

        const timestamp = try reader.readIntLittle(i128);

        var sr = StringReader(KeyLength).init(reader, alloc);
        const key = try sr.read();
        const value = try sr.read();

        return Kv{
            .op = op,
            .ts = timestamp,
            .key = key,
            .val = value,
            .alloc = alloc,
        };
    }

    pub fn storageSize(self: Kv) usize {
        return 1 + @sizeOf(i128) + @sizeOf(KeyLength) + self.key.len + @sizeOf(ValueLength) + self.val.len;
    }

    pub fn write(self: Kv, writer: *ReaderWriterSeeker) !usize {
        var start_offset = try writer.getPos();

        // Op
        try writer.writeByte(@intFromEnum(self.op));

        // Timestamp
        try writer.writeIntLittle(@TypeOf(self.ts), self.ts);

        var sw = StringWriter(KeyLength).init(writer);
        // Key
        try sw.write(self.key);

        // Value
        try sw.write(self.val);

        const end_offset = try writer.getPos();

        const written = end_offset - start_offset;

        return written;
    }

    pub fn writeIndexingValue(self: Kv, writer: *ReaderWriterSeeker) !void {
        try writer.writeIntNative(KeyLength, @intCast(self.key.len));
        try writer.writeAll(self.key);
    }

    // comptime lessThanFn: fn (@TypeOf(context), lhs: T, rhs: T) bool,
    pub fn compare(self: Kv, other: Data) bool {
        return lexicographical_compare({}, self, other.kv);
    }

    pub fn sortFn(_: Kv, lhs: Data, rhs: Data) bool {
        return lexicographical_compare({}, lhs.kv, rhs.kv);
    }

    pub fn readIndexingValue(reader: *ReaderWriterSeeker, alloc: Allocator) !Data {
        var sr = StringReader(KeyLength).init(reader, alloc);
        const firstkey = try sr.read();
        return Data{ .kv = Kv{ .alloc = alloc, .key = firstkey, .op = Op.Upsert, .ts = 0, .val = undefined } };
    }

    pub fn clone(self: Kv, alloc: Allocator) !Data {
        return Data{ .kv = Kv{
            .op = self.op,
            .ts = self.ts,
            .key = try alloc.dupe(u8, self.key),
            .val = try alloc.dupe(u8, self.val),
            .alloc = alloc,
        } };
    }

    pub fn debug(self: Kv, log: anytype) void {
        log.debug("\t\t[Row] Key: {s}, Ts: {}, Val: {s}\n", .{ self.key, self.ts, self.val });
    }

    pub fn default() Data {
        return Data{ .kv = Kv{
            .op = Op.Skip,
            .ts = 0,
            .key = undefined,
            .val = undefined,
            .alloc = undefined,
        } };
    }
};

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
    var alloc = std.testing.allocator;

    const row = Kv.new("key", "val", Op.Upsert);
    var buf = try alloc.alloc(u8, row.storageSize());
    defer alloc.free(buf);

    var rws = ReaderWriterSeeker.initBuf(buf);
    const bytes_written = try row.write(&rws);
    try std.testing.expectEqual(row.storageSize(), bytes_written);
    try std.testing.expectEqual(@as(usize, 27), bytes_written);

    try rws.seekTo(0);

    var row2 = try Kv.read(&rws, alloc);
    defer row2.deinit();

    try std.testing.expectEqualStrings(row.key, row2.key);
    try std.testing.expectEqualStrings(row.val, row2.val);
    try std.testing.expectEqual(row.ts, row2.ts);
    try std.testing.expectEqual(row.op, row2.op);
}
