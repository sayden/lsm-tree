const std = @import("std");

const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const File = std.fs.File;
const fs = std.fs;
const math = std.math;
const os = std.os;
const system = std.os.system;

const Op = @import("./ops.zig").Op;
const Data = @import("./data.zig").Data;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

const KeyLength = u16;
const ValueLength = u16;

pub const Column = struct {
    op: Op,

    ts: i128,
    val: f64,

    pub fn new(ts: i128, v: f64, op: Op) Column {
        return Column{
            .op = op,
            .ts = ts,
            .val = v,
        };
    }

    pub fn deinit(_: Column) void {}

    pub fn read(reader: *ReaderWriterSeeker, _: Allocator) !Column {
        // Op
        const opbyte = try reader.readByte();
        const op: Op = @enumFromInt(opbyte);

        const ts = try reader.readIntLittle(i128);
        const value = try reader.readFloat(f64);

        return Column{
            .op = op,
            .ts = ts,
            .val = value,
        };
    }

    pub fn write(self: Column, writer: *ReaderWriterSeeker) !usize {
        var start_offset = try writer.getPos();

        // Op
        try writer.writeByte(@intFromEnum(self.op));

        // Timestamp
        try writer.writeIntNative(@TypeOf(self.ts), self.ts);

        // Value
        try writer.writeFloat(@TypeOf(self.val), self.val);

        const end_offset = try writer.getPos();

        const written = end_offset - start_offset;

        return written;
    }

    pub fn clone(self: Column, _: Allocator) !Data {
        return Data{ .col = Column{
            .op = self.op,
            .ts = self.ts,
            .val = self.val,
        } };
    }

    pub fn cloneTo(self: Column, other: *Data, _: Allocator) !void {
        other.col.op = self.op;
        other.col.ts = self.ts;
        other.col.val = self.val;
    }

    /// Returns true if self < other
    pub fn compare(self: Column, other: Data) bool {
        return self.ts < other.col.ts;
    }

    pub fn sortFn(_: Column, lhs: Data, rhs: Data) bool {
        return math.order(lhs.col.ts, rhs.col.ts).compare(math.CompareOperator.lte);
    }

    pub fn writeIndexingValue(self: Column, writer: *ReaderWriterSeeker) !void {
        return writer.writeIntNative(@TypeOf(self.ts), self.ts);
    }

    pub fn readIndexingValue(reader: *ReaderWriterSeeker, _: Allocator) !Data {
        const ts = try reader.readIntNative(i128);
        return Data{ .col = Column{ .ts = ts, .val = undefined, .op = Op.Upsert } };
    }

    pub fn equals(self: Column, other: Data) bool {
        return self.ts == other.col.ts;
    }

    pub fn debug(self: Column, log: anytype) void {
        log.debug("\t\t[Column] Ts: {}, Val: {}", .{ self.ts, self.val });
    }

    pub fn defaultLastKey() Data {
        return Data{ .col = Column{
            .op = Op.Skip,
            .ts = 0,
            .val = 0,
        } };
    }

    pub fn defaultFirstKey() Data {
        return Data{ .col = Column{
            .op = Op.Skip,
            .ts = std.math.maxInt(i128),
            .val = 0,
        } };
    }
};

test "Column" {
    var alloc = std.testing.allocator;

    const row = Column.new(99, @as(f64, 24.8), Op.Upsert);
    var buf = try alloc.alloc(u8, 128);
    defer alloc.free(buf);

    var rws = ReaderWriterSeeker.initBuf(buf);
    const bytes_written = try row.write(&rws);
    try std.testing.expectEqual(@as(usize, 22), bytes_written);

    try rws.seekTo(0);

    var row2 = try Column.read(&rws, alloc);
    defer row2.deinit();

    try std.testing.expectEqual(row.val, row2.val);
    try std.testing.expectEqual(row.ts, row2.ts);
    try std.testing.expectEqual(row.op, row2.op);
}
