const std = @import("std");
const os = std.os;
const system = std.os.system;
const fs = std.fs;
const math = std.math;
const Op = @import("./ops.zig").Op;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const strings = @import("./strings.zig");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Iterator = @import("./iterator.zig").Iterator;
const MutableIterator = @import("./iterator.zig").MutableIterator;
const bytes = @import("./bytes.zig");
const StringReader = bytes.StringReader;
const StringWriter = bytes.StringWriter;
const DataNs = @import("./data.zig");
const Data = DataNs.Data;
const DataChunk = DataNs.DataChunk;
const DataTableReader = DataNs.DataTableReader;
const DataTableWriter = DataNs.DataTableWriter;

const KeyLength = u16;
const ValueLength = u16;

pub const Error = error{
    NotEnoughSpace,
    EmptyWal,
};

pub const Column = struct {
    op: Op,

    ts: i128,
    val: f64,

    alloc: ?Allocator = null,

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

    pub fn compare(self: Column, other: Data) bool {
        return math.order(self.ts, other.col.ts).compare(math.CompareOperator.lt);
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

    pub fn debug(self: Column, log: anytype) void {
        log.debug("\t\t[Column] Ts: {}, Val: {}", .{ self.ts, self.val });
    }

    pub fn default() Data {
        return Data{ .col = Column{
            .op = Op.Skip,
            .ts = 0,
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
