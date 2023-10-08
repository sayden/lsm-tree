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
const Data = @import("./data.zig").Data;

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

    pub fn compare(self: *Column, other: Column) math.Order {
        return math.order(self.ts, other.ts);
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
};

pub const ColumnChunkWriter = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;

    //Metadata
    magicnumber: u16,
    rows_count: usize = 0,
    datasize: usize = 0,

    // known when persist
    min_t: ?i128 = null,
    max_t: ?i128 = null,

    mem: ArrayList(Column),

    alloc: Allocator,

    pub fn init(alloc: Allocator) ColumnChunkWriter {
        return ColumnChunkWriter{
            .mem = ArrayList(Column).init(alloc),
            .alloc = alloc,
            .magicnumber = @as(u16, 0),
        };
    }

    pub fn deinit(self: *ColumnChunkWriter) void {
        self.mem.deinit();
    }

    pub fn appendKv(self: *ColumnChunkWriter, t: i128, v: f64, op: Op) !void {
        var row = Column.new(t, v, op);
        const max_t = if (self.max_t) |curmax| @max(curmax, t) else t;

        if (self.min_t) |min_t| {
            if (max_t - min_t > std.time.ns_per_hour) {
                return Error.NotEnoughSpace;
            }
        }

        try self.mem.append(row);
        self.rows_count += 1;
        try self.updateFirstAndLastTimestamp(t);
    }

    pub fn updateFirstAndLastTimestamp(self: *ColumnChunkWriter, t: i128) !void {
        if (self.min_t) |min_t| {
            if (t <= min_t) {
                self.min_t = t;
            }

            if (t > self.max_t.?) {
                self.max_t = t;
            }
        } else {
            self.min_t = t;
            self.max_t = t;
        }
    }

    pub fn write(self: *ColumnChunkWriter, writer: *ReaderWriterSeeker) !usize {
        self.sort();

        var offset = try writer.getPos();

        try writer.writeIntNative(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeIntNative(@TypeOf(self.rows_count), self.rows_count);
        try writer.writeIntNative(i128, self.min_t.?);
        try writer.writeIntNative(i128, self.max_t.?);

        // jump 8 bytes to leave room for the data length
        // and store the offset position
        const datasize_pos_sentinel = try writer.getPos();
        try writer.seekBy(@sizeOf(@TypeOf(self.datasize)));

        var iter = Iterator(Column).init(self.mem.items);
        while (iter.next()) |row| {
            _ = try row.write(writer);
        }

        var final_offset = try writer.getPos();

        // Write the data length now
        const datasize = final_offset - datasize_pos_sentinel;
        try writer.seekTo(datasize_pos_sentinel);
        try writer.writeIntNative(@TypeOf(self.datasize), datasize);

        const bytes_written = final_offset - offset;
        return bytes_written;
    }

    fn sort(self: ColumnChunkWriter) void {
        std.sort.insertion(Column, self.mem.items, {}, ts_compare);
    }

    fn ts_compare(_: void, lhs: Column, rhs: Column) bool {
        const res = math.order(lhs.ts, rhs.ts);

        // If keys and ops are the same, return the lowest string
        if (res == math.Order.eq and lhs.op == rhs.op) {
            return res.compare(math.CompareOperator.lte);
        } else if (res == math.Order.eq) {
            return @intFromEnum(lhs.op) < @intFromEnum(rhs.op);
        }

        return res.compare(math.CompareOperator.lte);
    }
};

pub const ColumnsChunkReader = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;

    //Metadata
    magicnumber: u16,
    rows_count: usize = 0,
    datasize: usize = 0,

    // known when persist
    min_t: ?i128 = null,
    max_t: ?i128 = null,

    mem: ArrayList(Column),
    own_offset: usize,
    reader: *ReaderWriterSeeker,

    alloc: Allocator,

    pub fn deinit(self: *ColumnsChunkReader) void {
        var iter = MutableIterator(Column).init(self.mem.items);
        while (iter.next()) |row| {
            row.deinit();
        }

        self.mem.deinit();
    }

    pub fn readHead(reader: *ReaderWriterSeeker, alloc: Allocator) !ColumnsChunkReader {
        var magicn = try reader.readIntLittle(u16);
        var rows_count = try reader.readIntLittle(usize);
        var size = try reader.readIntLittle(usize);

        const min_t = try reader.readIntLittle(i128);
        const max_t = try reader.readIntLittle(i128);

        return ColumnsChunkReader{
            .mem = ArrayList(Column).init(alloc),
            .min_t = min_t,
            .max_t = max_t,
            .alloc = alloc,
            .datasize = size,
            .magicnumber = magicn,
            .rows_count = rows_count,
            .own_offset = try reader.getPos(),
            .reader = reader,
        };
    }

    pub fn readData(self: *ColumnsChunkReader) !void {
        try self.reader.seekTo(self.own_offset);

        for (0..self.rows_count) |_| {
            var row = try Column.read(self.reader, self.alloc);
            errdefer row.deinit();

            try self.mem.append(row);
        }
    }
};

pub const ChunksTableWriter = struct {
    const MAX_SIZE = std.mem.page_size * 32;

    magicnumber: u16 = 0,
    mem: ArrayList(ColumnChunkWriter),
    chunks_count: usize = 0,
    min_t: i128,
    max_t: i128,

    alloc: Allocator,

    pub fn init(min_t: i128, max_t: i128, alloc: Allocator) !ChunksTableWriter {
        return ChunksTableWriter{
            .min_t = min_t,
            .max_t = max_t,
            .mem = ArrayList(ColumnChunkWriter).init(alloc),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: ChunksTableWriter) void {
        var iter = MutableIterator(ColumnChunkWriter).init(self.mem.items);
        while (iter.next()) |rows| {
            rows.deinit();
        }

        self.mem.deinit();
    }

    pub fn append(self: *ChunksTableWriter, rows: ColumnChunkWriter) !void {
        try self.mem.append(rows);
        self.chunks_count += 1;
    }

    pub fn write(self: ChunksTableWriter, writer: *ReaderWriterSeeker) !usize {
        const start_offset = try writer.getPos();

        try writer.writeIntNative(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeIntNative(@TypeOf(self.chunks_count), self.chunks_count);
        try writer.writeIntNative(i128, self.min_t);
        try writer.writeIntNative(i128, self.max_t);

        var written = try writer.getPos() - start_offset;

        var offsets_start = try writer.getPos();

        var offsets = try ArrayList(usize).initCapacity(self.alloc, self.mem.items.len);
        defer offsets.deinit();

        var iter = MutableIterator(ColumnChunkWriter).init(self.mem.items);

        // Move forward to the place where the offsets have finished
        var n: i64 = @intCast(@sizeOf(usize) * self.mem.items.len);
        try writer.seekBy(n);

        // Get the current offset pos
        var chunk_offset_pos = try writer.getPos();

        while (iter.next()) |chunk| {
            try offsets.append(chunk_offset_pos);
            var bytes_written = try chunk.write(writer);
            written += bytes_written;
            chunk_offset_pos += written;
        }

        // Go back to the position where the offsets are written, to actually write them
        try writer.seekTo(offsets_start);
        for (offsets.items) |offset| {
            try writer.writeIntNative(usize, offset);
        }

        written += @sizeOf(usize) * offsets.items.len;

        return written;
    }
};

const ChunksTableReader = struct {
    const MAX_SIZE = std.mem.page_size * 32;

    file: fs.File,
    addr: []align(std.mem.page_size) u8,
    reader: ReaderWriterSeeker,
    offsets: ArrayList(usize),
    alloc: Allocator,
    magicnumber: u16 = 0,
    chunks_count: usize = 0,
    min_t: i128,
    max_t: i128,

    pub fn deinit(self: *ChunksTableReader) void {
        os.munmap(self.addr);
        self.file.close();
        self.offsets.deinit();
    }

    pub fn readRowChunkHead(self: *ChunksTableReader, array_pos: usize, alloc: Allocator) !ColumnsChunkReader {
        try self.reader.seekTo(self.offsets.items[array_pos]);
        return try ColumnsChunkReader.readHead(&self.reader, alloc);
    }

    pub fn read(f: fs.File, alloc: Allocator) !ChunksTableReader {
        var addr = try os.mmap(null, ChunksTableReader.MAX_SIZE, system.PROT.READ, std.os.MAP.SHARED, f.handle, 0);
        var reader = ReaderWriterSeeker.initBuf(addr);

        const magicn = try reader.readIntLittle(u16);
        const chunks_count = try reader.readIntLittle(usize);

        const min_t = try reader.readIntLittle(i128);
        const max_t = try reader.readIntLittle(i128);

        var offsets = try alloc.alloc(usize, chunks_count);
        errdefer alloc.free(offsets);

        for (0..chunks_count) |i| {
            offsets[i] = try reader.readIntLittle(usize);
        }

        return ChunksTableReader{
            .addr = addr,
            .file = f,
            .reader = reader,
            .offsets = ArrayList(usize).fromOwnedSlice(alloc, offsets),
            .magicnumber = magicn,
            .chunks_count = chunks_count,
            .min_t = min_t,
            .max_t = max_t,
            .alloc = alloc,
        };
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

test "ChunkColumn" {
    var alloc = std.testing.allocator;

    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    var rows = ColumnChunkWriter.init(alloc);
    defer rows.deinit();

    try rows.appendKv(999, 44.5, Op.Upsert); //+27 bytes

    try std.testing.expectEqual(@as(usize, 1), rows.mem.items.len);
    try std.testing.expectEqual(@as(usize, 1), rows.rows_count);

    var reader_writer = ReaderWriterSeeker.initBuf(buf);
    const bytes_written = try rows.write(&reader_writer);
    _ = bytes_written;

    try reader_writer.seekTo(0);

    var rows2 = try ColumnsChunkReader.readHead(&reader_writer, alloc);
    defer rows2.deinit();
    try rows2.readData();

    try std.testing.expectEqual(@as(usize, 1), rows2.mem.items.len);
    try std.testing.expectEqual(rows.mem.getLast().ts, rows2.mem.getLast().ts);
    try std.testing.expectEqual(rows.mem.getLast().val, rows2.mem.getLast().val);
    try std.testing.expectEqual(rows.rows_count, rows2.rows_count);
}

test "ChunksTable" {
    var alloc = std.testing.allocator;

    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();
    var file = try tmpdir.dir.createFile("temp", fs.File.CreateFlags{ .read = true });
    var reader_writer = ReaderWriterSeeker.initFile(file);

    var rows = ColumnChunkWriter.init(alloc);

    try rows.appendKv(9999, 123.45, Op.Upsert);

    var chunks = try ChunksTableWriter.init(rows.min_t.?, rows.max_t.?, alloc);
    defer chunks.deinit();

    try chunks.append(rows);

    const bytes_written = try chunks.write(&reader_writer);
    _ = bytes_written;
    // try std.testing.expectEqual(@as(usize, 95), bytes_written);

    // This simulates a newly opened file
    try reader_writer.seekTo(0);

    var table2 = try ChunksTableReader.read(file, alloc);
    defer table2.deinit();

    try std.testing.expectEqual(table2.chunks_count, chunks.chunks_count);

    var rc = try table2.readRowChunkHead(0, alloc);
    defer rc.deinit();

    try rc.readData();

    try std.testing.expectEqual(@as(i128, 9999), rc.mem.getLast().ts);
    try std.testing.expectEqual(@as(f64, 123.45), rc.mem.getLast().val);
}
