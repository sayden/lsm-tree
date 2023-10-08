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

pub const Row = struct {
    op: Op,
    ts: i128,

    key: []const u8,
    val: []const u8,

    alloc: ?Allocator = null,

    pub fn new(k: []const u8, v: []const u8, op: Op) Row {
        return Row{
            .ts = std.time.nanoTimestamp(),
            .op = op,
            .key = k,
            .val = v,
        };
    }

    pub fn deinit(self: Row) void {
        if (self.alloc) |alloc| {
            alloc.free(self.key);
            alloc.free(self.val);
        }
    }

    pub fn read(reader: *ReaderWriterSeeker, alloc: Allocator) !Row {
        // Op
        const opbyte = try reader.readByte();
        const op: Op = @enumFromInt(opbyte);

        const timestamp = try reader.readIntLittle(i128);

        var sr = StringReader(KeyLength).init(reader, alloc);
        const key = try sr.read();
        const value = try sr.read();

        return Row{
            .op = op,
            .ts = timestamp,
            .key = key,
            .val = value,
            .alloc = alloc,
        };
    }

    pub fn storageSize(self: Row) usize {
        return 1 + @sizeOf(i128) + @sizeOf(KeyLength) + self.key.len + @sizeOf(ValueLength) + self.val.len;
    }

    pub fn write(self: Row, writer: *ReaderWriterSeeker) !usize {
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

    pub fn writeIndexingValue(self: Row, writer: *ReaderWriterSeeker) !void {
        try writer.writeAll(self.key);
    }

    pub fn compare(self: *Row, other: Row) math.Order {
        return lexicographical_compare(.{}, self, other);
    }

    pub fn sortFn(_: Row, lhs: Data, rhs: Data) bool {
        return lexicographical_compare({}, lhs.row, rhs.row);
    }

    pub fn readIndexingValue(reader: *ReaderWriterSeeker, alloc: Allocator) !Data {
        var sr = StringReader(KeyLength).init(reader, alloc);
        const firstkey = try sr.read();
        return Data{ .row = Row{ .alloc = alloc, .key = firstkey, .op = Op.Upsert, .ts = 0, .val = undefined } };
    }

    pub fn clone(self: Row, alloc: Allocator) !Data {
        return Data{ .row = Row{
            .op = self.op,
            .ts = self.ts,
            .key = try alloc.dupe(u8, self.key),
            .val = try alloc.dupe(u8, self.val),
            .alloc = alloc,
        } };
    }

    pub fn debug(self: Row, log: anytype) void {
        log.debug("\t\t[Row] Key: {s}, Ts: {}, Val: {s}\n", .{ self.key, self.ts, self.val });
    }
};

pub const RowsChunkWriter = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;

    //Metadata
    magicnumber: u16,
    rows_count: usize = 0,
    datasize: usize = 0,

    // known when persist
    firstkey: ?[]u8 = null,
    lastkey: ?[]u8 = null,

    mem: ArrayList(Row),

    alloc: Allocator,

    pub fn init(alloc: Allocator) RowsChunkWriter {
        return RowsChunkWriter{
            .mem = ArrayList(Row).init(alloc),
            .alloc = alloc,
            .magicnumber = @as(u16, 0),
        };
    }

    pub fn deinit(self: *RowsChunkWriter) void {
        var iter = MutableIterator(Row).init(self.mem.items);
        while (iter.next()) |row| {
            row.deinit();
        }

        self.mem.deinit();

        if (self.firstkey) |key| {
            self.alloc.free(key);
        }

        if (self.lastkey) |key| {
            self.alloc.free(key);
        }
    }

    pub fn appendKv(self: *RowsChunkWriter, k: []const u8, v: []const u8, op: Op) !void {
        var row = Row.new(k, v, op);
        // TODO if (row.storageSize() + self.storageSize() > RowsChunkWriter.MAX_SIZE) {
        // return Error.NotEnoughSpace;
        // }

        try self.mem.append(row);
        self.datasize += row.storageSize();
        self.rows_count += 1;
        try self.updateFirstAndLastKey(k);
    }

    pub fn updateFirstAndLastKey(self: *RowsChunkWriter, k: []const u8) !void {
        if (self.firstkey) |first_key| {
            if (strings.lte(k, first_key)) {
                var buf = try self.alloc.realloc(self.firstkey.?, k.len);
                @memcpy(buf, k);
            }

            if (strings.gt(k, self.lastkey.?)) {
                var buf = try self.alloc.realloc(self.lastkey.?, k.len);
                @memcpy(buf, k);
            }
        } else {
            self.firstkey = try self.alloc.dupe(u8, k);
            self.lastkey = try self.alloc.dupe(u8, k);
        }
    }

    pub fn write(self: *RowsChunkWriter, writer: *ReaderWriterSeeker) !usize {
        var size = self.datasize;
        // Update the size to include first and last key
        if (self.firstkey) |key| {
            size += key.len;
        } else {
            // Assume no data
            return 0;
        }

        self.sort();

        size += self.lastkey.?.len;

        var offset = try writer.getPos();

        try writer.writeIntLittle(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeIntLittle(@TypeOf(self.rows_count), self.rows_count);
        try writer.writeIntLittle(@TypeOf(self.datasize), size);
        var sw = StringWriter(KeyLength).init(writer);
        try sw.write(self.firstkey.?);
        try sw.write(self.lastkey.?);
        var iter = Iterator(Row).init(self.mem.items);
        while (iter.next()) |row| {
            _ = try row.write(writer);
        }

        var final_offset = try writer.getPos();
        const bytes_written = final_offset - offset;

        return bytes_written;
    }

    fn sort(self: RowsChunkWriter) void {
        std.sort.insertion(Row, self.mem.items, {}, lexicographical_compare);
    }
};

fn lexicographical_compare(_: void, lhs: Row, rhs: Row) bool {
    const res = strings.strcmp(lhs.key, rhs.key);

    // If keys and ops are the same, return the lowest string
    if (res == math.Order.eq and lhs.op == rhs.op) {
        return res.compare(math.CompareOperator.lte);
    } else if (res == math.Order.eq) {
        return @intFromEnum(lhs.op) < @intFromEnum(rhs.op);
    }

    return res.compare(math.CompareOperator.lte);
}

pub const RowsChunkReader = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;

    //Metadata
    magicnumber: u16,
    rows_count: usize = 0,
    datasize: usize = 0,

    // known when persist
    firstkey: ?[]u8 = null,
    lastkey: ?[]u8 = null,

    mem: ArrayList(Row),
    own_offset: usize,
    reader: *ReaderWriterSeeker,

    alloc: Allocator,

    pub fn deinit(self: *RowsChunkReader) void {
        var iter = MutableIterator(Row).init(self.mem.items);
        while (iter.next()) |row| {
            row.deinit();
        }

        self.mem.deinit();

        if (self.firstkey) |key| {
            self.alloc.free(key);
        }

        if (self.lastkey) |key| {
            self.alloc.free(key);
        }
    }

    pub fn readHead(reader: *ReaderWriterSeeker, alloc: Allocator) !RowsChunkReader {
        var magicn = try reader.readIntLittle(u16);
        var rows_count = try reader.readIntLittle(usize);
        var size = try reader.readIntLittle(usize);

        var sr = StringReader(KeyLength).init(reader, alloc);
        const firstkey = try sr.read();
        errdefer alloc.free(firstkey);
        const lastkey = try sr.read();
        errdefer alloc.free(lastkey);

        return RowsChunkReader{
            .mem = ArrayList(Row).init(alloc),
            .firstkey = firstkey,
            .lastkey = lastkey,
            .alloc = alloc,
            .datasize = size,
            .magicnumber = magicn,
            .rows_count = rows_count,
            .own_offset = try reader.getPos(),
            .reader = reader,
        };
    }

    pub fn readData(self: *RowsChunkReader) !void {
        try self.reader.seekTo(self.own_offset);

        for (0..self.rows_count) |_| {
            var row = try Row.read(self.reader, self.alloc);
            errdefer row.deinit();

            try self.mem.append(row);
        }
    }
};

pub const ChunksTableWriter = struct {
    const MAX_SIZE = std.mem.page_size * 32;

    magicnumber: u16 = 0,
    mem: ArrayList(RowsChunkWriter),
    chunks_count: usize = 0,
    firstkey: []const u8,
    lastkey: []const u8,

    alloc: Allocator,

    pub fn init(firstkey: []const u8, lastkey: []const u8, alloc: Allocator) !ChunksTableWriter {
        return ChunksTableWriter{
            .firstkey = try alloc.dupe(u8, firstkey),
            .lastkey = try alloc.dupe(u8, lastkey),
            .mem = ArrayList(RowsChunkWriter).init(alloc),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: ChunksTableWriter) void {
        self.alloc.free(self.firstkey);
        self.alloc.free(self.lastkey);

        var iter = MutableIterator(RowsChunkWriter).init(self.mem.items);
        while (iter.next()) |rows| {
            rows.deinit();
        }

        self.mem.deinit();
    }

    pub fn append(self: *ChunksTableWriter, rows: RowsChunkWriter) !void {
        try self.mem.append(rows);
        self.chunks_count += 1;
    }

    pub fn write(self: ChunksTableWriter, writer: *ReaderWriterSeeker) !usize {
        const start_offset = try writer.getPos();

        try writer.writeIntLittle(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeIntLittle(@TypeOf(self.chunks_count), self.chunks_count);
        var sw = StringWriter(KeyLength).init(writer);
        try sw.write(self.firstkey);
        try sw.write(self.lastkey);

        var written = try writer.getPos() - start_offset;

        var offsets_start = try writer.getPos();

        var offsets = try ArrayList(usize).initCapacity(self.alloc, self.mem.items.len);
        defer offsets.deinit();

        var iter = MutableIterator(RowsChunkWriter).init(self.mem.items);

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
    firstkey: []const u8,
    lastkey: []const u8,

    pub fn deinit(self: *ChunksTableReader) void {
        os.munmap(self.addr);
        self.file.close();
        self.alloc.free(self.firstkey);
        self.alloc.free(self.lastkey);
        self.offsets.deinit();
    }

    pub fn readRowChunkHead(self: *ChunksTableReader, array_pos: usize, alloc: Allocator) !RowsChunkReader {
        try self.reader.seekTo(self.offsets.items[array_pos]);
        return try RowsChunkReader.readHead(&self.reader, alloc);
    }

    pub fn read(f: fs.File, alloc: Allocator) !ChunksTableReader {
        var addr = try os.mmap(null, ChunksTableReader.MAX_SIZE, system.PROT.READ, std.os.MAP.SHARED, f.handle, 0);
        var reader = ReaderWriterSeeker.initBuf(addr);

        const magicn = try reader.readIntLittle(u16);
        const chunks_count = try reader.readIntLittle(usize);

        var sr = StringReader(KeyLength).init(&reader, alloc);
        const firstkey = try sr.read();
        errdefer alloc.free(firstkey);
        const lastkey = try sr.read();
        errdefer alloc.free(lastkey);

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
            .firstkey = firstkey,
            .lastkey = lastkey,
            .alloc = alloc,
        };
    }
};

test "Row" {
    var alloc = std.testing.allocator;

    const row = Row.new("key", "val", Op.Upsert);
    var buf = try alloc.alloc(u8, row.storageSize());
    defer alloc.free(buf);

    var rws = ReaderWriterSeeker.initBuf(buf);
    const bytes_written = try row.write(&rws);
    try std.testing.expectEqual(row.storageSize(), bytes_written);
    try std.testing.expectEqual(@as(usize, 27), bytes_written);

    try rws.seekTo(0);

    var row2 = try Row.read(&rws, alloc);
    defer row2.deinit();

    try std.testing.expectEqualStrings(row.key, row2.key);
    try std.testing.expectEqualStrings(row.val, row2.val);
    try std.testing.expectEqual(row.ts, row2.ts);
    try std.testing.expectEqual(row.op, row2.op);
}

test "RowsChunk" {
    var alloc = std.testing.allocator;

    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    var rows = RowsChunkWriter.init(alloc);
    defer rows.deinit();

    //26 -4 of first and last key which are null yet == 22
    // try std.testing.expectEqual(@as(usize, 22), rows.storageSize());

    try rows.appendKv("key", "val", Op.Upsert); //+27 bytes

    //22 initial data + 27 of the kv + 6 bytes for newly found first and last key ('key' and 'val') == 55
    // try std.testing.expectEqual(@as(usize, 55), rows.storageSize());

    try std.testing.expectEqual(@as(usize, 1), rows.mem.items.len);
    try std.testing.expectEqual(@as(usize, 1), rows.rows_count);

    var reader_writer = ReaderWriterSeeker.initBuf(buf);
    const bytes_written = try rows.write(&reader_writer);
    _ = bytes_written;

    // 55 +6 bytes of writing the first and last key ('key' and 'val' respectively) in the RowsChunk
    // try std.testing.expectEqual(rows.storageSize(), bytes_written);

    try reader_writer.seekTo(0);

    var rows2 = try RowsChunkReader.readHead(&reader_writer, alloc);
    defer rows2.deinit();
    try rows2.readData();

    try std.testing.expectEqual(@as(usize, 1), rows2.mem.items.len);
    try std.testing.expectEqualStrings(rows.mem.getLast().key, rows2.mem.getLast().key);
    try std.testing.expectEqualStrings(rows.mem.getLast().val, rows2.mem.getLast().val);
    try std.testing.expectEqual(rows.rows_count, rows2.rows_count);
}

test "ChunksTable" {
    var alloc = std.testing.allocator;

    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();
    var file = try tmpdir.dir.createFile("temp", fs.File.CreateFlags{ .read = true });
    var reader_writer = ReaderWriterSeeker.initFile(file);

    var rows = RowsChunkWriter.init(alloc);

    try rows.appendKv("hello", "world", Op.Upsert);

    var chunks = try ChunksTableWriter.init(rows.firstkey.?, rows.lastkey.?, alloc);
    defer chunks.deinit();

    try chunks.append(rows);

    const bytes_written = try chunks.write(&reader_writer);
    try std.testing.expectEqual(@as(usize, 95), bytes_written);

    // This simulates a newly opened file
    try reader_writer.seekTo(0);

    var table2 = try ChunksTableReader.read(file, alloc);
    defer table2.deinit();

    try std.testing.expectEqual(table2.chunks_count, chunks.chunks_count);

    var rc = try table2.readRowChunkHead(0, alloc);
    defer rc.deinit();
    try rc.readData();

    try std.testing.expectEqualStrings("hello", rc.mem.getLast().key);
}
