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
const ChunkNs = @import("./chunk.zig");
const Row = ChunkNs.Row;
const ColumnNs = @import("./columnar.zig");
const Column = ColumnNs.Column;

pub const Error = error{
    NotEnoughSpace,
    EmptyWal,
};

pub const Data = union(enum) {
    row: Row,
    col: Column,

    pub fn new(comptime T: type, data: T) Data {
        return switch (T) {
            Row => return Data{ .row = data },
            inline else => return Data{ .col = data },
        };
    }

    pub fn deinit(self: Data) void {
        return switch (self) {
            inline else => |case| case.deinit(),
        };
    }

    pub fn read(comptime T: type, reader: *ReaderWriterSeeker, alloc: Allocator) !Data {
        var result: T = try T.read(reader, alloc);
        return switch (T) {
            Row => Data{ .row = result },
            inline else => Data{ .col = result },
        };
    }

    pub fn write(self: Data, writer: *ReaderWriterSeeker) !usize {
        return switch (self) {
            inline else => |case| case.write(writer),
        };
    }

    pub fn compare(self: Data, other: Data) math.Order {
        return switch (self) {
            inline else => |case| case.compare(other),
        };
    }

    pub fn writeIndexingValue(self: Data, writer: *ReaderWriterSeeker) !void {
        return switch (self) {
            inline else => |case| case.writeIndexingValue(writer),
        };
    }

    pub fn sortFn(_: void, self: Data, other: Data) bool {
        return switch (self) {
            inline else => |case| case.sortFn(self, other),
        };
    }

    pub fn getKey(self: *const Data, comptime T: type) T {
        return switch (@TypeOf(self)) {
            *Row => @as(*const Row, @ptrCast(self)).key,
            inline else => @as(*const Column, @ptrCast(self)).ts,
        };
    }

    pub fn getTs(self: Data) i128 {
        return switch (self) {
            inline else => |case| case.ts,
        };
    }

    pub fn getVal(self: *const Data, comptime T: type) T {
        return switch (@TypeOf(self)) {
            *Row => @as(*const Row, @ptrCast(self)).val,
            inline else => @as(*const Column, @ptrCast(self)).val,
        };
    }

    pub fn clone(self: Data, alloc: Allocator) !Data {
        return switch (self) {
            inline else => |case| case.clone(alloc),
        };
    }

    pub fn debug(self: Data, log: anytype) void {
        return switch (self) {
            inline else => |case| case.debug(log),
        };
    }
};

pub const DataChunk = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;

    meta: ?Metadata = null,

    mem: ArrayList(Data),

    alloc: Allocator,

    pub fn init(alloc: Allocator) DataChunk {
        return DataChunk{
            .mem = ArrayList(Data).init(alloc),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *DataChunk) void {
        var iter = MutableIterator(Data).init(self.mem.items);
        while (iter.next()) |row| {
            row.deinit();
        }

        self.mem.deinit();

        if (self.meta) |*meta| {
            meta.deinit();
        }
    }

    pub fn append(self: *DataChunk, d: Data) !void {
        try self.mem.append(d);
        if (self.meta) |_| {} else {
            self.meta = Metadata{
                .firstkey = d,
                .lastkey = d,
            };
        }
        self.meta.?.count += 1;
    }

    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !DataChunk {
        //read the metadata header
        const meta = try Metadata.read(reader, T, alloc);

        var mem = ArrayList(Data).init(alloc);
        for (0..meta.count) |_| {
            // read a single data entry
            var row = try Data.read(T, reader, alloc);
            errdefer row.deinit();

            try mem.append(row);
        }

        return DataChunk{
            .meta = meta,
            .mem = mem,
            .alloc = alloc,
        };
    }

    pub fn write(self: *DataChunk, writer: *ReaderWriterSeeker) !usize {
        // Update the size to include first and last key
        if (self.mem.items.len == 0) {
            return 0;
        }

        self.sort();

        self.meta.?.firstkey = self.mem.items[0];
        self.meta.?.lastkey = self.mem.items[self.mem.items.len - 1];

        var chunk_zero_offset = try writer.getPos();

        // write metadata header for this chunk
        try self.meta.?.write(writer);

        var iter = Iterator(Data).init(self.mem.items);
        while (iter.next()) |data| {
            // write a single data entry
            _ = try data.write(writer);
        }

        const bytes_written = try writer.getPos() - chunk_zero_offset;
        return bytes_written;
    }

    fn sort(self: DataChunk) void {
        std.sort.insertion(Data, self.mem.items, {}, Data.sortFn);
    }
};

pub const DataTableWriter = struct {
    const log = std.log.scoped(.DataTableWriter);

    meta: Metadata,

    mem: ArrayList(DataChunk),
    alloc: Allocator,

    pub fn init(firstkey: Data, lastkey: Data, alloc: Allocator) !DataTableWriter {
        return DataTableWriter{
            .meta = Metadata{
                .firstkey = try firstkey.clone(alloc),
                .lastkey = try lastkey.clone(alloc),
            },
            .mem = ArrayList(DataChunk).init(alloc),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: DataTableWriter) void {
        self.meta.deinit();

        var iter = MutableIterator(DataChunk).init(self.mem.items);
        while (iter.next()) |rows| {
            rows.deinit();
        }

        self.mem.deinit();
    }

    pub fn append(self: *DataTableWriter, rows: DataChunk) !void {
        try self.mem.append(rows);
        self.meta.count += 1;
    }

    pub fn write(self: *DataTableWriter, writer: *ReaderWriterSeeker) !usize {
        var start_size: usize = try writer.getPos();

        // write the metadata header
        try self.meta.write(writer);

        var offsets = try ArrayList(usize).initCapacity(self.alloc, self.mem.items.len);
        defer offsets.deinit();

        // Move forward to the place where the offsets should have finished
        const start_offset = try writer.getPos();
        const n: i64 = @intCast(@sizeOf(usize) * self.mem.items.len);
        try writer.seekBy(n);

        // Get the current offset pos, after moving forward
        var offset_sentinel = try writer.getPos();

        var iter = MutableIterator(DataChunk).init(self.mem.items);
        while (iter.next()) |chunk| {
            // we are going to write in position 'offset_sentinel', store this position in the array
            // to write it later on the file
            try offsets.append(offset_sentinel);

            // write the chunk data
            var bytes_written = try chunk.write(writer);

            // move the sentinel forward
            offset_sentinel += bytes_written;
        }

        const final_pos = offset_sentinel;

        // go back to the position to write the offsets list
        try writer.seekTo(start_offset);

        // Finally, write the array of offsets
        for (offsets.items) |offset| {
            try writer.writeIntNative(usize, offset);
        }

        return final_pos - start_size;
    }
};

pub const DataTableReader = struct {
    const log = std.log.scoped(.DataTableReader);
    meta: Metadata,

    offsets: ArrayList(usize),
    file: fs.File,
    addr: []align(std.mem.page_size) u8,
    reader: ReaderWriterSeeker,
    alloc: Allocator,

    pub fn deinit(self: *DataTableReader) void {
        os.munmap(self.addr);
        self.file.close();
        self.offsets.deinit();
        self.meta.deinit();
    }

    pub fn readChunk(self: *DataTableReader, array_pos: usize, comptime T: type, alloc: Allocator) !DataChunk {
        try self.reader.seekTo(self.offsets.items[array_pos]);
        return try DataChunk.read(&self.reader, T, alloc);
    }

    pub fn read(f: fs.File, comptime T: type, alloc: Allocator) !DataTableReader {
        var addr = try os.mmap(null, Metadata.MAX_SIZE, system.PROT.READ, std.os.MAP.SHARED, f.handle, 0);
        errdefer os.munmap(addr);

        var reader = ReaderWriterSeeker.initBuf(addr);

        const meta = try Metadata.read(&reader, T, alloc);
        errdefer meta.deinit();

        var offsets = try alloc.alloc(usize, meta.count);
        errdefer alloc.free(offsets);

        for (0..meta.count) |i| {
            offsets[i] = try reader.readIntNative(usize);
        }

        return DataTableReader{
            .meta = meta,
            .addr = addr,
            .file = f,
            .reader = reader,
            .offsets = ArrayList(usize).fromOwnedSlice(alloc, offsets),
            .alloc = alloc,
        };
    }
};

const Metadata = struct {
    const log = std.log.scoped(.Metadata);

    const MAX_SIZE = std.mem.page_size * 32;

    firstkey: Data,
    lastkey: Data,
    magicnumber: u16 = 0,
    count: usize = 0,

    pub fn deinit(self: Metadata) void {
        self.firstkey.deinit();
        self.lastkey.deinit();
    }

    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !Metadata {
        const magicn = try reader.readIntNative(u16);
        const count = try reader.readIntNative(usize);
        const firstkey = try T.readIndexingValue(reader, alloc);
        const lastkey = try T.readIndexingValue(reader, alloc);

        return Metadata{
            .magicnumber = magicn,
            .count = count,
            .firstkey = firstkey,
            .lastkey = lastkey,
        };
    }

    pub fn write(self: Metadata, writer: *ReaderWriterSeeker) !void {
        try writer.writeIntNative(@TypeOf(self.magicnumber), self.magicnumber);
        try writer.writeIntNative(@TypeOf(self.count), self.count);
        try self.firstkey.writeIndexingValue(writer);
        try self.lastkey.writeIndexingValue(writer);
    }

    pub fn debug(m: Metadata) void {
        log.debug("--------", .{});
        log.debug("Metadata", .{});
        log.debug("--------", .{});
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
    const data = Data.new(Column, col1);

    var meta = Metadata{
        .firstkey = data,
        .lastkey = data,
        .magicnumber = 123,
        .count = 456,
    };

    var buf: [512]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);

    try meta.write(&rws);
    try rws.seekTo(0);

    var alloc = std.testing.allocator;
    const meta2 = try Metadata.read(&rws, Column, alloc);
    meta2.deinit();

    try std.testing.expectEqual(meta.magicnumber, meta2.magicnumber);
    try std.testing.expectEqual(meta.count, meta2.count);
    try std.testing.expectEqual(meta.firstkey.col.ts, meta2.firstkey.col.ts);
    try std.testing.expectEqual(meta.lastkey.col.ts, meta2.lastkey.col.ts);
}

test "Data_Row" {
    var alloc = std.testing.allocator;

    var row = Row.new("hello", "world", Op.Upsert);

    var data = Data{ .row = row };
    defer data.deinit();

    var buf: [64]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);
    const bytes_written = try data.write(&rws);
    _ = bytes_written;

    try rws.seekTo(0);

    var row2 = try Data.read(Row, &rws, alloc);
    defer row2.deinit();

    try std.testing.expectEqualStrings("hello", row2.row.key);
}

test "DataChunkWriter" {
    std.testing.log_level = .debug;

    // Setup
    var alloc = std.testing.allocator;

    var first_t = std.time.nanoTimestamp();
    std.time.sleep(100);
    var second_t = std.time.nanoTimestamp();
    var col1 = Column.new(second_t, 123.2, Op.Upsert);
    var col2 = Column.new(first_t, 200.2, Op.Upsert);

    var data = Data.new(Column, col1);
    var data2 = Data.new(Column, col2);

    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();
    var file = try tmpdir.dir.createFile("DataChunkWriter", fs.File.CreateFlags{ .read = true });
    // defer file.close();
    var file_rws = ReaderWriterSeeker.initFile(file);

    // DataChunk
    var original_chunk = DataChunk.init(alloc);
    // defer dcw.deinit();
    try original_chunk.append(data);
    try original_chunk.append(data2);

    try std.testing.expectEqual(first_t, original_chunk.mem.getLast().getKey(i128));
    try std.testing.expectEqual(@as(f64, 200.2), original_chunk.mem.getLast().getVal(f64));

    // DataChunk write
    var buf: [128]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);
    _ = try original_chunk.write(&rws);
    try rws.seekTo(0);

    // DataChunk read
    var chunk_read = try DataChunk.read(&rws, Column, alloc);
    defer chunk_read.deinit();

    try std.testing.expectEqual(original_chunk.mem.items.len, chunk_read.mem.items.len);
    try std.testing.expectEqual(original_chunk.mem.getLast().col.val, chunk_read.mem.getLast().col.val);

    // DataTable
    var table_writer = try DataTableWriter.init(data2, data, alloc);
    defer table_writer.deinit();
    try table_writer.append(original_chunk);

    // DataTable write
    const bytes_written = try table_writer.write(&file_rws);
    try std.testing.expectEqual(@as(usize, 138), bytes_written);

    // DataTable read
    var table_reader = try DataTableReader.read(file, Column, alloc);
    defer table_reader.deinit();
    try std.testing.expectEqual(@as(usize, 1), table_reader.offsets.items.len);
    try std.testing.expectEqual(@as(usize, 0), table_reader.meta.magicnumber);
    try std.testing.expectEqual(first_t, table_reader.meta.firstkey.getTs());
    try std.testing.expectEqual(@as(usize, 1), table_reader.meta.count);

    var chunk_read2 = try table_reader.readChunk(0, Column, alloc);
    defer chunk_read2.deinit();

    try std.testing.expectEqual(chunk_read.mem.items.len, chunk_read2.mem.items.len);
    try std.testing.expectEqual(col1.ts, chunk_read2.mem.getLast().getTs());
}
