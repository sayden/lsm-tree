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
const KvkNs = @import("./kv.zig");
const Kv = KvkNs.Kv;
const ColumnNs = @import("./columnar.zig");
const Column = ColumnNs.Column;

pub const Error = error{
    NotEnoughSpace,
    EmptyWal,
    TableFull,
    UnknownChunkSize,
};

pub const Data = union(enum) {
    kv: Kv,
    col: Column,

    pub fn new(data: anytype) Data {
        return switch (@TypeOf(data)) {
            Kv => return Data{ .kv = data },
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
            Kv => Data{ .kv = result },
            inline else => Data{ .col = result },
        };
    }

    pub fn write(self: Data, writer: *ReaderWriterSeeker) !usize {
        return switch (self) {
            inline else => |case| case.write(writer),
        };
    }

    pub fn compare(self: Data, other: Data) bool {
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

pub const Chunk = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;
    const MAX_VALUES = 10;

    meta: Metadata,

    mem: ArrayList(Data),
    size: ?usize = 0,

    alloc: Allocator,

    pub fn init(comptime T: type, alloc: Allocator) Chunk {
        return Chunk{ .mem = ArrayList(Data).init(alloc), .alloc = alloc, .meta = Metadata{
            .firstkey = T.default(),
            .lastkey = T.default(),
        } };
    }

    pub fn deinit(self: *Chunk) void {
        var iter = MutableIterator(Data).init(self.mem.items);
        while (iter.next()) |data| {
            data.deinit();
        }

        self.mem.deinit();
        self.meta.deinit();
    }

    pub fn append(self: *Chunk, d: Data) !void {
        if (self.meta.count >= Chunk.MAX_VALUES) {
            return Error.NotEnoughSpace;
        }

        try self.mem.append(d);
        self.meta.count += 1;
    }

    /// Reads the bytes content of the reader. The reader must be positioned already at the
    /// beginning of the content, the order of data that it expects is the following:
    ///
    /// Metadata
    /// Size in bytes of the content that follows
    /// [0..n]Data (where n is a usize stored in Metadata.count)
    ///
    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !Chunk {
        //read the metadata header
        const meta = try Metadata.read(reader, T, alloc);

        const size = try reader.readIntNative(usize);

        var mem = ArrayList(Data).init(alloc);
        for (0..meta.count) |_| {
            // read a single data entry
            var row = try Data.read(T, reader, alloc);
            errdefer row.deinit();

            try mem.append(row);
        }

        return Chunk{
            .meta = meta,
            .mem = mem,
            .alloc = alloc,
            .size = size,
        };
    }

    /// Serializes the sorted contents of Chunk into writer. The format of the chunk is the following
    ///
    /// Metadata?   - (if write_meta == true)
    /// Size        - in bytes of the content that follows
    /// [1..n]Data  - where n is in the items in the memory array of Data which should be equal to Metadata.count.
    ///                 Be aware that if no data is stored, nothing will be written, hence the minimum of 1 in the array)
    ///
    /// Writing the chunk updates the size of the Chunk. Note that the size is unknown until a first write is executed.
    pub fn write(self: *Chunk, writer: *ReaderWriterSeeker, write_meta: bool) !usize {
        // Update the size to include first and last key
        if (self.mem.items.len == 0) {
            return 0;
        }

        self.sort();

        self.meta.firstkey = self.mem.items[0];
        self.meta.lastkey = self.mem.items[self.mem.items.len - 1];

        if (write_meta) {
            // write metadata header for this chunk
            try self.meta.write(writer);
        }

        const chunk_zero_offset = try writer.getPos();

        // Leave space for the chunk size
        try writer.seekBy(@as(i64, @sizeOf(usize)));

        var iter = Iterator(Data).init(self.mem.items);
        while (iter.next()) |data| {
            // write a single data entry
            _ = try data.write(writer);
        }

        // 8 represents the jump forward after setting chunk_zero_offset
        const bytes_written = try writer.getPos() - chunk_zero_offset + 8;
        self.size = bytes_written;

        //Go to beginning to write the size
        try writer.seekTo(chunk_zero_offset);
        try writer.writeIntNative(@TypeOf(bytes_written), bytes_written);

        // reset position to the beginning of chunk
        try writer.seekBy(@as(i64, -@sizeOf(@TypeOf(bytes_written))));

        return bytes_written;
    }

    fn sort(self: Chunk) void {
        std.sort.insertion(Data, self.mem.items, {}, Data.sortFn);
    }
};

pub const Wal = struct {
    const log = std.log.scoped(.TableWriter);
    const MAX_VALUES = 100;
    const MAX_SIZE = 1_000;
    // const MAX_SIZE = 128_000_000;

    meta: Metadata,
    datasize: usize = 0,
    file: std.fs.File,
    writer: ReaderWriterSeeker,

    chunks: ArrayList(Chunk),
    alloc: Allocator,

    pub fn init(comptime T: type, wal: std.fs.File, alloc: Allocator) !Wal {
        var writer = ReaderWriterSeeker.initFile(wal);

        return Wal{
            .meta = Metadata{
                .firstkey = T.default(),
                .lastkey = T.default(),
            },
            .chunks = ArrayList(Chunk).init(alloc),
            .alloc = alloc,
            .file = wal,
            .writer = writer,
        };
    }

    pub fn deinit(self: Wal) void {
        self.meta.deinit();

        for (self.chunks.items) |*rows| {
            rows.deinit();
        }

        self.chunks.deinit();
    }

    pub fn append(self: *Wal, chunk: Chunk) !void {
        if (chunk.size) |chunksize| {
            if (self.datasize + chunksize >= Wal.MAX_SIZE) {
                return Error.TableFull;
            }
        } else {
            return Error.UnknownChunkSize;
        }

        var mutablechunk = @constCast(&chunk);
        _ = try mutablechunk.write(&self.writer, true);
        try self.chunks.append(mutablechunk.*);
        self.datasize += chunk.size.?;
        self.meta.count += 1;
    }

    /// Writes the contents of the Table into file, which is enlarged to MAX_SIZE, the format is the following:
    ///
    /// Metadata
    /// [1..IndexData]
    /// [1..Chunk]
    ///
    pub fn persist(self: *Wal, file: fs.File) !usize {
        if (self.chunks.items.len == 0) {
            return 0;
        }

        var rws = ReaderWriterSeeker.initFile(file);
        var writer = &rws;

        // Move forward to the end of the file
        try writer.seekBy(Wal.MAX_SIZE);

        // create the index array
        var offsets = try ArrayList(IndexItem).initCapacity(self.alloc, self.chunks.items.len);
        defer offsets.deinit();

        // Get the current offset pos, after moving forward, this should be the same than TableWriter.MAX_SIZE
        var offset_sentinel = try writer.getPos();

        // store the first and last key of the entire index
        var firstkey: ?Data = undefined;
        var lastkey: ?Data = undefined;

        for (self.chunks.items) |*chunk| {
            // go backwards just enough to write the chunk
            const size: i64 = @as(i64, @intCast(chunk.size.?));
            try writer.seekBy(-size);
            offset_sentinel = try writer.getPos();

            // we are going to write in position 'offset_sentinel', store this position in the array
            // to write it later on the file
            try offsets.append(IndexItem{
                .offset = offset_sentinel,
                .firstkey = chunk.meta.firstkey,
                .lastkey = chunk.meta.lastkey,
            });

            // Update the first and last record from the index.
            if (firstkey) |fk| {
                if (fk.compare(chunk.meta.firstkey)) {
                    firstkey = chunk.meta.firstkey;
                }
            } else {
                firstkey = chunk.meta.firstkey;
                lastkey = chunk.meta.lastkey;
            }

            if (lastkey) |lk| {
                if (lk.compare(chunk.meta.lastkey)) {
                    lastkey = chunk.meta.lastkey;
                }
            }

            // write the chunk data
            var bytes_written = try chunk.write(writer, true);

            // Move backwards again, to the position where this chunk began writing
            try writer.seekBy(-@as(i64, @intCast(chunk.size.?)));
            if (bytes_written != chunk.size) {
                std.debug.print("{} != {}\n", .{ bytes_written, chunk.size.? });
                @panic("regression");
            }
        }

        // Once chunks are written, we can come back to the beginning of the file
        try writer.seekTo(0);

        // update metadata with the info about the first and last key
        self.meta.firstkey = firstkey.?;
        self.meta.lastkey = lastkey.?;

        // write the metadata header
        try self.meta.write(writer);

        // Finally, write the array of indices
        for (offsets.items) |index| {
            try index.write(writer);
        }

        return Wal.MAX_SIZE;
    }

    pub fn recover(file: fs.File, comptime T: type, alloc: Allocator) !Wal {
        var reader = ReaderWriterSeeker.initFile(file);

        var chunks = ArrayList(Chunk).init(alloc);

        var count: usize = 0;

        // store the first and last key of the entire index
        var firstkey: ?Data = undefined;
        var lastkey: ?Data = undefined;

        while (true) {
            const chunk = Chunk.read(&reader, T, alloc) catch |err| {
                if (err == error.EndOfStream) {
                    break;
                }

                return undefined;
            };

            // Update the first and last record from the index.
            if (firstkey) |fk| {
                if (fk.compare(chunk.meta.firstkey)) {
                    firstkey = chunk.meta.firstkey;
                }
            } else {
                firstkey = chunk.meta.firstkey;
                lastkey = chunk.meta.lastkey;
            }

            try chunks.append(chunk);
            count += 1;
        }

        return Wal{
            .meta = Metadata{
                .count = count,
                .firstkey = firstkey.?,
                .lastkey = lastkey.?,
                .magicnumber = 0,
            },
            .file = file,
            .writer = reader,
            .chunks = chunks,
            .alloc = alloc,
        };
    }

    pub fn debug(self: Wal) void {
        std.debug.print("\n___________\nSTART TableWriter\n", .{});
        self.meta.debug();
        std.debug.print("END TableWriter\n---------------\n", .{});
    }
};

const IndexItem = struct {
    const log = std.log.scoped(.IndexItem);

    offset: usize = 0,
    firstkey: Data,
    lastkey: Data,

    pub fn deinit(self: IndexItem) void {
        self.firstkey.deinit();
        self.lastkey.deinit();
    }

    /// Serializes the contents:
    ///
    /// Offset:     usize
    /// FirstKey:   Depends, either a pair of (n:u16,[0..n]u8) of a Kv type of Data or a i128 of a timestamp from a Column type of Data
    /// LastKey:    Like FirstKey
    pub fn write(self: IndexItem, writer: *ReaderWriterSeeker) !void {
        try writer.writeIntNative(@TypeOf(self.offset), self.offset);
        try self.firstkey.writeIndexingValue(writer);
        return self.lastkey.writeIndexingValue(writer);
    }

    /// Deserializes the contents, the expected format is the following
    ///
    /// Offset:     usize
    /// FirstKey:   Depends, either a pair of (n:u16,[0..n]u8) of a Kv type of Data or a i128 of a timestamp from a Column type of Data
    /// LastKey:    Like FirstKey
    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !IndexItem {
        const offset = try reader.readIntNative(usize);
        const firstkey = try T.readIndexingValue(reader, alloc);
        const lastkey = try T.readIndexingValue(reader, alloc);

        return IndexItem{
            .offset = offset,
            .firstkey = firstkey,
            .lastkey = lastkey,
        };
    }

    pub fn debug(i: IndexItem) void {
        log.debug("Offset:\t{}", .{i.offset});
        i.firstkey.debug(log);
        i.lastkey.debug(log);
    }
};

pub const TableFileReader = struct {
    const log = std.log.scoped(.TableReader);
    meta: Metadata,

    addr: []align(std.mem.page_size) u8,
    reader: ReaderWriterSeeker,
    alloc: Allocator,
    indices: ArrayList(IndexItem),

    pub fn deinit(self: *TableFileReader) void {
        os.munmap(self.addr);

        var iter = MutableIterator(IndexItem).init(self.indices.items);
        while (iter.next()) |rows| {
            rows.deinit();
        }
        self.indices.deinit();
        self.meta.deinit();
    }

    pub fn readChunk(self: *TableFileReader, array_pos: usize, comptime T: type, alloc: Allocator) !Chunk {
        const index = self.indices.items[array_pos];
        try self.reader.seekTo(index.offset);
        return try Chunk.read(&self.reader, T, alloc);
    }

    /// Mmap incoming file containing Data of type T. The indexing bytes of the file are read to build an in-memory ArrayList(IndexItem)
    /// Reading of chunk data happens lazily
    pub fn read(file: fs.File, comptime T: type, alloc: Allocator) !TableFileReader {
        const stat = try file.stat();
        var addr = try os.mmap(null, stat.size, system.PROT.READ | system.PROT.WRITE, std.os.MAP.SHARED, file.handle, 0);
        errdefer os.munmap(addr);

        var reader = ReaderWriterSeeker.initFile(file);
        try reader.seekTo(0);

        const meta = try Metadata.read(&reader, T, alloc);
        errdefer meta.deinit();

        var indices = try alloc.alloc(IndexItem, meta.count);
        errdefer alloc.free(indices);

        for (0..meta.count) |i| {
            var index = try IndexItem.read(&reader, T, alloc);
            errdefer index.deinit();
            indices[i] = index;
        }

        return TableFileReader{
            .meta = meta,
            .addr = addr,
            .reader = reader,
            .indices = ArrayList(IndexItem).fromOwnedSlice(alloc, indices),
            .alloc = alloc,
        };
    }

    pub fn debug(self: TableFileReader) void {
        std.debug.print("\n___________\nSTART TableReader\n", .{});
        self.meta.debug();
        std.debug.print("Indices\n", .{});
        for (self.indices.items) |index| {
            index.debug();
        }
        std.debug.print("END Tablereader\n---------------\n", .{});
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
    const data = Data.new(col1);

    var meta = Metadata{
        .firstkey = data,
        .lastkey = data,
        .magicnumber = 123,
        .count = 456,
    };
    defer meta.deinit();

    var buf: [64]u8 = undefined;
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
}

test "Data_Row" {
    var alloc = std.testing.allocator;

    var row = Kv.new("hello", "world", Op.Upsert);

    var data = Data{ .kv = row };
    defer data.deinit();

    var buf: [64]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);
    const bytes_written = try data.write(&rws);
    _ = bytes_written;

    try rws.seekTo(0);

    var row2 = try Data.read(Kv, &rws, alloc);
    defer row2.deinit();

    try std.testing.expectEqualStrings("hello", row2.kv.key);
}

pub fn testChunk(alloc: Allocator) !Chunk {
    var col1 = Column.new(5678, 123.2, Op.Upsert);
    var col2 = Column.new(1234, 200.2, Op.Upsert);

    var data = Data.new(col1);
    var data2 = Data.new(col2);

    // Chunk
    var original_chunk = Chunk.init(Column, alloc);
    // defer original_chunk.deinit();
    try original_chunk.append(data);
    try original_chunk.append(data2);

    return original_chunk;
}

test "Chunk" {
    std.testing.log_level = .debug;

    // Setup
    var alloc = std.testing.allocator;
    var original_chunk = try testChunk(alloc);
    defer original_chunk.deinit();

    try std.testing.expectEqual(@as(i128, 1234), original_chunk.mem.getLast().col.ts);
    try std.testing.expectEqual(@as(f64, 200.2), original_chunk.mem.getLast().col.val);

    // Chunk write
    var buf: [128]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);
    _ = try original_chunk.write(&rws, true);
    try rws.seekTo(0);

    // Chunk read
    var chunk_read = try Chunk.read(&rws, Column, alloc);
    defer chunk_read.deinit();

    try std.testing.expectEqual(original_chunk.mem.items.len, chunk_read.mem.items.len);
    try std.testing.expectEqual(original_chunk.mem.getLast().col.val, chunk_read.mem.getLast().col.val);
}

test "TableWriter" {
    std.testing.log_level = .debug;

    // Setup
    var alloc = std.testing.allocator;

    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();
    var wal = try tmpdir.dir.createFile("TableWriter", fs.File.CreateFlags{ .read = true });
    var table_file = try tmpdir.dir.createFile("TableWriter2", fs.File.CreateFlags{ .read = true });
    defer wal.close();
    defer table_file.close();

    var original_chunk = try testChunk(alloc);
    // deinit happens inside TableWriter

    var table_writer = try Wal.init(Column, wal, alloc);
    defer table_writer.deinit();
    try table_writer.append(original_chunk);

    // DataTable write
    const bytes_written: usize = try table_writer.persist(table_file);
    try std.testing.expectEqual(@as(usize, Wal.MAX_SIZE), bytes_written);
}

test "Wal_recover" {
    std.testing.log_level = .debug;

    // Setup
    var alloc = std.testing.allocator;
    var original_chunk = try testChunk(alloc);

    // tmp folder
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    // tmp wal and table files
    var walfile = try tmpdir.dir.createFile("TableRead", fs.File.CreateFlags{ .read = true });
    defer walfile.close();

    // write mock data
    var table_writer = try Wal.init(Column, walfile, alloc);
    defer table_writer.deinit();
    try table_writer.append(original_chunk);

    // recover wal data
    try walfile.seekTo(0);
    var wal_recovered = try Wal.recover(walfile, Column, alloc);
    defer wal_recovered.deinit();
    try std.testing.expectEqual(@as(usize, 1), wal_recovered.meta.count);
    try std.testing.expectEqual(@as(usize, 1), wal_recovered.chunks.items.len);
    try std.testing.expectEqual(@as(usize, 0), wal_recovered.meta.magicnumber);
    try std.testing.expectEqual(@as(i128, 1234), wal_recovered.meta.firstkey.col.ts);
}

test "Wal_persist_read" {
    std.testing.log_level = .debug;

    // Setup
    var alloc = std.testing.allocator;
    var original_chunk = try testChunk(alloc);

    // tmp folder
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    // tmp wal and table files
    var walfile = try std.fs.openFileAbsolute("/dev/null", fs.File.OpenFlags{ .mode = .write_only });
    defer walfile.close();
    var table_file = try tmpdir.dir.createFile("TablePersist", fs.File.CreateFlags{ .read = true });
    defer table_file.close();

    // write mock data
    var table_writer = try Wal.init(Column, walfile, alloc);
    defer table_writer.deinit();
    try table_writer.append(original_chunk);

    // read persisted data
    const bytes_written = try table_writer.persist(table_file);
    try std.testing.expectEqual(@as(usize, Wal.MAX_SIZE), bytes_written);
    try table_file.seekTo(0);
    var table_reader = try TableFileReader.read(table_file, Column, alloc);
    defer table_reader.deinit();

    try std.testing.expectEqual(@as(usize, 1), table_reader.meta.count);
    try std.testing.expectEqual(@as(usize, 1), table_reader.indices.items.len);
    try std.testing.expectEqual(@as(usize, 0), table_reader.meta.magicnumber);
    try std.testing.expectEqual(@as(i128, 1234), table_reader.meta.firstkey.col.ts);

    // parse a specific item from the mmap data
    var chunk_read2 = try table_reader.readChunk(0, Column, alloc);
    defer chunk_read2.deinit();

    try std.testing.expectEqual(original_chunk.mem.items.len, chunk_read2.mem.items.len);
    try std.testing.expectEqual(@as(i128, 5678), chunk_read2.mem.getLast().col.ts);
}
