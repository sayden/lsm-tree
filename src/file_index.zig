const std = @import("std");
const ArrayList = std.ArrayList;
const os = std.os;
const fs = std.fs;
const File = std.fs.File;
const Allocator = std.mem.Allocator;
const system = std.os.system;

const Metadata = @import("./metadata.zig").Metadata;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const IndexEntry = @import("./index_entry.zig").IndexEntry;
const MutableIterator = @import("./iterator.zig").MutableIterator;
const Chunk = @import("./chunk.zig").Chunk;
const Data = @import("./data.zig").Data;

pub const FileIndex = struct {
    const log = std.log.scoped(.TableFileReader);
    pub const Error = error{EmptyFile};

    meta: Metadata,

    addr: []align(std.mem.page_size) u8,
    reader: ReaderWriterSeeker,
    indices: ArrayList(IndexEntry),

    pub fn deinit(self: *FileIndex) void {
        os.munmap(self.addr);

        var iter = MutableIterator(IndexEntry).init(self.indices.items);
        while (iter.next()) |rows| {
            rows.deinit();
        }
        self.reader.file.close();
        self.indices.deinit();
        self.meta.deinit();
    }

    pub fn readChunk(self: *FileIndex, array_pos: usize, comptime T: type, alloc: Allocator) !Chunk {
        const index = self.indices.items[array_pos];
        try self.reader.seekTo(index.offset);
        return try Chunk.read(&self.reader, T, alloc);
    }

    /// Mmap incoming file containing Data of type T. The indexing bytes of the file are read to build an in-memory ArrayList(IndexItem)
    /// Reading of chunk data happens lazily
    pub fn read(file: fs.File, comptime T: type, alloc: Allocator) !FileIndex {
        const stat = try file.stat();
        if (stat.size == 0) {
            return Error.EmptyFile;
        }

        var addr = try os.mmap(null, stat.size, system.PROT.READ | system.PROT.WRITE, std.os.MAP.SHARED, file.handle, 0);
        errdefer os.munmap(addr);

        var reader = ReaderWriterSeeker.initFile(file);
        try reader.seekTo(0);

        const meta = try Metadata.read(&reader, T, alloc);
        errdefer meta.deinit();

        var indices = try alloc.alloc(IndexEntry, meta.count);
        errdefer alloc.free(indices);

        for (0..meta.count) |i| {
            var index = try IndexEntry.read(&reader, T, alloc);
            errdefer index.deinit();
            indices[i] = index;
        }

        return FileIndex{
            .meta = meta,
            .addr = addr,
            .reader = reader,
            .indices = ArrayList(IndexEntry).fromOwnedSlice(alloc, indices),
        };
    }

    pub fn isBetween(self: FileIndex, d: Data) ?IndexEntry {
        for (self.indices.items) |index| {
            if (d.compare(index.lastkey) or index.firstkey.compare(d) or index.firstkey.equals(d) or index.lastkey.equals(d)) {
                return index;
            }
        }

        return null;
    }

    pub fn debug(self: FileIndex) void {
        std.debug.print("\n___________\nSTART TableReader\n", .{});
        self.meta.debug();
        std.debug.print("Indices\n", .{});
        for (self.indices.items) |index| {
            index.debug();
        }
        std.debug.print("END Tablereader\n---------------\n", .{});
    }
};

test "Wal_recover" {
    const Op = @import("./ops.zig").Op;
    const Column = @import("./columnar.zig").Column;
    const Wal = @import("./wal.zig").Wal;

    // Setup
    var alloc = std.testing.allocator;
    var col1 = Column.new(5678, 123.2, Op.Upsert);
    var col2 = Column.new(1234, 200.2, Op.Upsert);

    var data = Data.new(col1);
    var data2 = Data.new(col2);

    // tmp folder
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    // tmp wal and table files
    var walfile = try tmpdir.dir.createFile("Wal_recover", fs.File.CreateFlags{ .read = true });
    var chunk_tmp_file = try tmpdir.dir.createFile("Wal_recoverTmp", fs.File.CreateFlags{ .read = true });

    // write mock data
    var wal = try Wal(Column).init(walfile, chunk_tmp_file, alloc);
    defer wal.deinit();
    var result = try wal.append(data);
    if (result != .Ok) return error.TestUnexpectedResult;

    result = try wal.append(data2);
    if (result != .Ok) return error.TestUnexpectedResult;

    // recover wal data
    try walfile.seekTo(0);
    try chunk_tmp_file.seekTo(0);

    var chunk_recovered = try Wal(Column).recoverChunkFile(chunk_tmp_file, alloc);
    defer chunk_recovered.deinit();

    try std.testing.expectEqual(@as(usize, 2), chunk_recovered.mem.items.len);
    try std.testing.expectEqual(@as(i128, 1234), chunk_recovered.meta.firstkey.col.ts);
    try std.testing.expectEqual(@as(i128, 5678), chunk_recovered.meta.lastkey.col.ts);
}

test "Wal_persist_read" {
    const Column = @import("./columnar.zig").Column;
    const Wal = @import("./wal.zig").Wal;

    // Setup
    var alloc = std.testing.allocator;
    var original_chunk = try @import("./chunk.zig").testChunk(alloc);

    // tmp folder
    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();

    // tmp wal and table files
    var wal_file = try std.fs.openFileAbsolute("/dev/null", fs.File.OpenFlags{ .mode = .write_only });
    var chunk_file = try tmpdir.dir.createFile("TablePersist", fs.File.CreateFlags{ .read = true });

    // write mock data
    var table_writer = try Wal(Column).init(wal_file, chunk_file, alloc);
    defer table_writer.deinit();
    _ = try table_writer.appendChunk(original_chunk);

    // read persisted data
    const bytes_written = try table_writer.persist(chunk_file);
    try std.testing.expectEqual(@as(usize, Wal(Column).MAX_SIZE), bytes_written);
    try chunk_file.seekTo(0);

    var chunk_path_buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    var chunk_path = try std.os.getFdPath(chunk_file.handle, &chunk_path_buf);
    var reopened_chunk_file = try std.fs.openFileAbsolute(chunk_path, .{ .mode = .read_write });
    var table_reader = try FileIndex.read(reopened_chunk_file, Column, alloc);
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
