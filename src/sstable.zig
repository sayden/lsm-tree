const std = @import("std");
const ArrayList = std.ArrayList;
const os = std.os;
const fs = std.fs;
const File = std.fs.File;
const Allocator = std.mem.Allocator;

const Metadata = @import("./metadata.zig").Metadata;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const MutableIterator = @import("./iterator.zig").MutableIterator;
const Kv = @import("./kv.zig").Kv;
const Wal = @import("./mmap_wal.zig").Wal;
const StringWriter = @import("./bytes.zig").StringWriter;

/// When converting from a WAL to a SSTable, a temporary SSTable is created to hold the contents
/// of the WAL. This is called the TempSSTable. It is used to sort the contents of the WAL and
/// serialize them to a file.
const TempSSTable = struct {
    const log = std.log.scoped(.SSTable);
    const Errors = error{EmptyFile};

    meta: Metadata,

    addr: []align(std.mem.page_size) u8,
    keys_size: usize,
    data_size: usize,
    kvs: ArrayList(Kv) = undefined,
    alloc: Allocator,
    arena: std.heap.ArenaAllocator,

    pub fn newSSTableFileFromWal(source: File, destination: File, alloc: Allocator) !void {
        const sstable = try TempSSTable.newFromWal(source, alloc);
        defer sstable.deinit();

        const stat = try source.stat();
        if (stat.size == 0) {
            return error{EmptyFile};
        }

        const addr = try std.posix.mmap(null, stat.size, std.posix.PROT.READ |
            std.posix.PROT.WRITE, std.posix.MAP{ .TYPE = .SHARED }, source.handle, 0);

        const written = try sstable.serialize(addr, alloc);
        try destination.write(addr[0..written]);
    }

    // TODO: Duplicates must be removed as well as Deleted keys
    pub fn newFromWal(file: File, alloc: Allocator) !TempSSTable {
        const stat = try file.stat();
        if (stat.size == 0) {
            return Errors.EmptyFile;
        }

        var arena = std.heap.ArenaAllocator.init(alloc);
        const arena_alloc = arena.allocator();
        const wal_read_result: Wal.ReadResult = try Wal.read(file, arena_alloc);

        const kvs = wal_read_result.kvs;

        sort(kvs.items);

        // create metadata based on the contents of the wal
        var meta = Metadata.initDefault(Metadata.Kind.Index);
        meta.count = kvs.items.len;
        meta.firstkey = kvs.items[0].key;
        meta.lastkey = kvs.items[kvs.items.len - 1].key;
        meta.size = meta.sizeInBytes();
        meta.firstkeyoffset = meta.size.? + wal_read_result.keys_only_size + (@sizeOf(usize) * kvs.items.len);
        meta.lastkeyoffset =
            meta.size.? +
            wal_read_result.total_bytes -
            kvs.items[kvs.items.len - 1].sizeInBytes();

        const addr = try std.posix.mmap(null, stat.size, std.posix.PROT.READ |
            std.posix.PROT.WRITE, std.posix.MAP{ .TYPE = .SHARED }, file.handle, 0);

        return TempSSTable{
            .meta = meta,
            .addr = addr,
            .keys_size = wal_read_result.keys_only_size,
            .data_size = wal_read_result.total_bytes,
            .kvs = kvs,
            .alloc = arena_alloc,
            .arena = arena,
        };
    }

    pub fn deinit(self: TempSSTable) void {
        self.arena.deinit();
        std.posix.munmap(self.addr);
    }

    /// Writes the contents of the SSTable to a buffer. Nothing to free after.
    /// The allocator is needed to create an intermediate array of offses values,
    /// only known at runtime after the keys have been serialized, but it is freed after the
    /// serialization.
    ///
    /// The format of the SSTable is as follows:
    ///
    /// | ------------------|-----------|-----------------------------------------------------------------|
    /// | Name              | Size      | Description                                                     |
    /// | ------------------|-----------|-----------------------------------------------------------------|
    /// | checksum (u32)    | 4 bytes   | CRC32 checksum of the metadata + [keys+offsets] + [keys+values] |
    /// | metadata          | n bytes   | Metadata of the file                                            |
    /// | [keys+offsets]    | n bytes   | Keys and their offsets in the file                              |
    /// | [keys+values]     | n bytes   | Keys and their values in the file                               |
    /// | ------------------|-----------|-----------------------------------------------------------------|
    ///
    pub fn serialize(self: TempSSTable, buf: []u8, alloc: Allocator) !usize {
        var writer = ReaderWriterSeeker.initBuffer(buf);

        // leave space for the checksum
        try writer.writeU32(0);

        // write metadata
        try self.meta.serialize(&writer);

        var offset = writer.getPos();
        const metadata_end_position = offset;

        // advance the offset to the end of the keys+offset block
        offset += self.kvs.items.len * @sizeOf(usize);

        const offsets = alloc.alloc(usize, self.indices.items.len);
        defer alloc.free(offsets);

        // write keys AND values now
        for (self.kvs.items, 0..) |kv, i| {
            offsets[i] = offset;
            const written = try kv.serialize(buf[offset..]);
            offset += written;
        }

        const end_position = try writer.getPos();

        try writer.seekTo(metadata_end_position);

        // write keys and offsets only, no values
        const st = StringWriter(u32).init();
        for (offsets, 0..) |key_offset, i| {
            st.write(self.kvs[i].firstkey);
            st.writeIntLittle(usize, key_offset);
        }

        // calculate the and write the checksum
        const checksum = std.hash.Crc32.hash(buf[@sizeOf(u32)..end_position]);
        try writer.seekTo(0);
        try writer.writeU32(checksum);
    }
};

/// SSTable is a sorted key-value store that is stored on disk. They are created from a TempSSTable
/// file, which is a temporary file created from a WAL file. SSTables are immutable and are read-only.
///
/// The format of the SSTable is as follows:
///
/// | ------------------|-----------|---------------------------------------------------------------------|
/// | Name              | Size      | Description                                                         |
/// | ------------------|-----------|---------------------------------------------------------------------|
/// | checksum (u32)    | 4 bytes   | CRC32 checksum of the metadata + [ keys+offsets ] + [ keys+values ] |
/// | metadata          | n bytes   | Metadata of the file                                                |
/// | [indices]         | n bytes   | Pairs of key+offsets                                                |
/// | data              | n bytes   | Keys and their values                                               |
/// | ------------------|-----------|---------------------------------------------------------------------|
///
///
/// Fields on the SSTable struct:
/// | ------------------|-----------|-------------------------------------------------------------------------------|
/// | Name              | Size      | Description                                                                   |
/// | ------------------|-----------|-------------------------------------------------------------------------------|
/// | mmaped_file       | n bytes   | The file is mmaped into memory                                                |
/// | file              | n bytes   | The file is kept open for the duration of the SSTable                         |
/// | reader            | n bytes   | The reader contains high level methods to read the file easily                |
/// | indices           | n bytes   | The indices are used to find the offset of the key in the file                |
/// | allocator         | n bytes   | The allocator is used to allocate memory for the indices                      |
/// | arena             | n bytes   | The arena is used to deallocate all the memory allocated by `alloc` above     |
/// | ------------------|-----------|-------------------------------------------------------------------------------|
///
pub const SSTable = struct {
    const log = std.log.scoped(.SSTable);
    const EmptyFileError = error{EmptyFile};

    meta: Metadata,
    checksum: u32,

    mmap_file: []align(std.mem.page_size) u8,
    reader: ReaderWriterSeeker,
    indices: ArrayList(IndexEntry),

    alloc: Allocator,
    arena: std.heap.ArenaAllocator,

    // deinit frees the mmaped file and closes the file handle
    pub fn deinit(self: *SSTable) void {
        std.posix.munmap(self.mmap_file);

        self.reader.file.close();
        self.indices.deinit();
        self.arena.deinit();
    }

    pub fn read(file: fs.File, alloc: Allocator) !SSTable {
        const stat = try file.stat();
        if (stat.size == 0) {
            return EmptyFileError;
        }

        // mmap file
        const addr = try std.posix.mmap(null, stat.size, std.posix.PROT.READ |
            std.posix.PROT.WRITE, std.posix.MAP{ .TYPE = .SHARED }, file.handle, 0);
        errdefer std.posix.munmap(addr);

        var reader = ReaderWriterSeeker.initFile(file);

        // read checksum and validate it
        const checksum = try reader.readIntLittle(u32);
        const computed_checksum = std.hash.Crc32.hash(addr[@sizeOf(u32)..]);
        if (checksum != computed_checksum) {
            return error{ChecksumMismatch};
        }

        // read metadata
        const meta = try Metadata.read(&reader, alloc);
        errdefer meta.deinit();

        // read indices
        var indices = try alloc.alloc(IndexEntry, meta.count);
        errdefer alloc.free(indices);

        for (0..meta.count) |i| {
            var index = try IndexEntry.read(&reader, alloc);
            errdefer index.deinit();
            indices[i] = index;
        }

        return SSTable{
            .meta = meta,
            .addr = addr,
            .reader = reader,
            .indices = ArrayList(IndexEntry).fromOwnedSlice(alloc, indices),
        };
    }

    pub fn isBetween(self: SSTable, d: Kv) ?IndexEntry {
        for (self.indices.items) |index| {
            if (d.compare(index.lastkey) or
                index.firstkey.compare(d) or
                index.firstkey.equals(d) or
                index.lastkey.equals(d))
            {
                return index;
            }
        }

        return null;
    }

    pub fn debug(self: SSTable) void {
        std.debug.print("\n___________\nSTART TableReader\n", .{});
        self.meta.debug();
        std.debug.print("Indices\n", .{});
        for (self.indices.items) |index| {
            index.debug();
        }
        std.debug.print("END Tablereader\n---------------\n", .{});
    }
};

fn sort(list: []Kv) void {
    std.sort.insertion(Kv, list, {}, Kv.sortFn);
}

pub const IndexEntry = struct {
    offset: usize = 0,
    firstkey: []const u8,
};

test "TempSSTable" {
    std.testing.log_level = .debug;
    const alloc = std.testing.allocator;

    const wal_file = try std.fs.cwd().openFile("testing/testdata/0001.wal", .{ .mode = .read_write });
    // create a sstable from the wal
    var sstable = try TempSSTable.newFromWal(wal_file, alloc);
    defer sstable.deinit();

    // create an empty temp file of WAL_SIZE size
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_file = try tmp_dir.dir.createFile("sstable.zig", .{ .read = true });
    _ = try tmp_file.setEndPos(Wal.WAL_SIZE);
    try tmp_file.seekTo(0);

    try std.testing.expectEqualStrings("hello", sstable.meta.firstkey);
    try std.testing.expectEqualStrings("mario", sstable.meta.lastkey);

    try std.testing.expectEqual(sstable.meta.size.? +
        sstable.kvs.items[0].keySize() + @sizeOf(usize) +
        sstable.kvs.items[1].keySize() + @sizeOf(usize), sstable.meta.firstkeyoffset);
    try std.testing.expectEqual(sstable.kvs.items[0].sizeInBytes(), sstable.meta.lastkeyoffset);
}
