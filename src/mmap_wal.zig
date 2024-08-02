const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;

const FileManager = @import("./file_manager.zig").FileManager;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Metadata = @import("./metadata.zig").Metadata;
const Iterator = @import("./iterator.zig").Iterator;
const KvNs = @import("./kv.zig");
const KV = KvNs.Kv;
const File = std.fs.File;

const log = std.log.scoped(.WAL);

/// The Write Ahead Log (WAL) is a file that stores all the operations that are performed on the database.
/// The format is an array as follows:
/// - 4 bytes: checksum of the KV entry
/// - 8 bytes: lentgh n of the key
/// - n bytes: key
/// - 8 bytes: lentgh n of the value
/// - n bytes: value
pub const Wal = struct {
    const Self = @This();

    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;
    pub const WAL_SIZE = 1024 * 1024 * 5; // 5KB

    mmap: []align(std.mem.page_size) u8,
    offset: usize,

    meta: Metadata,

    // The wal uses an arena allocator to store the data
    pub fn new(empty_file: File) !Wal {
        // mmap the file
        const fd = empty_file.handle;
        const addr = try std.posix.mmap(null, WAL_SIZE, std.posix.PROT.READ | std.posix.PROT.WRITE, std.posix.MAP{ .TYPE = .SHARED }, fd, 0);

        return Wal{ .mmap = addr, .offset = 0, .meta = Metadata.initDefault(Metadata.Kind.Wal) };
    }

    pub fn deinit(self: Wal) void {
        std.posix.munmap(self.mmap);
    }

    /// write directly into disk. The format is as follows:
    /// - 4 bytes: checksum of the KV entry
    /// - n bytes: KV entry as serialized
    pub fn append(self: *Wal, kv: KV) !void {
        // FIXME: Is there any way to not rely in a fixed value here?
        var buf = self.mmap[self.offset .. self.offset + 1024];

        const checksumLen = @sizeOf(u32);
        const kvLen = kv.sizeInBytes();
        // FIXME: Check if the offset won't move over the WAL_SIZE

        const checksumSlice = buf[0..checksumLen];

        const kvSlice = buf[checksumLen .. checksumLen + kvLen];
        _ = try kv.serialize(kvSlice);

        // calculate and write checksum at the beginning of the buffer
        const checksum = std.hash.Crc32.hash(kvSlice);
        std.mem.writeInt(u32, checksumSlice, checksum, std.builtin.Endian.little);
        const written = checksumLen + kvLen;

        self.offset += written;

        // update the metadata
        self.meta.count += 1;
        self.meta.updateFirstAndLastKey(kv);
    }

    /// Read the WAL file and return an array of KV entries
    /// kvs: the array of KV entries
    /// total_bytes: the total size of the keys and values, not including checksums, metadata or keys
    /// keys_only_size: the size of the keys only, not including checksums, metadata or values
    pub const ReadResult = struct {
        kvs: ArrayList(KV),
        total_bytes: usize,
        keys_only_size: usize,
    };

    /// When reading a WAL file, just an array of KV entries is expected. No metadata or other information is stored.
    /// Extra data is required to build the SSTable like the keys size
    pub fn read(file: File, alloc: Allocator) !ReadResult {
        const stat = try file.stat();

        const addr = try std.posix.mmap(null, stat.size, std.posix.PROT.READ | std.posix.PROT.WRITE, std.posix.MAP{ .TYPE = .SHARED }, file.handle, 0);
        defer std.posix.munmap(addr);

        var reader = ReaderWriterSeeker.initReadMmap(addr);

        // keep track of the bytes read in the file
        // generally speaking, EOF shoult trigger the end of the while loop
        // but just in case
        var bytes_read: usize = 0;

        var kvs = ArrayList(KV).init(alloc);
        errdefer kvs.deinit();

        // this is used by the sstable to know the offset where
        // valeus start and how big the file will be
        var total_bytes: usize = 0;
        var keys_only_size: usize = 0;

        while (bytes_read < stat.size) {
            // read checksum
            const checksum = try reader.readIntLittle(u32);

            // get the position of the reader, to calculate checksum later
            const pos = try reader.getPos();

            // read kv
            const kv = KV.read(&reader, alloc) catch |err| {
                if (err == error.EndOfStream) {
                    return ReadResult{ .kvs = kvs, .total_bytes = total_bytes, .keys_only_size = keys_only_size };
                }
                return err;
            };

            // sum the size of the kv
            total_bytes += kv.sizeInBytes();

            // sum the size of the key only (keylength + key only)
            keys_only_size += kv.keySize();

            // calculate checksum
            const new_checksum = std.hash.Crc32.hash(addr[pos .. pos + kv.sizeInBytes()]);
            if (new_checksum != checksum) {
                return ReadResult{ .kvs = kvs, .total_bytes = total_bytes, .keys_only_size = keys_only_size };
            }

            try kvs.append(kv);
            bytes_read += kv.sizeInBytes();
        }

        return ReadResult{ .kvs = kvs, .total_bytes = total_bytes, .keys_only_size = keys_only_size };
    }
};

test "Wal" {
    std.testing.log_level = .debug;

    const Op = @import("./ops.zig").Op;

    const kv1 = KV.new("mario", "caster", Op.Upsert);
    const kv2 = KV.new("hello", "world", Op.Upsert);

    // create an empty temp file of WAL_SIZE size
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const tmp_file = try tmp_dir.dir.createFile("wal.zig", .{ .read = true });
    _ = try tmp_file.setEndPos(Wal.WAL_SIZE);
    try tmp_file.seekTo(0);

    // create a wal and write 2 kv on it
    var original_wal = try Wal.new(tmp_file);
    defer original_wal.deinit();
    _ = try original_wal.append(kv1);
    _ = try original_wal.append(kv2);

    // reset the file
    try tmp_file.seekTo(0);
    try tmp_file.sync();

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // read the file and check if the data is the same
    var wal_read_result = try Wal.read(tmp_file, alloc);
    defer wal_read_result.kvs.deinit();

    try std.testing.expectEqual(2, wal_read_result.kvs.items.len);
}
