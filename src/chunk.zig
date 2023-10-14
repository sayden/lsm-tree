const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;

const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Data = @import("./data.zig").Data;
const Metadata = @import("./metadata.zig").Metadata;
const Iterator = @import("./iterator.zig").Iterator;

pub const Error = error{UnknownChunkSize};

pub const Chunk = struct {
    // Use OS page size to optimize mmap usage
    const MAX_SIZE = std.mem.page_size;
    const MAX_VALUES = 10;

    meta: Metadata,

    mem: ArrayList(Data),
    size: ?usize = 0,

    alloc: Allocator,

    pub fn init(comptime T: type, alloc: Allocator) Chunk {
        return Chunk{ .mem = ArrayList(Data).init(alloc), .alloc = alloc, .meta = Metadata.initDefault(Metadata.Kind.Chunk, T) };
    }

    pub fn deinit(self: Chunk) void {
        for (self.mem.items) |data| {
            data.deinit();
        }

        self.mem.deinit();
        self.meta.deinit();
    }

    pub fn append(self: *Chunk, d: Data) !enum { ChunkFull, Ok } {
        try self.mem.append(d);
        self.meta.count += 1;

        self.meta.updateSelfFirstAndLastKey(d);

        return .Ok;
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

        var iter = self.getIterator();
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

    fn getIterator(self: Chunk) Iterator(Data) {
        return Iterator(Data).init(self.mem.items);
    }

    fn sort(self: Chunk) void {
        std.sort.insertion(Data, self.mem.items, {}, Data.sortFn);
    }
};

pub fn testChunk(alloc: Allocator) !Chunk {
    const Column = @import("./columnar.zig").Column;
    const Op = @import("./ops.zig").Op;

    var col1 = Column.new(5678, 123.2, Op.Upsert);
    var col2 = Column.new(1234, 200.2, Op.Upsert);

    var data = Data.new(col1);
    var data2 = Data.new(col2);

    // Chunk
    var original_chunk = Chunk.init(Column, alloc);
    // defer original_chunk.deinit();
    _ = try original_chunk.append(data);
    _ = try original_chunk.append(data2);

    return original_chunk;
}
test "Chunk" {
    const Column = @import("./columnar.zig").Column;

    std.testing.log_level = .debug;

    // Setup
    var alloc = std.testing.allocator;
    var original_chunk = try testChunk(alloc);
    defer original_chunk.deinit();

    try std.testing.expectEqual(@as(i128, 1234), original_chunk.mem.getLast().col.ts);
    try std.testing.expectEqual(@as(f64, 200.2), original_chunk.mem.getLast().col.val);

    // Chunk write
    var buf: [256]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);
    _ = try original_chunk.write(&rws, true);
    try rws.seekTo(0);

    // Chunk read
    var chunk_read = try Chunk.read(&rws, Column, alloc);
    defer chunk_read.deinit();

    try std.testing.expectEqual(original_chunk.mem.items.len, chunk_read.mem.items.len);
    try std.testing.expectEqual(original_chunk.mem.getLast().col.val, chunk_read.mem.getLast().col.val);
}
