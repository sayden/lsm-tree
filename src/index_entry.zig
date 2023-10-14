const std = @import("std");
const Allocator = std.mem.Allocator;

const Data = @import("./data.zig").Data;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

pub const IndexEntry = struct {
    const log = std.log.scoped(.IndexItem);

    offset: usize = 0,
    firstkey: Data,
    lastkey: Data,

    pub fn deinit(self: IndexEntry) void {
        self.firstkey.deinit();
        self.lastkey.deinit();
    }

    /// Serializes the contents:
    ///
    /// Offset:     usize
    /// FirstKey:   Depends, either a pair of (n:u16,[0..n]u8) of a Kv type of Data or a i128 of a timestamp from a Column type of Data
    /// LastKey:    Like FirstKey
    pub fn write(self: IndexEntry, writer: *ReaderWriterSeeker) !void {
        try writer.writeIntNative(@TypeOf(self.offset), self.offset);
        try self.firstkey.writeIndexingValue(writer);
        return self.lastkey.writeIndexingValue(writer);
    }

    /// Deserializes the contents, the expected format is the following
    ///
    /// Offset:     usize
    /// FirstKey:   Depends, either a pair of (n:u16,[0..n]u8) of a Kv type of Data or a i128 of a timestamp from a Column type of Data
    /// LastKey:    Like FirstKey
    pub fn read(reader: *ReaderWriterSeeker, comptime T: type, alloc: Allocator) !IndexEntry {
        const offset = try reader.readIntNative(usize);
        const firstkey = try T.readIndexingValue(reader, alloc);
        const lastkey = try T.readIndexingValue(reader, alloc);

        return IndexEntry{
            .offset = offset,
            .firstkey = firstkey,
            .lastkey = lastkey,
        };
    }

    pub fn debug(i: IndexEntry) void {
        log.debug("Offset:\t{}", .{i.offset});
        i.firstkey.debug(log);
        i.lastkey.debug(log);
    }
};
