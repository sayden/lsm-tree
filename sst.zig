const std = @import("std");
const pointer = @import("./pointer.zig");
const Pointer = pointer.Pointer;
const wal_ns = @import("./wal.zig");
const Wal = wal_ns.Wal;
const record_ns = @import("./record.zig");
const Record = record_ns.Record;
const dm_ns = @import("./disk_manager.zig");
const DiskManager = dm_ns.DiskManager;

/// A SST or Sorted String Table is created from a Wal object. The structure is the following:
/// 
/// HEADER:
/// 8 bytes of magic number
/// 8 bytes with the offset of the first key in the "data" chunk.
/// 8 bytes with the offset of the last key in the "data" chunk.
/// 8 bytes with the offset of the beginning of the "keys" chunk.
/// 
/// DATA CHUNK:
/// Contiguous array of records
/// 
/// KEYS CHUNK
/// Contiguous array of keys only with pointers to values in the data chunk
pub fn Sst(comptime WalType: type) type {
    return struct {
        const header_size: usize = 8 + 8 + 8 + 8;
        const Self = @This();
        head_offset: usize = 0,
        tail_offset: usize,
        file: *std.fs.File,
        wal: *WalType,

        pub fn init(w: *WalType, f: *std.fs.File) Self {
            return Self{
                .tail_offset = header_size + w.current_size,
                .wal = w,
                .file = f,
            };
        }

        /// writes into provided file the contents of the sst. Including pointers
        /// and the header. The allocator is required as a temporary buffer for
        /// data but it's freed inside the function
        pub fn persist(self: *Self, allocator: *std.mem.Allocator) !usize {
            var iter = self.wal.iterator();

            var written: usize = 0;
            var total_record_bytes: usize = 0;
            var total_pointer_bytes: usize = 0;

            // Write the header
            var header_buf: [8]u8 = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
            var hbuf: []u8 = header_buf[0..];

            //write the magic number
            written += try self.file.pwrite(hbuf, 0);
            self.head_offset += 8;

            // write position of the first data chunk, just after header
            std.mem.writeIntSliceLittle(usize, hbuf, header_size);
            written += try self.file.pwrite(hbuf, self.head_offset);
            self.head_offset += 8;

            //position of the last data chunk. total_pointer_bytes has the size of the last record
            const last_record_offset = self.wal.current_size - total_pointer_bytes;
            std.mem.writeIntSliceLittle(usize, hbuf, last_record_offset);
            written += try self.file.pwrite(hbuf, self.head_offset);
            self.head_offset += 8;

            //offset position of the beginning of the keys
            std.mem.writeIntSliceLittle(usize, hbuf, self.wal.current_size + header_size);
            written += try self.file.pwrite(hbuf, self.head_offset);
            self.head_offset += 8;

            // header finished TODO remove hardcoding on the next line
            var buf = try allocator.alloc(u8, 4096);
            defer allocator.free(buf);

            // Write the data and pointers chunks
            while (iter.next()) |record| {
                // Write the record at the beginning of the file (head offset)
                total_record_bytes = try record.bytes(buf);
                written += try self.file.pwrite(buf[0..total_record_bytes], self.head_offset);
                self.head_offset += total_record_bytes;

                //Write pointer on the end of the file (tail offset)
                total_pointer_bytes = record.getPointerBytesForOffset(buf[0..]);
                written += try self.file.pwrite(buf[0..total_pointer_bytes], self.tail_offset);
                self.tail_offset += total_pointer_bytes;
            }

            return written;
        }
    };
}

test "sst.persist" {
    var allocator = std.testing.allocator;
    const RecordType = Record(u32, u64);
    const WalType = Wal(512, RecordType);

    var wal = try WalType.init(allocator);
    defer wal.deinit_cascade();

    var r = try Record(u32, u64).init("hell0", "world", allocator);
    try wal.add_record(r);
    std.debug.print("\nrecord size: {d}\n", .{r.size()});
    try wal.add_record(try Record(u32, u64).init("hell1", "world", allocator));
    try wal.add_record(try Record(u32, u64).init("hell2", "world", allocator));
    wal.sort();

    var dm = DiskManager(WalType, RecordType).init("/tmp");
    var file = try dm.new_sst_file(allocator);

    const SstType = Sst(WalType);
    var sst = SstType.init(wal, &file);
    const bytes = sst.persist(allocator);

    std.debug.print("{d} bytes written into sst file\n", .{bytes});

    // Close file and open it again with write permissions
    file.close();

    file = try std.fs.openFileAbsolute("/tmp/1.sst", std.fs.File.OpenFlags{});
    defer file.close();
    try std.fs.deleteFileAbsolute("/tmp/1.sst");

    var header: [SstType.header_size]u8 = undefined;
    _ = try file.read(&header);

    var content = std.mem.readIntSliceLittle(usize, header[0..8]);
    std.debug.print("header size: {d}\n", .{content});

    content = std.mem.readIntSliceLittle(usize, header[8..16]);
    std.debug.print("first key in data: {d}\n", .{content});

    content = std.mem.readIntSliceLittle(usize, header[16..24]);
    std.debug.print("last key in data: {d}\n", .{content});

    content = std.mem.readIntSliceLittle(usize, header[24..32]);
    std.debug.print("beginning of keys: {d}\n", .{content});
}
