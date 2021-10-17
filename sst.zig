const std = @import("std");
const pointer = @import("./pointer.zig");
const Pointer = pointer.Pointer;
const wal_ns = @import("./wal.zig");
const Wal = wal_ns.Wal;
const record_ns = @import("./record.zig");
const Record = record_ns.Record;
const dm_ns = @import("./disk_manager.zig");
const DiskManager = dm_ns.DiskManager;
const header = @import("./header.zig");
const Header = header.Header;

/// A SST or Sorted String Table is created from a Wal object. The structure is the following:
/// 
/// HEADER:
/// 
/// DATA CHUNK:
/// Contiguous array of records
/// 
/// KEYS CHUNK
/// Contiguous array of keys only with pointers to values in the data chunk
pub fn Sst(comptime WalType: type) type {
    return struct {
        const Self = @This();

        header: Header,
        head_offset: usize = header.headerSize(),
        tail_offset: usize,
        file: *std.fs.File,
        wal: *WalType,

        pub fn init(w: *WalType, f: *std.fs.File) Self {
            var h = Header.init(WalType, w);
            return Self{
                .tail_offset = h.pointers_byte_offset,
                .wal = w,
                .file = f,
                .header = h,
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

            // header finished TODO remove hardcoding on the next line
            var buf = try allocator.alloc(u8, 4096);
            defer allocator.free(buf);

            // Write the data and pointers chunks
            while (iter.next()) |record| {
                // record
                total_record_bytes = try record.bytes(buf);
                written += try self.file.pwrite(buf[0..total_record_bytes], self.head_offset);
                self.head_offset += total_record_bytes;

                // pointer
                total_pointer_bytes = record.getPointerInBytes(buf[0..], self.head_offset - total_record_bytes);
                written += try self.file.pwrite(buf[0..total_pointer_bytes], self.tail_offset);
                self.tail_offset += total_pointer_bytes;
            }

            // update last unknown data on the header
            self.header.last_key_offset = self.tail_offset - total_pointer_bytes;

            // Write the header
            written += try self.writeHeader();

            return written;
        }

        fn writeHeader(self: *Self) !usize {
            var header_buf: [header.headerSize()]u8 = undefined;
            try header.toBytes(&self.header, &header_buf);
            return try self.file.pwrite(&header_buf, 0);
        }
    };
}

test "sst.persist" {
    var allocator = std.testing.allocator;
    const RecordType = Record(u32, u64);
    const WalType = Wal(512, RecordType);

    var wal = try WalType.init(allocator);
    defer wal.deinit_cascade();

    var r = try Record(u32, u64).init("hell0", "world1", allocator);
    try wal.add_record(r);
    try wal.add_record(try Record(u32, u64).init("hell1", "world2", allocator));
    try wal.add_record(try Record(u32, u64).init("hell2", "world3", allocator));
    wal.sort();
    std.debug.print("\nrecord size: {d}\n", .{r.size()});

    std.debug.print("wal size in bytes {d}\n", .{wal.current_size});
    std.debug.print("wal total records {d}\n", .{wal.total_records});

    var dm = DiskManager(WalType, RecordType).init("/tmp");
    var file = try dm.new_sst_file(allocator);

    const SstType = Sst(WalType);
    var sst = SstType.init(wal, &file);
    std.debug.print("Header length {d}\n",.{header.headerSize()});
    const bytes = sst.persist(allocator);

    std.debug.print("{d} bytes written into sst file\n", .{bytes});

    // Close file and open it again with write permissions
    file.close();

    file = try std.fs.openFileAbsolute("/tmp/1.sst", std.fs.File.OpenFlags{});
    defer file.close();

    var headerBuf: [header.headerSize()]u8 = undefined;
    _ = try file.read(&headerBuf);
    defer std.fs.deleteFileAbsolute("/tmp/1.sst") catch unreachable;

    var magic = std.mem.readIntSliceLittle(u32, headerBuf[0..4]);
    std.debug.print("magic number: {d}\n", .{magic});

    var content = std.mem.readIntSliceLittle(usize, headerBuf[4..12]);
    std.debug.print("first key in data: {d}\n", .{content});

    content = std.mem.readIntSliceLittle(usize, headerBuf[12..20]);
    std.debug.print("last key in data: {d}\n", .{content});

    content = std.mem.readIntSliceLittle(usize, headerBuf[20..28]);
    std.debug.print("beginning of keys: {d}\n", .{content});

    content = std.mem.readIntSliceLittle(usize, headerBuf[28..36]);
    std.debug.print("total records: {d}\n", .{content});

    var i: usize = sst.wal.total_records;

    var file_bytes: [512]u8 = undefined;
    try file.seekTo(sst.header.first_key_offset);
    _ = try file.readAll(&file_bytes);

    var offset: usize = 0;
    var p: Pointer(u32) = undefined;
    while (i > 0) : (i -= 1) {
        p = pointer.readPointer(u32, file_bytes[offset..]);
        offset += p.bytesLength();
        std.debug.print("key: {s}, offset: {d}\n", .{p.key,p.byte_offset});
    }

    //read value of last record
    try file.seekTo(0);
    _ = try file.readAll(file_bytes[0..]);

    var r1 = RecordType.read_record(file_bytes[p.byte_offset..], std.testing.allocator).?;
    defer r1.deinit();

    std.debug.print("last pointer value = {s}\n", .{r1.value});
}
