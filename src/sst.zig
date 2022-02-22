const std = @import("std");
const wal_ns = @import("./wal.zig");
const pointer = @import("./pointer.zig");
const record_ns = @import("./record.zig");
const dm_ns = @import("./disk_manager.zig");
const header = @import("./header.zig");

const serialize = @import("serialize");

const Pointer = pointer.Pointer;
const Wal = wal_ns.Wal;
const Record = record_ns.Record;
const DiskManager = dm_ns.DiskManager;
const Header = header.Header;
const Op = @import("./ops.zig").Op;

/// A SST or Sorted String Table is created from a Wal object. The structure is the following:
/// 
/// HEADER: Check the header.zig file for details
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
        file: *std.fs.File,
        wal: *WalType,

        pub fn init(w: *WalType, f: *std.fs.File) Self {
            var h = Header.init(WalType, w);
            return Self{
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

            // TODO remove hardcoding on the next line
            var buf = try allocator.alloc(u8, 4096);
            defer allocator.free(buf);

            var head_offset: usize = header.headerSize();
            var tail_offset: usize = self.wal.current_size;
            var pointer_total_bytes: usize = 0;
            // Write the data and pointers chunks
            while (iter.next()) |record| {
                // record
                var record_total_bytes = try serialize.record.toBytes(record, buf);
                written += try self.file.pwrite(buf[0..record_total_bytes], head_offset);
                head_offset += record_total_bytes;

                // pointer
                var pointer_ = pointer.Pointer{
                    .op = record.op,
                    .key = record.key,
                    .byte_offset = tail_offset,
                };
                pointer_total_bytes = try serialize.pointer.toBytes(pointer_, buf);
                written += try self.file.pwrite(buf[0..pointer_total_bytes], tail_offset);
                tail_offset += pointer_total_bytes;
            }

            // update last unknown data on the header
            self.header.last_key_offset = tail_offset - pointer_total_bytes;

            // Write the header
            written += try self.writeHeader();

            return written;
        }

        fn writeHeader(self: *Self) !usize {
            var header_buf: [header.headerSize()]u8 = undefined;
            _ = try serialize.header.toBytes(&self.header, &header_buf);
            return try self.file.pwrite(&header_buf, 0);
        }
    };
}

test "sst.persist" {
    var allocator = std.testing.allocator;
    const WalType = Wal(512);

    var wal = try WalType.init(&allocator);
    defer wal.deinit_cascade();

    var r = try Record.init("hell0", "world1", Op.Update, &allocator);
    try wal.add_record(r);
    try wal.add_record(try Record.init("hell1", "world2", Op.Delete, &allocator));
    try wal.add_record(try Record.init("hell2", "world3", Op.Delete, &allocator));
    wal.sort();
    try std.testing.expectEqual(@as(usize, 22), r.bytesLen());
    std.debug.print("\nrecord size: {d}\n", .{r.bytesLen()});

    std.debug.print("wal size in bytes {d}\n", .{wal.current_size});
    std.debug.print("wal total records {d}\n", .{wal.total_records});

    var dm = try DiskManager(WalType).init("/tmp");
    var file = try dm.new_sst_file(&allocator);

    const SstType = Sst(WalType);
    var sst = SstType.init(wal, &file);
    std.debug.print("Header length {d}\n", .{header.headerSize()});
    const bytes = sst.persist(&allocator);

    std.debug.print("{d} bytes written into sst file\n", .{bytes});

    // Close file and open it again with write permissions
    file.close();

    file = try std.fs.openFileAbsolute("/tmp/1.sst", std.fs.File.OpenFlags{});
    defer file.close();

    var headerBuf: [header.headerSize()]u8 = undefined;
    _ = try file.read(&headerBuf);
    defer std.fs.deleteFileAbsolute("/tmp/1.sst") catch unreachable;

    // var i: usize = sst.wal.total_records;

    // var file_bytes: [512]u8 = undefined;
    // try file.seekTo(sst.header.first_key_offset);
    // _ = try file.readAll(&file_bytes);
}
