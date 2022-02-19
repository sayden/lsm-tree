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
                // TODO Double check this line and the fix after it: `total_record_bytes = try record.len(buf);`
                total_record_bytes = record.len();
                written += try self.file.pwrite(buf[0..total_record_bytes], self.head_offset);
                self.head_offset += total_record_bytes;

                // pointer
                total_pointer_bytes = try serialize.record.toBytes(record, buf[0..]);
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

// test "sst.persist" {
//     var allocator = std.testing.allocator;
//     const WalType = Wal(512);

//     var wal = try WalType.init(&allocator);
//     defer wal.deinit_cascade();

//     var r = try Record.init("hell0", "world1", Op.Update, &allocator);
//     try wal.add_record(r);
//     try wal.add_record(try Record.init("hell1", "world2", Op.Delete, &allocator));
//     try wal.add_record(try Record.init("hell2", "world3", Op.Delete, &allocator));
//     wal.sort();
//     try std.testing.expectEqual(@as(usize, 22), r.len());
//     std.debug.print("\nrecord size: {d}\n", .{r.len()});

//     std.debug.print("wal size in bytes {d}\n", .{wal.current_size});
//     std.debug.print("wal total records {d}\n", .{wal.total_records});

//     var dm = try DiskManager(WalType).init("/tmp");
//     var file = try dm.new_sst_file(&allocator);

//     const SstType = Sst(WalType);
//     var sst = SstType.init(wal, &file);
//     std.debug.print("Header length {d}\n", .{header.headerSize()});
//     const bytes = sst.persist(&allocator);

//     std.debug.print("{d} bytes written into sst file\n", .{bytes});

//     // Close file and open it again with write permissions
//     file.close();

//     file = try std.fs.openFileAbsolute("/tmp/1.sst", std.fs.File.OpenFlags{});
//     defer file.close();

//     var headerBuf: [header.headerSize()]u8 = undefined;
//     _ = try file.read(&headerBuf);
//     defer std.fs.deleteFileAbsolute("/tmp/1.sst") catch unreachable;

//     var magic = std.mem.readIntSliceLittle(u8, headerBuf[0..1]);
//     std.debug.print("magic number: {d}\n", .{magic});

//     var content = std.mem.readIntSliceLittle(usize, headerBuf[1..9]);
//     std.debug.print("first key in data: {d}\n", .{content});

//     content = std.mem.readIntSliceLittle(usize, headerBuf[9..17]);
//     std.debug.print("last key in data: {d}\n", .{content});

//     content = std.mem.readIntSliceLittle(usize, headerBuf[17..25]);
//     std.debug.print("beginning of keys: {d}\n", .{content});

//     content = std.mem.readIntSliceLittle(usize, headerBuf[25..33]);
//     std.debug.print("total records: {d}\n", .{content});

//     var i: usize = sst.wal.total_records;

//     var file_bytes: [512]u8 = undefined;
//     try file.seekTo(sst.header.first_key_offset);
//     _ = try file.readAll(&file_bytes);

//     var offset: usize = 0;
//     var p: Pointer = undefined;
//     while (i > 0) : (i -= 1) {
//         std.debug.print("info: '{s}'\n", .{file_bytes[offset..]});
//         p = serialize.pointer.fromBytes(file_bytes[offset..]);
//         std.debug.print("key: {s}, offset: {d}\n", .{ p.key, p.byte_offset });
//         offset += p.bytesLen();
//     }

//     //read value of last record
//     try file.seekTo(0);
//     _ = try file.readAll(file_bytes[0..]);

//     var r1 = serialize.record.fromBytes(file_bytes[p.byte_offset..], &allocator).?;
//     defer r1.deinit();

//     std.debug.print("last pointer value = ({d}){s}\n", .{ r1.value.len, r1.value });
// }
