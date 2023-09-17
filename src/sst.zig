const std = @import("std");
const wal_ns = @import("./memory_wal.zig");
const pointer = @import("./pointer.zig");
const record_ns = @import("./record.zig");
const dm_ns = @import("./disk_manager.zig");
const HeaderPkg = @import("./header.zig");

const Pointer = pointer.Pointer;
const Wal = wal_ns.MemoryWal;
const Record = record_ns.Record;
const DiskManager = dm_ns.DiskManager;
const Header = HeaderPkg.Header;
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
pub const Sst = struct {
        const Self = @This();

        header: Header,
        allocator: *std.mem.Allocator,

        mem: []*Record,
        pointers: []*Pointer,

        // first_pointer: *Pointer,
        // last_pointer: *Pointer,

        pub fn init(f: *std.fs.File, allocator: *std.mem.Allocator) !Self {
            var stat = try f.stat();

            var data = try allocator.alloc(u8, stat.size);
            defer allocator.free(data); //delete

            const bytes_read = try f.readAll(data);
            _ = bytes_read;

            const h = try Header.fromBytes(data);
            

            return Self{
                .header = h,
                .mem = try allocator.alloc(*Record, h.records_size),
                .pointers = try allocator.alloc(*Pointer, h.records_size),
                .allocator = allocator,
            };
        }

        pub fn mytest(path: []const u8) !void {
            var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});
            defer f.close();
            var stat = try f.stat();
            std.debug.print("Size: {}\n", .{stat.size});
        }

        pub fn fromBytes(self: *Self, fileBytes: []u8) !usize {
            std.debug.print("{}\n", .{self.header});
            var offset = HeaderPkg.headerSize();

            // Read records
            while (offset < HeaderPkg.headerSize() + self.header.records_size) {
                var r = Record.fromBytes(fileBytes[offset..], self.allocator) orelse return offset;
                std.debug.print("Record: key: {s}, value: {s}\n", .{ r.key, r.value });
                self.mem[self.current_mem_index] = r;
                self.current_mem_index += 1;
                offset += r.bytesLen();
                std.debug.print("Offset: {d}, len: {d}\n", .{ offset, fileBytes.len });
            }

            //Read pointers?
            var current_pointer_index: usize = 0;
            while(offset < fileBytes.len) {
                var p = try try Pointer.fromBytes(fileBytes[offset..]);
                self.pointers[current_pointer_index] = p;
                offset += p.bytesLen();
            }

            return offset;
        }
};

// test "sst.fromBytes" {
//     var allocator = std.testing.allocator;
//     const WalType = @import("./memory_wal.zig").MemoryWal(4098);

//     var wal = try WalType.init(&allocator);

//     try wal.append(try Record.init("hell0", "world1", Op.Update, &allocator));
//     try wal.append(try Record.init("hell1", "world2", Op.Delete, &allocator));
//     try wal.append(try Record.init("hell2", "world3", Op.Delete, &allocator));
//     wal.sort();

//     var buf = try allocator.alloc(u8, 4096);
//     defer allocator.free(buf);

//     var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});
//     defer f.close();

//     const bytes_written = try wal.toBytes(buf);
//     std.debug.print("{} bytes written\n{s}\n", .{ bytes_written, buf[0..bytes_written] });

//     wal.deinit_cascade();

//     const sst = Sst.init(null, &allocator)

//     const bytes_read = try wal.fromBytes(buf[0..bytes_written]);
//     std.debug.print("{} bytes read\n", .{bytes_read});
// }