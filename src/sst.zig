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
            while(offset < fileBytes.len) {
                var p = try Pointer.fromBytes(fileBytes[offset..]);
                _ = p;

            }

            return offset;
        }
};

test "sdfasdf" {
    var allocator = std.testing.allocator;
    var WalType = Wal(512);
    var wal = WalType.init(&allocator);
    
    try wal.appendKv("hell0", "world");
    try wal.appendKv("hell1", "world");
    try wal.appendKv("hell2", "world");
    try wal.appendKv("hell0", "world0");

    var f = try std.fs.openFileAbsolute("/tmp/hello", std.fs.File.OpenFlags{});
    defer f.close();

    _ = try Sst.init(&f, &allocator);
}
