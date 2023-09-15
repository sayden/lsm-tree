const std = @import("std");
const wal_ns = @import("./memory_wal.zig");
const pointer = @import("./pointer.zig");
const record_ns = @import("./record.zig");
const dm_ns = @import("./disk_manager.zig");
const header = @import("./header.zig");

const Pointer = pointer.Pointer;
const Wal = wal_ns.MemoryWal;
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
        // first_pointer: *Pointer,
        // last_pointer: *Pointer,

        pub fn init(f: *std.fs.File, allocator: *std.mem.Allocator) !Self {
            var stat = try f.stat();

            var data = try allocator.alloc(u8, stat.size);
            defer allocator.free(data); //delete

            const bytes_read = try f.readAll(data);
            _ = bytes_read;

            const h = try Header.fromBytes(data);
            var wal = try WalType.init(allocator);
            defer wal.deinit(); //delete
            return Self{
                .wal = wal,
                .file = f,
                .header = h,
            };
        }

        pub fn mytest(path: []const u8) !void {
            var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});
            defer f.close();
            var stat = try f.stat();
            std.debug.print("Size: {}\n", .{stat.size});
        }
    };
}

test "sdfasdf" {
    var allocator = std.testing.allocator;
    var f = try std.fs.openFileAbsolute("/tmp/hello", std.fs.File.OpenFlags{});
    defer f.close();

    const WalType = Wal(100);
    _ = try Sst(WalType).init(&f, &allocator);
}
