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
        first_pointer: *Pointer,
        last_pointer: *Pointer,

        pub fn init(w: *WalType, f: *std.fs.File) Self {
            // Read the file

            var h = Header.init(WalType, w);
            return Self{
                .wal = w,
                .file = f,
                .header = h,
            };
        }
    };
}
