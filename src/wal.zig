const std = @import("std");
const RecordNs = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = RecordNs.Record;
const RecordError = RecordNs.RecordError;
const HeaderNs = @import("./header.zig");
const Header = HeaderNs.Header;
const lsmtree = @import("./main.zig");
const Pointer = @import("./pointer.zig").Pointer;
const Strings = @import("./strings.zig");
const strcmp = Strings.strcmp;
const Math = std.math;
const IteratorNs = @import("./iterator.zig");
const Iterator = IteratorNs.Iterator;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const DiskManager = @import("./disk_manager.zig").DiskManager;
const BytesIterator = IteratorNs.BytesIterator;
const DebugNs = @import("./debug.zig");

const WalMemoryNs = @import("./wal_memory.zig");
const WalLevel1 = WalMemoryNs.WalLevel1;
const MemoryWalLevel = WalMemoryNs.WalLevel;
const WalRuntime = WalMemoryNs.RuntimeMemWal;
const initial_wal_size = WalMemoryNs.initial_wal_size;

const WalFileNs = @import("./wal_file.zig");
const WalFile = WalFileNs.FileWal;

const println = DebugNs.println;
const prints = DebugNs.prints;
const print = std.debug.print;

pub const Error = error{
    MaxSizeReached,
    CantCreateRecord,
    EmptyWal,
} || RecordError || std.mem.Allocator.Error;

const Kind = enum {
    ComptimeMemory,
    RuntimeMemory,
    File,
};

const Wal = union {
    comptimeMemoryWalLevel: MemoryWalLevel,
    runtimeMemoryWal: WalRuntime,
    file: WalFile,
};

pub fn newWal(k: Kind, alloc: std.mem.Allocator, disk_manager: ?DiskManager) !Wal {
    return switch (k) {
        .ComptimeMemory => {
            return Wal{ .comptimeMemoryWalLevel = MemoryWalLevel{ .level_1 = WalLevel1.init(alloc) } };
        },
        .RuntimeMemory => {
            return Wal{ .runtimeMemoryWal = WalRuntime.init(initial_wal_size, alloc) };
        },
        .File => {
            if (disk_manager) |dm| {
                return Wal{ .file = WalFile.init(initial_wal_size, dm, alloc) };
            } else {
                return error{NullDiskManager};
            }
        },
    };
}

/// writes into provided file the contents of the sst. Including pointers
/// and the header. The allocator is required as a temporary buffer for
/// data but it's freed inside the function
///
/// Format is as following:
/// Header
/// Pointer
/// Pointer
/// Pointer
/// ...
/// Record
/// Record
/// Record
/// ...
/// EOF
pub fn persist(records: []*Record, header: *Header, ws: *ReaderWriterSeeker) !usize {
    if (header.total_records == 0) {
        return Error.EmptyWal;
    }

    std.sort.insertion(*Record, records[0..header.total_records], {}, lexicographical_compare);

    // Write first and last pointer in the header. We cannot write this before
    // because we need to know their offsets after writing. It can be calculated
    // now, but maybe not later if compression comes in place
    header.first_pointer_offset = HeaderNs.headerSize();
    header.last_pointer_offset = header.pointers_size + HeaderNs.headerSize() - records[header.total_records - 1].pointerSize();

    try ws.seekTo(HeaderNs.headerSize());

    // Move offset after header, which will be written later
    var record_offset = HeaderNs.headerSize() + header.pointers_size;

    var written: usize = 0;
    // Write pointer
    for (0..header.total_records) |i| {
        records[i].pointer.offset = record_offset;
        written += try records[i].writePointer(ws);

        record_offset += records[i].valueLen();
    }

    // Write records
    for (0..header.total_records) |i| {
        // records[i].pointer.offset = record_offset;
        written += try records[i].write(ws);
    }

    // Write the header
    try ws.seekTo(0);
    written += try header.write(ws);

    return written;
}

pub fn appendKv(ctx: anytype, k: []const u8, v: []const u8, alloc: std.mem.Allocator) !void {
    var r = try Record.init(k, v, Op.Create, alloc);
    errdefer r.deinit();
    return ctx.appendOwn(r);
}

/// Add a new record to the in memory WAL
pub fn preAppend(ctx: anytype, r: *Record) Error!void {
    const record_size: usize = r.len();

    // Check if there's available space in the WAL
    if (ctx.getWalSize() + record_size >= ctx.max_size) {
        return Error.MaxSizeReached;
    }
}

/// Add a new record to the in memory WAL
pub fn postAppend(header: *Header, r: *Record) void {
    header.total_records += 1;
    header.records_size += r.valueLen();
    header.pointers_size += r.pointerSize();
}

pub fn find(iter: anytype, key_to_find: []const u8, alloc: std.mem.Allocator) !?*Record {
    while (iter.next()) |r| {
        if (std.mem.eql(u8, r.pointer.key, key_to_find)) {
            return r.clone(alloc);
        }
    }

    return null;
}

pub fn lexicographical_compare(_: void, lhs: *Record, rhs: *Record) bool {
    const res = strcmp(lhs.pointer.key, rhs.pointer.key);
    return res.compare(Math.CompareOperator.lte);
}
