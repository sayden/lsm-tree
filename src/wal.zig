const std = @import("std");
const RecordPkg = @import("./record.zig");
const Record = RecordPkg.Record;
const RecordError = RecordPkg.RecordError;

pub const WalError = error{
    MaxSizeReached,
    CantCreateRecord,
} || RecordError || std.mem.Allocator.Error;

const Wal = struct {
    appendFn: fn (iface: *Wal) WalError!void,
    findFn: fn (iface: *Wal) ?*Record,
    appendKvFn: fn (iface: *Wal, k: []const u8, v: []const u8) WalError!void,
    deinitFn: fn () anyerror!void,
    walSizeFn: fn (iface: *Wal) usize,

    pub fn append(iface: *Wal) WalError!void {
        return iface.appendFn(iface);
    }

    pub fn find(iface: *Wal, key_to_find: []const u8) ?*Record {
        return iface.findFn(iface, key_to_find);
    }

    fn appendKv(iface: *Wal, k: []const u8, v: []const u8) WalError!void {
        return iface.appendKvFn(iface, k, v);
    }

    pub fn deinit(iface: *Wal) !void {
        return iface.deinitFn();
    }

    pub fn walSize(iface: *Wal) usize {
        return iface.walSizeFn();
    }
};
