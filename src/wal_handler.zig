const std = @import("std");
const Record = @import("./record.zig").Record;
const DiskManager = @import("./disk_manager.zig").DiskManager;
const Op = @import("./ops.zig").Op;
const FileData = @import("./disk_manager.zig").FileData;
const PointerNs = @import("./pointer.zig");
const Pointer = PointerNs.Pointer;
const PointerError = PointerNs.Error;

pub const Result = enum {
    Ok,
    WalSwitched,
};

pub const Error = error{
    EmptyWal,
};

pub fn WalHandler(comptime WalType: type) type {
    return struct {
        const Self = @This();

        disk_manager: *DiskManager,

        old: ?*WalType,
        current: *WalType,
        next: *WalType,

        alloc: std.mem.Allocator,

        pub fn init(dm: *DiskManager, alloc: std.mem.Allocator) !*Self {
            var wh: *Self = try alloc.create(Self);

            wh.alloc = alloc;
            wh.disk_manager = dm;

            wh.old = null;

            // Create a WAL to use as current
            wh.current = try WalType.init(alloc);

            // Create also "next" WAL to switch when 'current' is full
            wh.next = try WalType.init(alloc);

            return wh;
        }

        /// Persist the current WAL and deinitializes everything.
        /// Returns the absolute file path of the file created
        /// with the contents of the current WAL, if an allocator is passed
        pub fn deinit(self: *Self) void {
            self.current.deinit();
            self.next.deinit();

            if (self.old) |old| {
                old.deinit();
            }

            self.alloc.destroy(self);
        }

        fn persistCurrent(self: *Self, allocator: ?std.mem.Allocator) ![]const u8 {
            var filename: ?[]const u8 = null;
            var fileData_or_error = self.persist(self.current);

            if (fileData_or_error) |fileData| {
                if (allocator) |alloc| {
                    filename = try alloc.dupe(u8, fileData.filename);
                }
                fileData.deinit();
            } else |err| switch (err) {
                Error.EmptyWal => {},
                else => {
                    errdefer err;
                    {}
                },
            }

            return filename;
        }

        fn persist(self: *Self, wal: *WalType) !FileData {
            if (wal.header.total_records == 0) {
                return Error.EmptyWal;
            }

            //Get a new file to persist the wal
            var filedata = try self.disk_manager.getNewFile(self.alloc);
            errdefer {
                std.debug.print("Deleting file {s}\n", .{filedata.filename});
                std.fs.deleteFileAbsolute(filedata.filename) catch |err| {
                    std.debug.print("Error trying to delete file {}\n", .{err});
                };
            }

            var file: *std.fs.File = &filedata.file;

            _ = try wal.persist(file);

            return filedata;
        }

        pub fn append(self: *Self, r: *Record) !Result {
            if (self.hasEnoughCapacity(r.len())) {
                try self.current.append(r);
                return Result.Ok;
            }

            try self.switchWal();
            try self.current.append(r);

            return Result.WalSwitched;
        }

        fn switchWal(self: *Self) !void {
            //Get a new file to persist the wal
            var fileData = try self.disk_manager.getNewFile(self.alloc);
            defer fileData.deinit();
            var f = &fileData.file;

            _ = try self.current.persist(f);
            self.old = self.current;
            self.current = self.next;

            self.next = try WalType.init(self.alloc);
        }

        /// Finds the record in the current WAL in use
        pub fn find(self: *Self, key: []const u8, alloc: std.mem.Allocator) !?*Record {
            return self.current.find(key, alloc);
        }

        pub fn hasEnoughCapacity(self: *Self, size: usize) bool {
            return self.current.availableBytes() >= size;
        }
    };
}

const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;

const MemoryWal = @import("./memory_wal.zig").MemoryWal;
const SstIndex = @import("./sst_manager.zig").SstIndex;

test "wal_handler_append" {
    var alloc = std.testing.allocator;

    var dm = try DiskManager.init("/tmp", alloc);
    defer dm.deinit();

    var wh = try WalHandler(MemoryWal(2048)).init(dm, alloc);
    defer wh.deinit();

    var r = try Record.init("hello", "world", Op.Create, alloc);
    defer r.deinit();
    _ = try wh.append(r);

    try expectEqual(@as(usize, 1), wh.current.header.total_records);
    try std.testing.expectError(PointerError.NullOffset, wh.current.mem[0].getOffset());
    try expect(r != wh.current.mem[0]);
    try expectEqualStrings(r.getKey(), wh.current.mem[0].getKey());
    try expectEqualStrings(r.getVal(), wh.current.mem[0].getVal());

    var r2 = try Record.init("hello2", "world2", Op.Create, alloc);
    defer r2.deinit();

    _ = try wh.append(r2);

    try expectEqual(@as(usize, 2), wh.current.header.total_records);
    try expect(r2 != wh.current.mem[0]);
    try expect(r2.getKey().len != wh.current.mem[0].getKey().len);
    try expect(r2.getVal().len != wh.current.mem[0].getVal().len);
}

test "wal_handler_persist" {
    var alloc = std.testing.allocator;

    var dm = try DiskManager.init("/tmp", alloc);
    defer dm.deinit();

    var wh = try WalHandler(MemoryWal(2048)).init(dm, alloc);
    defer wh.deinit();

    var r = try Record.init("hello", "world", Op.Create, alloc);
    defer r.deinit();
    _ = try wh.append(r);

    var fileData = try wh.persist(wh.current);
    // TODO check if the file that has been created has the expected contents
    defer fileData.deinit();
    defer std.fs.deleteFileAbsolute(fileData.filename) catch |err| {
        std.debug.print("Could not delete file {s}: {},\n", .{ fileData.filename, err });
    };

    try fileData.file.seekTo(0);

    var sst: *SstIndex = try SstIndex.init(fileData.filename, alloc);
    defer sst.deinit();

    try expectEqual(@as(usize, 1), sst.header.total_records);

    const r1 = (try sst.get("hello", alloc)).?;
    defer r1.deinit();

    try expectEqualStrings("hello", r1.getKey());
    try expectEqualStrings("world", r1.getVal());
}
