const std = @import("std");
const Allocator = std.mem.Allocator;

const RecordNS = @import("./record.zig");
const Record = RecordNS.Record;
const StorageManager = @import("./storage_manager.zig").StorageManager;
const Op = @import("./ops.zig").Op;
const FileData = @import("./disk_manager.zig").FileData;
const Wal = @import("./wal.zig");
const Pointer = RecordNS.Pointer;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const SstIndex = @import("./sst_index.zig").SstIndex;

pub const WalHandler = WalHandlerTBuilder(Wal.InUse);

pub fn WalHandlerTBuilder(comptime WalType: type) type {
    return struct {
        const Self = @This();
        const log = std.log.scoped(.WalHandler);

        disk_manager: *StorageManager,

        current: WalType.Type,
        next: WalType.Type,

        alloc: Allocator,

        pub fn init(dm: *StorageManager, wal_size: usize, alloc: Allocator) !*Self {
            var wh: *Self = try alloc.create(Self);

            wh.alloc = alloc;
            wh.disk_manager = dm;

            // Create a WAL to use as current
            wh.current = try WalType.init(wal_size, dm, alloc);

            // Create also "next" WAL to switch when 'current' is full
            wh.next = try WalType.init(wal_size, dm, alloc);

            return wh;
        }

        /// Persist the current WAL and deinitializes everything.
        /// Returns the absolute file path of the file created
        /// with the contents of the current WAL, if an allocator is passed
        pub fn deinit(self: *Self) void {
            // remove empty files first
            self.current.deinit();
            self.next.deinit();

            self.alloc.destroy(self);
        }

        pub fn persistCurrent(self: *Self, alloc: Allocator) !?FileData {
            if (self.current.ctx.header.total_records == 0) {
                return null;
            }

            return self.switchWal(alloc) catch |err| switch (err) {
                Wal.Error.EmptyWal => return null,
                else => {
                    return err;
                },
            };
        }

        pub fn persist(self: *Self, wal: WalType.Type, alloc: Allocator) !?FileData {
            if (wal.ctx.header.total_records == 0) {
                return null;
            }

            //Get a new file to persist the wal
            var fileData = try self.disk_manager.getNewFile("sst", alloc);
            errdefer fileData.deinit();

            var ws = ReaderWriterSeeker.initFile(fileData.file);
            _ = try wal.persist(&ws);

            return fileData;
        }

        pub fn append(self: *Self, r: *Record, alloc: Allocator) !?FileData {
            if (self.hasEnoughCapacity(r.len())) {
                try self.current.append(r);
                return null;
            }

            const fileData = try self.switchWal(alloc);
            errdefer fileData.deinit();

            try self.current.append(r);

            return fileData;
        }

        pub fn appendOwn(self: *Self, r: *Record, alloc: Allocator) !?FileData {
            if (self.hasEnoughCapacity(r.len())) {
                try self.current.appendOwn(r);
                return null;
            }

            const fileData = try self.switchWal(alloc);
            errdefer fileData.deinit();

            try self.current.appendOwn(r);

            return fileData;
        }

        fn switchWal(self: *Self, alloc: Allocator) !FileData {
            log.debug("Persisting wal with {} records", .{self.current.ctx.header.total_records});

            //Get a new file to persist the wal
            var fileData = try self.disk_manager.getNewFile("sst", alloc);
            errdefer fileData.deinit();

            var ws = ReaderWriterSeeker.initFile(fileData.file);
            _ = try self.current.persist(&ws);
            self.current.deinit();

            self.current = self.next;
            self.next = try WalType.init(Wal.initial_wal_size, self.disk_manager, self.alloc);

            return fileData;
        }

        /// Finds the record in the current WAL in use
        pub fn find(self: *Self, key: []const u8, alloc: Allocator) !?*Record {
            return self.current.find(key, alloc);
        }

        pub fn totalRecords(self: *Self) usize {
            return self.current.ctx.header.total_records;
        }

        pub fn hasEnoughCapacity(self: *Self, size: usize) bool {
            return self.current.availableBytes() > size;
        }
    };
}

const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;

test "wal_handler_append" {
    var alloc = std.testing.allocator;

    var dm = try StorageManager.init("/tmp", alloc);
    defer dm.deinit();

    var wh = try WalHandler.init(dm, 512, alloc);
    defer wh.deinit();

    var r = try Record.init("hello", "world", Op.Upsert, alloc);
    defer r.deinit();
    const maybe_result = try wh.append(r, alloc);
    if (maybe_result) |result| {
        defer result.deinit();
    }

    try expectEqual(@as(usize, 1), wh.current.ctx.header.total_records);
    try std.testing.expectError(RecordNS.Error.NullOffset, wh.current.ctx.mem[0].getOffset());
    try expect(r != wh.current.ctx.mem[0]);
    try expectEqualStrings(r.getKey(), wh.current.ctx.mem[0].getKey());
    try expectEqualStrings(r.getVal(), wh.current.ctx.mem[0].getVal());

    var r2 = try Record.init("hello2", "world2", Op.Upsert, alloc);
    defer r2.deinit();

    const maybe_file_data = try wh.append(r2, alloc);
    if (maybe_file_data) |file_data| {
        defer file_data.deinit();
    }

    try expectEqual(@as(usize, 2), wh.current.ctx.header.total_records);
    try expect(r2 != wh.current.ctx.mem[0]);
    try expect(r2.getKey().len != wh.current.ctx.mem[0].getKey().len);
    try expect(r2.getVal().len != wh.current.ctx.mem[0].getVal().len);
}

test "wal_handler_persist" {
    var alloc = std.testing.allocator;

    var dm = try StorageManager.init("/tmp", alloc);
    defer dm.deinit();

    var wh = try WalHandler.init(dm, 512, alloc);
    defer wh.deinit();

    var r = try Record.init("hello", "world", Op.Upsert, alloc);

    const maybe_file_data = try wh.appendOwn(r, alloc);
    if (maybe_file_data) |file_data| {
        defer file_data.deinit();
        try expect(false);
    }

    var fileData = (try wh.persistCurrent(alloc)).?;
    // TODO check if the file that has been created has the expected contents

    try fileData.file.seekTo(0);

    var sst: *SstIndex = try SstIndex.initFile(fileData, alloc);
    defer sst.deinit();
    defer std.fs.deleteFileAbsolute(fileData.filename) catch {};

    try expectEqual(@as(usize, 1), sst.header.total_records);

    const r1 = (try sst.get("hello", alloc)).?;
    defer r1.deinit();

    try expectEqualStrings("hello", r1.getKey());
    try expectEqualStrings("world", r1.getVal());
}
