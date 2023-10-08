const std = @import("std");
const Allocator = std.mem.Allocator;

const DiskManager = @import("./disk_manager.zig").DiskManager;
const Op = @import("./ops.zig").Op;
const FileData = @import("./disk_manager.zig").FileData;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const DataNs = @import("./data.zig");
const DataChunk = DataNs.DataChunk;
const DataTableWriter = DataNs.DataTableWriter;
const ChunkError = DataNs.Error;
const KvNs = @import("./chunk.zig");
const Column = @import("./columnar.zig").Column;
const Kv = KvNs.Kv;
const Data = DataNs.Data;

pub fn WalHandler(comptime T: type) type {
    return struct {
        const Self = @This();
        const log = std.log.scoped(.WalHandler);

        disk_manager: *DiskManager,

        current: DataChunk,

        table: DataTableWriter,

        alloc: Allocator,

        pub fn init(dm: *DiskManager, alloc: Allocator) !*Self {
            var wh: *Self = try alloc.create(Self);

            wh.alloc = alloc;
            wh.disk_manager = dm;

            // Create a WAL to use as current
            wh.current = DataChunk.init(T, alloc);
            wh.table = try DataTableWriter.init(T, alloc);

            return wh;
        }

        /// Persist the current WAL and deinitializes everything.
        /// Returns the absolute file path of the file created
        /// with the contents of the current WAL, if an allocator is passed
        pub fn deinit(self: *Self) void {
            // remove empty files first
            self.current.deinit();
            self.table.deinit();

            self.alloc.destroy(self);
        }

        pub fn persistCurrent(self: *Self, alloc: Allocator) !?FileData {
            if (self.current.meta.count == 0) {
                return null;
            }

            return self.switchChunk(alloc);
        }

        // pub fn persist(self: *Self, wal: DataChunk, alloc: Allocator) !?FileData {
        //     if (wal.ctx.header.total_records == 0) {
        //         return null;
        //     }

        //     //Get a new file to persist the wal
        //     var fileData = try self.disk_manager.getNewFile("sst", alloc);
        //     errdefer fileData.deinit();

        //     var ws = ReaderWriterSeeker.initFile(fileData.file);
        //     _ = try wal.persist(&ws);

        //     return fileData;
        // }

        pub fn append(self: *Self, d: Data, alloc: Allocator) !?FileData {
            self.current.append(d) catch |err| {
                switch (err) {
                    ChunkError.NotEnoughSpace => {
                        var maybe_filedata = try self.switchChunk(alloc);
                        try self.current.append(d);
                        if (maybe_filedata) |filedata| {
                            return filedata;
                        }
                    },
                    inline else => return err,
                }
            };

            return null;
        }

        fn switchTable(self: *Self, alloc: Allocator) !FileData {
            log.debug("Persisting wal with ~{} records", .{self.table.meta.count * self.table.mem.items[0].mem.items.len});

            //Get a new file to persist the wal
            var fileData = try self.disk_manager.getNewFile("sst", alloc);
            errdefer fileData.deinit();

            var ws = ReaderWriterSeeker.initFile(fileData.file);
            _ = try self.table.write(&ws);
            self.table.deinit();
            self.table = try DataTableWriter.init(T, alloc);

            return fileData;
        }

        fn switchChunk(self: *Self, alloc: Allocator) !?FileData {
            self.table.append(self.current) catch |switchChunkErr| {
                switch (switchChunkErr) {
                    ChunkError.TableFull => {
                        var filedata = try self.switchTable(alloc);
                        errdefer filedata.deinit();

                        try self.table.append(self.current);
                        self.current = DataChunk.init(T, alloc);

                        return filedata;
                    },
                    inline else => return switchChunkErr,
                }
            };

            self.current = DataChunk.init(T, alloc);

            return null;
        }

        /// Finds the record in the current WAL in use
        pub fn find(self: *Self, key: []const u8, alloc: Allocator) !?*Data {
            return self.current.find(key, alloc);
        }

        pub fn totalRecords(self: *Self) usize {
            return self.current.ctx.header.total_records;
        }
    };
}

const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;

test "wal_append" {
    var alloc = std.testing.allocator;

    var dm = try DiskManager.init("/tmp", alloc);
    defer dm.deinit();

    var wh = try WalHandler(Kv).init(dm, alloc);
    defer wh.deinit();

    var r = Data.new(Kv.new("hello", "world", Op.Upsert));
    defer r.deinit();
    const maybe_result = try wh.append(r, alloc);
    if (maybe_result) |result| {
        defer result.deinit();
    }

    try expectEqual(@as(usize, 1), wh.current.meta.count);
    // try std.testing.expectError(RecordNS.Error.NullOffset, wh.current.ctx.mem[0].getOffset());
    // try expect(r != wh.current.mem.items[0]);
    try expectEqualStrings(r.kv.key, wh.current.mem.items[0].kv.key);
    try expectEqualStrings(r.kv.val, wh.current.mem.items[0].kv.val);

    var r2 = Data.new(Kv.new("hello2", "world2", Op.Upsert));
    defer r2.deinit();

    const maybe_file_data = try wh.append(r2, alloc);
    if (maybe_file_data) |file_data| {
        defer file_data.deinit();
    }

    try expectEqual(@as(usize, 2), wh.current.meta.count);
    // try expect(r2 != wh.current.mem.items[0]);
    try expect(r2.kv.key.len != wh.current.mem.items[0].kv.key.len);
    try expect(r2.kv.val.len != wh.current.mem.items[0].kv.val.len);
}

test "wal_test" {
    std.testing.log_level = .debug;
    var alloc = std.testing.allocator;

    var dm = try DiskManager.init("/tmp/test", alloc);
    defer dm.deinit();

    var wal = try WalHandler(Column).init(dm, alloc);
    defer wal.deinit();

    for (0..5000) |i| {
        var maybe_filedata = try wal.append(Data{ .col = Column{ .ts = std.time.nanoTimestamp(), .val = @as(f64, @floatFromInt(i)), .op = Op.Upsert } }, alloc);
        if (maybe_filedata) |filedata| {
            std.debug.print("Created file {s}\n", .{filedata.filename});
            defer filedata.deinit();
        }
    }
}
