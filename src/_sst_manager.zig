const std = @import("std");
const fs = std.fs;
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const File = std.fs.File;
const Order = std.math.Order;

const SSTable = @import("./sstable.zig").SSTable;
// const Wal = @import("./wal.zig").Wal;
const FileManager = @import("./file_manager.zig").FileManager;
const Kv = @import("./kv.zig").Kv;
const strings = @import("./strings.zig");
const Iterator = @import("./iterator.zig").Iterator;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

const log = std.log.scoped(.SstManager);

pub const SstManager = struct {
    const Self = @This();

    // wal: Wal(T),
    disk_manager: *FileManager,

    indices: ArrayList(SSTable),

    alloc: Allocator,

    pub fn init(sm: *FileManager, alloc: Allocator) !Self {
        // const recovered_wal = try recoverWal(sm, T, alloc);
        // const recovered_chunk = try recoverChunk(sm, T, alloc);

        // var wal = try Wal(T).initRecover(sm, recovered_wal, recovered_chunk, alloc);
        // const wal = try Wal(T).initWithStorageManager(sm, alloc);

        const file_entries = try sm.getFiles("sst", alloc);
        defer file_entries.deinit();

        log.debug("Found {} SST Files", .{file_entries.items.len});

        var mng = Self{
            .alloc = alloc,
            .disk_manager = sm,
            // .wal = wal,
            .indices = ArrayList(SSTable).init(alloc),
        };

        errdefer mng.deinit();

        // for (file_entries.items) |file| {
        //     const idx = FileIndex.read(file, T, alloc) catch |err| {
        //         switch (err) {
        //             FileIndex.Error.EmptyFile => {
        //                 // mustDeleteFile(file);
        //                 continue;
        //             },
        //             else => return err,
        //         }
        //     };
        //     errdefer idx.deinit();

        //     try mng.addNewIndex(idx);
        // }

        // const newfiles = try Compacter.attemptCompaction(&mng.indices, wh, alloc);
        // defer newfiles.deinit();

        // for (newfiles.items) |filedata| {
        //     try mng.notifyNewIndexFileCreated(filedata);
        // }

        return mng;
    }

    pub fn deinit(self: *Self) void {
        for (self.indices.items) |*item| {
            item.deinit();
        }
        self.indices.deinit();
        self.wal.deinit();
    }

    pub fn findOne(self: *Self, key: *Kv, alloc: Allocator) !?*Kv {
        // Check in wal first
        if (try self.wal.find(key, alloc)) |data| {
            return data;
        }

        const maybe_chunk = try self.find(key, alloc);
        if (maybe_chunk) |chunk| {
            for (chunk.mem.items) |data| {
                if (data.equals(key.*)) {
                    try data.cloneTo(key, alloc);
                    return key;
                }
            }
        }

        return null;
    }

    // fn addNewIndex(self: *Self, idx: FileIndex) !void {
    //     try self.indices.append(idx);
    //     try self.updateFirstAndLastPointer(idx);
    // }

    // fn notifyNewIndexFilenameCreated(self: *Self, file: File) !void {
    //     const idx: FileIndex = try FileIndex.read(file, T, self.alloc);

    //     return self.addNewIndex(idx);
    // }

    // /// Consumes filedata, giving ownership to the newly created index.
    // /// Deinitialization happens when deinitializing self
    // fn notifyNewIndexFileCreated(self: *Self, file: File) !void {
    //     const idx: FileIndex = try FileIndex.initFile(file, self.alloc);
    //     errdefer idx.deinit();

    //     try self.addNewIndex(idx);
    // }

    // // checks if key is in the range of keys
    // pub fn isBetween(self: *Self, key: []const u8) bool {
    //     if (self.first_key) |first_key| {
    //         if (self.last_key) |last_key| {
    //             return strings.strcmp(key, first_key).compare(std.math.CompareOperator.gte) and strings.strcmp(key, last_key).compare(std.math.CompareOperator.lte);
    //         }
    //     }

    //     return false;
    // }

    // fn indicesHaveOverlappingKeys(level: u8, idx1: FileIndex, idx2: FileIndex) bool {
    //     var id1: [36]u8 = undefined;
    //     var id2: [36]u8 = undefined;
    //     _ = idx1.header.getId(&id1);
    //     _ = idx2.header.getId(&id2);

    //     if (idx1.header.level == level and idx2.header.level == level) {
    //         if (strings.strcmp(idx1.lastKey(), idx2.firstKey()) == Order.gt) {
    //             log.debug("\nFound overlapping keys in index '{s}' and '{s}'\n", .{ id1, id2 });
    //             return true;
    //         }

    //         if (strings.strcmp(idx2.lastKey(), idx1.firstKey()) == Order.gt) {
    //             log.debug("\nFound overlapping keys in index '{s}' and '{s}'\n", .{ id1, id2 });
    //             return true;
    //         }
    //     }

    //     return false;
    // }
};

test "sstmanager_findone" {
    const Op = @import("./ops.zig").Op;

    const alloc = std.testing.allocator;

    var sm = try FileManager.init("/tmp", alloc);
    defer sm.deinit();

    var man = try SstManager.init(&sm, alloc);
    defer man.deinit();

    var searched_data = Kv.new("hello", "world", Op.Upsert);
    defer searched_data.deinit();

    const maybe_data = try man.findOne(&searched_data, alloc);
    if (maybe_data) |data| {
        try std.testing.expectEqual(@as(f64, 124), data.col.val);
        try std.testing.expectEqualDeep(searched_data, data.*);
        try std.testing.expectEqual(@intFromPtr(&searched_data), @intFromPtr(data));
    }
}

test "sstmanager_findone_with_index" {
    const Op = @import("./ops.zig").Op;

    const alloc = std.testing.allocator;

    var sm = try FileManager.init("/tmp/test", alloc);
    defer sm.deinit();

    var man = try SstManager.init(&sm, alloc);
    defer man.deinit();

    var written: usize = 0;
    for (0..100) |_| {
        written += try man.append(Kv.new(100, "hello", "world"));
        std.debug.print("Written {}\n", .{written});
    }

    try std.testing.expectEqual(@as(usize, 0), man.wal.chunks.items.len);
    try std.testing.expectEqual(man.wal.current_chunk.mem.items.len, 100);

    var f = try sm.getNewFile("sst");
    defer f.close();

    const bytes_written = try man.wal.persist(f);
    std.debug.print("{} bytes written\n", .{bytes_written});
    try f.seekTo(0);

    const idx = try SSTable.read(f, alloc);
    try man.indices.append(idx);

    var searched_data = Kv.new("mario", "caster", Op.Upsert);
    const maybe_data = try man.findOne(&searched_data, alloc);
    if (maybe_data) |data| {
        try std.testing.expectEqual(@as(f64, 177), data.col.val);
        try std.testing.expectEqualDeep(searched_data, data.*);
        try std.testing.expectEqual(@intFromPtr(&searched_data), @intFromPtr(data));
    }
}
