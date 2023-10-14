const std = @import("std");
const fs = std.fs;
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const File = std.fs.File;
const Order = std.math.Order;

const FileIndex = @import("./file_index.zig").FileIndex;
const Wal = @import("./wal.zig").Wal;
const StorageManager = @import("./storage_manager.zig");
const Data = @import("./data.zig").Data;
const strings = @import("./strings.zig");
const Iterator = @import("./iterator.zig").Iterator;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const recoverWal = @import("./wal.zig").recoverWal;
const recoverChunk = @import("./wal.zig").recoverChunk;

const log = std.log.scoped(.SstManager);

pub fn SstManager(comptime T: type) type {
    return struct {
        const Self = @This();

        wal: Wal(T),
        disk_manager: *StorageManager,

        indices: ArrayList(FileIndex),

        alloc: Allocator,

        pub fn init(sm: *StorageManager, alloc: Allocator) !Self {
            const recovered_wal = try recoverWal(sm, T, alloc);
            const recovered_chunk = try recoverChunk(sm, T, alloc);

            var wal = try Wal(T).initRecover(sm, recovered_wal, recovered_chunk, alloc);

            const file_entries = try sm.getFiles("sst", alloc);
            defer file_entries.deinit();

            log.debug("Found {} SST Files", .{file_entries.len});

            var mng = Self{
                .alloc = alloc,
                .disk_manager = sm,
                .wal = wal,
                .indices = try ArrayList(FileIndex).init(alloc),
            };

            errdefer mng.deinit();

            for (file_entries) |file| {
                const idx = FileIndex.read(file, T, alloc) catch |err| {
                    switch (err) {
                        FileIndex.Error.EmptyFile => {
                            mng.mustDeleteFile(file);
                            continue;
                        },
                        else => return err,
                    }
                };
                errdefer idx.deinit();

                try mng.addNewIndex(idx);
            }

            // const newfiles = try Compacter.attemptCompaction(&mng.indices, wh, alloc);
            // defer newfiles.deinit();

            // for (newfiles.items) |filedata| {
            //     try mng.notifyNewIndexFileCreated(filedata);
            // }

            return mng;
        }

        pub fn append(self: *Self, d: Data) !void {
            const append_result = try self.wal.append(d);
            switch (append_result) {
                Wal(T).AppendResult.Ok => return,
                Wal(T).AppendResult.TableFull => {
                    var new_file = try self.disk_manager.getNewFile("sst");
                    const bytes_written = try self.wal.persist(new_file);
                    log.debug("New file SST written with {} bytes", .{bytes_written});

                    try self.notifyNewIndexFileCreated(new_file);
                },
            }
        }

        pub fn deinit(self: *Self) void {
            for (self.indices.items) |item| {
                item.deinit();
            }
            self.indices.deinit();

            if (self.first_key) |key| {
                key.deinit();
            }
            if (self.last_key) |key| {
                key.deinit();
            }
        }

        fn addNewIndex(self: *Self, idx: FileIndex) !void {
            try self.indices.append(idx);
            try self.updateFirstAndLastPointer(idx);
        }

        fn notifyNewIndexFilenameCreated(self: *Self, file: File) !void {
            const idx: FileIndex = try FileIndex.read(file, T, self.alloc);

            return self.addNewIndex(idx);
        }

        /// Consumes filedata, giving ownership to the newly created index.
        /// Deinitialization happens when deinitializing self
        fn notifyNewIndexFileCreated(self: *Self, file: File) !void {
            const idx: FileIndex = try FileIndex.initFile(file, self.alloc);
            errdefer idx.deinit();

            try self.addNewIndex(idx);
        }

        // Looks for the key in the WAL, if not present, checks in the indices
        pub fn find(self: *Self, key: []const u8, alloc: Allocator) !?*Data {
            // Check in wal first
            if (try self.wal.find(key, alloc)) |record| {
                return record;
            }

            // if it does, retrieve the index (file) that contains the record
            const idx = self.findIndexForKey(key);
            if (idx) |index| {
                return index.get(key, alloc);
            }

            return null;
        }

        const IndexIterator = Iterator(FileIndex);
        fn getIterator(self: *Self) IndexIterator {
            return IndexIterator.init(self.indices.items);
        }

        // checks if key is in the range of keys
        pub fn isBetween(self: *Self, key: []const u8) bool {
            if (self.first_key) |first_key| {
                if (self.last_key) |last_key| {
                    return strings.strcmp(key, first_key).compare(std.math.CompareOperator.gte) and strings.strcmp(key, last_key).compare(std.math.CompareOperator.lte);
                }
            }

            return false;
        }

        fn indicesHaveOverlappingKeys(level: u8, idx1: FileIndex, idx2: FileIndex) bool {
            var id1: [36]u8 = undefined;
            var id2: [36]u8 = undefined;
            _ = idx1.header.getId(&id1);
            _ = idx2.header.getId(&id2);

            if (idx1.header.level == level and idx2.header.level == level) {
                if (strings.strcmp(idx1.lastKey(), idx2.firstKey()) == Order.gt) {
                    log.debug("\nFound overlapping keys in index '{s}' and '{s}'\n", .{ id1, id2 });
                    return true;
                }

                if (strings.strcmp(idx2.lastKey(), idx1.firstKey()) == Order.gt) {
                    log.debug("\nFound overlapping keys in index '{s}' and '{s}'\n", .{ id1, id2 });
                    return true;
                }
            }

            return false;
        }

        fn findIndexForKey(self: *Self, key: []const u8) ?FileIndex {
            for (self.indices.items) |index| {
                if (index.isBetween(key)) {
                    return index;
                }
            }

            return null;
        }
    };
}
