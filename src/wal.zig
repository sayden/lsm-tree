const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const fs = std.fs;
const File = std.fs.File;

const StorageManager = @import("./storage_manager.zig").StorageManager;
const Op = @import("./ops.zig").Op;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const ChunkNs = @import("./chunk.zig");
const ChunkError = ChunkNs.Error;
const Chunk = ChunkNs.Chunk;
const KvNs = @import("./kv.zig");
const Column = @import("./columnar.zig").Column;
const Kv = KvNs.Kv;
const Data = @import("./data.zig").Data;
const Metadata = @import("./metadata.zig").Metadata;
const IndexItem = @import("./index_entry.zig").IndexEntry;
const FileIndex = @import("./file_index.zig").SSTable;
const Iterator = @import("./iterator.zig").Iterator;
const strings = @import("./strings.zig");

const log = std.log.scoped(.Wal);

// The Wal is a Write Ahead Log that stores the data in a temporary file until it reaches a certain
pub fn Wal(comptime T: type) type {
    return struct {
        const Self = @This();
        const Error = error{EmptyWal};
        pub const MAX_VALUES = 100;
        pub const MAX_SIZE = 1_000;
        // const MAX_SIZE = 128_000_000;

        meta: Metadata,
        datasize: usize = 0,
        wal_writer: ReaderWriterSeeker,
        chunk_writer: ReaderWriterSeeker,

        current_chunk: Chunk,
        chunks: ArrayList(Chunk),
        alloc: Allocator,

        pub fn init(wal_tmp_file: File, chunk_tmp_file: File, alloc: Allocator) !Self {
            const wal_writer = ReaderWriterSeeker.initFile(wal_tmp_file);
            const chunk_writer = ReaderWriterSeeker.initFile(chunk_tmp_file);

            return Self{
                .meta = Metadata.initDefault(Metadata.Kind.Wal, T),
                .current_chunk = Chunk.init(T, alloc),
                .chunks = ArrayList(Chunk).init(alloc),
                .wal_writer = wal_writer,
                .chunk_writer = chunk_writer,
                .alloc = alloc,
            };
        }

        pub fn initRecover(sm: *StorageManager, recovered_wal: ?RecoveredWal, recovered_chunk: ?Chunk, alloc: Allocator) !Self {
            const wal_tmp_file = try sm.getNewFile("wal");
            const chunk_tmp_file = try sm.getNewFile("chk");

            if (recovered_wal) |wal| {
                return initRecoverWal(sm, wal, alloc);
            } else if (recovered_chunk) |*chunk| {
                defer chunk.deinit();
                return initRecoverChunk(sm, @constCast(chunk), alloc);
            } else if (recovered_chunk == null and recovered_wal == null) {
                return init(wal_tmp_file, chunk_tmp_file, alloc);
            }

            // Otherwise, both types have been found

            const wal_writer = ReaderWriterSeeker.initFile(wal_tmp_file);
            const chunk_writer = ReaderWriterSeeker.initFile(chunk_tmp_file);

            return Self{
                .meta = Metadata.initDefault(Metadata.Kind.Wal, T),
                .current_chunk = recovered_chunk.?,
                .chunks = recovered_wal.?.chunks,
                .wal_writer = wal_writer,
                .alloc = alloc,
                .chunk_writer = chunk_writer,
            };
        }

        pub fn initRecoverWal(sm: *StorageManager, recovered_wal: RecoveredWal, alloc: Allocator) !Self {
            const wal_tmp_file = try sm.getNewFile("wal");
            const chunk_tmp_file = try sm.getNewFile("chk");

            const wal_writer = ReaderWriterSeeker.initFile(wal_tmp_file);
            const chunk_writer = ReaderWriterSeeker.initFile(chunk_tmp_file);

            return Self{
                .meta = Metadata.init(Metadata.Kind.Wal, recovered_wal.firstkey, recovered_wal.lastkey),
                .current_chunk = Chunk.init(T, alloc),
                .chunks = recovered_wal.chunks,
                .wal_writer = wal_writer,
                .alloc = alloc,
                .chunk_writer = chunk_writer,
            };
        }

        pub fn initRecoverChunk(sm: *StorageManager, recovered_chunk: *Chunk, alloc: Allocator) !Self {
            const wal_tmp_file = try sm.getNewFile("wal");
            const chunk_tmp_file = try sm.getNewFile("chk");

            const wal_writer = ReaderWriterSeeker.initFile(wal_tmp_file);
            var chunk_writer = ReaderWriterSeeker.initFile(chunk_tmp_file);
            _ = try recovered_chunk.write(&chunk_writer, false);
            defer recovered_chunk.deinit();

            return Self{
                .meta = Metadata.init(Metadata.Kind.Wal, try recovered_chunk.meta.firstkey.clone(alloc), try recovered_chunk.meta.lastkey.clone(alloc)),
                .current_chunk = Chunk.init(T, alloc),
                .chunks = ArrayList(Chunk).init(alloc),
                .wal_writer = wal_writer,
                .alloc = alloc,
                .chunk_writer = chunk_writer,
            };
        }

        /// The StorageManager is not stored into the Wal and it is just used to create
        /// some files required to setup the Wal.
        pub fn initWithStorageManager(sm: *StorageManager, alloc: Allocator) !Self {
            const wal_file = try sm.getNewFile("wal");
            const chunks_file = try sm.getNewFile("chk");
            return Self.init(wal_file, chunks_file, alloc);
        }

        pub fn deinit(self: Self) void {
            self.meta.deinit();

            for (self.chunks.items) |*rows| {
                rows.deinit();
            }

            self.wal_writer.file.close();
            self.chunk_writer.file.close();

            self.current_chunk.deinit();
            self.chunks.deinit();
        }

        const AppendResultFull = enum { TableFull };
        pub const AppendResult = union(enum) { TableFull: AppendResultFull, Ok: usize };

        /// Appends a new Data to the current chunk. Triggers switchChunk if `ChunkFull` is
        /// returned when attempting to append to the chunk. It can return `TableFull` which
        /// still appends the data to the underlying chunk (no need to retry)
        pub fn append(self: *Self, d: Data) !AppendResult {
            const bytes_written = try d.write(&self.chunk_writer);

            const result = try self.current_chunk.append(d);
            return switch (result) {
                .ChunkFull => self.switchChunk(self.alloc),
                .Ok => AppendResult{ .Ok = bytes_written },
            };
        }

        pub fn find(self: Self, d: *Data, alloc: Allocator) !?*Data {
            for (self.chunks.items) |chunk| {
                for (chunk.mem.items) |item| {
                    if (item.equals(d.*)) {
                        try item.cloneTo(d, alloc);
                        return d;
                    }
                }
            }

            return null;
        }

        /// stores the current chunk, creates a new one
        pub fn switchChunk(self: *Self, alloc: Allocator) !AppendResult {
            const result = try self.appendChunk(self.current_chunk);

            //Remove the data from the current Chunk Wal
            const new_chunk_file = try resetTmpFile(self.chunk_writer.file);
            self.chunk_writer = ReaderWriterSeeker.initFile(new_chunk_file);

            const new_chunk = Chunk.init(T, alloc);
            self.current_chunk = new_chunk;

            return result;
        }

        /// appends the chunk into the current memory and wal disk.
        /// Returns error if the memory has trespassed the current limit.
        pub fn appendChunk(self: *Self, chunk: Chunk) !AppendResult {
            const bytes_written = try self.mustAppendChunk(chunk);

            if (chunk.size) |chunksize| {
                if (self.datasize + chunksize >= Self.MAX_SIZE) {
                    return .TableFull;
                }
            } else {
                return ChunkError.UnknownChunkSize;
            }

            return AppendResult{ .Ok = bytes_written };
        }

        // appends the chunk into the current memory and wal disk.
        fn mustAppendChunk(self: *Self, chunk: Chunk) !usize {
            var mutablechunk = @constCast(&chunk);
            const bytes_written = try mutablechunk.write(&self.wal_writer, true);
            try self.chunks.append(mutablechunk.*);
            self.datasize += chunk.size.?;
            self.meta.count += 1;
            return bytes_written;
        }

        /// Writes the contents of the Table into file, which is enlarged to MAX_SIZE, the format is the following:
        ///
        /// Metadata
        /// [1..IndexData]
        /// [1..Chunk]
        ///
        pub fn persist(self: *Self, file: File) !usize {
            if (self.chunks.items.len == 0 and self.current_chunk.mem.items.len == 0) {
                return 0;
            }

            var rws = ReaderWriterSeeker.initFile(file);
            var writer = &rws;

            // Move forward to the end of the file
            try writer.seekBy(Self.MAX_SIZE);

            // create the index array
            var offsets = try ArrayList(IndexItem).initCapacity(self.alloc, self.chunks.items.len);
            defer offsets.deinit();

            // Get the current offset pos, after moving forward, this should be the same than TableWriter.MAX_SIZE
            var offset_sentinel = try writer.getPos();

            // store the first and last key of the entire index
            var firstkey: ?Data = null;
            var lastkey: ?Data = null;

            // Persist the data in the current chunk
            std.debug.print("Items {}\n", .{self.current_chunk.size.?});
            try self.persistChunk(writer, &offset_sentinel, &offsets, &self.current_chunk, &firstkey, &lastkey);
            _ = try self.switchChunk(self.alloc);

            for (self.chunks.items) |*chunk| {
                try self.persistChunk(writer, &offset_sentinel, &offsets, chunk, &firstkey, &lastkey);
            }

            // Once chunks are written, we can come back to the beginning of the file
            try writer.seekTo(0);

            // update metadata with the info about the first and last key
            self.meta.firstkey = firstkey.?;
            self.meta.lastkey = lastkey.?;

            // write the metadata header
            try self.meta.write(writer);

            // Finally, write the array of indices
            for (offsets.items) |index| {
                try index.write(writer);
            }

            // if we have reached this point, the current WAL can be truncated
            const new_wal_file = try resetTmpFile(self.wal_writer.file);
            self.wal_writer = ReaderWriterSeeker.initFile(new_wal_file);

            return Self.MAX_SIZE;
        }

        // Writes the chunk into the file, updating `offset_sentinel` arg and the `offsets` arg
        fn persistChunk(_: *Self, writer: *ReaderWriterSeeker, offset_sentinel: *usize, offsets: *ArrayList(IndexItem), chunk: *Chunk, firstkey: *?Data, lastkey: *?Data) !void {
            // go backwards just enough to write the chunk
            const size: i64 = @as(i64, @intCast(chunk.size.?));
            std.debug.print("Size {}\n", .{size});
            try writer.seekBy(-size);
            offset_sentinel.* = try writer.getPos();

            // we are going to write in position 'offset_sentinel', store this position in the array
            // to write it later on the file
            try offsets.append(IndexItem{
                .offset = offset_sentinel.*,
                .firstkey = chunk.meta.firstkey,
                .lastkey = chunk.meta.lastkey,
            });

            // Update the first and last record from the index.
            Metadata.updateFirstAndLastKey(firstkey, lastkey, chunk.meta);

            // write the chunk data
            const bytes_written = try chunk.write(writer, true);

            // Move backwards again, to the position where this chunk began writing
            try writer.seekBy(-@as(i64, @intCast(chunk.size.?)));
            if (bytes_written != chunk.size) {
                std.debug.print("{} != {}\n", .{ bytes_written, chunk.size.? });
                @panic("regression");
            }
        }

        pub fn recoverWalFile(chunks_file: File, alloc: Allocator) !RecoveredWal {
            var reader = ReaderWriterSeeker.initFile(chunks_file);

            var chunks = ArrayList(Chunk).init(alloc);

            // store the first and last key of the entire index
            var firstkey: ?Data = null;
            var lastkey: ?Data = null;

            const stat = try chunks_file.stat();
            log.debug("WAL file has {} bytes", .{stat.size});

            while (true) {
                const chunk = Chunk.read(&reader, T, alloc) catch |err| {
                    if (err == error.EndOfStream) {
                        break;
                    }

                    return undefined;
                };

                Metadata.updateFirstAndLastKey(&firstkey, &lastkey, chunk.meta);

                try chunks.append(chunk);
            }

            if (firstkey == null) {
                return Self.Error.EmptyWal;
            }

            return RecoveredWal{
                .chunks = chunks,
                .firstkey = firstkey.?,
                .lastkey = lastkey.?,
            };
        }

        pub fn recoverChunkFile(chunk_file: File, alloc: Allocator) !Chunk {
            var tmpreader = ReaderWriterSeeker.initFile(chunk_file);

            const stattmp = try chunk_file.stat();
            log.debug("Tmp file has {} bytes", .{stattmp.size});

            // Read the records that weren't persisted as a chunk
            var chunk = Chunk.init(T, alloc);
            errdefer chunk.deinit();

            var data_not_found: bool = true;
            while (true) {
                const data_or_error = Data.read(T, &tmpreader, alloc);
                if (data_or_error) |data| {
                    _ = try chunk.append(data);
                    data_not_found = false;

                    // Update the first and last record from the index.
                    chunk.meta.updateSelfFirstAndLastKey(data);
                } else |err| {
                    if (err == error.EndOfStream) {
                        break;
                    } else {
                        return err;
                    }
                }
            }

            if (data_not_found) {
                return error.EmptyChunk;
            }

            return chunk;
        }
        pub fn debug(self: Self) void {
            std.debug.print("\n___________\nSTART TableWriter\n", .{});
            self.meta.debug();
            std.debug.print("END TableWriter\n---------------\n", .{});
        }
    };
}

pub const RecoveredWal = struct {
    chunks: ArrayList(Chunk),
    firstkey: Data,
    lastkey: Data,

    pub fn deinit(self: *RecoveredWal) void {
        for (self.chunks.items) |chunk| {
            chunk.deinit();
        }
        self.chunks.deinit();
        self.firstkey.deinit();
        self.lastkey.deinit();
    }
};

pub fn recoverWal(sm: *StorageManager, comptime T: type, alloc: Allocator) !?RecoveredWal {
    const wal_files: ArrayList(File) = try sm.getFiles("wal", alloc);
    defer wal_files.deinit();

    if (wal_files.items.len == 0) {
        return null;
    }

    if (wal_files.items.len > 1) {
        return error.UnexpectedNumberOfWalFiles;
    }

    var file = wal_files.items[0];
    const stat = try file.stat();
    if (stat.size == 0) {
        mustDeleteFile(file);
    }

    log.debug("Trying to recover file with fd {}", .{file.handle});

    const recovered_wal = try Wal(T).recoverWalFile(file, alloc);
    return recovered_wal;
}

pub fn recoverChunk(sm: *StorageManager, comptime T: type, alloc: Allocator) !?Chunk {
    const chunk_files: ArrayList(File) = try sm.getFiles("chk", alloc);
    defer chunk_files.deinit();

    if (chunk_files.items.len == 0) {
        return null;
    }

    if (chunk_files.items.len > 1) {
        return error.UnexpectedNumberOfWalFiles;
    }

    var file = chunk_files.items[0];
    const stat = try file.stat();
    if (stat.size == 0) {
        mustDeleteFile(file);
    }

    log.debug("Trying to recover file with fd {}", .{file.handle});

    const chunk = try Wal(T).recoverChunkFile(file, alloc);
    return chunk;
}

/// Silently deletes the provided file, logging an error in such case
fn mustDeleteFile(file: File) void {
    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = std.os.getFdPath(file.handle, &buf) catch |err| {
        log.err("Could not get path from file description {}, aborting deletion of file: {}", .{ file.handle, err });
        return;
    };

    fs.deleteFileAbsolute(path) catch |delete_error| {
        log.err("Found empty file '{s}' but there was an error attempting to delete it: {}", .{ path, delete_error });
    };
}

/// closes, truncates and reopen the incoming file, returning it
fn resetTmpFile(file: File) !File {
    defer file.close();
    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try std.os.getFdPath(file.handle, &buf);
    const new_file = try std.fs.createFileAbsolute(path, .{ .read = true, .truncate = true });
    return new_file;
}

const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;
const expectEqualStrings = std.testing.expectEqualStrings;

test "wal.persist" {
    std.testing.log_level = .debug;

    // Setup
    const alloc = std.testing.allocator;

    var tmpdir = std.testing.tmpDir(.{});
    defer tmpdir.cleanup();
    const wal_file = try tmpdir.dir.createFile("TableWriter", File.CreateFlags{ .read = true });
    const chunks_file = try tmpdir.dir.createFile("TestWalTmp", File.CreateFlags{ .read = true });
    const dest_file = try tmpdir.dir.createFile("TableWriter2", File.CreateFlags{ .read = true });

    const original_chunk = try ChunkNs.testChunk(alloc);
    // deinit happens inside TableWriter

    var wal = try Wal(Column).init(wal_file, chunks_file, alloc);
    defer wal.deinit();
    _ = try wal.appendChunk(original_chunk);

    // DataTable write
    const bytes_written: usize = try wal.persist(dest_file);
    try std.testing.expectEqual(@as(usize, Wal(Column).MAX_SIZE), bytes_written);
}

test "wal.appendChunk" {
    const alloc = std.testing.allocator;

    var dm = try StorageManager.init("/tmp", alloc);
    defer dm.deinit();

    var wh = try Wal(Kv).initWithStorageManager(&dm, alloc);
    defer wh.deinit();

    var data = Data.new(Kv.new("hello", "world", Op.Upsert));
    defer data.deinit();
    _ = try wh.append(data);

    try expectEqual(@as(usize, 1), wh.current_chunk.meta.count);
    try expectEqualStrings(data.kv.key, wh.current_chunk.mem.items[0].kv.key);
    try expectEqualStrings(data.kv.val, wh.current_chunk.mem.items[0].kv.val);

    var r2 = Data.new(Kv.new("hello2", "world2", Op.Upsert));
    defer r2.deinit();

    const result = try wh.append(r2);
    if (result != Wal(Kv).AppendResult.Ok) return error.TestUnexpectedResult;

    try expectEqual(@as(usize, 2), wh.current_chunk.meta.count);
    try expect(r2.kv.key.len != wh.current_chunk.mem.items[0].kv.key.len);
    try expect(r2.kv.val.len != wh.current_chunk.mem.items[0].kv.val.len);
}

test "wal.switchChunk" {
    std.testing.log_level = .debug;
    const alloc = std.testing.allocator;

    var dm = try StorageManager.init("/tmp/test", alloc);
    defer dm.deinit();

    var wal = try Wal(Column).initWithStorageManager(&dm, alloc);
    defer wal.deinit();

    for (0..5000) |i| {
        _ = try wal.append(Data{ .col = Column{ .ts = std.time.nanoTimestamp(), .val = @as(f64, @floatFromInt(i)), .op = Op.Upsert } });
    }
}

test "wal.recoverWal" {
    const alloc = std.testing.allocator;

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const fdpath = try std.os.getFdPath(tmp_dir.dir.fd, &buf);

    var sm = try StorageManager.init(fdpath, alloc);
    defer sm.deinit();

    // Create a Wal and write a couple of Data in it
    const WalColumn = Wal(Column);
    var wal = try WalColumn.initWithStorageManager(&sm, alloc);
    defer wal.deinit();

    // This should create a chunk
    _ = try wal.append(Data{ .col = Column{ .ts = std.time.nanoTimestamp(), .op = Op.Upsert, .val = 1 } });
    std.time.sleep(1000);
    _ = try wal.append(Data{ .col = Column{ .ts = std.time.nanoTimestamp(), .op = Op.Upsert, .val = 2 } });

    const result = try wal.switchChunk(alloc);
    if (result != WalColumn.AppendResult.Ok) {
        // defer wal.deinit();
        try std.testing.expect(false);
        return;
    }

    // Now add 2 more data
    const res1 = try wal.append(Data{ .col = Column{ .ts = std.time.nanoTimestamp(), .op = Op.Upsert, .val = 3 } });
    if (res1 != WalColumn.AppendResult.Ok) {
        try std.testing.expect(false);
    }
    std.time.sleep(1000);
    const res2 = try wal.append(Data{ .col = Column{ .ts = std.time.nanoTimestamp(), .op = Op.Upsert, .val = 4 } });
    if (res2 != WalColumn.AppendResult.Ok) {
        try std.testing.expect(false);
    }

    // So now we have a persisted chunk in the wal file with values 1 and 2 and a temp chunk
    // in a chunk file with values 3 and 4
    // Try to recover those files:
    const maybe_recovered_chunk = try recoverChunk(&sm, Column, alloc);
    var maybe_recovered_wal = try recoverWal(&sm, Column, alloc);

    if (maybe_recovered_chunk) |chunk| {
        try std.testing.expectEqual(@as(usize, 2), chunk.mem.items.len);
        defer chunk.deinit();
    } else {
        try std.testing.expect(false);
    }

    if (maybe_recovered_wal) |*recovered_wal| {
        try std.testing.expectEqual(@as(usize, 1), recovered_wal.chunks.items.len);
        defer recovered_wal.deinit();
    } else {
        try std.testing.expect(false);
    }
}
