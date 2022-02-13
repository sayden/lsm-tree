const std = @import("std");
const Wal = @import("wal.zig").Wal;
const Record = @import("record.zig").Record;
const Op = @import("ops.zig").Op;
const File = std.fs.File;
const ArrayList = std.ArrayList;
const lsmtree = @import("main.zig");
const MakeDirError = std.os.MakeDirError;
const OpenFileError = std.is.OpenFileError;

/// Tracks the files that belong to the system.
pub fn DiskManager(comptime WalType: type) type {
    return struct {
        const Self = @This();
        folder_path: []const u8,
        id_file: ?File = null,

        pub fn init(p: []const u8) !Self {
            std.log.debug("using folder '{s}'\n", .{p});
            std.os.mkdir(p) catch |err| {
                switch (err) {
                    MakeDirError.PathAlreadyExists => void,
                    else => return err,
                }
            };

            //TODO Read contents of folder, return error if unexpected content
            // Find SST ID File by its extension
            const dir = try std.fs.openDirAbsolute(p);
            var iter = dir.iterate();
            while (iter.next()) |maybe_file| {
                std.debug.print("Hello {any}", .{maybe_file.?});
            }

            return Self{
                .folder_path = p,
            };
        }

        /// No deallocations are needed.
        ///
        /// Callers must close the file when they are done with it. Unfortunately
        /// there's not "WriterCloser" to return a writer than can be closed so the
        /// concrete File implementation must be returned. 
        ///
        /// TODO build an wrapper to allow using a File like a WriterCloser interface to allow switching
        /// implementations (to transparently do compression, for example).
        pub fn new_sst_file(self: *Self, allocator: std.mem.Allocator) !File {
            const file_id: []u8 = try self.get_new_file_id(allocator);
            defer allocator.free(file_id);

            var full_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{ self.folder_path, file_id });
            defer allocator.free(full_path);

            var f = try std.fs.createFileAbsolute(full_path, File.CreateFlags{ .exclusive = true });
            return f;
        }

        /// Writes all the contents of a WAL to disk, requesting a new file to itself
        pub fn persist_wal(self: *Self, wal: *WalType) !usize {
            //TODO Use a one time allocator somehow
            //Create a new file
            var f = try self.new_sst_file(std.testing.allocator);
            defer f.close();

            //Sort the wal in place
            wal.sort();

            //Iterate
            var iter = wal.iterator();
            var written: usize = 0;
            var total_record_bytes: usize = 0;
            var buf: [2048]u8 = undefined;
            while (iter.next()) |record| {
                total_record_bytes = try lsmtree.serialize.record.toBytes(record, &buf);
                written += try f.write(buf[0..total_record_bytes]);
            }

            return written;
        }

        /// No deallocations are needed.
        pub fn read_file(self: *Self, filename: []const u8, allocator: *std.mem.Allocator) !ArrayList(*Record) {
            var full_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{ self.folder_path, filename });
            defer allocator.free(full_path);

            var f = try std.fs.openFileAbsolute(full_path, File.OpenFlags{});
            var all = try f.readToEndAlloc(allocator, 4096);
            defer allocator.free(all);

            var list = std.ArrayList(*Record).init(allocator);
            var seek_pos: usize = 0;
            while (lsmtree.serialize.record.fromBytes(all[seek_pos..], allocator)) |r| {
                seek_pos += r.record_size_in_bytes;
                try list.append(r);
            }

            return list;
        }

        // TODO it must return a unique numeric id for the file being created.
        fn get_new_file_id(_: *Self, allocator: std.mem.Allocator) ![]u8 {
            // var full_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{ self.folder_path, filename });
            // std.fs.openFileAbsolute();
            var buf = try std.fmt.allocPrint(allocator, "{d}", .{1});

            return buf;
        }
    };
}

test "disk_manager.create or open SST ID file" {}

test "disk_manager.read file" {
    try testWriteWalToDisk("/tmp");
    // Remove testing file
    defer _ = std.fs.deleteFileAbsolute("/tmp/1.sst") catch null;

    var path = "/tmp".*;

    var dm = DiskManager(Wal(100)){ .folder_path = path[0..] };

    var list = try dm.read_file("1", std.testing.allocator);
    while (list.popOrNull()) |r| {
        r.deinit();
    }
    defer list.deinit();
}

fn testWriteWalToDisk(path: []const u8) !void {
    const WalType = Wal(100);

    var alloc = std.testing.allocator;
    var wal = try WalType.init(&alloc);
    defer wal.deinit_cascade();

    try wal.add_record(try Record.init("hell", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell2", "world", Op.Create, &alloc));

    var dm = DiskManager(WalType){ .folder_path = path[0..] };
    const total_bytes = try dm.persist_wal(wal);
    try std.testing.expectEqual(@as(usize, 62), total_bytes);
}

test "disk_manager.write wal" {
    const path = "/tmp";
    const WalType = Wal(100);

    var alloc = std.testing.allocator;
    var wal = try WalType.init(&alloc);
    defer wal.deinit_cascade();

    try wal.add_record(try Record.init("hell", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell2", "world", Op.Create, &alloc));

    var dm = DiskManager(WalType){ .folder_path = path[0..] };
    const total_bytes = try dm.persist_wal(wal);
    try std.testing.expectEqual(@as(usize, 62), total_bytes);

    // Remove testing file
    _ = std.fs.deleteFileAbsolute("/tmp/1.sst") catch null;
}

test "disk_manager.get new file id" {
    const WalType = Wal(100);
    var dm = DiskManager(WalType){ .folder_path = "/tmp" };

    var f = try dm.get_new_file_id(std.testing.allocator);
    defer std.testing.allocator.free(f);
}

test "disk_manager.create file" {
    const WalType = Wal(100);
    var dm = DiskManager(WalType){ .folder_path = "/tmp" };
    var f = try dm.new_sst_file(std.testing.allocator);
    defer f.close();

    // Remove testing file
    defer _ = std.fs.deleteFileAbsolute("/tmp/1.sst") catch null;
}
