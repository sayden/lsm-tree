const std = @import("std");
const Wal = @import("wal.zig").Wal;
const Record = @import("record.zig").Record;
const Op = @import("ops.zig").Op;
const File = std.fs.File;
const ArrayList = std.ArrayList;
const lsmtree = @import("main.zig");
const MakeDirError = std.os.MakeDirError;
const OpenFileError = std.is.OpenFileError;
const RndGen = std.rand.DefaultPrng;

/// Tracks the files that belong to the system.
pub fn DiskManager(comptime WalType: type) type {
    return struct {
        const Self = @This();

        folder_path: []const u8,
        id_file: ?File = null,

        pub fn init(folder_path: []const u8) !Self {
            std.log.debug("using folder '{s}'\n", .{folder_path});

            // Create a folder to store data and continue if the folder already exists so it is opened.
            std.os.mkdir(folder_path, 600) catch |err| {
                _ = switch (err) {
                    MakeDirError.PathAlreadyExists => void, //open the content of the folder,
                    else => return err,
                };
            };

            //TODO Read contents of folder, return error if unexpected content
            // Find SST ID File by its extension
            var dir = try std.fs.openIterableDirAbsolute(folder_path, std.fs.Dir.OpenDirOptions{});
            var iter = dir.iterate();

            //TODO URGENT fix this
            while (true) {
                var next = try iter.next();
                if (next == null) {
                    break;
                }
                // std.debug.print("File found in folder '{s}': {s}\n", .{ folder_path, next.?.name });
            }

            return Self{
                .folder_path = folder_path,
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
        pub fn new_file(self: *Self, allocator: *std.mem.Allocator) !File {
            const file_id: []u8 = try self.get_new_file_id(allocator);
            defer allocator.free(file_id);

            var full_path = try std.fmt.allocPrint(allocator.*, "{s}/{s}.sst", .{ self.folder_path, file_id });
            defer allocator.free(full_path);

            var f = try std.fs.createFileAbsolute(full_path, File.CreateFlags{ .exclusive = true });
            return f;
        }

        /// Writes all the contents of a WAL to disk, requesting a new file to itself
        pub fn persist_wal(self: *Self, wal: *WalType) !usize {
            //TODO Use a one time allocator somehow in the following line
            //Create a new file
            var alloc = std.testing.allocator;
            var f = try self.new_file(&alloc);
            defer f.close();

            //Sort the wal in place
            wal.sort();

            //Iterate
            var iter = wal.iterator();
            var written: usize = 0;
            var total_record_bytes: usize = 0;
            var buf: [2048]u8 = undefined;
            while (iter.next()) |record| {
                total_record_bytes = try Record.toBytes(record, &buf);
                written += try f.write(buf[0..total_record_bytes]);
            }

            return written;
        }

        /// No deallocations are needed.
        pub fn read_file(self: *Self, filename: []const u8, allocator: *std.mem.Allocator) !ArrayList(*Record) {
            var full_path = try std.fmt.allocPrint(allocator.*, "{s}/{s}.sst", .{ self.folder_path, filename });
            defer allocator.free(full_path);

            var f = try std.fs.openFileAbsolute(full_path, File.OpenFlags{});
            var all = try f.readToEndAlloc(allocator.*, 4096);
            defer allocator.free(all);

            var list = std.ArrayList(*Record).init(allocator.*);
            var seek_pos: usize = 0;
            var alloc = allocator;
            while (Record.fromBytes(all[seek_pos..], alloc)) |r| {
                seek_pos += r.record_size_in_bytes;
                try list.append(r);
            }

            return list;
        }

        // TODO it must return a unique numeric id for the file being created.
        fn get_new_file_id(self: *Self, allocator: *std.mem.Allocator) ![]u8 {
            _ = self;
            // var full_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{ self.folder_path, filename });
            // std.fs.openFileAbsolute();

            var u64Value: u64 = @as(u64, @intCast(std.time.timestamp()));

            var rnd = RndGen.init(u64Value);
            var n = rnd.random().int(u32);

            var buf = try std.fmt.allocPrint(allocator.*, "{}", .{n});

            return buf;
        }

        // TODO Return a list of the SST files in the folder.
        fn get_files(_: *Self) !void {
            // Read every file from self.folder_path
            // Discard all unknown files
            // Return the array of files
        }
    };
}

test "disk_manager.create or open SST ID file" {
    const WalType = Wal(100);

    var alloc = std.testing.allocator;
    var wal = try WalType.init(&alloc);
    defer wal.deinit_cascade();

    var path = "/tmp".*;
    var dm = DiskManager(Wal(100)){ .folder_path = path[0..] };
    var id = try dm.get_new_file_id(&alloc);
    defer alloc.free(id);

    std.debug.print("Hello World! {s}\n", .{id});
}

test "disk_manager.read file" {
    try testWriteWalToDisk("/tmp");
    // Remove testing file
    defer _ = std.fs.deleteFileAbsolute("/tmp/1.sst") catch null;

    var path = "/tmp".*;

    var dm = DiskManager(Wal(100)){ .folder_path = path[0..] };

    var alloc = std.testing.allocator;
    var list = try dm.read_file("1", &alloc);
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

    try wal.append(try Record.init("hell", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell2", "world", Op.Create, &alloc));

    var dm = DiskManager(WalType){ .folder_path = path[0..] };
    const total_bytes = try dm.persist_wal(wal);
    try std.testing.expectEqual(@as(usize, 62), total_bytes);
}

// test "disk_manager.write wal" {
//     const path = "/tmp";
//     const WalType = Wal(100);

//     var alloc = std.testing.allocator;
//     var wal = try WalType.init(&alloc);
//     defer wal.deinit_cascade();

//     try wal.add_record(try Record.init("hell", "world", Op.Create, &alloc));
//     try wal.add_record(try Record.init("hell1", "world", Op.Create, &alloc));
//     try wal.add_record(try Record.init("hell2", "world", Op.Create, &alloc));

//     var dm = DiskManager(WalType){ .folder_path = path[0..] };
//     const total_bytes = try dm.persist_wal(wal);
//     try std.testing.expectEqual(@as(usize, 62), total_bytes);

//     // Remove testing file
//     _ = std.fs.deleteFileAbsolute("/tmp/1.sst") catch null;
// }

// test "disk_manager.get new file id" {
//     const WalType = Wal(100);
//     var dm = DiskManager(WalType){ .folder_path = "/tmp" };

//     var alloc = std.testing.allocator;
//     var f = try dm.get_new_file_id(&alloc);
//     defer alloc.free(f);
// }

// test "disk_manager.create file" {
//     const WalType = Wal(100);
//     var dm = DiskManager(WalType){ .folder_path = "/tmp" };
//     var alloc = std.testing.allocator;
//     var f = try dm.new_file(&alloc);
//     defer f.close();

//     // Remove testing file
//     defer _ = std.fs.deleteFileAbsolute("/tmp/1.sst") catch null;
// }

// test "disk_manager.size on memory" {
//     try std.testing.expectEqual(32, @sizeOf(DiskManager(Wal(100))));
// }
