const std = @import("std");
const Wal = @import("memory_wal.zig").MemoryWal;
const Record = @import("record.zig").Record;
const Op = @import("ops.zig").Op;
const File = std.fs.File;
const ArrayList = std.ArrayList;
const lsmtree = @import("main.zig");
const MakeDirError = std.os.MakeDirError;
const OpenFileError = std.is.OpenFileError;
const RndGen = std.rand.DefaultPrng;

const FileData = struct {
    file: std.fs.File,
    filename: []const u8,
    alloc: *std.mem.Allocator,

    pub fn deinit(self: *FileData) void {
        self.file.close();
        self.alloc.free(self.filename);
    }
};

/// Tracks the files that belong to the system.
pub const DiskManager = struct {
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
    pub fn new_file(self: *Self, allocator: *std.mem.Allocator) !FileData {
        var totalAttempts: usize = 0;
        var file_id: []u8 = try self.get_new_file_id(allocator);
        var full_path: []u8 = try std.fmt.allocPrint(allocator.*, "{s}/{s}.sst", .{ self.folder_path, file_id });

        var f: ?std.fs.File = null;
        while (true) {
            f = std.fs.openFileAbsolute(full_path, std.fs.File.OpenFlags{}) catch |err| {
                switch (err) {
                    std.fs.File.OpenError.FileNotFound => {
                        allocator.free(file_id);
                        break;
                    },
                    std.fs.File.OpenError.PathAlreadyExists => {
                        std.debug.print("File {s} already exists, retrying\n", .{full_path});
                        f.?.close();

                        if (totalAttempts > 100) {
                            allocator.free(file_id);
                            allocator.free(full_path);
                            return err;
                        }

                        totalAttempts += 1;

                        allocator.free(file_id);
                        file_id = try self.get_new_file_id(allocator);

                        allocator.free(full_path);
                        full_path = try std.fmt.allocPrint(allocator.*, "{s}/{s}.sst", .{ self.folder_path, file_id });
                        continue;
                    },
                    else => {
                        return err;
                    },
                }
            };
        }

        std.debug.print("Creating file {s}\n", .{full_path});

        var file = try std.fs.createFileAbsolute(full_path, std.fs.File.CreateFlags{});

        return FileData{
            .file = file,
            .filename = full_path,
            .alloc = allocator,
        };
    }

    // It must return a unique numeric id for the file being created. Caller is owner of array response
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
    fn get_files(_: *Self, alloc: *std.mem.Allocator) ![]std.fs.IterableDir.Entry {
        // Read every file from self.folder_path
        var dirIterator = try std.fs.openIterableDirAbsolute("/tmp", .{ .no_follow = true });

        var iterator = dirIterator.iterate();
        var index: usize = 0;

        while (try iterator.next()) |item| {
            if (std.mem.eql(u8, item.name[item.name.len - 4 .. item.name.len], ".sst")) {
                index += 1;
            }
        }
        dirIterator.close();

        var kinds = try alloc.alloc(std.fs.IterableDir.Entry, index);

        // Restart the operation
        dirIterator = try std.fs.openIterableDirAbsolute("/tmp", .{ .no_follow = true });
        defer dirIterator.close();

        iterator = dirIterator.iterate();
        index = 0;

        while (try iterator.next()) |item| {
            if (std.mem.eql(u8, item.name[item.name.len - 4 .. item.name.len], ".sst")) {
                kinds[index] = item;
                index += 1;
            }
        }

        return kinds;
    }
};
