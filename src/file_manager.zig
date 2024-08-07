const std = @import("std");
const fs = std.fs;
const ArrayList = std.ArrayList;
const File = std.fs.File;
const Allocator = std.mem.Allocator;
const MakeDirError = std.os.MakeDirError;
const RndGen = std.rand.DefaultPrng;

pub const FileData = struct {
    file: std.fs.File,
    filename: []const u8,
    alloc: std.mem.Allocator,
};

/// Tracks the files that belong to the system. It probably should be called FileManager.
pub const FileManager = struct {
    const log = std.log.scoped(.DiskManager);
    const Self = @This();
    alloc: Allocator,

    absolute_path: []const u8,
    idNumber: u32,

    pub fn init(path: []const u8, alloc: Allocator) !FileManager {
        var real: []u8 = undefined;

        if (std.fs.path.isAbsolute(path)) {
            real = try alloc.dupe(u8, path);
        } else {
            real = try std.fs.realpathAlloc(alloc, path);
        }

        _ = std.fs.openDirAbsolute(real, std.fs.Dir.OpenDirOptions{ .iterate = true }) catch |err| switch (err) {
            std.fs.Dir.OpenError.FileNotFound => try std.fs.makeDirAbsolute(path),
            else => return err,
        };

        const rndnumber = FileManager.getRandomNumber();

        return FileManager{
            .absolute_path = real,
            .alloc = alloc,
            .idNumber = rndnumber,
        };
    }

    pub fn deinit(dm: *Self) void {
        dm.alloc.free(dm.absolute_path);
    }

    pub fn createNewMmapFile(self: *FileManager, ext: []const u8) !File {
        var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;

        const filename = try self.getNewFilename(ext, &buf);

        log.debug("Creating file {s}", .{filename});

        const file = try std.fs.createFileAbsolute(filename, std.fs.File.CreateFlags{ .read = true });

        return file;
    }

    /// Callers must close the file when they are done with it. Creates a new file with the extension provided in the
    /// data directory. The file name will be a generated UUID
    pub fn getNewFile(self: *FileManager, ext: []const u8) !File {
        var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;

        const filename = try self.getNewFilename(ext, &buf);

        log.debug("Creating file {s}", .{filename});

        const file = try std.fs.createFileAbsolute(filename, std.fs.File.CreateFlags{ .read = true }); //adding reading for tests

        return file;
    }

    /// By providing an extension, this returns a full path filename. The path is whatever was defined when creating the struct
    /// (for example "/tmp/data") and a UUID filename.
    fn getNewFilename(dm: *FileManager, ext: []const u8, buf: *[std.fs.MAX_PATH_BYTES]u8) ![]const u8 {
        var totalAttempts: usize = 0;

        var full_path: []const u8 = try dm.getNewFilepath(ext, buf);

        while (true) {
            var file: std.fs.File = std.fs.openFileAbsolute(full_path, std.fs.File.OpenFlags{}) catch |err| {
                switch (err) {
                    std.fs.File.OpenError.FileNotFound => {
                        return full_path;
                    },
                    else => {
                        log.err("Unknown error {s}.\n{!}", .{ full_path, err });
                        return err;
                    },
                }
            };
            file.close();

            log.debug("File {s} already exists, retrying", .{full_path});

            if (totalAttempts > 100) {
                return std.fs.File.OpenError.Unexpected;
            }

            totalAttempts += 1;

            full_path = try dm.getNewFilepath(ext, buf);
        }

        return std.fs.File.OpenError.Unexpected;
    }

    fn getAbsolutePath(self: *FileManager) []const u8 {
        return self.absolute_path;
    }

    /// It must return a unique numeric filename for the file being created. Caller is owner of array response
    fn getNewFilepath(dm: *FileManager, ext: []const u8, buf: *[std.fs.MAX_PATH_BYTES]u8) ![]const u8 {
        const n = dm.getNewFileID();
        dm.idNumber = n;
        const filename = try std.fmt.bufPrint(buf, "{s}/{}.{s}", .{ dm.getAbsolutePath(), n, ext });

        return filename;
    }

    /// It must return a unique numeric id for the file being created. Caller is owner of array response
    fn getNewFileID(dm: FileManager) u32 {
        const overflow = @addWithOverflow(dm.idNumber, 1);
        var id_number: u32 = dm.idNumber;
        if (overflow[1] == 0) {
            //all good
            id_number += 1;
        } else {
            id_number = FileManager.getRandomNumber();
        }

        return id_number;
    }

    fn getRandomNumber() u32 {
        const u64Value: u64 = @as(u64, @intCast(std.time.milliTimestamp()));

        var rnd = RndGen.init(u64Value);
        const n = rnd.random().int(u32);
        return n;
    }

    /// deinit the returned value
    pub fn getFilenames(self: *Self, ext: []const u8, alloc: Allocator) !FileEntries {
        log.debug("Reading folder: {s}", .{self.getAbsolutePath()});

        var files = std.ArrayList([]const u8).init(alloc);
        errdefer files.deinit();

        var dirIterator = try std.fs.openDirAbsolute(self.getAbsolutePath(), .{ .no_follow = true, .iterate = true });
        defer dirIterator.close();

        var iterator = dirIterator.iterate();

        const absolute_path = self.getAbsolutePath();
        while (try iterator.next()) |item| {
            if (std.mem.eql(u8, item.name[item.name.len - 3 .. item.name.len], ext)) {
                const filename = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ absolute_path, item.name });
                errdefer alloc.free(filename);
                try files.append(filename);
            }
        }

        return FileEntries{ .entries = files, .alloc = alloc };
    }
    /// deinit the returned value
    pub fn getFiles(self: *Self, ext: []const u8, alloc: Allocator) !ArrayList(File) {
        log.debug("Reading folder: {s}", .{self.getAbsolutePath()});

        var files = std.ArrayList(File).init(alloc);
        errdefer files.deinit();

        var dirIterator = try fs.openDirAbsolute(self.getAbsolutePath(), .{ .no_follow = true, .iterate = true });
        defer dirIterator.close();

        var iterator = dirIterator.iterate();

        const absolute_path = self.getAbsolutePath();
        var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;

        while (try iterator.next()) |item| {
            if (item.kind == File.Kind.file and std.mem.eql(u8, item.name[item.name.len - 3 .. item.name.len], ext)) {
                const filename = try std.fmt.bufPrint(&buf, "{s}/{s}", .{ absolute_path, item.name });
                const file = try fs.openFileAbsolute(filename, File.OpenFlags{ .mode = .read_write });
                try files.append(file);
            }
        }

        return files;
    }

    pub fn debug(dm: *Self) void {
        log.debug("\n------------\nStorage Manager\n------------\nAbsolut path:\t{s}\nId Number:\t{}\n", .{ dm.getAbsolutePath(), dm.idNumber });
    }
};

pub const FileEntries = struct {
    entries: std.ArrayList([]const u8),
    alloc: Allocator,

    pub fn deinit(self: *FileEntries) void {
        for (self.entries.items) |entry| {
            self.alloc.free(entry);
        }
        self.entries.deinit();
    }
};

test "file_manager_get_files" {
    const alloc = std.testing.allocator;

    const folder = "/tmp";

    var dm = try FileManager.init(folder, alloc);
    defer dm.deinit();

    var files = try dm.getFilenames("sst", alloc);
    defer files.deinit();

    for (files) |file_| {
        const file: File = file_;
    }
}

test "file_manager_init" {
    const alloc = std.testing.allocator;

    const folder = "/tmp";

    var dm = try FileManager.init(folder, alloc);
    defer dm.deinit();

    var f = try dm.getNewFile("sst");
    defer f.close();

    return deleteFile(f);
}

test "file_manager_getNewFile" {
    const alloc = std.testing.allocator;

    const folder = "/tmp";

    var dm = try FileManager.init(folder, alloc);
    defer dm.deinit();

    var file: File = try dm.getNewFile("sst");
    defer file.close();

    try deleteFile(file);

    var file2 = try dm.getNewFile("sst");
    defer file2.close();

    return deleteFile(file2);
}

fn deleteFile(f: File) !void {
    var buf: [std.fs.MAX_PATH_BYTES]u8 = undefined;
    const path = try std.os.getFdPath(f.handle, &buf);
    try std.fs.deleteFileAbsolute(path);
}
