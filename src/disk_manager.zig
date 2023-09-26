const std = @import("std");
const File = std.fs.File;
const MakeDirError = std.os.MakeDirError;
const OpenFileError = std.is.OpenFileError;
const RndGen = std.rand.DefaultPrng;

pub const FileData = struct {
    file: std.fs.File,
    filename: []const u8,
    alloc: std.mem.Allocator,

    pub fn deinit(self: *FileData) void {
        self.file.close();
        self.alloc.free(self.filename);
    }
};

/// Tracks the files that belong to the system.
pub const DiskManager = struct {
    const Self = @This();
    alloc: std.mem.Allocator,

    absolute_path: []const u8,
    idNumber: u32,

    pub fn init(relative: []const u8, alloc: std.mem.Allocator) !*DiskManager {
        // Create a folder to store data and continue if the folder already exists so it is opened.
        std.os.mkdir(relative, 600) catch |err| {
            _ = switch (err) {
                MakeDirError.PathAlreadyExists => void, //open the content of the folder,
                else => return err,
            };
        };

        var dm: *DiskManager = try alloc.create(DiskManager);
        dm.absolute_path = try std.fs.cwd().realpathAlloc(alloc, relative);
        dm.alloc = alloc;
        dm.idNumber = DiskManager.getRandomNumber();

        return dm;
    }

    pub fn deinit(dm: *Self) void {
        dm.alloc.free(dm.absolute_path);
        dm.alloc.destroy(dm);
    }

    /// Callers must close the file when they are done with it. Unfortunately
    /// there's not "WriterCloser" to return a writer than can be closed so the
    /// concrete File implementation must be returned.
    /// deinit the returned value when done. It will also close the file
    pub fn getNewFile(self: *DiskManager, alloc: std.mem.Allocator) !FileData {
        const filename = try self.getNewFilename(alloc);

        std.debug.print("[DM] Creating file {s}\n", .{filename});

        var file = try std.fs.createFileAbsolute(filename, std.fs.File.CreateFlags{ .read = true }); //adding reading for tests

        var fileData = FileData{
            .file = file,
            .filename = filename,
            .alloc = alloc,
        };
        return fileData;
    }

    fn getNewFilename(dm: *DiskManager, alloc: std.mem.Allocator) ![]const u8 {
        var totalAttempts: usize = 0;

        var full_path: []const u8 = try dm.getNewFilepath(alloc);

        while (true) {
            var file: std.fs.File = std.fs.openFileAbsolute(full_path, std.fs.File.OpenFlags{}) catch |err| {
                switch (err) {
                    std.fs.File.OpenError.FileNotFound => {
                        return full_path;
                    },
                    else => {
                        std.debug.print("[DM] Unknown error {s}.\n{!}\n", .{ full_path, err });
                        return err;
                    },
                }
            };
            file.close();

            std.debug.print("[DM] File {s} already exists, retrying\n", .{full_path});

            if (totalAttempts > 100) {
                alloc.free(full_path);
                return std.fs.File.OpenError.Unexpected;
            }

            totalAttempts += 1;

            alloc.free(full_path);
            full_path = try dm.getNewFilepath(alloc);
        }

        return std.fs.File.OpenError.Unexpected;
    }

    fn getAbsolutePath(self: *DiskManager) []const u8 {
        return self.absolute_path;
    }

    /// It must return a unique numeric filename for the file being created. Caller is owner of array response
    fn getNewFilepath(dm: *DiskManager, alloc: std.mem.Allocator) ![]const u8 {
        var n = dm.getNewFileID();

        var buf = try std.fmt.allocPrint(alloc, "{s}/{}.sst", .{ dm.getAbsolutePath(), n });

        return buf;
    }

    /// TODO: FIX this is not working as expected
    /// It must return a unique numeric id for the file being created. Caller is owner of array response
    fn getNewFileID(dm: *Self) u32 {
        const overflow = @addWithOverflow(dm.idNumber, 1);
        if (overflow[1] == 0) {
            //all good
            dm.idNumber += 1;
        } else {
            dm.idNumber = DiskManager.getRandomNumber();
        }

        return dm.idNumber;
    }

    fn getRandomNumber() u32 {
        var u64Value: u64 = @as(u64, @intCast(std.time.milliTimestamp()));

        var rnd = RndGen.init(u64Value);
        var n = rnd.random().int(u32);
        return n;
    }

    /// FREE the returned value
    pub fn getFilenames(self: *Self, alloc: std.mem.Allocator) ![][]const u8 {
        // Read every file from self.folder_path
        std.debug.print("[DM] Reading folder: {s}\n", .{self.getAbsolutePath()});

        var dirIterator = try std.fs.openIterableDirAbsolute(self.getAbsolutePath(), .{ .no_follow = true });
        defer dirIterator.close();

        var iterator = dirIterator.iterate();

        var names = try alloc.alloc([]const u8, 64);

        var i: usize = 0;
        const absolute_path = self.getAbsolutePath();
        while (try iterator.next()) |item| {
            if (i % 64 == 0) {
                _ = alloc.resize(names, names.len + 64);
            }

            if (std.mem.eql(u8, item.name[item.name.len - 4 .. item.name.len], ".sst")) {
                names[i] = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ absolute_path, item.name });
                i += 1;
            }
        }

        _ = alloc.resize(names, i);
        names.len = i;
        return names[0..i];
    }

    pub fn debug(dm: *Self) void {
        std.debug.print("\n------------\nDisk Manager\n------------\nAbsolut path:\t{s}\nId Number:\t{}\n\n", .{ dm.getAbsolutePath(), dm.idNumber });
    }
};

test "disk_manager_get_files" {
    var alloc = std.testing.allocator;

    var folder = "./testing";

    var dm = try DiskManager.init(folder, alloc);
    defer dm.deinit();

    var files = try dm.getFilenames(alloc);
    defer alloc.free(files);
    for (files) |file| {
        alloc.free(file);
    }
}

test "disk_manager_init" {
    var alloc = std.testing.allocator;

    var folder = "/tmp";

    var dm = try DiskManager.init(folder, alloc);
    defer dm.deinit();

    var f = try dm.getNewFile(alloc);
    defer f.deinit();
}

test "disk_manager_getNewFile" {
    var alloc = std.testing.allocator;

    var folder = "/tmp";

    var dm = try DiskManager.init(folder, alloc);
    defer dm.deinit();

    var fileData = try dm.getNewFile(alloc);
    defer fileData.deinit();
    try std.fs.deleteFileAbsolute(fileData.filename);

    var fileData2 = try dm.getNewFile(alloc);
    defer fileData2.deinit();

    return std.fs.deleteFileAbsolute(fileData2.filename);
}
