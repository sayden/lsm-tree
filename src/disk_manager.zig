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
    absolute_path: []u8,
    alloc: std.mem.Allocator,

    pub fn init(relative: []u8, alloc: std.mem.Allocator) !*DiskManager {
        std.log.debug("using folder '{s}'\n", .{relative});

        // Create a folder to store data and continue if the folder already exists so it is opened.
        std.os.mkdir(relative, 600) catch |err| {
            _ = switch (err) {
                MakeDirError.PathAlreadyExists => void, //open the content of the folder,
                else => return err,
            };
        };

        var absolute = try std.fs.cwd().realpathAlloc(alloc, relative);

        var dm: *DiskManager = try alloc.create(DiskManager);
        dm.absolute_path = absolute;
        dm.alloc = alloc;

        return dm;
    }

    pub fn deinit(self: *DiskManager) void {
        self.alloc.free(self.absolute_path);
        self.alloc.destroy(self);
    }

    // No deallocations are needed.
    //
    // Callers must close the file when they are done with it. Unfortunately
    // there's not "WriterCloser" to return a writer than can be closed so the
    // concrete File implementation must be returned.
    pub fn getNewFile(self: *DiskManager, allocator: std.mem.Allocator) !FileData {
        var totalAttempts: usize = 0;
        var file_id: []u8 = try self.getNewFileID(allocator);

        var full_path: []u8 = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{ self.absolute_path, file_id });

        var file: ?std.fs.File = null;
        while (true) {
            file = std.fs.openFileAbsolute(full_path, std.fs.File.OpenFlags{}) catch |err| {
                switch (err) {
                    std.fs.File.OpenError.FileNotFound => {
                        allocator.free(file_id);
                        break;
                    },
                    else => {
                        std.debug.print("Unknown error {s}.\n{!}\n", .{ full_path, err });
                        file.?.close();
                        return err;
                    },
                }
            };
            file.?.close();

            std.debug.print("File {s} already exists, retrying\n", .{full_path});

            if (totalAttempts > 100) {
                allocator.free(file_id);
                allocator.free(full_path);
                return std.fs.File.OpenError.Unexpected;
            }

            totalAttempts += 1;

            allocator.free(file_id);
            file_id = try self.getNewFileID(allocator);

            allocator.free(full_path);
            full_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{ self.absolute_path, file_id });
        }

        std.debug.print("\nCreating file {s}\n", .{full_path});

        var file2 = try std.fs.createFileAbsolute(full_path, std.fs.File.CreateFlags{});

        return FileData{
            .file = file2,
            .filename = full_path,
            .alloc = allocator,
        };
    }

    /// It must return a unique numeric id for the file being created. Caller is owner of array response
    fn getNewFileID(_: *DiskManager, allocator: std.mem.Allocator) ![]u8 {
        var u64Value: u64 = @as(u64, @intCast(std.time.milliTimestamp()));

        var rnd = RndGen.init(u64Value);
        var n = rnd.random().int(u32);

        var buf = try std.fmt.allocPrint(allocator, "{}", .{n});

        return buf;
    }

    /// FREE the returned value
    pub fn getFilenames(self: *DiskManager, alloc: std.mem.Allocator) ![][]u8 {
        // Read every file from self.folder_path
        std.debug.print("Reading folder: {s}\n", .{self.absolute_path});

        var dirIterator = try std.fs.openIterableDirAbsolute(self.absolute_path, .{ .no_follow = true });

        var iterator = dirIterator.iterate();

        var names = try alloc.alloc([]u8, 64);

        var i: usize = 0;
        while (try iterator.next()) |item| : (i += 1) {
            if (i % 64 == 0) {
                _ = alloc.resize(names, names.len + 64);
            }

            if (std.mem.eql(u8, item.name[item.name.len - 4 .. item.name.len], ".sst")) {
                names[i] = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.absolute_path, item.name });
            }
        }
        dirIterator.close();

        _ = alloc.resize(names, i);

        return names[0..i];
    }
};

test "disk_manager_get_files" {
    var alloc = std.testing.allocator;

    var folder = try alloc.dupe(u8, "./testing");
    defer alloc.free(folder);

    var dm = try DiskManager.init(folder, alloc);
    defer dm.deinit();

    var files = try dm.getFilenames(alloc);
    defer alloc.free(files);
    for (files) |file| {
        alloc.free(file);
    }
}
