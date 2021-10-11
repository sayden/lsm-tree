const std = @import("std");

const DiskManager = struct {
    const Self = @This();
    path: []const u8,

    pub fn init(p: []u8)Self{
        return DiskManager{
            .path = p,
        };
    }

    // callers must close the file when they are done with it. Unfortunately
    // there's not "WriterCloser" to return a writer than can be closed so the
    // concrete File implementation must be returned. TODO build an wrapper
    // to allow using a File like a WriterCloser interface to allow switching
    // implementations (to transparently do compression, for example).
    fn new_sst_file(self: *Self) !std.fs.File {
        // Paths can't be longer than 512 bytes, which is a hard limitation ATM.
        var buf: [512]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        var allocator = &fba.allocator;

        const file_id: []u8 = try self.get_new_file_id(allocator);
        defer allocator.free(file_id);
        
        var full_path = try std.fmt.allocPrint(allocator, "{s}/{s}.sst", .{self.path,file_id});
        defer allocator.free(full_path);
        
        var f = try std.fs.createFileAbsolute(full_path, std.fs.File.CreateFlags{.exclusive=true});
        return f;
    }

    // pub fn write_wal(self: *Self) !void {

    // }


    // TODO it must return a unique numeric id for the file being created.
    fn get_new_file_id(_: *Self, allocator: *std.mem.Allocator) ![]u8 {
        var buf = try std.fmt.allocPrint(allocator, "{d}", .{1});
        
        return buf;
    }
};

test "disk_manager.get new file id" {
    var dm = DiskManager{.path= "/tmp"};
    
    var f = try dm.get_new_file_id(std.testing.allocator);
    defer std.testing.allocator.free(f);
}

test "disk_manager.create file" {
    var dm = DiskManager{.path= "/tmp"};
    var f = try dm.new_sst_file();
    defer f.close();
    _ = f;
}