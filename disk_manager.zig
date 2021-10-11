const std = @import("std");

const DiskManager = struct {
    const Self = @This();
    path: []u8,

    pub fn path(p: []u8)Self{
        return DiskManager{
            .path = p,
        };
    }

    fn new_sst_file(self: *Self, id: usize) void!std.fs.File{
        //TODO
        
    }
};