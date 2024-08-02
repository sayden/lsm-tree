const toml = @import("./src/pkg/zig-toml/src/toml.zig");
const std = @import("std");

pub fn Config() type {
    return struct {
        const Self = @This();

        dataPath: []u8,
        alloc: std.mem.Allocator,

        pub fn deinit(self: *Self) void {
            self.alloc.free(self.dataPath);
        }

        pub fn read_config(alloc: std.mem.Allocator) !Self {
            const cfgPath = "config.toml";

            var parser = try toml.parseFile(alloc, cfgPath);
            defer parser.deinit();

            const table = try parser.parse();
            defer table.deinit();

            const data = table.keys.get("data") orelse unreachable;
            const config = Self{ .dataPath = try alloc.dupe(u8, data.String), .alloc = alloc };

            return config;
        }
    };
}

pub fn main() !void {
    var Gpa = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    const allocator = Gpa.allocator();

    defer {
        const leaked = Gpa.deinit();
        switch (leaked) {
            .ok => std.debug.print("No leaks\n", .{}),
            .leak => unreachable,
        }
    }

    var config = try Config().read_config(allocator);
    defer config.deinit();
    std.debug.print("{s}\n", .{config.dataPath});
}
