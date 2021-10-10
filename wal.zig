const std = @import("std");

fn Wal(comptime size: usize) type {
    return struct {
        const Self = @This();

        current_offset: usize,
        records: usize,
        maxsize: usize,

        mem: []u8,

        // Default values doesn't work here so I have to do an init function
        pub fn init(self: *Self, allocator: *std.mem.Allocator) !*Self {
            self.current_offset = 0;
            self.records = 0;
            self.maxsize = 1000 * size;
            self.mem = try allocator.alloc(u8, size);

            return self;
        }

        pub fn add(self: *Self, bytes: []const u8) WalError!void {
            for (bytes) |b, i| {
                self.mem[self.current_offset + i] = b;
            }

            //Write operation success
            self.records += 1;
            self.current_offset += bytes.len;
        }
    };
}

const WalError = error{
    MaxSizeReached,
};

test "test add" {
    var page = std.heap.page_allocator;
    var arena = std.heap.ArenaAllocator.init(page);
    defer arena.deinit();

    const allocator = &arena.allocator;
    var temp = try allocator.create(Wal(100));

    var wal = try temp.init(allocator);
    try wal.add("hello");

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_offset == "hello".len);
    try std.testing.expect(wal.mem.len == 100);
    try std.testing.expect(std.mem.eql(u8, wal.mem[0..wal.current_offset], "hello"));

    try wal.add("world");
    try std.testing.expect(wal.records == 2);
    try std.testing.expect(wal.current_offset == "hello".len + "world".len);
    try std.testing.expect(wal.mem.len == 100);
    try std.testing.expect(std.mem.eql(u8, wal.mem[0..wal.current_offset], "helloworld"));
}
