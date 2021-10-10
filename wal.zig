const std = @import("std");
const record = @import("./record.zig");
const Record = record.Record;
const RecordError = record.RecordError;

const WalError = error{
    MaxSizeReached,
} || RecordError || std.mem.Allocator.Error;

fn Wal(comptime size: usize, comptime RecordType: type) type {
    return struct {
        const Self = @This();

        current_offset: usize,
        records: usize,
        maxsize: usize,

        mem: [size]u8,

        pub fn init(self: *Self, _: *std.mem.Allocator) !*Self {
            self.current_offset = 0;
            self.records = 0;
            self.maxsize = 1000 * size;
            // self.mem = try allocator.alloc(u8, size);

            return self;
        }

        pub fn add(self: *Self, bytes: []u8) WalError!void {
            // Check if there's available space in the WAL
            if (self.mem.len - self.current_offset < bytes.len) {
                return WalError.MaxSizeReached;
            }

            for (bytes) |b, i| {
                self.mem[self.current_offset + i] = b;
            }

            //Write operation success
            self.records += 1;
            self.current_offset += bytes.len;
        }

        pub fn add_record(self: *Self, r: *const RecordType, alloc: *std.mem.Allocator) WalError!void {
            const record_length = r.len();

            // Check if there's available space in the WAL
            if (self.mem.len - self.current_offset < record_length) {
                return WalError.MaxSizeReached;
            }

            var buf: []u8 = try alloc.alloc(u8, record_length);
            defer alloc.free(buf);
            try r.bytes(buf);

            return self.add(buf);
        }

        // TODO Find returns first ocurrence when it should be returning last ocurrence found which
        // is the most recent
        pub fn find(self: *Self, key_to_find: []u8) WalError!?RecordType {
            var offset: usize = 0;
            var r: RecordType = undefined;

            while (offset < self.current_offset) {
                r = RecordType.read_record(self.mem[offset..]);
                if (std.mem.eql(u8, r.key, key_to_find)) {
                    return r;
                }
                offset += r.len();
            }

            return null;
        }
    };
}

test "add bytes" {
    var alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var hello = "hello".*;
    var world = "world".*;

    const allocator = &arena.allocator;
    var temp = try allocator.create(Wal(100, u8));

    var wal = try temp.init(allocator);
    try wal.add(hello[0..]);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_offset == hello.len);
    try std.testing.expect(wal.mem.len == 100);
    try std.testing.expect(std.mem.eql(u8, wal.mem[0..wal.current_offset], hello[0..]));

    try wal.add(world[0..]);
    try std.testing.expect(wal.records == 2);
    try std.testing.expect(wal.current_offset == hello.len + world.len);
    try std.testing.expect(wal.mem.len == 100);
    try std.testing.expect(std.mem.eql(u8, wal.mem[0..wal.current_offset], "helloworld"));
}

test "add record" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(std.mem.page_size * 1000, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var key = "hello".*;
    var value = "world".*;
    const r = Record(u32, u64){
        .key = key[0..],
        .value = value[0..],
    };
    try wal.add_record(&r, alloc);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_offset == r.len());
    try std.testing.expectStringEndsWith(wal.mem[0..22], "helloworld");
}

test "max size reached" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(10, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var key = "hello".*;
    var value = "world".*;
    const r = Record(u32, u64){
        .key = key[0..],
        .value = value[0..],
    };

    if (wal.add_record(&r, alloc)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }

    var buf: [22]u8 = undefined;
    try r.bytes(buf[0..]);

    if (wal.add(buf[0..])) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }
}

test "find a key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(100, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var key = "hello".*;
    var value = "world".*;
    const r = Record(u32, u64){
        .key = key[0..],
        .value = value[0..],
    };
    try wal.add_record(&r, alloc);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_offset == r.len());
    try std.testing.expectStringEndsWith(wal.mem[0..22], "helloworld");

    const maybe_record = try wal.find(key[0..]);
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, value[0..]));

    var unknown_key = "unknokwn".*;

    const unkonwn_record = try wal.find(unknown_key[0..]);
    try std.testing.expect(unkonwn_record == null);
}
