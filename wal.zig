const std = @import("std");
const record = @import("./record.zig");
const Record = record.Record;
const RecordError = record.RecordError;

const WalError = error{
    MaxSizeReached,
} || RecordError || std.mem.Allocator.Error;

fn Wal(comptime size_in_bytes: usize, comptime RecordType: type) type {
    return struct {
        const Self = @This();

        current_size: usize,
        max_size: usize,

        records: usize,
        mem: []RecordType,

        pub fn init(self: *Self, allocator: *std.mem.Allocator) !*Self {
            self.current_size = 0;
            self.records = 0;
            self.max_size = size_in_bytes;
            self.mem = try allocator.alloc(RecordType, size_in_bytes / RecordType.minimum_size());

            return self;
        }

        pub fn add_record(self: *Self, r: RecordType) WalError!void {
            const record_size = r.size();

            // Check if there's available space in the WAL
            if (self.current_size + record_size > self.max_size or self.records >= self.mem.len - 1) {
                return WalError.MaxSizeReached;
            }

            self.mem[self.records] = r;
            self.records += 1;
            self.current_size += record_size;
        }

        // TODO Find returns first ocurrence when it should be returning last ocurrence found which
        // is the most recent
        pub fn find(self: *Self, key_to_find: []u8) WalError!?RecordType {
            for (self.mem) |r| {
                if (std.mem.eql(u8, r.key, key_to_find)) {
                    return r;
                }
            }

            return null;
        }

        pub fn sort(self: *Self) void {
            std.sort.sort(RecordType, self.mem[0..self.records], {}, lexicographical_compare);
        }

        fn lexicographical_compare(_: void, lhs: RecordType, rhs: RecordType) bool {
            const smaller_size: usize = if (lhs.key.len > rhs.key.len) rhs.key.len else lhs.key.len;

            var i: usize = 0;
            while (i < smaller_size) {
                if (lhs.key[i] == rhs.key[i]) {
                    i += 1;
                    continue;
                } else if (lhs.key[i] > rhs.key[i]) {
                    return true;
                } else {
                    return false;
                }
            }

            return false;
        }
    };
}

test "sort a wal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(100, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var key1 = "hellos".*;
    var value1 = "world".*;
    const r = Record(u32, u64){
        .key = key1[0..],
        .value = value1[0..],
    };
    try wal.add_record(r);

    var key2 = "hello".*;
    var value2 = "world".*;
    const r2 = Record(u32, u64){
        .key = key2[0..],
        .value = value2[0..],
    };
    try wal.add_record(r2);

    std.debug.print("\nKey ({d}): {s}\n", .{ 0, wal.mem[0].key });
    std.debug.print("Key ({d}): {s}\n", .{ 1, wal.mem[1].key });

    try std.testing.expect(std.mem.eql(u8, wal.mem[0].key, r.key));
    try std.testing.expect(std.mem.eql(u8, wal.mem[1].key, r2.key));

    wal.sort();

    std.debug.print("\nKey ({d}): {s}\n", .{ 0, wal.mem[0].key });
    std.debug.print("Key ({d}): {s}\n", .{ 1, wal.mem[1].key });

    try std.testing.expect(std.mem.eql(u8, wal.mem[0].key, r.key));
    try std.testing.expect(std.mem.eql(u8, wal.mem[1].key, r2.key));
}

test "add record" {
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
    try wal.add_record(r);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_size == r.size());
}

test "max size reached" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(23, Record(u32, u64)));
    var wal = try temp.init(alloc);

    try std.testing.expect(wal.mem.len == 1);

    var key = "hello".*;
    var value = "world".*;
    const r = Record(u32, u64){
        .key = key[0..],
        .value = value[0..],
    };

    if (wal.add_record(r)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }

    var buf: [24]u8 = undefined;
    try r.bytes(buf[0..]);

    if (wal.add_record(r)) |_| unreachable else |err| {
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
    try wal.add_record(r);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_size == r.size());

    const maybe_record = try wal.find(key[0..]);
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, value[0..]));

    var unknown_key = "unknokwn".*;

    const unkonwn_record = try wal.find(unknown_key[0..]);
    try std.testing.expect(unkonwn_record == null);
}
