const std = @import("std");
const rec = @import("./record.zig");
const Record = rec.Record;
const RecordError = rec.RecordError;
const expect = std.testing.expect;

pub const WalError = error{
    MaxSizeReached,
} || RecordError || std.mem.Allocator.Error;

pub fn RecordTypeIterator(comptime WalType: type, comptime RecordType: type) type {
    return struct {
        const Self = @This();

        pos: usize = 0,
        wal: *WalType,

        pub fn init(wal: *WalType) Self {
            return Self{
                .wal = wal,
            };
        }

        pub fn next(self: *Self) ?RecordType {
            if (self.pos == self.wal.records) {
                return null;
            }

            const r = self.wal.mem[self.pos];
            self.pos += 1;
            return r;
        }
    };
}

pub fn Wal(comptime size_in_bytes: usize, comptime RecordType: type) type {
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

        pub fn add_record(self: *Self, r: *RecordType) WalError!void {
            const record_size: usize = r.size();

            // Check if there's available space in the WAL
            if (self.current_size + record_size > self.max_size or self.records >= self.mem.len - 1) {
                return WalError.MaxSizeReached;
            }

            self.mem[self.records] = r.*;
            self.records += 1;
            self.current_size += record_size;
        }

        // TODO Find returns first ocurrence when it should be returning last ocurrence found which
        // is the most recent
        pub fn find(self: *Self, key_to_find: []u8) ?RecordType {
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

            // if all chars were equal, return shortest as true
            return (lhs.key.len < rhs.key.len);
        }

        pub fn iterator(self: *Self) RecordTypeIterator(Self, RecordType) {
            const iter = RecordTypeIterator(Self, RecordType).init(self);

            return iter;
        }
    };
}

test "wal.iterator" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(100, Record(u32, u64)));
    const wal = try temp.init(alloc);

    var r0 = try rec.mockRecord("hell0", "world", alloc);
    try wal.add_record(&r0);
    var r1 = try rec.mockRecord("hell1", "world", alloc);
    try wal.add_record(&r1);
    var r3 = try rec.mockRecord("hell2", "world", alloc);
    try wal.add_record(&r3);

    var iter = wal.iterator();

    var total: usize = 0;
    while (iter.next()) |record| {
        try std.testing.expectEqualSlices(u8, "world", record.value);
        total += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), total);
}

test "wal.lexicographical_compare" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;

    var r1 = try rec.mockRecord("hello", "world", allocator);
    var r2 = try rec.mockRecord("hellos", "world", allocator);

    try std.testing.expect(!Wal(100, Record(u32, u64)).lexicographical_compare({}, r2, r1));
}

test "wal.sort a wal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var alloc = &arena.allocator;

    var temp = try alloc.create(Wal(100, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var r1 = try rec.mockRecord("hellos", "world", alloc);
    try wal.add_record(&r1);

    var r2 = try rec.mockRecord("hello", "world", alloc);
    try wal.add_record(&r2);

    try std.testing.expectEqualSlices(u8, wal.mem[0].key, r1.key);
    try std.testing.expectEqualSlices(u8, wal.mem[1].key, r2.key);

    wal.sort();

    try std.testing.expectEqualSlices(u8, wal.mem[1].key, r1.key);
    try std.testing.expectEqualSlices(u8, wal.mem[0].key, r2.key);
}

test "wal.add record" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var alloc = &arena.allocator;

    var temp = try alloc.create(Wal(100, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var r = try rec.mockRecord("hello", "world", alloc);
    try wal.add_record(&r);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_size == r.size());
}

test "wal.max size reached" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var alloc = &arena.allocator;

    var temp = try alloc.create(Wal(23, Record(u32, u64)));
    var wal = try temp.init(alloc);

    try std.testing.expect(wal.mem.len == 1);

    var r = try rec.mockRecord("hello", "world", alloc);
    if (wal.add_record(&r)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }

    var buf: [24]u8 = undefined;
    try r.bytes(buf[0..]);

    if (wal.add_record(&r)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }
}

test "wal.find a key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = &arena.allocator;

    var temp = try alloc.create(Wal(100, Record(u32, u64)));
    var wal = try temp.init(alloc);

    var r = try rec.mockRecord("hello", "world", alloc);
    try wal.add_record(&r);

    try std.testing.expect(wal.records == 1);
    try std.testing.expect(wal.current_size == r.size());

    const maybe_record = wal.find(r.key[0..]);
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, r.value[0..]));

    var unknown_key = "unknokwn".*;

    const unkonwn_record = wal.find(unknown_key[0..]);
    try std.testing.expect(unkonwn_record == null);
}
