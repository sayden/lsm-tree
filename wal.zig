const std = @import("std");
const rec = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = rec.Record;
const RecordError = rec.RecordError;
const expect = std.testing.expect;

pub const WalError = error{
    MaxSizeReached,
} || RecordError || std.mem.Allocator.Error;

pub fn Wal(comptime size_in_bytes: usize) type {
    return struct {
        const Self = @This();

        current_size: usize,
        max_size: usize,

        total_records: usize,
        mem: []*Record,

        allocator: *std.mem.Allocator,

        pub fn init(allocator: *std.mem.Allocator) !*Self {
            var wal = try allocator.create(Wal(size_in_bytes));

            wal.current_size = 0;
            wal.total_records = 0;
            wal.max_size = size_in_bytes;
            wal.allocator = allocator;
            wal.mem = try allocator.alloc(*Record, size_in_bytes / Record.minimum_size());

            return wal;
        }

        pub fn add_record(self: *Self, r: *Record) WalError!void {
            const record_size: usize = r.size();

            // Check if there's available space in the WAL
            if ((self.current_size + record_size > self.max_size) or (self.total_records >= self.mem.len)) {
                return WalError.MaxSizeReached;
            }

            self.mem[self.total_records] = r;
            self.total_records += 1;
            self.current_size += record_size;
        }

        // TODO Find returns first ocurrence when it should be returning last ocurrence found which
        // is the most recent
        pub fn find(self: *Self, key_to_find: []const u8) ?*Record {
            var iter = self.iterator();
            while (iter.next()) |r| {
                if (std.mem.eql(u8, r.key, key_to_find)) {
                    return r;
                }
            }

            return null;
        }

        pub fn sort(self: *Self) void {
            std.sort.sort(*Record, self.mem[0..self.total_records], {}, lexicographical_compare);
        }

        pub fn iterator(self: *Self) RecordIterator {
            const iter = RecordIterator.init(self.mem[0..self.total_records]);

            return iter;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.mem);
            self.allocator.destroy(self);
        }

        pub fn deinit_cascade(self: *Self) void {
            var iter = self.iterator();
            while (iter.next()) |r| {
                r.deinit();
            }
            self.deinit();
        }

        fn lexicographical_compare(_: void, lhs: *Record, rhs: *Record) bool {
            const smaller_size: usize = if (lhs.key.len > rhs.key.len) rhs.key.len else lhs.key.len;

            var i: usize = 0;
            while (i < smaller_size) {
                if (lhs.key[i] == rhs.key[i]) {
                    i += 1;
                    continue;
                } else if (lhs.key[i] > rhs.key[i]) {
                    return false;
                } else {
                    return true;
                }
            }

            // if all chars were equal, return shortest as true
            return (lhs.key.len < rhs.key.len);
        }

        const RecordIterator = struct {
            pos: usize = 0,
            records: []*Record,

            pub fn init(records: []*Record) RecordIterator {
                return RecordIterator{
                    .records = records,
                };
            }

            pub fn next(self: *RecordIterator) ?*Record {
                if (self.pos == self.records.len) {
                    return null;
                }

                const r = self.records[self.pos];
                self.pos += 1;
                return r;
            }
        };
    };
}

test "wal.iterator" {
    var alloc = std.testing.allocator;
    var wal = try Wal(100).init(std.testing.allocator);
    defer wal.deinit_cascade();

    try wal.add_record(try Record.init("hell0", "world", Op.Create, alloc));
    try wal.add_record(try Record.init("hell1", "world", Op.Create, alloc));
    try wal.add_record(try Record.init("hell2", "world", Op.Create, alloc));

    var iter = wal.iterator();

    var total: usize = 0;
    while (iter.next()) |record| {
        try std.testing.expectEqualSlices(u8, "world", record.value);
        total += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), total);
}

test "wal.lexicographical_compare" {
    var alloc = std.testing.allocator;

    var r1 = try Record.init("hello", "world", Op.Create, alloc);
    var r2 = try Record.init("hellos", "world", Op.Create, alloc);

    defer r1.deinit();
    defer r2.deinit();

    try std.testing.expect(!Wal(100).lexicographical_compare({}, r2, r1));
}

test "wal.sort a wal" {
    var alloc = std.testing.allocator;

    var wal = try Wal(100).init(std.testing.allocator);
    defer wal.deinit_cascade();

    var r1 = try Record.init("hellos", "world", Op.Create, alloc);
    var r2 = try Record.init("hello", "world", Op.Create, alloc);

    try wal.add_record(r1);
    try wal.add_record(r2);

    try std.testing.expectEqualSlices(u8, wal.mem[0].key, r1.key);
    try std.testing.expectEqualSlices(u8, wal.mem[1].key, r2.key);

    wal.sort();

    try std.testing.expectEqualSlices(u8, wal.mem[1].key, r1.key);
    try std.testing.expectEqualSlices(u8, wal.mem[0].key, r2.key);
}

test "wal.add record" {
    var alloc = std.testing.allocator;

    var wal = try Wal(100).init(alloc);
    defer wal.deinit_cascade();

    var r = try Record.init("hello", "world", Op.Create, alloc);
    try wal.add_record(r);

    try std.testing.expect(wal.total_records == 1);
    try std.testing.expect(wal.current_size == r.size());
}

test "wal.max size reached" {
    var alloc = std.testing.allocator;

    var wal = try Wal(23).init(alloc);
    defer wal.deinit_cascade();

    try std.testing.expectEqual(@as(usize, 1), wal.mem.len);
    var r = try Record.init("hello", "world", Op.Create, alloc);

    try std.testing.expectEqual(@as(usize, 21), r.size());

    wal.add_record(r) catch unreachable;

    var buf: [24]u8 = undefined;
    _ = try r.bytes(buf[0..]);

    if (wal.add_record(r)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }
}

test "wal.find a key" {
    var wal = try Wal(100).init(std.testing.allocator);
    defer wal.deinit_cascade();

    var r = try Record.init("hello", "world", Op.Create, std.testing.allocator);

    try wal.add_record(r);

    try std.testing.expect(wal.total_records == 1);
    try std.testing.expect(wal.current_size == r.size());

    const maybe_record = wal.find(r.key[0..]);
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, r.value[0..]));

    const unkonwn_record = wal.find("unknokwn");
    try std.testing.expect(unkonwn_record == null);
}
