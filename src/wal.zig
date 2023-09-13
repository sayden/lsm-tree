const std = @import("std");
const rec = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = rec.Record;
const RecordError = rec.RecordError;
const expect = std.testing.expect;
const lsmtree = @import("./main.zig");
const record_serializer = @import("./record_serializer.zig");

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

        // Start a new in memory WAL using the provided allocator
        // REMEMBER to call `deinit()` once you are done with the iterator,
        // for example after persisting it to disk.
        // CALL `deinitCascade()` if you want also to free all the records
        // stored in it.
        pub fn init(allocator: *std.mem.Allocator) !*Self {
            var wal = try allocator.create(Wal(size_in_bytes));

            wal.current_size = 0;
            wal.total_records = 0;
            wal.max_size = size_in_bytes;
            wal.allocator = allocator;
            wal.mem = try allocator.alloc(*Record, size_in_bytes / Record.minimum_size());

            return wal;
        }

        // Add a new record in order to the in memory WAL
        pub fn add_record(self: *Self, r: *Record) WalError!void {
            const record_size: usize = r.bytesLen();

            // Check if there's available space in the WAL
            if ((self.current_size + record_size > size_in_bytes) or (self.total_records >= self.mem.len)) {
                return WalError.MaxSizeReached;
            }

            self.mem[self.total_records] = r;
            self.total_records += 1;
            self.current_size += record_size;
        }

        // Compare the provided keys with the ones in memory and
        // returns the last record that is found (or none if none is found)
        pub fn find(self: *Self, key_to_find: []const u8) ?*Record {
            var iter = self.backwards_iterator();
            while (iter.next()) |r| {
                if (std.mem.eql(u8, r.key, key_to_find)) {
                    return r;
                }
            }

            return null;
        }

        // Sort the list of records in lexicographical order
        pub fn sort(self: *Self) void {
            std.sort.sort(*Record, self.mem[0..self.total_records], {}, lexicographical_compare);
        }

        // Creates a forward iterator to go through the wal.
        pub fn iterator(self: *Self) RecordIterator {
            const iter = RecordIterator.init(self.mem[0..self.total_records]);

            return iter;
        }

        // Creates a forward iterator to go through the wal.
        pub fn backwards_iterator(self: *Self) RecordBackwardIterator {
            const iter = RecordBackwardIterator.init(self.mem[0..self.total_records]);

            return iter;
        }

        // Frees the array that contains the Records but leaving them untouched
        pub fn deinit(self: *Self) void {
            self.allocator.free(self.mem);
            self.allocator.destroy(self);
        }

        // Frees the array that contains the Records and the Records themselves.
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

        const RecordBackwardIterator = struct {
            pos: usize = 0,
            records: []*Record,
            finished: bool = false,

            pub fn init(records: []*Record) RecordBackwardIterator {
                return RecordBackwardIterator{
                    .records = records,
                    .pos = records.len - 1,
                };
            }

            pub fn next(self: *RecordBackwardIterator) ?*Record {
                if (self.pos == 0 and self.finished) {
                    return null;
                }

                const r = self.records[self.pos];
                if (self.pos != 0) {
                    self.pos -= 1;
                } else {
                    self.finished = true;
                }

                return r;
            }
        };
    };
}

test "wal.iterator backwards" {
    var alloc = std.testing.allocator;
    var wal = try Wal(100).init(&alloc);
    defer wal.deinit_cascade();

    try wal.add_record(try Record.init("hell0", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell2", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell0", "world0", Op.Create, &alloc));

    var iter = wal.backwards_iterator();

    var next = iter.next().?;
    try std.testing.expectEqualStrings("world0", next.value);
}

test "wal.iterator" {
    var alloc = std.testing.allocator;
    var wal = try Wal(100).init(&alloc);
    defer wal.deinit_cascade();

    try wal.add_record(try Record.init("hell0", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell2", "world", Op.Create, &alloc));
    try wal.add_record(try Record.init("hell0", "world0", Op.Create, &alloc));

    var iter = wal.iterator();

    _ = iter.next().?;
    _ = iter.next().?;
    _ = iter.next().?;
    var record = iter.next().?;
    try std.testing.expectEqualStrings("world0", record.value);
}

test "wal.lexicographical_compare" {
    var alloc = std.testing.allocator;

    var r1 = try Record.init("hello", "world", Op.Create, &alloc);
    var r2 = try Record.init("hellos", "world", Op.Create, &alloc);

    defer r1.deinit();
    defer r2.deinit();

    try std.testing.expect(!Wal(100).lexicographical_compare({}, r2, r1));
}

test "wal.sort a wal" {
    var alloc = std.testing.allocator;

    var wal = try Wal(100).init(&alloc);
    defer wal.deinit_cascade();

    var r1 = try Record.init("hellos", "world", Op.Create, &alloc);
    var r2 = try Record.init("hello", "world", Op.Create, &alloc);

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

    var wal = try Wal(100).init(&alloc);
    defer wal.deinit_cascade();

    var r = try Record.init("hello", "world", Op.Create, &alloc);
    try wal.add_record(r);

    try std.testing.expect(wal.total_records == 1);
    try std.testing.expect(wal.current_size == r.bytesLen());
}

test "wal.max size reached" {
    var alloc = std.testing.allocator;

    var wal = try Wal(23).init(&alloc);
    defer wal.deinit_cascade();

    try std.testing.expectEqual(@as(usize, 1), wal.mem.len);
    var r = try Record.init("hello", "world", Op.Create, &alloc);

    try std.testing.expectEqual(@as(usize, 21), r.bytesLen());

    wal.add_record(r) catch unreachable;

    var buf: [24]u8 = undefined;
    _ = try record_serializer.toBytes(r, buf[0..]);

    if (wal.add_record(r)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }
}

test "wal.find a key" {
    var alloc = std.testing.allocator;
    var wal = try Wal(100).init(&alloc);
    defer wal.deinit_cascade();

    var r1 = try Record.init("hello", "world", Op.Create, &alloc);
    var r2 = try Record.init("hello", "world1", Op.Create, &alloc);
    var r3 = try Record.init("hello", "world3", Op.Create, &alloc);
    var r4 = try Record.init("hello1", "world", Op.Create, &alloc);

    try wal.add_record(r1);
    try wal.add_record(r2);
    try wal.add_record(r3);
    try wal.add_record(r4);

    try std.testing.expect(wal.total_records == 4);

    const maybe_record = wal.find(r1.key[0..]);
    //we expect value of r3 as it's the last inserted using key `hello`
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, r3.value[0..]));

    const unkonwn_record = wal.find("unknokwn");
    try std.testing.expect(unkonwn_record == null);
}

test "wal.size on memory" {
    try std.testing.expectEqual(48, @sizeOf(Wal(100)));
}
