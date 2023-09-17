const std = @import("std");
const rec = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = rec.Record;
const RecordError = rec.RecordError;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const HeaderPkg = @import("./header.zig");
const Header = HeaderPkg.Header;
const lsmtree = @import("./main.zig");
const Pointer = @import("./pointer.zig").Pointer;
const DiskManager = @import("./disk_manager.zig").DiskManager;

pub const WalError = error{
    MaxSizeReached,
    CantCreateRecord,
} || RecordError || std.mem.Allocator.Error;

pub fn MemoryWal(comptime size_in_bytes: usize) type {
    return struct {
        const Self = @This();

        header: Header,

        max_size: usize,

        mem: []*Record,
        current_mem_index: usize,

        // Pointer size must be pre-calculated before persisting because the header is the first
        // thing written on files. Changing header to EOF would fix this problem
        pointers_size: usize,

        allocator: *std.mem.Allocator,

        // Start a new in memory WAL using the provided allocator
        // REMEMBER to call `deinit()` once you are done with the iterator,
        // for example after persisting it to disk.
        // CALL `deinitCascade()` if you want also to free all the records
        // stored in it.
        pub fn init(allocator: *std.mem.Allocator) !*Self {
            var wal = try allocator.create(MemoryWal(size_in_bytes));

            wal.max_size = size_in_bytes;
            wal.allocator = allocator;
            wal.mem = try allocator.alloc(*Record, size_in_bytes / Record.minimum_size());
            wal.current_mem_index = 0;
            wal.header = Header.init();
            wal.pointers_size = 0;

            return wal;
        }

        // Frees the array that contains the Records but leaving them untouched
        pub fn deinit(self: *Self) void {
            var iter = self.iterator();
            while (iter.next()) |r| {
                r.deinit();
            }
            self.allocator.free(self.mem);
            self.allocator.destroy(self);
        }

        pub fn appendKv(self: *Self, k: []const u8, v: []const u8) WalError!void {
            var r = try Record.init(k, v, Op.Create, self.allocator);
            try self.append(r);
        }

        // Add a new record in order to the in memory WAL
        pub fn append(self: *Self, r: *Record) WalError!void {
            const record_size: usize = r.bytesLen();

            // Check if there's available space in the WAL
            if ((self.recordsSize() + record_size > size_in_bytes) or (self.header.total_records >= self.mem.len)) {
                return WalError.MaxSizeReached;
            }

            self.mem[self.header.total_records] = r;
            self.header.total_records += 1;
            self.header.first_pointer_offset += record_size;
            self.header.records_size += record_size;
            self.pointers_size += r.expectedPointerSize();
        }

        // Compare the provided key with the ones in memory and
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
            std.sort.insertion(*Record, self.mem[0..self.header.total_records], {}, lexicographical_compare);
        }

        // Creates a forward iterator to go through the wal.
        pub fn iterator(self: *Self) RecordIterator {
            const iter = RecordIterator.init(self.mem[0..self.header.total_records]);

            return iter;
        }

        // Creates a forward iterator to go through the wal.
        pub fn backwards_iterator(self: *Self) RecordBackwardIterator {
            const iter = RecordBackwardIterator.init(self.mem[0..self.header.total_records]);

            return iter;
        }

        /// writes into provided file the contents of the sst. Including pointers
        /// and the header. The allocator is required as a temporary buffer for
        /// data but it's freed inside the function
        ///
        /// Format is as following:
        /// Header
        /// Record
        /// Record
        /// Record
        /// ...
        /// Pointer
        /// Pointer
        /// Pointer
        /// ...
        /// EOF
        pub fn toBytesWriter(self: *Self, writer: anytype) !usize {
            var iter = self.iterator();

            var record_total_bytes: usize = 0;
            _ = record_total_bytes;

            // Write the header
            _ = try self.header.toBytesWriter(writer);

            // Write only records
            while (iter.next()) |record| {
                // record
                _ = try record.toBytesWriter(writer);
            }

            return writer.context.getPos();
        }

        pub fn persist(self: *Self, file: *std.fs.File) !usize {
            var writer = file.writer();

            // Headers and records are written
            var written = try self.toBytesWriter(writer);
            var bytes_written: usize = 0;

            var iter = self.iterator();
            var tail_offset = written;

            // Write pointers now
            while (iter.next()) |record| {
                // pointer
                var pointer_ = Pointer{
                    .op = record.op,
                    .key = record.key,
                    .byte_offset = tail_offset,
                    .allocator = self.allocator,
                };

                bytes_written = try pointer_.toBytesWriter(writer);
                tail_offset += bytes_written;
                written += bytes_written;
            }

            file.sync() catch |err| {
                std.debug.print("Error syncing file: {}\n", .{err});
            };

            return writer.context.getPos();
        }

        pub fn toBytes(self: *Self, buf: []u8) !usize {
            var writerType = std.io.fixedBufferStream(buf);
            var writer = writerType.writer();
            return self.toBytesWriter(writer);
        }

        fn recordsSize(self: *Self) usize {
            return self.header.records_size;
        }

        fn writeHeader(self: *Self, file: *std.fs.File) !usize {
            var header_buf: [HeaderPkg.headerSize()]u8 = undefined;
            _ = try Header.toBytes(&self.header, &header_buf);
            return try file.pwrite(&header_buf, 0);
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

        pub fn fromBytes(self: *Self, fileBytes: []u8) !usize {
            // Read header
            self.header = try Header.fromBytes(fileBytes[0..HeaderPkg.headerSize()]);
            var offset = HeaderPkg.headerSize();

            // Read records
            while (offset < HeaderPkg.headerSize() + self.recordsSize()) {
                var r = try Record.fromBytes(fileBytes[offset..], self.allocator);
                // std.debug.print("Record: key: {s}, value: {s}\n", .{ r.key, r.value });
                self.mem[self.current_mem_index] = r;
                self.current_mem_index += 1;
                offset += r.bytesLen();
                // std.debug.print("Offset: {d}, len: {d}\n", .{ offset, fileBytes.len });
            }

            // WAL files does not have pointers

            return offset;
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
    var wal = try MemoryWal(100).init(&alloc);
    defer wal.deinit();

    try wal.append(try Record.init("hell0", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell2", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell0", "world0", Op.Create, &alloc));

    var iter = wal.backwards_iterator();

    var next = iter.next().?;
    try std.testing.expectEqualStrings("world0", next.value);
}

test "wal.iterator" {
    var alloc = std.testing.allocator;
    var wal = try MemoryWal(100).init(&alloc);
    defer wal.deinit();

    try wal.append(try Record.init("hell0", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell1", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell2", "world", Op.Create, &alloc));
    try wal.append(try Record.init("hell0", "world0", Op.Create, &alloc));

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

    try std.testing.expect(!MemoryWal(100).lexicographical_compare({}, r2, r1));
}

test "wal.sort a wal" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(100).init(&alloc);
    defer wal.deinit();

    var r1 = try Record.init("hellos", "world", Op.Create, &alloc);
    var r2 = try Record.init("hello", "world", Op.Create, &alloc);

    try wal.append(r1);
    try wal.append(r2);

    try std.testing.expectEqualSlices(u8, wal.mem[0].key, r1.key);
    try std.testing.expectEqualSlices(u8, wal.mem[1].key, r2.key);

    wal.sort();

    try std.testing.expectEqualSlices(u8, wal.mem[1].key, r1.key);
    try std.testing.expectEqualSlices(u8, wal.mem[0].key, r2.key);
}

test "wal.add record" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(100).init(&alloc);
    defer wal.deinit();

    var r = try Record.init("hello", "world", Op.Create, &alloc);
    try wal.append(r);

    try std.testing.expect(wal.header.total_records == 1);
    try std.testing.expect(wal.header.records_size == r.bytesLen());

    try wal.appendKv("hello2", "world2");
    try std.testing.expect(wal.header.total_records == 2);
}

test "wal.max size reached" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(23).init(&alloc);
    defer wal.deinit();

    try std.testing.expectEqual(@as(usize, 1), wal.mem.len);
    var r = try Record.init("hello", "world", Op.Create, &alloc);

    try std.testing.expectEqual(@as(usize, 21), r.bytesLen());

    wal.append(r) catch unreachable;

    var buf: [24]u8 = undefined;
    _ = try Record.toBytes(r, buf[0..]);

    if (wal.append(r)) |_| unreachable else |err| {
        try std.testing.expect(err == WalError.MaxSizeReached);
    }
}

test "wal.find a key" {
    var alloc = std.testing.allocator;
    var wal = try MemoryWal(100).init(&alloc);
    defer wal.deinit();

    var r1 = try Record.init("hello", "world", Op.Create, &alloc);
    var r2 = try Record.init("hello", "world1", Op.Create, &alloc);
    var r3 = try Record.init("hello", "world3", Op.Create, &alloc);
    var r4 = try Record.init("hello1", "world", Op.Create, &alloc);

    try wal.append(r1);
    try wal.append(r2);
    try wal.append(r3);
    try wal.append(r4);

    try std.testing.expect(wal.header.total_records == 4);

    const maybe_record = wal.find(r1.key[0..]);
    //we expect value of r3 as it's the last inserted using key `hello`
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, r3.value[0..]));

    const unkonwn_record = wal.find("unknokwn");
    try std.testing.expect(unkonwn_record == null);
}

test "wal.size on memory" {
    try std.testing.expectEqual(216, @sizeOf(MemoryWal(100)));
}

test "wal.persist" {
    var allocator = std.testing.allocator;
    const WalType = MemoryWal(4098);

    var wal = try WalType.init(&allocator);
    defer wal.deinit();

    var r = try Record.init("hell0", "world1", Op.Delete, &allocator);
    try wal.append(r);
    try wal.append(try Record.init("hell1", "world2", Op.Delete, &allocator));
    try wal.append(try Record.init("hell2", "world3", Op.Delete, &allocator));
    wal.sort();

    try expectEqual(@as(usize, 22), r.bytesLen());
    try expectEqual(@as(usize, 66), wal.header.records_size);
    try expectEqual(@as(usize, 3), wal.header.total_records);
    try expectEqual(@as(usize, 161), HeaderPkg.headerSize());
    try expectEqual(@as(usize, 16), r.expectedPointerSize());

    var dm = try DiskManager.init("/tmp");
    var fileData = try dm.new_file(&allocator);
    defer fileData.deinit();

    try expectEqual(@as(usize, 66), (r.bytesLen() * 3));
    try expectEqual(@as(usize, 48), (3 * r.expectedPointerSize()));
    try expectEqual(@as(usize, 275), HeaderPkg.headerSize() + (r.bytesLen() * 3) + (3 * r.expectedPointerSize()));

    const bytes_written = try wal.persist(&fileData.file);

    try expectEqual(@as(usize, HeaderPkg.headerSize() + (r.bytesLen() * 3) + (3 * r.expectedPointerSize())), bytes_written);

    try fileData.file.seekTo(0);
    const file = try std.fs.openFileAbsolute(fileData.filename, std.fs.File.OpenFlags{});
    defer file.close();

    var headerBuf: [HeaderPkg.headerSize()]u8 = undefined;
    _ = try file.read(&headerBuf);

    try std.fs.deleteFileAbsolute(fileData.filename);
}

test "wal_fromBytes" {
    var allocator = std.testing.allocator;
    const WalType = MemoryWal(4098);

    var wal = try WalType.init(&allocator);
    defer wal.deinit();

    try wal.append(try Record.init("hell0", "world1", Op.Update, &allocator));
    try wal.append(try Record.init("hell1", "world2", Op.Delete, &allocator));
    try wal.append(try Record.init("hell2", "world3", Op.Delete, &allocator));

    var buf = try allocator.alloc(u8, 4096);
    defer allocator.free(buf);

    const bytes_written: usize = try wal.toBytes(buf);

    try expectEqual(@as(usize, 227), bytes_written);

    var wal2 = try WalType.init(&allocator);
    defer wal2.deinit();

    const bytes_read = try wal2.fromBytes(buf[0..bytes_written]);
    try expectEqual(@as(usize, 227), bytes_read);
}

test "wal.toBytes" {
    var allocator = std.testing.allocator;
    const WalType = MemoryWal(4098);

    var wal = try WalType.init(&allocator);

    try wal.append(try Record.init("hell0", "world1", Op.Update, &allocator));
    try wal.append(try Record.init("hell1", "world2", Op.Delete, &allocator));
    try wal.append(try Record.init("hell2", "world3", Op.Delete, &allocator));
    wal.sort();

    var buf = try allocator.alloc(u8, 4096);
    defer allocator.free(buf);

    const bytes_written = try wal.toBytes(buf);

    wal.deinit();
    wal = try WalType.init(&allocator);
    defer wal.deinit();

    const bytes_read = try wal.fromBytes(buf[0..bytes_written]);
    try expectEqual(@as(usize, 227), bytes_read);
}
