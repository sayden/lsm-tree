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
const Strings = @import("./strings.zig");
const strcmp = Strings.strcmp;
const Math = std.math;

pub const WalError = error{
    MaxSizeReached,
    CantCreateRecord,
} || RecordError || std.mem.Allocator.Error;

pub fn MemoryWal(comptime max_size_in_bytes: usize) type {
    return struct {
        const Self = @This();

        header: Header,

        max_size: usize,

        mem: []*Record,
        current_mem_index: usize,

        // Pointer size must be pre-calculated before persisting because the header is the first
        // thing written on files. Changing header to EOF would fix this problem
        pointers_size: usize,

        allocator: std.mem.Allocator,

        // Start a new in memory WAL using the provided allocator
        // REMEMBER to call `deinit()` once you are done with the iterator,
        // for example after persisting it to disk.
        // CALL `deinitCascade()` if you want also to free all the records
        // stored in it.
        pub fn init(allocator: std.mem.Allocator) !*Self {
            var wal = try allocator.create(MemoryWal(max_size_in_bytes));

            wal.max_size = max_size_in_bytes;
            wal.allocator = allocator;
            wal.mem = try allocator.alloc(*Record, max_size_in_bytes / Record.minimum_size());
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

        /// Add a new record to the in memory WAL
        pub fn append(self: *Self, r: *Record) WalError!void {
            const record_size: usize = r.len();

            // Check if there's available space in the WAL
            if (self.getWalSize() + record_size >= max_size_in_bytes) {
                return WalError.MaxSizeReached;
            }

            self.mem[self.header.total_records] = r;
            self.header.total_records += 1;
            self.header.first_pointer_offset += record_size;
            self.header.records_size += record_size;
            self.pointers_size += r.pointerSize();
        }

        // Compare the provided key with the ones in memory and
        // returns the last record that is found (or none if none is found)
        pub fn find(self: *Self, key_to_find: []const u8) ?*Record {
            var iter = self.backwards_iterator();
            while (iter.next()) |r| {
                if (std.mem.eql(u8, r.pointer.key, key_to_find)) {
                    return r;
                }
            }

            return null;
        }

        pub fn availableBytes(self: *Self) usize {
            return max_size_in_bytes - self.getWalSize();
        }

        // Sort the list of records in lexicographical order
        pub fn sort(self: *Self) void {
            std.sort.insertion(*Record, self.mem[0..self.header.total_records], {}, lexicographical_compare);
        }

        pub fn getWalSize(self: *Self) usize {
            return HeaderPkg.headerSize() + self.recordsSize() + self.pointersSize();
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
        /// Pointer
        /// Pointer
        /// Pointer
        /// ...
        /// Record
        /// Record
        /// Record
        /// ...
        /// EOF
        pub fn persist(self: *Self, file: *std.fs.File) !usize {
            if (self.header.total_records == 0) {
                return 0;
            }
            self.sort();

            try file.seekTo(HeaderPkg.headerSize());
            var writer = file.writer();

            // Move offset after header, which will be written later
            var offset = HeaderPkg.headerSize() + self.pointers_size;

            // Write pointer
            for (0..self.header.total_records) |i| {
                self.mem[i].pointer.offset = offset;
                _ = try self.mem[i].writePointer(writer);
                offset += self.mem[i].len();
            }

            // Write records
            for (0..self.header.total_records) |i| {
                self.mem[i].pointer.offset = offset;
                offset = try self.mem[i].writeValue(writer);
            }

            var written = writer.context.getEndPos();

            // Write first and last pointer in the header. We cannot write this before
            // because we need to know their offsets after writing. It can be calculated
            // now, but maybe not later if compression comes in place
            self.header.first_pointer_offset = self.mem[0].getOffset();
            self.header.last_pointer_offset = self.mem[self.header.total_records - 1].getOffset();

            // Write the header
            try file.seekTo(0);
            var writer2 = file.writer();
            _ = try self.header.toBytesWriter(writer2);

            return written;
        }

        fn recordsSize(self: *Self) usize {
            return self.header.records_size;
        }

        fn pointersSize(self: *Self) usize {
            return self.pointers_size;
        }

        fn writeHeader(self: *Self, file: *std.fs.File) !usize {
            var header_buf: [HeaderPkg.headerSize()]u8 = undefined;
            _ = try Header.toBytes(&self.header, &header_buf);
            return try file.pwrite(&header_buf, 0);
        }

        fn lexicographical_compare(_: void, lhs: *Record, rhs: *Record) bool {
            const res = strcmp(lhs.pointer.key, rhs.pointer.key);
            return res.compare(Math.CompareOperator.lte);
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
                const tuple = @subWithOverflow(records.len, 1);
                if (tuple[1] != 0) {
                    //empty

                    return RecordBackwardIterator{
                        .records = records,
                        .pos = 0,
                        .finished = true,
                    };
                }
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

        pub fn debug(self: *Self) void {
            std.debug.print("\n---------------------\n---------------------\nWAL\n---\nMem index:\t{}\nMax Size:\t{}\nPointer size:\t{}\n", .{ self.current_mem_index, self.max_size, self.pointers_size });
            defer std.debug.print("\n---------------------\n---------------------\n", .{});
            self.header.debug();
        }

        pub fn full_debug(self: *Self) void {
            self.debug();
            for (self.mem) |record| {
                record.debug();
            }
        }
    };
}

fn createWal(alloc: std.mem.Allocator) !*MemoryWal(4096) {
    var wal = try MemoryWal(4096).init(alloc);

    var buf1 = try alloc.alloc(u8, 10);
    var buf2 = try alloc.alloc(u8, 10);
    defer alloc.free(buf1);
    defer alloc.free(buf2);

    for (0..50) |i| {
        const key = try std.fmt.bufPrint(buf1, "hello{}", .{i});
        const val = try std.fmt.bufPrint(buf2, "world{}", .{i});

        try wal.append(try Record.init(key, val, Op.Create, alloc));
    }

    return wal;
}

test "wal_iterator_backwards" {
    var alloc = std.testing.allocator;
    var wal = try createWal(alloc);
    defer wal.deinit();

    var iter = wal.backwards_iterator();

    var next = iter.next();
    try std.testing.expectEqualStrings("world49", next.?.value);
}

test "wal_iterator" {
    var alloc = std.testing.allocator;
    var wal = try createWal(alloc);
    defer wal.deinit();

    var iter = wal.iterator();

    _ = iter.next();
    _ = iter.next();
    _ = iter.next();
    var record = iter.next();

    try std.testing.expectEqualStrings("world3", record.?.value);
}

test "wal_lexicographical_compare" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(2048).init(alloc);
    defer wal.deinit();

    for (0..30) |i| {
        var key = try std.fmt.allocPrint(alloc, "hello{}", .{i});
        var val = try std.fmt.allocPrint(alloc, "world{}", .{i});
        try wal.append(try Record.init(key, val, Op.Create, alloc));
        alloc.free(key);
        alloc.free(val);
    }
    wal.sort();
}

test "wal_add_record" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(512).init(alloc);
    defer wal.deinit();

    var r = try Record.init("hello", "world", Op.Create, alloc);
    try wal.append(r);

    try std.testing.expect(wal.header.total_records == 1);
    try std.testing.expect(wal.header.records_size == r.len());

    try wal.appendKv("hello2", "world2");
    try std.testing.expect(wal.header.total_records == 2);
}

test "wal_find_a_key" {
    var alloc = std.testing.allocator;
    var wal = try createWal(alloc);
    defer wal.deinit();

    try std.testing.expect(wal.header.total_records == 50);

    const maybe_record = wal.find(wal.mem[3].getKey());
    try std.testing.expect(std.mem.eql(u8, maybe_record.?.value, wal.mem[3].value[0..]));

    const unkonwn_record = wal.find("unknokwn");
    try std.testing.expect(unkonwn_record == null);
}

test "wal_size_on_memory" {
    try std.testing.expectEqual(224, @sizeOf(MemoryWal(100)));
}

test "wal_persist" {
    var alloc = std.testing.allocator;

    var wal = try createWal(alloc);
    defer wal.deinit();

    var dm = try DiskManager.init("/tmp", alloc);
    defer dm.deinit();

    var fileData = try dm.getNewFile(alloc);
    defer fileData.deinit();

    _ = try wal.persist(&fileData.file);

    try fileData.file.seekTo(0);
    const file = try std.fs.openFileAbsolute(fileData.filename, std.fs.File.OpenFlags{});
    defer file.close();

    var headerBuf: [HeaderPkg.headerSize()]u8 = undefined;
    _ = try file.read(&headerBuf);

    try std.fs.deleteFileAbsolute(fileData.filename);
}
