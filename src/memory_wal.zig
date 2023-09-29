const std = @import("std");
const rec = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = rec.Record;
const RecordError = rec.RecordError;
const HeaderNs = @import("./header.zig");
const Header = HeaderNs.Header;
const lsmtree = @import("./main.zig");
const Pointer = @import("./pointer.zig").Pointer;
const Strings = @import("./strings.zig");
const strcmp = Strings.strcmp;
const Math = std.math;
const IteratorNs = @import("./iterator.zig");
const Iterator = IteratorNs.Iterator;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

const DebugNs = @import("./debug.zig");

const println = DebugNs.println;
const prints = DebugNs.prints;
const print = std.debug.print;

pub const Error = error{
    MaxSizeReached,
    CantCreateRecord,
    EmptyWal,
} || RecordError || std.mem.Allocator.Error;

pub fn MemoryWal(comptime max_size_in_bytes: usize) type {
    const log = std.log.scoped(.DiskManager);

    return struct {
        const Self = @This();
        const max_size: usize = max_size_in_bytes;

        header: Header,

        mem: []*Record,
        current_mem_index: usize = 0,

        alloc: std.mem.Allocator,

        // Start a new in memory WAL using the provided allocator
        // REMEMBER to call `deinit()` once you are done with the iterator,
        // for example after persisting it to disk.
        // CALL `deinitCascade()` if you want also to free all the records
        // stored in it.
        pub fn init(alloc: std.mem.Allocator) !*Self {
            var wal = try alloc.create(MemoryWal(max_size_in_bytes));
            errdefer alloc.destroy(wal);

            var mem = try alloc.alloc(*Record, max_size_in_bytes / Record.minimum_size());
            errdefer alloc.free(mem);

            var header = Header.init();

            wal.* = Self{
                .header = header,
                .mem = mem,
                .alloc = alloc,
            };

            return wal;
        }

        pub fn deinit(self: *Self) void {
            var iter = self.getIterator();
            while (iter.next()) |r| {
                r.deinit();
            }
            self.alloc.free(self.mem);
            self.alloc.destroy(self);
        }

        pub fn appendKv(self: *Self, k: []const u8, v: []const u8) Error!void {
            var r = try Record.init(k, v, Op.Create, self.alloc);
            errdefer r.deinit();
            return self.appendOwn(r);
        }

        pub fn appendOwn(self: *Self, r: *Record) !void {
            const record_size: usize = r.len();

            // Check if there's available space in the WAL
            if (self.getWalSize() + record_size >= max_size_in_bytes) {
                return Error.MaxSizeReached;
            }

            self.mem[self.header.total_records] = r;
            self.header.total_records += 1;
            self.header.records_size += r.valueLen();
            self.header.pointers_size += r.pointerSize();
        }

        /// Add a new record to the in memory WAL
        pub fn append(self: *Self, r: *Record) Error!void {
            const record_size: usize = r.len();

            // Check if there's available space in the WAL
            if (self.getWalSize() + record_size >= max_size_in_bytes) {
                return Error.MaxSizeReached;
            }

            self.mem[self.header.total_records] = try r.clone(self.alloc);
            errdefer self.mem[self.header.total_records].deinit();
            self.header.total_records += 1;
            self.header.records_size += r.valueLen();
            self.header.pointers_size += r.pointerSize();
        }

        // Compare the provided key with the ones in memory and
        // returns the last record that is found (or none if none is found)
        pub fn find(self: *Self, key_to_find: []const u8, alloc: std.mem.Allocator) !?*Record {
            var iter = self.getIterator();
            while (iter.next()) |r| {
                if (std.mem.eql(u8, r.pointer.key, key_to_find)) {
                    return r.clone(alloc);
                }
            }

            return null;
        }

        pub fn availableBytes(self: *Self) usize {
            return max_size_in_bytes - self.getWalSize();
        }

        // Sort in place the list of records in lexicographical order. Returns a reference to the list
        pub fn sort(self: *Self) []*Record {
            std.sort.insertion(*Record, self.mem[0..self.header.total_records], {}, lexicographical_compare);
            return self.mem;
        }

        pub fn getWalSize(self: *Self) usize {
            return HeaderNs.headerSize() + self.recordsSize() + self.pointersSize();
        }

        const IteratorType = Iterator(*Record);
        // Creates a forward iterator to go through the wal.
        pub fn getIterator(self: *Self) IteratorType {
            const iter = IteratorType.init(self.mem[0..self.header.total_records]);

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
        pub fn persist(self: *Self, f: std.fs.File) !usize {
            var ws = ReaderWriterSeeker.initFile(f);

            if (self.header.total_records == 0) {
                return Error.EmptyWal;
            }

            _ = self.sort();

            // Write first and last pointer in the header. We cannot write this before
            // because we need to know their offsets after writing. It can be calculated
            // now, but maybe not later if compression comes in place
            self.header.first_pointer_offset = HeaderNs.headerSize();
            self.header.last_pointer_offset = self.header.pointers_size + HeaderNs.headerSize() - self.mem[self.header.total_records - 1].pointerSize();

            try ws.seekTo(HeaderNs.headerSize());

            // Move offset after header, which will be written later
            var record_offset = HeaderNs.headerSize() + self.header.pointers_size;

            var written: usize = 0;
            // Write pointer
            for (0..self.header.total_records) |i| {
                self.mem[i].pointer.offset = record_offset;
                written += try self.mem[i].writePointer(&ws);

                record_offset += self.mem[i].valueLen();
            }

            // Write records
            for (0..self.header.total_records) |i| {
                // self.mem[i].pointer.offset = record_offset;
                written += try self.mem[i].write(&ws);
            }

            // Write the header
            try ws.seekTo(0);
            written += try self.header.write(&ws);

            return written;
        }

        fn recordsSize(self: *Self) usize {
            return self.header.records_size;
        }

        fn pointersSize(self: *Self) usize {
            return self.header.pointers_size;
        }

        fn lexicographical_compare(_: void, lhs: *Record, rhs: *Record) bool {
            const res = strcmp(lhs.pointer.key, rhs.pointer.key);
            return res.compare(Math.CompareOperator.lte);
        }

        pub fn debug(self: *Self) void {
            log.debug("\n---------------------\n---------------------\nWAL\n---\nMem index:\t{}\nMax Size:\t{}\nPointer size:\t{}", .{ self.current_mem_index, self.max_size, self.pointers_size });
            defer log.debug("\n---------------------\n---------------------", .{});
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

const log_testing = std.log.scoped(.TestingMemoryWal);
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

fn createWal(alloc: std.mem.Allocator) !*MemoryWal(4096) {
    var wal = try MemoryWal(4096).init(alloc);

    var buf1 = try alloc.alloc(u8, 10);
    var buf2 = try alloc.alloc(u8, 10);
    defer alloc.free(buf1);
    defer alloc.free(buf2);

    for (0..7) |i| {
        const key = try std.fmt.bufPrint(buf1, "hello{}", .{i});
        const val = try std.fmt.bufPrint(buf2, "world{}", .{i});

        const r = try Record.init(key, val, Op.Create, alloc);
        defer r.deinit();
        try wal.append(r);
    }

    return wal;
}

test "wal_iterator" {
    var alloc = std.testing.allocator;
    var wal = try createWal(alloc);
    defer wal.deinit();

    var iter = wal.getIterator();

    _ = iter.next();
    _ = iter.next();
    _ = iter.next();
    var record = iter.next();

    try expectEqualStrings("world3", record.?.value);
}

test "wal_lexicographical_compare" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(2048).init(alloc);
    defer wal.deinit();

    for (0..7) |i| {
        var key = try std.fmt.allocPrint(alloc, "hello{}", .{i});
        var val = try std.fmt.allocPrint(alloc, "world{}", .{i});
        try wal.appendOwn(try Record.init(key, val, Op.Create, alloc));
        alloc.free(key);
        alloc.free(val);
    }
}

test "wal_add_record" {
    var alloc = std.testing.allocator;

    var wal = try MemoryWal(512).init(alloc);
    defer wal.deinit();

    var r = try Record.init("hello", "world", Op.Create, alloc);

    try wal.appendOwn(r);

    try expect(wal.header.total_records == 1);
    try expect(wal.header.records_size == r.valueLen());

    try wal.appendKv("hello2", "world2");
    try expect(wal.header.total_records == 2);
}

test "wal_find_a_key" {
    var alloc = std.testing.allocator;
    var wal = try createWal(alloc);
    defer wal.deinit();

    try expectEqual(@as(usize, 7), wal.header.total_records);

    const maybe_record = try wal.find(wal.mem[3].getKey(), alloc);
    defer maybe_record.?.deinit();

    try expect(std.mem.eql(u8, maybe_record.?.value, wal.mem[3].value[0..]));

    const unkonwn_record = try wal.find("unknokwn", alloc);
    try expect(unkonwn_record == null);
}

test "wal_size_on_memory" {
    try std.testing.expectEqual(224, @sizeOf(MemoryWal(100)));
}

test "wal_persist" {
    var alloc = std.testing.allocator;

    var wal = try createWal(alloc);
    defer wal.deinit();

    // Create a temp file
    var tmp_dir = std.testing.tmpDir(std.fs.Dir.OpenDirOptions{});
    defer tmp_dir.cleanup();

    var file = try tmp_dir.dir.createFile("test.sst", std.fs.File.CreateFlags{ .read = true });
    defer file.close();
    // defer copyFileToTmp(file);

    const bytes_written = try wal.persist(file);
    try expectEqual(@as(usize, HeaderNs.headerSize() + wal.header.pointers_size + wal.header.records_size), bytes_written);

    try file.seekTo(0);

    var ws = ReaderWriterSeeker.initFile(file);
    const header = try Header.read(&ws);

    var calculated_pointer_size: usize = 17;
    var total_records: usize = wal.header.total_records;

    // Test header values
    try expectEqual(@as(usize, 7), header.total_records);
    try expectEqual(@as(usize, HeaderNs.headerSize()), header.first_pointer_offset);
    try expectEqual(@as(usize, calculated_pointer_size * total_records), wal.header.pointers_size);
    try expectEqual(@as(usize, HeaderNs.headerSize() + (total_records * calculated_pointer_size) - calculated_pointer_size), header.last_pointer_offset);

    try file.seekTo(HeaderNs.headerSize());

    // Read first 2 pointers
    var pointer1: *Pointer = try Pointer.read(&ws, alloc);
    defer pointer1.deinit();

    var pointer2: *Pointer = try Pointer.read(&ws, alloc);
    defer pointer2.deinit();

    try expectEqual(HeaderNs.headerSize() + total_records * calculated_pointer_size, try pointer1.getOffset());

    //Read the entire value
    var record1 = try pointer1.readValue(&ws, alloc);
    defer record1.deinit();

    try expectEqualStrings("hello0", record1.getKey());
    try expectEqualStrings("world0", record1.getVal());
    try expectEqual(HeaderNs.headerSize() + wal.header.pointers_size, try record1.getOffset());
    try expectEqual(Op.Create, record1.pointer.op);

    //Read the entire value
    var record2 = try pointer2.readValue(&ws, alloc);
    defer record2.deinit();

    try expectEqualStrings("hello1", record2.getKey());
    try expectEqualStrings("world1", record2.getVal());
    try expectEqual(HeaderNs.headerSize() + total_records * calculated_pointer_size + record1.valueLen(), try record2.getOffset());
    try expectEqual(Op.Create, record2.pointer.op);
}

fn copyFileToTmp(original_file: std.fs.File) void {
    var dest_file = std.fs.createFileAbsolute("/tmp/example.sst", std.fs.File.CreateFlags{}) catch |err| {
        log_testing.err("Error attempting to create a new file. Could not clone file in temp: {}", .{err});
        return;
    };
    defer dest_file.close();

    original_file.seekTo(0) catch |err| {
        log_testing.err("Error attempting seek operation on file. Could not clone file in temp: {}", .{err});
        return;
    };

    dest_file.writeFileAll(original_file, std.fs.File.WriteFileOptions{}) catch |err| {
        log_testing.err("Error attempting to write file. Could not clone file in temp: {}", .{err});
    };
}
