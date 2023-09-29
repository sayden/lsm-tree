const std = @import("std");

const WalNs = @import("./wal.zig");
const lexicographical_compare = WalNs.lexicographical_compare;
const Error = WalNs.Error;
const persist = WalNs.persist;
const walAppendKv = WalNs.appendKv;
const preAppend = WalNs.preAppend;
const walFind = WalNs.find;
const postAppend = WalNs.postAppend;

const HeaderNs = @import("./header.zig");
const Header = HeaderNs.Header;
const Record = @import("./record.zig").Record;
const Op = @import("./ops.zig").Op;
const Pointer = @import("./pointer.zig").Pointer;
const Iterator = @import("./iterator.zig").Iterator;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

pub const initial_wal_size: usize = 32768;
pub const WalLevel1 = ComptimeMemWal(initial_wal_size);
pub const WalLevel2 = ComptimeMemWal(initial_wal_size * 2);
pub const WalLevel3 = ComptimeMemWal(initial_wal_size * 4);
pub const WalLevel4 = ComptimeMemWal(initial_wal_size * 8);
pub const WalLevel5 = ComptimeMemWal(initial_wal_size * 16);
pub const WalLevel6 = ComptimeMemWal(initial_wal_size * 32);

pub const WalLevel = union {
    level_1: *WalLevel1,
    level_2: *WalLevel2,
    level_3: *WalLevel3,
    level_4: *WalLevel4,
    level_5: *WalLevel5,
    level_6: *WalLevel6,
};

pub fn ComptimeMemWal(comptime size: usize) type {
    return struct {
        pub fn init(alloc: std.mem.Allocator) !*CoreMemWal {
            return try CoreMemWal.init(size, alloc);
        }
    };
}

pub const RuntimeMemWal = struct {
    pub fn init(size: usize, alloc: std.mem.Allocator) !*CoreMemWal {
        return try CoreMemWal.init(size, alloc);
    }
};

pub const CoreMemWal = struct {
    const log = std.log.scoped(.CoreWal);
    const Self = @This();

    max_size: usize,
    header: Header,
    mem: []*Record,
    current_mem_index: usize = 0,

    alloc: std.mem.Allocator,

    // Start a new in memory WAL using the provided allocator
    // REMEMBER to call `deinit()` once you are done with the iterator,
    // for example after persisting it to disk.
    // CALL `deinitCascade()` if you want also to free all the records
    // stored in it.
    pub fn init(size: usize, alloc: std.mem.Allocator) !*Self {
        var wal = try alloc.create(CoreMemWal);
        errdefer alloc.destroy(wal);

        var mem = try alloc.alloc(*Record, size / Record.minimum_size());
        errdefer alloc.free(mem);

        var header = Header.init();

        wal.* = Self{
            .header = header,
            .mem = mem,
            .alloc = alloc,
            .max_size = size,
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

    pub fn appendKv(self: *Self, k: []const u8, v: []const u8) !void {
        try walAppendKv(self, k, v, self.alloc);
    }

    /// Add a new record to the in memory WAL
    pub fn append(self: *Self, r: *Record) Error!void {
        try preAppend(self, r);

        self.mem[self.header.total_records] = try r.clone(self.alloc);
        errdefer self.mem[self.header.total_records].deinit();

        postAppend(&self.header, r);
    }

    pub fn appendOwn(self: *Self, r: *Record) !void {
        try preAppend(self, r);

        self.mem[self.header.total_records] = r;

        postAppend(&self.header, r);
    }

    // Compare the provided key with the ones in memory and
    // returns the last record that is found (or none if none is found)
    pub fn find(self: *Self, key_to_find: []const u8, alloc: std.mem.Allocator) !?*Record {
        var iter = self.getIterator();
        return walFind(&iter, key_to_find, alloc);
    }

    pub fn availableBytes(self: *Self) usize {
        return self.max_size - self.getWalSize();
    }

    // Sort the list of records in lexicographical order
    pub fn sort(self: *Self) void {
        std.sort.insertion(*Record, self.mem[0..self.header.total_records], {}, lexicographical_compare);
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

    fn recordsSize(self: *Self) usize {
        return self.header.records_size;
    }

    fn pointersSize(self: *Self) usize {
        return self.header.pointers_size;
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

test "wal_embedded" {
    var alloc = std.testing.allocator;

    var mem_wal = try WalLevel1.init(alloc);
    defer mem_wal.deinit();

    try mem_wal.appendKv("hello", "world");
}

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

fn createWal(alloc: std.mem.Allocator) !*CoreMemWal {
    var wal = try WalLevel1.init(alloc);

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

    var wal = try WalLevel1.init(alloc);
    defer wal.deinit();

    for (0..7) |i| {
        var key = try std.fmt.allocPrint(alloc, "hello{}", .{i});
        var val = try std.fmt.allocPrint(alloc, "world{}", .{i});
        try wal.appendOwn(try Record.init(key, val, Op.Create, alloc));
        alloc.free(key);
        alloc.free(val);
    }
    wal.sort();
}

test "wal_add_record" {
    var alloc = std.testing.allocator;

    var wal = try WalLevel1.init(alloc);
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

    var ws = ReaderWriterSeeker.initFile(file);

    const bytes_written = try persist(wal.mem, &wal.header, &ws);
    try expectEqual(@as(usize, HeaderNs.headerSize() + wal.header.pointers_size + wal.header.records_size), bytes_written);

    try file.seekTo(0);

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
        std.debug.print("Error attempting to create a new file. Could not clone file in temp: {}\n", .{err});
        return;
    };
    defer dest_file.close();

    original_file.seekTo(0) catch |err| {
        std.debug.print("Error attempting seek operation on file. Could not clone file in temp: {}\n", .{err});
        return;
    };

    dest_file.writeFileAll(original_file, std.fs.File.WriteFileOptions{}) catch |err| {
        std.debug.print("Error attempting to write file. Could not clone file in temp: {}\n", .{err});
    };
}
