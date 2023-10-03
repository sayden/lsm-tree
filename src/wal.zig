const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const RecordNs = @import("./record.zig");
const Op = @import("./ops.zig").Op;
const Record = RecordNs.Record;
const RecordError = RecordNs.Error;
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
const DiskManager = @import("./disk_manager.zig").DiskManager;
const BytesIterator = IteratorNs.BytesIterator;
const BackwardsIterator = IteratorNs.IteratorBackwards;
const DebugNs = @import("./debug.zig");

const println = DebugNs.println;
const prints = DebugNs.prints;
const print = std.debug.print;

pub const Error = error{
    MaxSizeReached,
    CantCreateRecord,
    EmptyWal,
    ErrorWritingPointer,
    ErrorWritingValue,
} || RecordError || Allocator.Error;

// pub const initial_wal_size = if (builtin.is_test) 512 else 32000;
pub const initial_wal_size = 32000 * 32;

const logns = std.log.scoped(.Wal);

pub fn Wal(
    comptime Context: type,
    comptime IteratorType: type,
    comptime appendKvFn: fn (self: *Context, k: []const u8, v: []const u8) anyerror!void,
    comptime appendFn: fn (self: *Context, r: *Record) Error!void,
    comptime appendOwnFn: fn (self: *Context, r: *Record) anyerror!void,
    comptime findFn: fn (self: *Context, key_to_find: []const u8, alloc: Allocator) ?*Record,
    comptime availableBytesFn: fn (self: *Context) usize,
    comptime sortFn: fn (self: *Context) anyerror![]*Record,
    comptime persistFn: fn (self: *Context, ws: *ReaderWriterSeeker) anyerror!usize,
    comptime getWalSizeFn: fn (self: *Context) usize,
    comptime iteratorFn: fn (self: *Context, alloc: ?Allocator) anyerror!IteratorType,
    comptime deinitFn: fn (self: *Context) void,
    comptime debugFn: fn (self: *Context) void,
) type {
    return struct {
        const Self = @This();

        ctx: *Context,

        pub fn init(size: usize, alloc: Allocator) anyerror!Self {
            return Context.init(size, alloc);
        }

        pub fn deinit(self: Self) void {
            return deinitFn(self.ctx);
        }
        pub fn appendKv(self: Self, k: []const u8, v: []const u8) anyerror!void {
            return appendKvFn(self.ctx, k, v);
        }
        pub fn append(self: Self, r: *Record) Error!void {
            return appendFn(self.ctx, r);
        }
        pub fn appendOwn(self: Self, r: *Record) anyerror!void {
            return appendOwnFn(self.ctx, r);
        }
        pub fn find(self: Self, key_to_find: []const u8, alloc: Allocator) ?*Record {
            return findFn(self.ctx, key_to_find, alloc);
        }
        pub fn availableBytes(self: Self) usize {
            return availableBytesFn(
                self.ctx,
            );
        }
        pub fn getIterator(self: Self, alloc: ?Allocator) !IteratorType {
            return iteratorFn(self.ctx, alloc);
        }
        pub fn sort(self: Self) void {
            return sortFn(
                self.ctx,
            );
        }
        pub fn getWalSize(self: Self) usize {
            return getWalSizeFn(
                self.ctx,
            );
        }
        pub fn persist(self: Self, ws: *ReaderWriterSeeker) anyerror!usize {
            return persistFn(self.ctx, ws);
        }
        pub fn debug(self: *Self) void {
            return debugFn(self.ctx);
        }
    };
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
pub fn persistG(records: []*Record, header: *Header, ws: *ReaderWriterSeeker) !usize {
    if (header.total_records == 0) {
        return Error.EmptyWal;
    }

    std.sort.insertion(*Record, records[0..header.total_records], {}, lexicographical_compare);

    try ws.seekTo(HeaderNs.headerSize());

    // Move offset after header, which will be written later
    var record_offset = HeaderNs.headerSize() + header.pointers_size;

    var written: usize = 0;
    // Write pointer
    for (0..header.total_records) |i| {
        var record = records[i];
        if (record.pointer.op == Op.Skip) {
            continue;
        }
        if ((record.pointer.op == Op.Upsert or record.pointer.op == Op.Upsert) and i > 0) {
            // if previous record has the same id, skip it. Else, the record is somewhere else
            // and the update/delete operation will happen in compaction
            if (std.mem.eql(u8, records[i - 1].pointer.key, record.pointer.key)) {
                records[i - 1].pointer.op = Op.Skip;
            }
        }

        record.pointer.offset = record_offset;
        written += try record.writePointer(ws);

        record_offset += record.valueLen();
    }

    // Write records
    for (0..header.total_records) |i| {
        // records[i].pointer.offset = record_offset;
        written += try records[i].write(ws);
    }

    // Write first and last pointer in the header. We cannot write this before
    // because we need to know their offsets after writing. It can be calculated
    // now, but maybe not later if compression comes in place
    header.first_pointer_offset = HeaderNs.headerSize();
    header.last_pointer_offset = header.pointers_size + HeaderNs.headerSize() - records[header.total_records - 1].pointerSize();

    // Write the header
    try ws.seekTo(0);
    written += try header.write(ws);

    return written;
}

pub fn appendKvG(ctx: anytype, k: []const u8, v: []const u8, alloc: Allocator) anyerror!void {
    var r = try Record.init(k, v, Op.Upsert, alloc);
    errdefer r.deinit();
    return ctx.appendOwn(r);
}

/// Add a new record to the in memory WAL
pub fn preAppend(ctx: anytype, r: *Record) Error!void {
    const record_size: usize = r.len();

    // Check if there's available space in the WAL
    if (ctx.getWalSize() + record_size > ctx.max_size) {
        return Error.MaxSizeReached;
    }
}

/// Add a new record to the in memory WAL
pub fn postAppend(header: *Header, r: *Record) void {
    header.total_records += 1;
    header.records_size += r.valueLen();
    header.pointers_size += r.pointerSize();
}

pub fn findG(iter: anytype, key_to_find: []const u8, alloc: Allocator) ?*Record {
    while (iter.next()) |r| {
        if (std.mem.eql(u8, r.pointer.key, key_to_find)) {
            if (r.pointer.op == Op.Delete) {
                return null;
            }

            return r.clone(alloc) catch |err| {
                std.debug.print("{}\n", .{err});
                return null;
            };
        }
    }

    return null;
}

pub fn lexicographical_compare(_: void, lhs: *Record, rhs: *Record) bool {
    const res = strcmp(lhs.pointer.key, rhs.pointer.key);
    if (res == Math.Order.eq and lhs.pointer.op == rhs.pointer.op) {
        return res.compare(Math.CompareOperator.lte);
    } else if (res == Math.Order.eq) {
        return @intFromEnum(lhs.pointer.op) < @intFromEnum(rhs.pointer.op);
    }

    return res.compare(Math.CompareOperator.lte);
}

/// read a wal into a Mem wal and returns the latter
pub fn readWalIntoMem(reader: *ReaderWriterSeeker, alloc: Allocator) !Mem.Type {
    var wal = try Mem.init(initial_wal_size, null, alloc);

    try reader.seekTo(HeaderNs.headerSize());

    while (true) {
        var p = Pointer.read(reader, alloc) catch |err| {
            switch (err) {
                std.os.SeekError.Unseekable, error.EndOfStream, error.OutOfMemory => {
                    break;
                },
                else => {
                    logns.err("Unexpected error trying to recover wal file: {}", .{err});
                    return err;
                },
            }
        };

        const r = p.readValueReusePointer(reader, alloc) catch |err| {
            p.deinit();

            switch (err) {
                std.os.SeekError.Unseekable, error.EndOfStream, error.OutOfMemory => {
                    break;
                },
                RecordNs.Error.UnexpectedRecordLength => {
                    break;
                },
                else => {
                    logns.err("Unexpected error trying to recover wal file: {}", .{err});
                    return err;
                },
            }
        };

        errdefer r.deinit();

        try wal.appendOwn(r);
    }

    logns.debug("Found {} records", .{wal.ctx.header.total_records});

    return wal;
}

pub const Mem = struct {
    const log = std.log.scoped(.CoreWal);
    const Self = @This();

    max_size: usize,
    header: Header,
    mem: []*Record,
    current_mem_index: usize = 0,

    alloc: Allocator,

    pub const Type = Wal(
        Mem,
        Iterator(*Record),
        appendKv,
        append,
        appendOwn,
        find,
        availableBytes,
        sort,
        persist,
        getWalSize,
        getIterator,
        deinit,
        debug,
    );

    pub fn init(size: usize, _: ?*DiskManager, alloc: Allocator) !Type {
        var wal = try setup(size, alloc);
        return .{ .ctx = wal };
    }

    // Start a new in memory WAL using the provided allocator
    // REMEMBER to call `deinit()` once you are done with the iterator,
    // for example after persisting it to disk.
    fn setup(size: usize, alloc: Allocator) !*Mem {
        var wal = try alloc.create(Mem);
        errdefer alloc.destroy(wal);

        var mem = try alloc.alloc(*Record, size / Record.minimum_size());
        errdefer alloc.free(mem);

        var header = Header.init();

        wal.* = Mem{
            .header = header,
            .mem = mem,
            .alloc = alloc,
            .max_size = size,
        };

        return wal;
    }

    pub fn deinit(self: *Self) void {
        var iter = self.getIterator(null) catch unreachable;
        while (iter.next()) |r| {
            r.deinit();
        }

        self.alloc.free(self.mem);
        self.alloc.destroy(self);
    }

    pub fn appendKv(self: *Self, k: []const u8, v: []const u8) anyerror!void {
        return appendKvG(self, k, v, self.alloc);
    }

    /// Add a new record to the in memory WAL
    pub fn append(self: *Self, r: *Record) Error!void {
        try preAppend(self, r);

        self.mem[self.header.total_records] = try r.clone(self.alloc);
        errdefer self.mem[self.header.total_records].deinit();

        postAppend(&self.header, r);
    }

    pub fn appendOwn(self: *Self, r: *Record) Error!void {
        try preAppend(self, r);

        self.mem[self.header.total_records] = r;

        postAppend(&self.header, r);
    }

    // Compare the provided key with the ones in memory and
    // returns the last record that is found (or none if none is found)
    pub fn find(self: *Self, key_to_find: []const u8, alloc: Allocator) ?*Record {
        var iter = self.getBackwardsIterator(null) catch unreachable;
        return findG(&iter, key_to_find, alloc);
    }

    pub fn availableBytes(self: *Self) usize {
        return self.max_size - self.getWalSize();
    }

    // Sort the list of records in lexicographical order
    pub fn sort(self: *Self) ![]*Record {
        std.sort.insertion(*Record, self.mem[0..self.header.total_records], {}, lexicographical_compare);
        return self.mem;
    }

    pub fn getWalSize(self: *Self) usize {
        return HeaderNs.headerSize() + self.recordsSize() + self.pointersSize();
    }

    // Creates a forward iterator to go through the wal.
    fn getIterator(self: *Self, _: ?Allocator) !Iterator(*Record) {
        const iter = Iterator(*Record).init(self.mem[0..self.header.total_records]);

        return iter;
    }

    // Creates a forward iterator to go through the wal.
    fn getBackwardsIterator(self: *Self, _: ?Allocator) !IteratorNs.IteratorBackwards(*Record) {
        const iter = IteratorNs.IteratorBackwards(*Record).init(self.mem[0..self.header.total_records]);

        return iter;
    }

    pub fn persist(self: *Self, ws: *ReaderWriterSeeker) anyerror!usize {
        return persistG(self.mem, &self.header, ws);
    }

    fn recordsSize(self: *Self) usize {
        return self.header.records_size;
    }

    fn pointersSize(self: *Self) usize {
        return self.header.pointers_size;
    }

    pub fn debug(self: *Self) void {
        self.header.debug();
        log.debug("\n---------------------\n---------------------\nWAL\n---\nMem index:\t{}\nMax Size:\t{}\n", .{ self.current_mem_index, self.max_size });
        defer log.debug("\n---------------------\n---------------------", .{});
    }

    pub fn full_debug(self: *Self) void {
        self.debug();
        for (self.mem) |record| {
            record.debug();
        }
    }
};

/// FileWal do not use any memory to write the WAL. A file is created with ".wal" extension
/// for failure recovery. It's expected to have worse performance than the pure memory WAL.
/// Pointers and values are serialized directly into the disk without sorting. The format
/// of this file is:
///
/// Header
/// Pointer
/// Value
/// Pointer
/// Value
/// Pointer
/// Value
pub const File = struct {
    const log = std.log.scoped(.FileWal);
    const Self = @This();

    max_size: usize,
    header: Header,
    file: std.fs.File,
    disk_manager: *DiskManager,
    writer: ReaderWriterSeeker,
    current_offset: usize = HeaderNs.headerSize(),
    filepath: []const u8,

    alloc: Allocator,

    pub const Type = Wal(
        File,
        BytesIterator,
        appendKv,
        append,
        appendOwn,
        find,
        availableBytes,
        sort,
        persist,
        getWalSize,
        getIterator,
        deinit,
        debug,
    );

    pub fn init(size: usize, dm: *DiskManager, alloc: Allocator) !Type {
        var wal = try setup(size, dm, alloc);
        return .{ .ctx = wal };
    }

    // Start a new in memory WAL using the provided allocator
    // REMEMBER to call `deinit()` once you are done with the iterator,
    // for example after persisting it to disk.
    // CALL `deinitCascade()` if you want also to free all the records
    // stored in it.
    fn setup(size: usize, dm: *DiskManager, alloc: Allocator) !*Self {
        var wal = try alloc.create(File);
        errdefer alloc.destroy(wal);

        var filedata = try dm.getNewFile("wal", alloc);

        var header = Header.init();
        var writer = ReaderWriterSeeker.initFile(filedata.file);

        try writer.seekTo(header.header_size);

        wal.* = Self{
            .header = header,
            .alloc = alloc,
            .max_size = size,
            .file = filedata.file,
            .writer = writer,
            .disk_manager = dm,
            .filepath = filedata.filename,
        };

        return wal;
    }

    pub fn deinit(self: *Self) void {
        defer self.alloc.destroy(self);
        defer self.alloc.free(self.filepath);

        if (self.file.stat()) |stat| {
            if (stat.size == 0) {
                self.file.close();
                std.fs.deleteFileAbsolute(self.filepath) catch {};
            }
        } else |err| {
            log.warn("{}", .{err});
            self.file.close();
        }
    }

    pub fn appendKv(self: *Self, k: []const u8, v: []const u8) !void {
        return appendKvG(self, k, v, self.alloc);
    }

    pub fn append(self: *Self, r: *Record) Error!void {
        try preAppend(self, r);

        self.current_offset += r.pointer.len();
        r.pointer.offset = self.current_offset;

        _ = r.writePointer(&self.writer) catch |err| {
            log.err("{}", .{err});
            return Error.ErrorWritingPointer;
        };

        _ = r.write(&self.writer) catch |err| {
            log.err("{}", .{err});
            return Error.ErrorWritingValue;
        };

        self.current_offset += r.valueLen();

        postAppend(&self.header, r);
    }

    pub fn appendOwn(self: *Self, r: *Record) Error!void {
        defer r.deinit();
        try self.append(r);
    }

    // Compare the provided key with the ones in memory and
    // returns the last record that is found (or none if none is found)
    pub fn find(self: *Self, key_to_find: []const u8, alloc: Allocator) ?*Record {
        var found_records = std.ArrayList(*Record).init(alloc);
        defer found_records.deinit();

        var iter = self.getIterator() catch |err| {
            std.debug.print("Could not create iterator: {}\n", .{err});
            return null;
        };

        while (iter.next()) |r| {
            if (std.mem.eql(u8, r.pointer.key, key_to_find)) {
                try found_records.append(r);
            }
        }

        var sorted_records = try found_records.toOwnedSlice();
        std.sort.insertion(*Record, sorted_records, {}, lexicographical_compare);

        var backwardsiter = IteratorNs.IteratorBackwards(*Record).init(sorted_records);
        while (backwardsiter.next()) |record| {
            if (record.op == Op.Delete) {
                return null;
            }

            return record.clone(alloc);
        }

        return null;
    }

    pub fn persist(self: *Self, dest_writer: *ReaderWriterSeeker) anyerror!usize {
        try self.file.seekTo(0);

        var wal_file_reader = ReaderWriterSeeker.initFile(self.file);
        var wal = try readWalIntoMem(&wal_file_reader, self.alloc);
        defer wal.deinit();

        if (wal.ctx.header.total_records == 0) {
            return Error.EmptyWal;
        }

        _ = persistG(wal.ctx.mem, &wal.ctx.header, dest_writer) catch |err| {
            switch (err) {
                Error.EmptyWal => {
                    return 0;
                },
                else => return err,
            }
        };

        // After sucessful persit, delete the file
        var filepath = try self.alloc.alloc(u8, self.current.ctx.filepath.len);
        defer self.alloc.free(filepath);
        @memcpy(filepath, self.current.ctx.filepath);
        self.current.deinit();

        try std.fs.deleteFileAbsolute(filepath);

        return @as(usize, 0);
    }

    pub fn availableBytes(self: *Self) usize {
        return self.max_size - self.getWalSize();
    }

    // Sort the list of records in lexicographical order
    pub fn sort(self: *Self) ![]*Record {
        var records = std.ArrayList(*Record).init(self.alloc);
        defer records.deinit();
        errdefer {
            for (records.items) |record| {
                record.deinit();
            }
        }

        var iter = try self.getIterator(self.alloc);
        while (iter.next()) |record| {
            try records.append(record);
        }

        var sorted_records = try records.toOwnedSlice();
        std.sort.insertion(*Record, sorted_records, {}, lexicographical_compare);
        return sorted_records;
    }

    pub fn getWalSize(self: *Self) usize {
        return HeaderNs.headerSize() + self.recordsSize() + self.pointersSize();
    }

    // Creates a forward iterator to go through the wal.
    pub fn getIterator(self: *Self, alloc: ?Allocator) !BytesIterator {
        if (alloc) |al| {
            return BytesIterator.init(self.writer, al);
        }

        return error{NullAllocator};
    }

    fn recordsSize(self: *Self) usize {
        return self.header.records_size;
    }

    fn pointersSize(self: *Self) usize {
        return self.header.pointers_size;
    }

    pub fn debug(self: *Self) void {
        log.debug("\n---------------------\n---------------------\nWAL\nMax Size:\t{}\nPointer size:\t{}", .{ self.max_size, self.pointers_size });
        defer log.debug("\n---------------------\n---------------------", .{});
        self.header.debug();
    }
};

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

pub fn createWal(alloc: Allocator) !Mem.Type {
    var wal = try Mem.init(1024, null, alloc);

    var buf1 = try alloc.alloc(u8, 10);
    var buf2 = try alloc.alloc(u8, 10);
    defer alloc.free(buf1);
    defer alloc.free(buf2);

    for (0..7) |i| {
        const key = try std.fmt.bufPrint(buf1, "hello{}", .{i});
        const val = try std.fmt.bufPrint(buf2, "world{}", .{i});

        const r = try Record.init(key, val, Op.Upsert, alloc);
        defer r.deinit();

        try wal.append(r);
    }

    return wal;
}

test "wal_lexicographical_compare" {
    var alloc = std.testing.allocator;

    var r1 = try Record.init("hello0", "world00", Op.Upsert, alloc);
    var r2 = try Record.init("hello0", "world01", Op.Upsert, alloc);
    defer r1.deinit();
    defer r2.deinit();

    try expect(lexicographical_compare({}, r1, r2));

    var r3 = try Record.init("hello", "world0000", Op.Delete, alloc);
    defer r3.deinit();

    try expect(lexicographical_compare({}, r3, r1));
}

test "wal_iterator" {
    var alloc = std.testing.allocator;
    var wal = try createWal(alloc);
    defer wal.deinit();

    var iter = try wal.getIterator(null);

    _ = iter.next();
    _ = iter.next();
    _ = iter.next();
    var record = iter.next();

    try expectEqualStrings("world3", record.?.value);
}

test "wal_add_record" {
    var alloc = std.testing.allocator;

    var wal = try Mem.init(512, null, alloc);
    defer wal.deinit();

    var r = try Record.init("hello", "world", Op.Upsert, alloc);

    try wal.appendOwn(r);

    try expect(wal.ctx.header.total_records == 1);
    try expect(wal.ctx.header.records_size == r.valueLen());

    try wal.appendKv("hello2", "world2");
    try expect(wal.ctx.header.total_records == 2);
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
    defer copyFileToTmp(file);

    var ws = ReaderWriterSeeker.initFile(file);

    const bytes_written = try persistG(wal.ctx.mem, &wal.ctx.header, &ws);
    try expectEqual(@as(usize, HeaderNs.headerSize() + wal.ctx.header.pointers_size + wal.ctx.header.records_size), bytes_written);

    try file.seekTo(0);

    const header = try Header.read(&ws);

    var calculated_pointer_size: usize = 17;
    var total_records: usize = wal.ctx.header.total_records;

    // Test header values
    try expectEqual(@as(usize, 7), header.total_records);
    try expectEqual(@as(usize, HeaderNs.headerSize()), header.first_pointer_offset);
    try expectEqual(@as(usize, calculated_pointer_size * total_records), wal.ctx.header.pointers_size);
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
    try expectEqual(HeaderNs.headerSize() + wal.ctx.header.pointers_size, try record1.getOffset());
    try expectEqual(Op.Upsert, record1.pointer.op);

    //Read the entire value
    var record2 = try pointer2.readValue(&ws, alloc);
    defer record2.deinit();

    try expectEqualStrings("hello1", record2.getKey());
    try expectEqualStrings("world1", record2.getVal());
    try expectEqual(HeaderNs.headerSize() + total_records * calculated_pointer_size + record1.valueLen(), try record2.getOffset());
    try expectEqual(Op.Upsert, record2.pointer.op);
}

test "wal_find_key" {
    var alloc = std.testing.allocator;
    var wal = try Mem.init(1024, null, alloc);
    defer wal.deinit();

    for (0..5) |_| {
        var r = try Record.init("hello", "world", Op.Upsert, alloc);
        defer r.deinit();
        try wal.append(r);
    }

    var r = try Record.init("hello", "12345", Op.Upsert, alloc);
    try wal.appendOwn(r);

    var maybe_record = wal.find("hello", alloc);
    defer maybe_record.?.deinit();

    try expectEqualStrings("12345", maybe_record.?.getVal());

    var r1 = try Record.init("hello", "world", Op.Delete, alloc);
    try wal.appendOwn(r1);

    var deleted_record = wal.find("hello", alloc);
    try expect(deleted_record == null);
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
