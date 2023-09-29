const std = @import("std");
const DiskManager = @import("./disk_manager.zig").DiskManager;
const HeaderNs = @import("./header.zig");
const Header = HeaderNs.Header;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Record = @import("./record.zig").Record;
const Op = @import("./ops.zig").Op;
const Pointer = @import("./pointer.zig").Pointer;
const BytesIterator = @import("./iterator.zig").BytesIterator;

const WalNs = @import("./wal.zig");
const lexicographical_compare = WalNs.lexicographical_compare;
const persist = WalNs.persist;
const walAppendKv = WalNs.appendKv;
const preAppend = WalNs.preAppend;
const postAppend = WalNs.postAppend;
const walFind = WalNs.find;
const Error = WalNs.Error;

pub const FileWal = struct {
    const log = std.log.scoped(.FileWal);
    const Self = @This();

    max_size: usize,
    header: Header,
    file: std.fs.File,
    writer: ReaderWriterSeeker,
    current_offset: usize = 0,

    alloc: std.mem.Allocator,

    // Start a new in memory WAL using the provided allocator
    // REMEMBER to call `deinit()` once you are done with the iterator,
    // for example after persisting it to disk.
    // CALL `deinitCascade()` if you want also to free all the records
    // stored in it.
    pub fn init(size: usize, dm: *DiskManager, alloc: std.mem.Allocator) !*Self {
        var wal = try alloc.create(FileWal);
        errdefer alloc.destroy(wal);

        var filedata = try dm.getNewFile("wal", alloc);
        alloc.free(filedata.filename);

        var header = Header.init();
        var writer = ReaderWriterSeeker.initFile(filedata.file);

        try writer.seekTo(header.header_size);

        wal.* = Self{
            .header = header,
            .alloc = alloc,
            .max_size = size,
            .file = filedata.file,
            .writer = writer,
        };

        return wal;
    }

    pub fn deinit(self: *Self) void {
        defer self.alloc.destroy(self);
        defer self.file.close();

        self.writer.seekTo(0) catch |err| {
            log.err("{}", .{err});
        };

        _ = self.header.write(&self.writer) catch |err| {
            log.err("{}", .{err});
        };
    }

    pub fn appendKv(self: *Self, k: []const u8, v: []const u8) !void {
        return walAppendKv(self, k, v, self.alloc);
    }

    pub fn append(self: *Self, r: *Record) !void {
        try preAppend(self, r);

        r.pointer.offset = r.pointer.len() + self.current_offset;
        _ = try r.writePointer(&self.writer);
        _ = try r.write(&self.writer);

        postAppend(&self.header, r);
    }

    pub fn appendOwn(self: *Self, r: *Record) !void {
        defer r.deinit();
        try self.append(r);
    }

    // Compare the provided key with the ones in memory and
    // returns the last record that is found (or none if none is found)
    pub fn find(self: *Self, key_to_find: []const u8, alloc: std.mem.Allocator) !?*Record {
        var iter = try self.getIterator();
        return walFind(iter, key_to_find, alloc);
    }

    pub fn availableBytes(self: *Self) usize {
        return self.max_size - self.getWalSize();
    }

    // Sort the list of records in lexicographical order
    pub fn sort(self: *Self) []*Record {
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
    pub fn getIterator(self: *Self, alloc: std.mem.Allocator) !BytesIterator {
        return BytesIterator.init(self.writer, alloc);
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

test "wal_file" {
    var alloc = std.testing.allocator;

    var dm = try DiskManager.init("/tmp", alloc);
    defer dm.deinit();

    var filewal = try FileWal.init(512, dm, alloc);
    defer filewal.deinit();

    try filewal.appendKv("hello", "world");
}
