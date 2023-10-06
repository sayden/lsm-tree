const std = @import("std");
const Allocator = std.mem.Allocator;

const HeaderNs = @import("./header.zig");
const Header = HeaderNs.Header;
const RecordNS = @import("./record.zig");
const Record = RecordNS.Record;
const Pointer = RecordNS.Pointer;
const FileData = @import("./disk_manager.zig").FileData;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Sst = @import("./sst.zig").Sst;
const StringsNs = @import("./strings.zig");
const Iterator = @import("./iterator.zig").Iterator;
const strcmp = StringsNs.strcmp;

const Error = error{EmptyFile};

pub const SstIndex = struct {
    const log = std.log.scoped(.SstIndex);

    header: Header,
    first_key: []const u8,
    last_key: []const u8,
    pointers: []*Pointer,
    filedata: FileData,

    alloc: Allocator,

    pub fn init(relative: []const u8, alloc: Allocator) !*SstIndex {
        const path = try std.fs.cwd().realpathAlloc(alloc, relative);
        errdefer alloc.free(path);

        log.debug("Opening file {s}", .{path});

        var file = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{});
        const filedata = FileData{
            .file = file,
            .filename = path,
            .alloc = alloc,
        };
        return initFile(filedata, alloc);
    }

    pub fn initFile(filedata: FileData, alloc: Allocator) !*SstIndex {
        const stat = try filedata.file.stat();
        if (stat.size == 0) {
            return Error.EmptyFile;
        }

        var ws = ReaderWriterSeeker.initFile(filedata.file);

        // read header
        const header = try Header.read(&ws);

        // read pointers
        var pointers = try alloc.alloc(*Pointer, header.total_records);
        errdefer alloc.free(pointers);

        for (0..header.total_records) |i| {
            var p = try Pointer.read(&ws, alloc);
            errdefer p.deinit();
            pointers[i] = p;
        }

        var s: *SstIndex = try alloc.create(SstIndex);
        s.* = SstIndex{
            .header = header,
            .first_key = try alloc.dupe(u8, pointers[0].key),
            .last_key = try alloc.dupe(u8, pointers[header.total_records - 1].key),
            .filedata = filedata,
            .alloc = alloc,
            .pointers = pointers,
        };

        log.debug("Loaded index '{s}' from '{s}' with {} records", .{ s.header.id, s.filedata.filename, s.header.total_records });

        return s;
    }

    pub fn deinit(self: *SstIndex) void {
        for (self.pointers) |p| {
            p.deinit();
        }
        self.filedata.deinit();
        self.alloc.free(self.first_key);
        self.alloc.free(self.last_key);

        self.alloc.free(self.pointers);
        self.alloc.destroy(self);
    }

    pub fn size(self: *SstIndex) usize {
        // TODO To use HeaderNs.headerSize() is not backwards compatible with headers that contain less information
        return self.header.pointers_size + self.header.records_size + HeaderNs.headerSize();
    }

    pub fn getPointer(pointers: []*Pointer, index: usize) ?*Pointer {
        if (index >= pointers.len) {
            return null;
        }

        return pointers[index];
    }

    pub fn load(s: *SstIndex) !*Sst {
        return Sst.initWithIndex(s, s.alloc);
    }

    pub fn binarySearchFn(_: void, key: []const u8, mid_item: *Pointer) std.math.Order {
        return strcmp(key, mid_item.key);
    }

    pub fn find(idx: *SstIndex, key: []const u8) ?*Pointer {
        if (!idx.isBetween(key)) {
            return null;
        }

        //TODO Fix this binary search that seems to not work
        const i = std.sort.binarySearch(*Pointer, key, idx.pointers, {}, binarySearchFn);
        if (i) |index| {
            return idx.pointers[index];
        }

        return null;
    }

    pub fn get(idx: *SstIndex, key: []const u8, alloc: Allocator) !?*Record {
        const p = idx.find(key);
        if (p) |pointer| {
            return idx.retrieveRecordFromFile(pointer, alloc);
        }

        return null;
    }

    const PointerIterator = Iterator(*Pointer);
    pub fn getPointersIterator(self: *SstIndex) PointerIterator {
        return PointerIterator.init(self.pointers);
    }

    fn retrieveRecordFromFile(idx: *SstIndex, p: *Pointer, alloc: Allocator) !*Record {
        try idx.filedata.file.seekTo(try p.getOffset());
        var ws = ReaderWriterSeeker.initFile(idx.filedata.file);
        return try p.readValue(&ws, alloc);
    }

    // checks if key is in the range of keys of this sst
    pub fn isBetween(self: *SstIndex, key: []const u8) bool {
        return strcmp(key, self.firstKey()).compare(std.math.CompareOperator.gte) and strcmp(key, self.lastKey()).compare(std.math.CompareOperator.lte);
    }

    pub fn debug(self: *SstIndex) void {
        log.debug("\n--------\nSstIndex\n--------\nFirst key\t{s}\nLast key\t{s}", .{ self.first_key.key, self.last_key.key });
    }
};

test "sstindex_binary_search" {
    var alloc = std.testing.allocator;

    var idx = try SstIndex.init("./testing/example.sst", alloc);
    defer idx.deinit();

    var maybe_pointer = idx.find("hello6");

    try std.testing.expectEqualStrings(idx.pointers[6].key, maybe_pointer.?.key);
}

test "sstindex_init" {
    var alloc = std.testing.allocator;
    var idx = try SstIndex.init("testing/example.sst", alloc);
    defer idx.deinit();

    try std.testing.expectEqual(@as(usize, 7), idx.header.total_records);
    try std.testing.expectEqualStrings("hello0", idx.first_key);
    try std.testing.expectEqualStrings("hello6", idx.last_key);
}

test "sstindex_find" {
    var alloc = std.testing.allocator;
    var idx = try SstIndex.init("testing/example.sst", alloc);
    defer idx.deinit();

    const p1 = idx.find("hello1").?;
    try std.testing.expectEqualStrings("hello1", p1.key);

    const no_p2 = idx.find("not found");
    if (no_p2) |_| {
        try std.testing.expect(false);
    }
}

test "sstindex_isbetween" {
    var alloc = std.testing.allocator;
    var idx = try SstIndex.init("testing/example.sst", alloc);
    defer idx.deinit();

    try std.testing.expect(idx.isBetween("hello1"));
    try std.testing.expect(!idx.isBetween("not found"));
}

test "sstindex_retrieveRecordFromFile" {
    var alloc = std.testing.allocator;
    var idx = try SstIndex.init("testing/example.sst", alloc);
    defer idx.deinit();

    var pointer = idx.find("hello1").?;
    var record = try idx.retrieveRecordFromFile(pointer, alloc);
    defer record.deinit();

    try std.testing.expectEqualStrings("world1", record.getVal());
}
