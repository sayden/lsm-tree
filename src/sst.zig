const std = @import("std");
const Pointer = @import("./pointer.zig").Pointer;
const Record = @import("./record.zig").Record;
const HeaderNs = @import("./header.zig");
const SstIndex = @import("./sst_manager.zig").SstIndex;
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

const Header = HeaderNs.Header;

pub const Sst = struct {
    header: Header,
    alloc: std.mem.Allocator,

    mem: []*Record,
    index: ?*SstIndex = null,

    current_mem_index: usize = 0,
    current_pointer_index: usize = 0,

    stored_records: usize,

    pub fn init(file: *std.fs.File, alloc: std.mem.Allocator) !Sst {
        // read header
        var ws = ReaderWriterSeeker.initFile(file.*);
        const header = try Header.read(&ws);

        // init
        var pointers = try alloc.alloc(*Pointer, header.total_records);
        defer alloc.free(pointers);

        // read pointers
        for (0..header.total_records) |i| {
            const pointer = try Pointer.read(&ws, alloc);
            pointers[i] = pointer;
        }

        var stored_records: usize = 0;

        var mem = try alloc.alloc(*Record, header.total_records);
        errdefer alloc.free(mem);

        //read values
        for (0..header.total_records) |i| {
            var pointer: *Pointer = pointers[i];
            var r = try pointer.readValueReusePointer(&ws, alloc);
            errdefer r.deinit();
            mem[i] = r;
            stored_records += 1;
        }

        var s = Sst{
            .mem = mem,
            .alloc = alloc,
            .header = header,
            .stored_records = stored_records,
        };

        return s;
    }

    pub fn deinit(self: *Sst) void {
        for (0..self.stored_records) |i| {
            self.mem[i].deinit();
        }
        self.alloc.free(self.mem);

        if (self.index) |index| {
            index.deinit();
        }
    }

    pub fn initWithIndex(index: *SstIndex, alloc: std.mem.Allocator) !*Sst {
        var sst: Sst = Sst{
            .header = index.header,
            .alloc = alloc,
            .mem = try alloc.alloc(*Record, index.header.total_records),
            .index = index,
        };

        //read values
        for (0..index.header.total_records) |i| {
            try index.file.seekTo(index.getPointer(i).?.offset);
            var r = try index.pointers[i].readRecordClonePointer(index.file.reader(), alloc);
            sst.mem[i] = r;
        }

        return &sst;
    }

    pub fn getRecord(sst: *Sst, index: usize) ?*Record {
        if (index >= sst.header.total_records) {
            return null;
        }

        return sst.mem[index];
    }

    pub fn getIndex(sst: *Sst) ?*SstIndex {
        return sst.index;
    }
};

const expectEqualString = std.testing.expectEqualStrings;

test "sst_readFile" {
    var alloc = std.testing.allocator;

    var file = try std.fs.cwd().openFile("./testing/example.sst", std.fs.File.OpenFlags{ .mode = .read_only });
    defer file.close();

    var sst = try Sst.init(&file, alloc);
    defer sst.deinit();

    try expectEqualString("hello0", sst.getRecord(0).?.getKey());
    try expectEqualString("hello1", sst.getRecord(1).?.getKey());
    try expectEqualString("hello2", sst.getRecord(2).?.getKey());
}
