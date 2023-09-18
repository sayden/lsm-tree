const std = @import("std");
const Pointer = @import("./pointer.zig").Pointer;
const Record = @import("./record.zig").Record;
const HeaderPkg = @import("./header.zig");

const Header = HeaderPkg.Header;

pub const Sst = struct {
    const Self = @This();

    header: Header,
    allocator: std.mem.Allocator,

    mem: []*Record,
    // pointers: []*Pointer,

    current_mem_index: usize = 0,
    current_pointer_index: usize = 0,

    pub fn init(f: *std.fs.File, allocator: std.mem.Allocator) !*Self {
        var s = try allocator.create(Sst);
        s.allocator = allocator;
        var reader = f.reader();

        // read header
        s.header = try Header.fromReader(reader);

        // read pointers
        var pointers = try allocator.alloc(*Pointer, s.header.total_records);
        defer allocator.free(pointers);
        for (0..s.header.total_records) |i| {
            var p = try Pointer.read(reader, s.allocator);
            pointers[i] = p;
        }

        //read values
        s.mem = try s.allocator.alloc(*Record, s.header.total_records);
        for (0..s.header.total_records) |i| {
            var r = try pointers[i].readRecord(reader);
            s.mem[i] = r;
        }

        return s;
    }

    pub fn deinit(self: *Self) void {
        for (self.mem) |record| {
            record.deinit();
        }

        self.allocator.free(self.mem);
        self.allocator.destroy(self);
    }

    pub fn getRecord(sst: *Sst, index: usize) ?*Record {
        if (index >= sst.header.total_records) {
            return null;
        }

        return sst.mem[index];
    }
};

const expectEqualString = std.testing.expectEqualStrings;

test "sst_fromBytes" {
    var allocator = std.testing.allocator;

    var f = try std.fs.cwd().openFile("./testing/example.sst", std.fs.File.OpenFlags{ .mode = .read_only });
    defer f.close();

    var sst = try Sst.init(&f, allocator);
    defer sst.deinit();

    try expectEqualString("hello", sst.getRecord(0).?.getKey());
    try expectEqualString("hello", sst.getRecord(1).?.getKey());
    try expectEqualString("hello", sst.getRecord(2).?.getKey());
}
