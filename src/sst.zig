const std = @import("std");
const Pointer = @import("./pointer.zig").Pointer;
const Record = @import("./record.zig").Record;
const HeaderPkg = @import("./header.zig");

const Header = HeaderPkg.Header;

/// A SST or Sorted String Table is created from a Wal object. The structure is the following:
///
/// HEADER: Check the header.zig file for details
///
/// DATA CHUNK:
/// Contiguous areray of records
///
/// KEYS CHUNK
/// Contiguous arecodray of keys only with pointers to values in the data chunk
pub const Sst = struct {
    const Self = @This();

    header: Header,
    allocator: *std.mem.Allocator,

    mem: []*Record,
    pointers: []*Pointer,

    // first_pointer: *Pointer,
    // last_pointer: *Pointer,
    current_mem_index: usize = 0,
    current_pointer_index: usize = 0,

    pub fn init(f: *std.fs.File, allocator: *std.mem.Allocator) !*Self {
        var s = try allocator.create(Sst);
        s.allocator = allocator;

        var reader = f.reader();
        s.header = try Header.fromReader(reader);

        s.mem = try allocator.alloc(*Record, s.header.total_records);
        for (0..s.header.total_records) |i| {
            var r = try Record.fromBytesReader(allocator, reader);
            s.mem[i] = r;
        }

        s.pointers = try allocator.alloc(*Pointer, s.header.total_records);
        for (0..s.header.total_records) |i| {
            var p = try Pointer.fromBytesReader(allocator, reader);
            s.pointers[i] = p;
        }

        return s;
    }

    pub fn read_file(self: *Self) !void {
        _ = self;
    }

    pub fn deinit(self: *Self) void {
        for (self.mem) |record| {
            record.deinit();
        }
        for (self.pointers) |p| {
            p.deinit();
        }

        self.allocator.free(self.mem);
        self.allocator.free(self.pointers);
        self.allocator.destroy(self);
    }

    pub fn fromBytes(self: *Self, buf: []u8) !usize {
        var fixedReader = std.io.fixedBufferStream(buf);
        var reader = fixedReader.reader();
        return self.fromBytesReader(reader);
    }

    pub fn fromBytesReader(self: *Self, reader: anytype) !usize {
        //Read header
        self.header = try Header.fromReader(reader);
        self.header.str();

        // Read records
        std.debug.print("Reading {} records\n", .{self.header.total_records});
        for (0..self.header.total_records) |_| {
            var r = try Record.fromBytesReader(self.allocator, reader);
            std.debug.print("Record: key: {s}, value: {s}\n", .{ r.key, r.value });
            self.mem[self.current_mem_index] = r;
            self.current_mem_index += 1;
            // offset += r.bytesLen();
        }

        //Read pointers?
        for (0..self.header.total_records) |_| {
            var p = try Pointer.fromBytesReader(self.allocator, reader);
            self.pointers[self.current_pointer_index] = p;
            self.current_pointer_index += 1;
            // offset += p.bytesLen();
        }

        return reader.context.getPos();
    }
};

test "sst_fromBytes" {
    var allocator = std.testing.allocator;

    var f = try std.fs.cwd().openFile("../testing/example.sst", std.fs.File.OpenFlags{ .mode = .read_only });
    defer f.close();

    var sst = try Sst.init(&f, &allocator);
    defer sst.deinit();
}
