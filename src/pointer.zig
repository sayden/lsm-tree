const std = @import("std");
const Record = @import("./record.zig").Record;
const KeyLengthType = @import("./record.zig").KeyLengthType;
const Op = @import("ops.zig").Op;
const RecordLengthType = @import("./record.zig").RecordLengthType;

const Debug = @import("./debug.zig");
const println = Debug.println;
const prints = Debug.prints;
const print = std.debug.print;

pub const Error = error{ NullOffset, ArrayTooSmall };

// A pointer contains an Operation, a key and a offset to find the Value of the record.
// The pointer is stored as follows:
// 1 byte: Operation
// 2 bytes: Key size
// X bytes: Key
// 8 bytes: Offset in the data
pub const Pointer = struct {
    op: Op,
    key: []u8,
    offset: ?usize = null,
    alloc: std.mem.Allocator,

    const Self = @This();

    pub fn init(key: []const u8, op: Op, alloc: std.mem.Allocator) !*Self {
        var pointer = try alloc.create(Self);
        errdefer alloc.destroy(pointer);

        const key_ = try alloc.dupe(u8, key);
        errdefer alloc.free(key_);

        pointer.* = .{
            .key = key_,
            .op = op,
            .alloc = alloc,
        };

        return pointer;
    }

    pub fn deinit(self: *Self) void {
        self.alloc.free(self.key);
        self.alloc.destroy(self);
    }

    pub fn write(p: *Pointer, f: *std.fs.File) !usize {
        var writer = f.writer();

        const op = @intFromEnum(p.op);
        _ = try writer.writeByte(op);
        var bytes_written: usize = 1;

        // This truncate is not expected to fail
        const key_length = @as(KeyLengthType, @truncate(p.key.len));
        try writer.writeIntLittle(KeyLengthType, key_length);
        bytes_written += @sizeOf(KeyLengthType);

        bytes_written += try writer.write(p.key[0..key_length]);

        try writer.writeIntLittle(usize, try p.getOffset());
        bytes_written += @sizeOf(usize);

        return bytes_written;
    }

    pub fn read(f: *std.fs.File, alloc: std.mem.Allocator) !*Pointer {
        var p = try alloc.create(Pointer);
        errdefer alloc.destroy(p);

        var reader = f.reader();
        var op = @as(Op, @enumFromInt(try reader.readByte()));

        // read the key length
        const key_length = try reader.readIntLittle(KeyLengthType);

        // read the key
        var keybuf = try alloc.alloc(u8, key_length);
        errdefer alloc.free(keybuf);

        _ = try reader.readAtLeast(keybuf, key_length);

        //read the offset
        var offset = try reader.readIntLittle(usize);

        p.* = .{
            .op = op,
            .key = keybuf,
            .offset = offset,
            .alloc = alloc,
        };

        return p;
    }

    pub fn readValue(p: *Pointer, f: *std.fs.File, alloc: std.mem.Allocator) !*Record {
        var pointer = try p.clone(alloc);
        errdefer pointer.deinit();

        var r: *Record = try alloc.create(Record);
        errdefer alloc.destroy(r);
        r.pointer = pointer;

        _ = try r.read(f, alloc);

        return r;
    }

    pub fn readValueReusePointer(p: *Pointer, f: *std.fs.File, alloc: std.mem.Allocator) !*Record {
        var r: *Record = try alloc.create(Record);
        r.pointer = p;

        _ = try r.read(f, alloc);
        _ = r.len();

        return r;
    }

    pub fn getOffset(self: *Self) !usize {
        if (self.offset) |offset| {
            return offset;
        }

        return Error.NullOffset;
    }

    pub fn clone(self: *Self, alloc: std.mem.Allocator) !*Pointer {
        var pointer: *Pointer = try alloc.create(Pointer);
        errdefer alloc.destroy(pointer);

        var key = try alloc.dupe(u8, self.key);
        errdefer alloc.free(key);

        pointer.* = .{
            .alloc = alloc,
            .key = key,
            .op = self.op,
            .offset = self.offset,
        };

        return pointer;
    }

    pub fn len(self: *Self) usize {
        // last usize refers to size of offset field, which is indeed of type 'usize'
        return 1 + @sizeOf(KeyLengthType) + self.key.len + @sizeOf(usize);
    }

    pub fn debug(self: *Self) void {
        std.debug.print("\n--------\nPointer:\n--------\nKey:\t{s}\nOffset:\t{?}\nOp:\t{}\n--------\n", .{ self.key, self.offset, self.op });
    }
};

const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectEqualSlices = std.testing.expectEqualSlices;

test "pointer_init_deinit" {
    var alloc = std.testing.allocator;

    const key = "hello";
    const p = try Pointer.init(key, Op.Delete, alloc);
    defer p.deinit();

    try std.testing.expectError(Error.NullOffset, p.getOffset());
    try expectEqualStrings("hello", p.key);
    try std.testing.expect(key.ptr != p.key.ptr);
}

test "pointer_read_write" {
    var alloc = std.testing.allocator;

    const p = try Pointer.init("hello", Op.Update, alloc);
    defer p.deinit();

    var tmp_dir = std.testing.tmpDir(std.fs.Dir.OpenDirOptions{});
    defer tmp_dir.cleanup();

    var f = try tmp_dir.dir.createFile("test.sst", std.fs.File.CreateFlags{ .read = true });
    defer f.close();
    p.offset = 999;

    const bytes_written = try p.write(&f);

    try expectEqual(@as(usize, 16), bytes_written);
    try f.seekTo(0);

    var p1 = try Pointer.read(&f, alloc);
    defer p1.deinit();

    try expectEqual(@as(usize, 5), p1.key.len);
    try std.testing.expectEqualSlices(u8, p.key, p1.key);
    try expectEqual(p.offset, p1.offset);
    try expectEqualStrings(p.key, p1.key);
}

test "pointer_readRecord" {
    var alloc = std.testing.allocator;

    const pointer = try Pointer.init("hello", Op.Update, alloc);
    defer pointer.deinit();
    pointer.offset = pointer.len();

    const r = try Record.init("hello", "world", Op.Create, alloc);
    defer r.deinit();
    r.pointer.deinit();
    r.pointer = try pointer.clone(alloc);

    var tmp_dir = std.testing.tmpDir(std.fs.Dir.OpenDirOptions{});
    defer tmp_dir.cleanup();
    var f = try tmp_dir.dir.createFile("pointer.sst", std.fs.File.CreateFlags{ .read = true });
    defer f.close();

    var bytes_written = try pointer.write(&f);
    bytes_written += try r.write(&f);
    try expectEqual(r.len(), bytes_written);

    try f.seekTo(0);

    var p2 = try Pointer.read(&f, alloc);
    defer p2.deinit();
    const r1 = try p2.readValue(&f, alloc);
    defer r1.deinit();

    try expectEqualSlices(u8, r.getKey(), r1.getKey());
    try expectEqualSlices(u8, r.getVal(), r1.getVal());
    try expectEqual(r.getOffset(), r1.getOffset());
}

test "pointer_len" {
    var alloc = std.testing.allocator;

    const p = try Pointer.init("hello", Op.Update, alloc);
    defer p.deinit();

    try expectEqual(@as(usize, 16), p.len());
}
