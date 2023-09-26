const std = @import("std");
const Op = @import("./ops.zig").Op;
const lsmtree = @import("./record.zig");
const Pointer = @import("./pointer.zig").Pointer;

pub const RecordError = error{ BufferTooSmall, KeyTooBig };
pub const RecordLengthType: type = usize;
pub const KeyLengthType: type = u16;

const Debug = @import("./debug.zig");
const println = Debug.println;
const prints = Debug.prints;
const print = std.debug.print;

/// A record is an array of contiguous bytes in the following form:
///
/// 1 byte to store the op type of the record
/// 8 bytes T to store the total bytes that the record uses
/// 2 bytes L to store the key length
/// K bytes to store the key, where K = key.len
/// V bytes to store the value, where V = value.len
pub const Record = struct {
    pointer: *Pointer,

    value: []u8,

    allocator: std.mem.Allocator,

    const Self = @This();

    /// Call deinit() to deallocate this struct and its values
    pub fn init(key: []const u8, value: []const u8, op: Op, alloc: std.mem.Allocator) !*Self {
        var new_record: *Record = try alloc.create(Self);
        errdefer alloc.destroy(new_record);

        const pointer: *Pointer = try Pointer.init(key, op, alloc);
        errdefer pointer.deinit();

        const value_ = try alloc.dupe(u8, value);
        errdefer alloc.free(value_);

        new_record.* = Record{
            .pointer = pointer,
            .value = value_,
            .allocator = alloc,
        };

        return new_record;
    }

    pub fn deinit(self: *Self) void {
        self.pointer.deinit();
        self.allocator.free(self.value);
        self.allocator.destroy(self);
    }

    pub fn minimum_size() usize {
        return 1 + @sizeOf(RecordLengthType) + 1;
    }

    pub fn writePointer(self: *Self, f: *std.fs.File) !usize {
        return self.pointer.write(f);
    }

    pub fn write(self: *Self, f: *std.fs.File) !usize {
        var writer = f.writer();

        var bytes_written: usize = 0;
        const op = [_]u8{@intFromEnum(self.pointer.op)};

        bytes_written += try writer.write(&op);

        // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
        // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
        try writer.writeIntLittle(RecordLengthType, self.valueLen());
        bytes_written += @sizeOf(RecordLengthType);

        try writer.writeAll(self.value);
        bytes_written += self.value.len;

        return bytes_written;
    }

    pub fn read(r: *Self, f: *std.fs.File, alloc: std.mem.Allocator) !usize {
        try f.seekTo(try r.pointer.getOffset());
        var reader = f.reader();

        r.pointer.op = @as(Op, @enumFromInt(try reader.readByte()));
        var bytes_written: usize = 1;

        //Read the record length bytes (4 or 8 usually) to get the total length of the record
        const record_length = try reader.readIntLittle(RecordLengthType);
        bytes_written += @sizeOf(RecordLengthType);

        // read as many bytes as are left to get the value
        const value_length = record_length - @sizeOf(RecordLengthType) - 1;

        r.value = try alloc.alloc(u8, value_length);
        errdefer alloc.free(r.value);

        _ = try reader.readAtLeast(r.value, value_length);
        bytes_written += value_length;

        r.allocator = alloc;

        return bytes_written;
    }

    pub fn clone(r: *Record, alloc: std.mem.Allocator) !*Record {
        const value = try alloc.dupe(u8, r.value);
        errdefer alloc.free(value);

        const pointer = try r.pointer.clone(alloc);
        errdefer pointer.deinit();

        var new_record: *Record = try alloc.create(Record);
        errdefer alloc.destroy(new_record);

        new_record.* = Record{
            .pointer = pointer,
            .value = value,
            .allocator = alloc,
        };

        return new_record;
    }

    /// Length in bytes of the record, pointer included
    pub fn valueLen(self: *Self) usize {
        const record_len_type_len = @sizeOf(RecordLengthType);

        // Total
        var record_size_in_bytes = 1 + record_len_type_len + self.value.len;
        return record_size_in_bytes;
    }

    /// Length in bytes of the value, pointer NOT included
    pub fn len(self: *Self) usize {
        return self.valueLen() + self.pointer.len();
    }

    pub fn getKey(r: *Self) []const u8 {
        return r.pointer.key;
    }

    pub fn getOffset(r: *Self) !usize {
        return r.pointer.getOffset();
    }

    pub fn pointerSize(self: *Self) usize {
        return self.pointer.len();
    }

    pub fn getVal(self: *Self) []const u8 {
        return self.value;
    }

    pub fn debug(self: *Self) void {
        std.debug.print("\n-------\nRecord:\n-------\nOp:\t{}\nKey:\t{s}\nVal:\t{s}\nSize:\t{}\nVsize:\t{}\nOffset:\t{?}\n\n", .{ self.pointer.op, self.pointer.key, self.value, self.len(), self.valueLen(), self.pointer.offset });
    }
};

const expectEqualStrings = std.testing.expectEqualStrings;
const expectEqualSlices = std.testing.expectEqualSlices;
const expect = std.testing.expect;
const expectEq = std.testing.expectEqual;

test "record_init" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hell0", "world1", Op.Update, alloc);
    defer r.deinit();

    try expectEq(@as(usize, 15), r.valueLen());
    try expectEq(@as(usize, 31), r.len());

    try expectEqualStrings("hell0", r.pointer.key);
    try expectEqualStrings("world1", r.value);
    try expectEq(Op.Update, r.pointer.op);
}

test "record_len" {
    var alloc = std.testing.allocator;

    const r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();

    try expectEq(@as(usize, 14), r.valueLen());
    try expectEq(@as(usize, 30), r.len());
}

test "record_pointerSize" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();

    var size = r.pointerSize();
    try expectEq(@as(usize, 16), size);
}

test "record_minimum_size" {
    try expectEq(@as(usize, 10), Record.minimum_size());
}

test "record_write_readValues" {
    var alloc = std.testing.allocator;

    var record1 = try Record.init("hello", "world", Op.Delete, alloc);
    defer record1.deinit();
    record1.pointer.offset = record1.pointer.len() * 2;

    var record2 = try Record.init("hell1", "worl1", Op.Delete, alloc);
    defer record2.deinit();

    record2.pointer.offset = record2.pointer.len() * 2 + record2.valueLen();

    // Create a temp file
    var tmp_dir = std.testing.tmpDir(std.fs.Dir.OpenDirOptions{});
    defer tmp_dir.cleanup();

    var file = try tmp_dir.dir.createFile("test.sst", std.fs.File.CreateFlags{ .read = true });
    defer file.close();

    // write the first pointer and second pointer
    const pointer_bytes_written = try record1.writePointer(&file);
    try expectEq(@as(usize, 16), pointer_bytes_written);
    _ = try record2.writePointer(&file);

    // write the first record and second record
    const value_bytes_written = try record1.write(&file);
    try expectEq(@as(usize, 14), value_bytes_written);
    _ = try record2.write(&file);

    try file.seekTo(0);

    // read first and second pointers
    var pointer1 = try Pointer.read(&file, alloc);
    defer pointer1.deinit();

    var pointer2 = try Pointer.read(&file, alloc);
    defer pointer2.deinit();

    try expectEq(Op.Delete, pointer1.op);
    try std.testing.expectEqualDeep(record1.pointer, pointer1);
    try std.testing.expectEqualDeep(record2.pointer, pointer2);

    // read second then first, to ensure offseting is working as expected
    var record2_ = try pointer2.readValue(&file, alloc);
    defer record2_.deinit();

    var record1_ = try pointer1.readValue(&file, alloc);
    defer record1_.deinit();

    try std.testing.expectEqualDeep(record2, record2_);
    try std.testing.expectEqualDeep(record1, record1_);
}
