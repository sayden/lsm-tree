const std = @import("std");
const expect = std.testing.expect;
const expectEq = std.testing.expectEqual;

const Op = @import("./ops.zig").Op;
const lsmtree = @import("./record.zig");
const Pointer = @import("./pointer.zig").Pointer;

pub const RecordError = error{ BufferTooSmall, KeyTooBig };
pub const RecordLengthType: type = usize;
pub const KeyLengthType: type = u16;

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

    record_size_in_bytes: usize,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Call deinit() to deallocate this struct and its values
    pub fn init(key: []const u8, value: []const u8, op: Op, alloc: std.mem.Allocator) !*Self {
        var new_record: *Record = try alloc.create(Self);

        var p: *Pointer = try Pointer.init(key, alloc);

        p.op = op;
        new_record.pointer = p;

        new_record.value = try alloc.alloc(u8, value.len);
        @memcpy(new_record.value, value);

        new_record.allocator = alloc;
        new_record.record_size_in_bytes = 0;

        _ = new_record.len();

        return new_record;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.value);
        self.pointer.deinit();
        self.allocator.destroy(self);
    }

    /// total size in bytes of the record
    pub fn len(self: *Self) usize {
        const record_len_type_len = @sizeOf(RecordLengthType);

        // Total
        self.record_size_in_bytes = 1 + record_len_type_len + self.value.len;
        return self.record_size_in_bytes;
    }

    pub fn minimum_size() usize {
        return 1 + @sizeOf(RecordLengthType) + 1;
    }

    pub fn writePointer(self: *Record, writer: anytype) !usize {
        return self.pointer.write(writer);
    }

    pub fn writeValue(self: *Record, writer: anytype) !usize {
        const op = [_]u8{@intFromEnum(self.pointer.op)};
        _ = try writer.write(&op);

        // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
        // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
        try writer.writeIntLittle(RecordLengthType, self.record_size_in_bytes);

        _ = try writer.writeAll(self.value);

        return writer.context.getPos();
    }

    pub fn readValue(r: *Record, reader: anytype, allocator: std.mem.Allocator) !*Record {
        r.op = @as(Op, @enumFromInt(try reader.readByte()));

        //Read the record length bytes (4 or 8 usually) to get the total length of the record
        const record_length = try reader.readIntLittle(RecordLengthType);

        // read as many bytes as are left to get the value
        const value_length = record_length - @sizeOf(RecordLengthType) - 1;

        r.value = try allocator.alloc(u8, value_length);
        _ = try reader.readAtLeast(r.value, value_length);

        r.allocator = allocator;

        _ = r.len();

        return r;
    }

    pub fn getKey(r: *Record) []const u8 {
        return r.pointer.key;
    }

    pub fn getOffset(r: *Record) usize {
        return r.pointer.offset;
    }

    pub fn pointerSize(self: *Record) usize {
        return self.pointer.len();
        // return Pointer.bytesLen(self.getKey().len);
    }

    pub fn debug(self: *Self) void {
        std.debug.print("\nOp:\t{}\nKey:\t{s}\nVal:\t{s}\nSize:\t{}\nOffset:\t{}\n\n", .{ self.pointer.op, self.pointer.key, self.value, self.record_size_in_bytes, self.pointer.offset });
    }
};

test "record_expected_pointer_size" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();

    var size = Record.pointerSize(r);
    try std.testing.expectEqual(@as(usize, 16), size);
}

test "record_init" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hell0", "world1", Op.Update, alloc);
    defer r.deinit();

    try expectEq(@as(usize, 15), r.record_size_in_bytes);
    try expectEq(@as(usize, 15), r.len());

    try std.testing.expectEqualStrings("hell0", r.pointer.key);
    try std.testing.expectEqualStrings("world1", r.value);
    try expectEq(Op.Update, r.pointer.op);
}

test "record_size" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hello", "world", Op.Create, alloc);
    defer r.deinit();

    const size = r.len();
    try expectEq(@as(u64, 14), size);
}

test "record_minimum_size" {
    try expectEq(@as(usize, 10), Record.minimum_size());
}

test "record_write_readValues" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();

    r.pointer.offset = 100;

    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    var bufferStream = std.io.fixedBufferStream(buf);
    var writer = bufferStream.writer();

    _ = try r.writePointer(writer);
    _ = try r.writeValue(writer);

    try bufferStream.seekTo(0);
    var reader = bufferStream.reader();

    var pointer = try Pointer.read(reader, alloc);
    defer pointer.deinit();

    try std.testing.expectEqualStrings(r.pointer.key, pointer.key);
    try std.testing.expectEqual(@as(usize, 100), pointer.offset);

    var r1 = try pointer.readRecord_clone_pointer(reader, alloc);
    defer r1.deinit();

    try std.testing.expectEqualSlices(u8, r.value, r1.value);
    try std.testing.expectEqual(@as(usize, 14), r1.record_size_in_bytes);
}
