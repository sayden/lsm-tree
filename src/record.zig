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
    op: Op = Op.Create,

    key: []u8,
    value: ?[]u8,
    offset: usize = 0,

    record_size_in_bytes: usize,
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Call deinit() to deallocate this struct and its values
    pub fn init(key: []const u8, value: []const u8, op: Op, alloc: std.mem.Allocator) !*Self {
        var s = try alloc.create(Self);

        s.op = op;

        s.key = try alloc.alloc(u8, key.len);
        std.mem.copy(u8, s.key, key);

        s.value = try alloc.alloc(u8, value.len);
        std.mem.copy(u8, s.value.?, value);

        s.allocator = alloc;
        s.record_size_in_bytes = 0;

        _ = s.bytesLen();

        return s;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.key);

        if (self.value != null)
            self.allocator.free(self.value.?);

        self.allocator.destroy(self);
    }

    pub fn init_string(key: []const u8, value: []const u8, alloc: *std.mem.Allocator) !*Self {
        return Record.init(key[0..], value[0..], alloc);
    }

    pub fn valueSize(self: *Self) usize {
        return self.value.len;
    }

    /// length of the op + key + the key length type
    fn totalKeyLen(self: *const Self) usize {
        // K bytes to store a number that indicates how many bytes the key has
        const key_length = @sizeOf(KeyLengthType);
        return 1 + key_length + self.key.len;
    }

    /// total size in bytes of the record
    pub fn bytesLen(self: *Self) usize {
        const record_len_type_len = @sizeOf(RecordLengthType);

        // Total
        // self.record_size_in_bytes = self.totalKeyLen() + record_len_type_len + self.value.len;
        self.record_size_in_bytes = 1 + record_len_type_len + self.value.?.len;
        return self.record_size_in_bytes;
    }

    // the minimum size possible for a record:
    // op + key length type + key 1 byte + value size + value 1 byte
    //  1 + 8               + 1          + 8          + 1
    pub fn minimum_size() usize {
        // return 1 + @sizeOf(KeyLengthType) + 1 + @sizeOf(RecordLengthType) + 1;
        return 1 + @sizeOf(RecordLengthType) + 1;
    }

    pub fn writeValue(self: *Record, writer: anytype) !usize {
        const op = [_]u8{@intFromEnum(self.op)};
        _ = try writer.write(&op);

        // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
        // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
        try writer.writeIntLittle(RecordLengthType, self.record_size_in_bytes);

        _ = try writer.writeAll(self.value.?);

        return writer.context.getPos();
    }

    pub fn writeKey(self: *Record, writer: anytype) !usize {
        const op = [_]u8{@intFromEnum(self.op)};
        _ = try writer.write(&op);

        // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
        // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
        // try writer.writeIntLittle(KeyLengthType, @as(KeyLengthType, @truncate(self.key.len)));

        // We can truncate here because we have already checked above that the size will fit
        const key_length = @as(u16, @truncate(self.key.len));
        try writer.writeIntLittle(u16, key_length);

        _ = try writer.writeAll(self.key);

        _ = try writer.writeIntLittle(usize, self.offset);

        return writer.context.getPos();
    }

    pub fn keySize(self: *Record) usize {
        return 1 + @sizeOf(KeyLengthType) + self.key + self.key.len;
    }

    pub fn readKey(reader: anytype, allocator: std.mem.Allocator) !*Record {
        var r = try allocator.create(Record);
        r.value = null;

        r.op = @as(Op, @enumFromInt(try reader.readByte()));

        // read the key length
        const key_length = try reader.readIntLittle(KeyLengthType);

        // read the key
        var buf = try allocator.alloc(u8, key_length);
        _ = try reader.readAtLeast(buf, key_length);
        r.key = buf;

        //read the offset
        r.offset = try reader.readIntLittle(usize);

        r.allocator = allocator;

        return r;
    }

    pub fn readValue(r: *Record, reader: anytype, allocator: std.mem.Allocator) !*Record {
        r.op = @as(Op, @enumFromInt(try reader.readByte()));

        //Read the record length bytes (4 or 8 usually) to get the total length of the record
        const record_length = try reader.readIntLittle(RecordLengthType);

        // read as many bytes as are left to get the value
        const value_length = record_length - @sizeOf(RecordLengthType) - 1;

        r.value = try allocator.alloc(u8, value_length);
        _ = try reader.readAtLeast(r.value.?, value_length);

        r.allocator = allocator;

        _ = r.bytesLen();

        return r;
    }

    pub fn toBytes(self: *Record, buf: []u8) !usize {
        var writerType = std.io.fixedBufferStream(buf);
        var writer = writerType.writer();
        return self.toBytesWriter(writer);
    }

    pub fn toBytesWriter(record: *Record, writer: anytype) !usize {
        if (record.key.len > std.math.maxInt(KeyLengthType)) {
            return RecordError.KeyTooBig;
        }

        const op = [_]u8{@intFromEnum(record.op)};
        _ = try writer.write(&op);

        // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
        // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
        try writer.writeIntLittle(RecordLengthType, record.record_size_in_bytes);

        // We can truncate here because we have already checked above that the size will fit
        const key_length = @as(u16, @truncate(record.key.len));
        try writer.writeIntLittle(u16, key_length);

        _ = try writer.writeAll(record.key);

        _ = try writer.writeAll(record.value.?);

        return writer.context.getPos();
    }

    pub fn fromBytesReader(allocator: std.mem.Allocator, reader: anytype) !*Record {
        var r = try allocator.create(Self);

        r.op = @as(Op, @enumFromInt(try reader.readByte()));

        //Read the record length bytes (4 or 8 usually) to get the total length of the record
        const record_length = try reader.readIntLittle(RecordLengthType);

        // read the key length
        const key_length = try reader.readIntLittle(KeyLengthType);
        r.key = try allocator.alloc(u8, key_length);

        // read the key
        _ = try reader.readAtLeast(r.key, key_length);

        // read as many bytes as are left to get the value
        const value_length = record_length - @sizeOf(KeyLengthType) - key_length - @sizeOf(RecordLengthType) - 1;

        r.value = try allocator.alloc(u8, value_length);
        _ = try reader.readAtLeast(r.value.?, value_length);

        r.allocator = allocator;

        _ = r.bytesLen();

        return r;
    }

    pub fn fromBytes(buf: []u8, allocator: *std.mem.Allocator) !*Record {
        var fixedReader = std.io.fixedBufferStream(buf);
        var reader = fixedReader.reader();
        return Record.fromBytesReader(allocator, reader);
    }

    pub fn expectedPointerSize(self: *Record) usize {
        var p = Pointer{
            .key = self.key,
            .op = Op.Create,
            .byte_offset = 0,
            .allocator = self.allocator,
        };
        return p.bytesLen();
    }

    pub fn debug(self: *Self) void {
        std.debug.print("Op:\t{}\nKey:\t{s}\nVal:\t{s}\nSize:\t{}\nOffset:\t{}\n", .{ self.op, self.key, self.value orelse "?", self.record_size_in_bytes, self.offset });
    }
};

test "record_expected_pointer_size" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();
    var size = Record.expectedPointerSize(r);
    try std.testing.expectEqual(@as(usize, 16), size);
}

test "record_init" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hell0", "world1", Op.Update, alloc);
    defer r.deinit();

    try expectEq(@as(usize, 15), r.record_size_in_bytes);
    try expectEq(@as(usize, 15), r.bytesLen());
    try expectEq(@as(usize, 8), r.totalKeyLen());

    try std.testing.expectEqualStrings("hell0", r.key);
    try std.testing.expectEqualStrings("world1", r.value.?);
    try expectEq(Op.Update, r.op);
}

test "record_size" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Create, alloc);
    defer r.deinit();

    const size = r.bytesLen();
    try expectEq(@as(u64, 14), size);
}

test "record_minimum_size" {
    try expectEq(@as(usize, 10), Record.minimum_size());
}

test "record_write_readValues" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();

    var buf = try alloc.alloc(u8, 512);
    defer alloc.free(buf);

    var fixedWriter = std.io.fixedBufferStream(buf);
    var writer = fixedWriter.writer();

    _ = try r.writeKey(writer);
    _ = try r.writeValue(writer);

    var fixedReader = std.io.fixedBufferStream(buf);
    var reader = fixedReader.reader();

    var new_r_reader = try Record.readKey(reader, alloc);
    defer new_r_reader.deinit();
    try std.testing.expectEqualStrings(r.key, new_r_reader.key);

    _ = try new_r_reader.readValue(reader, alloc);

    try std.testing.expectEqualStrings(r.value.?, new_r_reader.value.?);
    try std.testing.expectEqual(@as(usize, 14), new_r_reader.record_size_in_bytes);
}
