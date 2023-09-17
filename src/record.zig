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
    value: []u8,

    record_size_in_bytes: usize = 0,
    allocator: *std.mem.Allocator,

    const Self = @This();

    /// Call deinit() to deallocate this struct and its values
    pub fn init(key: []const u8, value: []const u8, op: Op, alloc: *std.mem.Allocator) !*Self {
        var s = try alloc.create(Self);

        s.op = op;

        s.key = try alloc.alloc(u8, key.len);
        std.mem.copy(u8, s.key, key);

        s.value = try alloc.alloc(u8, value.len);
        std.mem.copy(u8, s.value, value);

        s.allocator = alloc;
        s.record_size_in_bytes = 0;

        _ = s.bytesLen();

        return s;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.key);
        self.allocator.free(self.value);
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
        if (self.record_size_in_bytes != 0) {
            return self.record_size_in_bytes;
        }

        // total bytes (usually 8 bytes) to store the total bytes in the entire record
        const record_len_type_len = @sizeOf(RecordLengthType);

        // Total
        self.record_size_in_bytes = self.totalKeyLen() + record_len_type_len + self.value.len;
        return self.record_size_in_bytes;
    }

    // the minimum size possible for a record:
    // op + key length type + key 1 byte + value size + value 1 byte
    //  1 + 8               + 1          + 8          + 1
    pub fn minimum_size() usize {
        return 1 + @sizeOf(KeyLengthType) + 1 + @sizeOf(RecordLengthType) + 1;
    }

    /// TODO Update comment. Writes into the provided buf the data of the record in a contiguous array as described
    /// in fn Record()
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
        const temp = @as(u16, @truncate(record.key.len));
        try writer.writeIntLittle(u16, temp);

        _ = try writer.writeAll(record.key);
        _ = try writer.writeAll(record.value);

        return writer.context.getPos();
    }

    pub fn toBytesAlloc(self: *Record, alloc: *std.mem.Allocator) ![]u8 {
        var buf = try alloc.alloc(u8, self.bytesLen());
        _ = try self.toBytes(buf);
        return buf;
    }

    pub fn fromBytesReader(allocator: *std.mem.Allocator, reader: anytype) !*Record {
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
        _ = try reader.readAtLeast(r.value, value_length);

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
};

test "record.expected pointer size" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();
    var size = Record.expectedPointerSize(r);
    try std.testing.expectEqual(@as(usize, 16), size);
}

test "record.init" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hell0", "world1", Op.Update, &alloc);
    defer r.deinit();

    try expectEq(@as(usize, 22), r.record_size_in_bytes);
    try expectEq(@as(usize, 22), r.bytesLen());
    try expectEq(@as(usize, 8), r.totalKeyLen());

    try std.testing.expectEqualStrings("hell0", r.key);
    try std.testing.expectEqualStrings("world1", r.value);
    try expectEq(Op.Update, r.op);
}

test "record.size" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Create, &alloc);
    defer r.deinit();

    const size = r.bytesLen();
    try expectEq(@as(u64, 21), size);
}

test "record.minimum size" {
    try expectEq(@as(usize, 13), Record.minimum_size());
}
test "record.toBytesAlloc" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();

    var buf = try Record.toBytesAlloc(r, &alloc);
    defer alloc.free(buf);
    try std.testing.expectStringEndsWith(buf, "helloworld");
    try expect(!std.mem.eql(u8, buf, "helloworld"));
    try expectEq(@as(usize, 21), r.bytesLen());
    try expectEq(buf[0], @intFromEnum(Op.Delete));
}

test "record.toBytes returns a contiguous array with the record" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();

    var buf = try alloc.alloc(u8, r.bytesLen());
    defer alloc.free(buf);

    const total_bytes = try Record.toBytes(r, buf);
    try expectEq(@as(usize, 21), total_bytes);
    try std.testing.expectStringEndsWith(buf, "helloworld");
    try expect(!std.mem.eql(u8, buf, "helloworld"));
    try expectEq(@as(usize, 21), r.bytesLen());
    try expectEq(buf[0], @intFromEnum(Op.Delete));
}

test "record.read_record having an slice, read a record starting at an offset" {
    // var offset = 0;
    var record_bytes = [_]u8{
        0, //Op
        21, 0, 0, 0, 0, 0, 0, 0, //21 bytes
        5, 0, //5 bytes of key
        104, 101, 108, 108, 111, //hello (the key)
        119, 111, 114, 108, 100, //world (the value)
    };

    var alloc = std.testing.allocator;

    const r = try Record.fromBytes(record_bytes[0..], &alloc);
    defer r.deinit();

    try std.testing.expectEqualStrings("hello", r.key);
    try std.testing.expectEqualStrings("world", r.value);
}

test "record_fromBytes" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();

    var buf = try alloc.alloc(u8, r.bytesLen());
    defer alloc.free(buf);

    _ = try Record.toBytes(r, buf);
    var fixedReader = std.io.fixedBufferStream(buf);
    var reader = fixedReader.reader();

    var new_r_reader = try Record.fromBytesReader(&alloc, reader);
    defer new_r_reader.deinit();

    try std.testing.expectEqualStrings(r.key, new_r_reader.key);
    try std.testing.expectEqualStrings(r.value, new_r_reader.value);
}
