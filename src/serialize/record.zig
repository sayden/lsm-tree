const std = @import("std");
const Record = @import("lsmtree").Record;
const RecordError = @import("lsmtree").RecordError;
const KeyLengthType = @import("lsmtree").KeyLengthType;
const RecordLengthType = @import("lsmtree").RecordLengthType;
const Op = @import("lsmtree").Op;
const lsmtree = @import("lsmtree");
const expect = std.testing.expect;
const expectEq = std.testing.expectEqual;

/// TODO Update comment. Writes into the provided buf the data of the record in a contiguous array as described
/// in fn Record()
pub fn toBytes(record: *Record, buf: []u8) RecordError!usize {
    var offset: usize = 0;

    //Abort early if necessary
    if (buf.len < record.record_size_in_bytes) {
        return RecordError.BufferTooSmall;
    }

    if (record.key.len > std.math.maxInt(KeyLengthType)) {
        return RecordError.KeyTooBig;
    }
    buf[0] = @enumToInt(record.op);
    offset += 1;

    // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
    // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
    std.mem.writeIntSliceLittle(RecordLengthType, buf[offset .. offset + @sizeOf(RecordLengthType)], record.record_size_in_bytes);
    offset += @sizeOf(RecordLengthType);

    // We can truncate here because we have already checked that the size will fit above
    const temp = @truncate(u16, record.key.len);
    std.mem.writeIntSliceLittle(u16, buf[offset .. offset + @sizeOf(u16)], temp);
    offset += @sizeOf(u16);

    // TODO Write a function that "Reads as stream" (alas Read interface) instead of copying values
    std.mem.copy(u8, buf[offset .. offset + record.key.len], record.key);
    offset += record.key.len;

    std.mem.copy(u8, buf[offset .. offset + record.value.len], record.value);

    return record.record_size_in_bytes;
}

pub fn toBytesAlloc(record: *Record, alloc: *std.mem.Allocator) ![]u8 {
    var buf = try alloc.alloc(u8, record.bytesLen());
    _ = toBytes(record, buf) catch |err|
        return err;

    return buf;
}

pub fn fromBytes(buf: []u8, allocator: *std.mem.Allocator) ?*Record {
    // Check if there's enough data to read the record size
    if (buf.len < @sizeOf(RecordLengthType)) {
        return null;
    }
    var offset: usize = 0;

    var op = @intToEnum(Op, buf[offset]);
    offset += 1;

    //Read the record length bytes (4 or 8 usually) to get the total length of the record
    const bytes_for_record_length = buf[offset .. offset + @sizeOf(RecordLengthType)];
    const record_length = std.mem.readIntSliceLittle(RecordLengthType, bytes_for_record_length);
    offset += @sizeOf(RecordLengthType);

    // check if the buffer actually has the amount of bytes that the record_length says
    if (buf.len < record_length) {
        return null;
    }

    // read the key length
    const bytes_for_key_length = buf[offset .. offset + @sizeOf(KeyLengthType)];
    const key_length = std.mem.readIntSliceLittle(KeyLengthType, bytes_for_key_length);
    offset += @sizeOf(u16);

    // read the key
    const key = buf[offset .. offset + key_length];
    offset += key_length;

    // read as many bytes as left to get the value
    const value_length = record_length - bytes_for_key_length.len - key_length - @sizeOf(RecordLengthType) - 1;
    const value = buf[offset .. offset + value_length];

    var r = Record.init(key, value, op, allocator) catch return null;
    return r;
}

test "record.toBytesAlloc" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();

    var buf = try toBytesAlloc(r, &alloc);
    defer alloc.free(buf);
    try std.testing.expectStringEndsWith(buf, "helloworld");
    try expect(!std.mem.eql(u8, buf, "helloworld"));
    try expectEq(@as(usize, 21), r.bytesLen());
    try expectEq(buf[0], @enumToInt(Op.Delete));
}

test "record.toBytes returns a contiguous array with the record" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();

    var buf = try alloc.alloc(u8, r.bytesLen());
    defer alloc.free(buf);

    const total_bytes = try toBytes(r, buf);
    try expectEq(@as(usize, 21), total_bytes);
    try std.testing.expectStringEndsWith(buf, "helloworld");
    try expect(!std.mem.eql(u8, buf, "helloworld"));
    try expectEq(@as(usize, 21), r.bytesLen());
    try expectEq(buf[0], @enumToInt(Op.Delete));
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

    const r = lsmtree.serialize.record.fromBytes(record_bytes[0..], &alloc).?;
    defer r.deinit();

    try std.testing.expectEqualStrings("hello", r.key);
    try std.testing.expectEqualStrings("world", r.value);

    // return none if there's not enough data for a record in the buffer
    // starting from 20, there's not enough data to read a potential record size
    const r2 = lsmtree.serialize.record.fromBytes(record_bytes[20..], &alloc);

    try expect(r2 == null);

    // return none in case of some corruption where I can read the record
    // size but there's not enough data. For example if record size says that
    // the record has 30 bytes but the buffer actually has 10
    const r3 = lsmtree.serialize.record.fromBytes(record_bytes[0..10], &alloc);
    try expect(r3 == null);
}
