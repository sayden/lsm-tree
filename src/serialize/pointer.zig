const std = @import("std");

const Record = @import("lsmtree").Record;
const KeyLengthType = @import("lsmtree").KeyLengthType;
const Pointer = @import("lsmtree").Pointer;
const Op = @import("lsmtree").Op;

const Error = error{ArrayTooSmall};

// Writes into the provided array a Pointer byte array using the provided Record
pub fn fromRecord(r: Record, buf: []u8, file_offset: usize) usize {
    // op
    buf[0] = @enumToInt(r.op);
    var offset: usize = 1;

    // key length
    std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + @sizeOf(KeyLengthType)], @intCast(KeyLengthType, r.key.len));
    offset += @sizeOf(KeyLengthType);

    // key
    std.mem.copy(u8, buf[offset .. offset + r.key.len], r.key);
    offset += r.key.len;

    //offset
    std.mem.writeIntSliceLittle(usize, buf[offset .. offset + @sizeOf(@TypeOf(file_offset))], file_offset);
    offset += @sizeOf(@TypeOf(file_offset));

    return offset;
}

// Get a byte array representation of a pointer using the provided allocator.
// FREE the returned array using the provided allocator. The array look like this:
// 1 byte: Operation
// 2 bytes: Key size
// X bytes: Key
// 8 bytes: Offset in the data
pub fn toBytesAlloc(self: Pointer, allocator: *std.mem.Allocator) ![]u8 {
    var buf = try allocator.alloc(u8, self.bytesLen());
    _ = toBytes(self, buf) catch |err|
        return err;

    return buf;
}

// Get a byte array representation of a pointer using the provided array.
// It must be at least of the size of Pointer.bytesLen(). The array look like this:
// 1 byte: Operation
// 2 bytes: Key size
// X bytes: Key
// 8 bytes: Offset in the data
pub fn toBytes(self: Pointer, buf: []u8) Error!usize {
    if (self.bytesLen() < buf.len) {
        return Error.ArrayTooSmall;
    }

    var offset: usize = 0;

    // Op
    buf[0] = @enumToInt(self.op);
    // std.mem.writeIntSliceLittle(u8, buf[0], @enumToInt(self.op));
    offset += 1;

    // key length
    std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + @sizeOf(KeyLengthType)], @truncate(KeyLengthType, self.key.len));
    offset += @sizeOf(KeyLengthType);

    // key
    std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
    offset += self.key.len;

    //offset
    std.mem.writeIntSliceLittle(usize, buf[offset .. offset + @sizeOf(@TypeOf(self.byte_offset))], self.byte_offset);
    offset += @sizeOf(@TypeOf(self.byte_offset));

    return offset;
}

// Reads the provided array and return a Pointer from the contents. If the contents of the array
// are not correct, it will return a corrupted Pointer.
// The size of this array is expected to be X + 11 being X the key length
pub fn fromBytes(bytes: []u8) !Pointer {
    // A minimum size no the array of 12 is expected or the array doesn't have
    // the minimum information
    if (bytes.len < 12) {
        return Error.ArrayTooSmall;
    }

    //Op
    var op = @intToEnum(Op, bytes[0]);
    var offset: usize = 1;

    //Key length
    var key_length = std.mem.readIntSliceLittle(KeyLengthType, bytes[offset .. offset + @sizeOf(KeyLengthType)]);
    offset += @sizeOf(KeyLengthType);

    // Key
    var key = bytes[offset .. offset + key_length];
    offset += key_length;

    // Offset
    var byte_offset = std.mem.readIntSliceLittle(usize, bytes[offset .. offset + @sizeOf(usize)]);

    return Pointer{
        .key = key,
        .byte_offset = byte_offset,
        .op = op,
    };
}

test "pointer.fromBytes" {
    var buf = [_]u8{
        0, //Op
        5, 0, //key length
        104, 101, 108, 108, 111, //hello (the key)
        100, 0, 0, 0, 0, 0, 0, 0, //offset
    };

    const p = try fromBytes(&buf);
    const eq = std.testing.expectEqual;
    try eq(@as(usize, 5), p.key.len);
    try eq(@as(usize, 100), p.byte_offset);

    try std.testing.expectEqualSlices(u8, "hello", p.key);

    try eq(buf.len, p.bytesLen());
}

test "pointer.toBytes" {
    var buf = [_]u8{
        0, //Op
        5, 0, //key length
        104, 101, 108, 108, 111, //hello (the key)
        100, 0, 0, 0, 0, 0, 0, 0, //offset
    };

    var p = Pointer{
        .op = Op.Create,
        .key = "hello",
        .byte_offset = 100,
    };

    var alloc = std.testing.allocator;
    var res = try toBytesAlloc(p, &alloc);
    defer alloc.free(res);

    for (buf) |b, i| {
        // std.debug.print("{d} vs {d}\n", .{ res[i], b });
        try std.testing.expectEqual(res[i], b);
    }
}

test "pointer.fromRecord" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Delete, &alloc);
    defer r.deinit();

    var buf: [20]u8 = undefined;
    const size = fromRecord(r.*, &buf, 99);
    try std.testing.expectEqual(@as(usize, 16), size);
    try std.testing.expectEqual(@as(u8, 5), buf[1]);
    try std.testing.expectEqual(@as(u8, 99), buf[8]);
    try std.testing.expectEqualStrings("hello", buf[3..8]);
}

test "pointer.try contains" {
    const String = @import("string").String;
    var alloc = std.testing.allocator;
    var s = String.init(&alloc);
    defer s.deinit();

    try s.concat("hello");
    const res = s.find("ello");
    try std.testing.expect(res.? == 1);
}
