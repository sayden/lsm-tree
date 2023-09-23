const std = @import("std");
const Record = @import("./record.zig").Record;
const KeyLengthType = @import("./record.zig").KeyLengthType;
const Op = @import("ops.zig").Op;
const Error = error{ArrayTooSmall};
const RecordLengthType = @import("./record.zig").RecordLengthType;

const Debug = @import("./debug.zig");
const println = Debug.println;
const prints = Debug.prints;
const print = std.debug.print;

// A pointer contains an Operation, a key and a offset to find the Value of the record.
// The pointer is stored as follows:
// 1 byte: Operation
// 2 bytes: Key size
// X bytes: Key
// 8 bytes: Offset in the data
pub const Pointer = struct {
    op: Op,
    key: []u8,
    offset: usize = 0,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.key);
        self.allocator.destroy(self);
    }

    pub fn keySize(self: *Record) usize {
        return 1 + @sizeOf(KeyLengthType) + self.key + self.key.len;
    }

    pub fn write(p: *Pointer, writer: anytype) !usize {
        const op = @intFromEnum(p.op);
        _ = try writer.writeByte(op);

        // This truncate is not expected to fail
        const key_length = @as(KeyLengthType, @truncate(p.key.len));
        try writer.writeIntLittle(KeyLengthType, key_length);

        _ = try writer.write(p.key[0..key_length]);

        _ = try writer.writeIntLittle(usize, p.offset);

        return writer.context.getPos();
    }

    pub fn read(reader: anytype, allocator: std.mem.Allocator) !*Pointer {
        var p = try allocator.create(Pointer);

        p.op = @as(Op, @enumFromInt(try reader.readByte()));

        // read the key length
        const key_length = try reader.readIntLittle(KeyLengthType);

        // read the key
        var buf = try allocator.alloc(u8, key_length);
        _ = try reader.readAtLeast(buf, key_length);
        p.key = buf;

        //read the offset
        p.offset = try reader.readIntLittle(usize);

        p.allocator = allocator;

        return p;
    }

    pub fn readRecord(p: *Pointer, reader: anytype, alloc: std.mem.Allocator) !*Record {
        var r = try alloc.create(Record);

        p.op = @as(Op, @enumFromInt(try reader.readByte()));

        //Read the record length bytes (4 or 8 usually) to get the total length of the record
        const record_length = try reader.readIntLittle(RecordLengthType);

        // read as many bytes as are left to get the value
        const value_length = record_length - @sizeOf(RecordLengthType) - 1;

        r.value = try alloc.alloc(u8, value_length);
        _ = try reader.readAtLeast(r.value, value_length);

        r.allocator = alloc;
        r.pointer = p;

        _ = r.bytesLen();

        return r;
    }

    pub fn clone(self: Self, alloc: std.mem.Allocator) !*Pointer {
        var p: *Pointer = try alloc.create(Pointer);
        p.op = self.op;
        p.key = alloc.dupe(u8, self.key) catch |err| {
            alloc.destroy(p);
            return err;
        };
        p.offset = self.offset;
        p.allocator = alloc;

        return p;
    }

    pub fn bytesLen(keylen: usize) usize {
        // last usize refers to size of offset field, which is indeed of type 'usize'
        return 1 + @sizeOf(KeyLengthType) + keylen + @sizeOf(usize);
    }

    pub fn debug(self: *Self) void {
        std.debug.print("\nKey:\t{s}\nOffset:\t{}\nOp:\t{}\n", .{ self.key, self.offset, self.op });
    }
};

const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;

test "pointer_bytesLen" {
    var allocator = std.testing.allocator;
    var hello = try allocator.alloc(u8, 5);
    defer allocator.free(hello);
    @memcpy(hello, "hello");

    var len = Pointer.bytesLen(hello.len);
    try expectEqual(@as(usize, 16), len);
}

test "pointer_write" {
    var allocator = std.testing.allocator;
    var hello = try allocator.alloc(u8, 5);
    @memcpy(hello, "hello");

    var p = Pointer{
        .op = Op.Create,
        .key = hello,
        .offset = 100,
        .allocator = allocator,
    };
    defer allocator.free(p.key);

    var buf = try allocator.alloc(u8, 50);
    defer allocator.free(buf);
    var fixedWriter = std.io.fixedBufferStream(buf);
    var writer = fixedWriter.writer();

    _ = try p.write(writer);

    var fixedReader = std.io.fixedBufferStream(buf);
    var reader = fixedReader.reader();
    var p1 = try Pointer.read(reader, allocator);
    defer p1.deinit();

    try expectEqual(@as(usize, 5), p1.key.len);
    try std.testing.expectEqualSlices(u8, p.key, p1.key);
    try expectEqual(p.offset, p1.offset);
    try expectEqualStrings(p.key, p1.key);
}
