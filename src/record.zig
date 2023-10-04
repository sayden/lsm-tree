const std = @import("std");
const Op = @import("./ops.zig").Op;
const lsmtree = @import("./record.zig");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

pub const Error = error{ BufferTooSmall, KeyTooBig, UnexpectedRecordLength, NullOffset, ArrayTooSmall };
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
    ts: i128,

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
            .ts = std.time.nanoTimestamp(),
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

    pub fn writePointer(self: *Self, ws: *ReaderWriterSeeker) !usize {
        return self.pointer.write(ws);
    }

    /// writes the value of the record in the format `op + val length + value + ts`
    pub fn write(self: *Self, ws: *ReaderWriterSeeker) !usize {
        var bytes_written: usize = 0;
        var op = [_]u8{@intFromEnum(self.pointer.op)};

        bytes_written += try ws.write(&op);

        // Indicate the size of the value of the record (op+val length+value+ts)
        try ws.writeIntLittle(RecordLengthType, self.valueLen());
        bytes_written += @sizeOf(RecordLengthType);

        try ws.writeAll(self.value);
        bytes_written += self.value.len;

        try ws.writeIntLittle(i128, self.ts);
        bytes_written += @sizeOf(i128);

        return bytes_written;
    }

    pub fn read(r: *Self, rs: *ReaderWriterSeeker, alloc: std.mem.Allocator) !usize {
        try rs.seekTo(try r.pointer.getOffset());

        r.pointer.op = @as(Op, @enumFromInt(try rs.readByte()));
        var bytes_written: usize = 1;

        //Read the record length to get the total length of the record
        const record_length = try rs.readIntLittle(RecordLengthType);
        bytes_written += @sizeOf(RecordLengthType);

        if (record_length <= 0) {
            return Error.UnexpectedRecordLength;
        }

        // We store the total size of the Record Value (op+val length+value+ts), but not of the data itself.
        const value_length = record_length - @sizeOf(RecordLengthType) - 1 - @sizeOf(i128);

        r.value = try alloc.alloc(u8, value_length);
        errdefer alloc.free(r.value);

        _ = try rs.readAtLeast(r.value, value_length);
        bytes_written += value_length;

        r.ts = try rs.readIntLittle(i128);

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
            .ts = r.ts,
        };

        return new_record;
    }

    /// Length in bytes of the record, pointer NOT included
    pub fn valueLen(self: *Self) usize {
        const record_len_type_len = @sizeOf(RecordLengthType);

        // Total
        var record_size_in_bytes = 1 + record_len_type_len + self.value.len + @sizeOf(i128);
        return record_size_in_bytes;
    }

    /// Length in bytes of the value, pointer included
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
        std.debug.print("\n-------\nRecord:\n-------\nOp:\t{}\nKey:\t{s}\nVal:\t{s}\nSize:\t{}\nVsize:\t{}\nOffset:\t{?}\nTS:\t{}\n", .{
            self.pointer.op, self.pointer.key, self.value, self.len(), self.valueLen(), self.pointer.offset, self.ts,
        });
    }
};

// A pointer contains an Operation, a key and a offset to find the Value of the record.
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

    pub fn write(p: *Pointer, ws: *ReaderWriterSeeker) !usize {
        const op = @intFromEnum(p.op);
        _ = try ws.writeByte(op);
        var bytes_written: usize = 1;

        // This truncate is not expected to fail
        const key_length = @as(KeyLengthType, @truncate(p.key.len));
        try ws.writeIntLittle(KeyLengthType, key_length);
        bytes_written += @sizeOf(KeyLengthType);

        bytes_written += try ws.write(p.key[0..key_length]);

        try ws.writeIntLittle(usize, try p.getOffset());
        bytes_written += @sizeOf(usize);

        return bytes_written;
    }

    pub fn read(rs: *ReaderWriterSeeker, alloc: std.mem.Allocator) !*Pointer {
        var p = try alloc.create(Pointer);
        errdefer alloc.destroy(p);

        var op = @as(Op, @enumFromInt(try rs.readByte()));

        // read the key length
        const key_length = try rs.readIntLittle(KeyLengthType);

        // read the key
        var keybuf = try alloc.alloc(u8, key_length);
        errdefer alloc.free(keybuf);

        _ = try rs.readAtLeast(keybuf, key_length);

        //read the offset
        var offset = try rs.readIntLittle(usize);

        p.* = .{
            .op = op,
            .key = keybuf,
            .offset = offset,
            .alloc = alloc,
        };

        return p;
    }

    pub fn readValue(p: *Pointer, rs: *ReaderWriterSeeker, alloc: std.mem.Allocator) !*Record {
        var pointer = try p.clone(alloc);
        errdefer pointer.deinit();

        var r: *Record = try alloc.create(Record);
        errdefer alloc.destroy(r);
        r.pointer = pointer;

        _ = try r.read(rs, alloc);

        return r;
    }

    pub fn readValueReusePointer(p: *Pointer, ws: *ReaderWriterSeeker, alloc: std.mem.Allocator) !*Record {
        var r: *Record = try alloc.create(Record);
        errdefer r.deinit();

        r.pointer = p;

        _ = try r.read(ws, alloc);
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

const expectEqualStrings = std.testing.expectEqualStrings;
const expectEqualSlices = std.testing.expectEqualSlices;
const expect = std.testing.expect;
const expectEq = std.testing.expectEqual;

test "record_init" {
    var alloc = std.testing.allocator;

    var r = try Record.init("hell0", "world1", Op.Upsert, alloc);
    defer r.deinit();

    try expectEq(@as(usize, 31), r.valueLen());
    try expectEq(@as(usize, 47), r.len());

    try expectEqualStrings("hell0", r.pointer.key);
    try expectEqualStrings("world1", r.value);
    try expectEq(Op.Upsert, r.pointer.op);
}

test "record_len" {
    var alloc = std.testing.allocator;

    const r = try Record.init("hello", "world", Op.Delete, alloc);
    defer r.deinit();

    try expectEq(@as(usize, 30), r.valueLen());
    try expectEq(@as(usize, 46), r.len());
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

    var buf: [256]u8 = undefined;
    var wss = ReaderWriterSeeker.initBuf(&buf);
    var ws = &wss;

    // write the first pointer and second pointer
    const pointer_bytes_written = try record1.writePointer(ws);
    try expectEq(@as(usize, 16), pointer_bytes_written);
    _ = try record2.writePointer(ws);

    // write the first record and second record
    const value_bytes_written = try record1.write(ws);
    try expectEq(@as(usize, 30), value_bytes_written);
    _ = try record2.write(ws);

    try ws.seekTo(0);

    // read first and second pointers
    var pointer1 = try Pointer.read(ws, alloc);
    defer pointer1.deinit();

    var pointer2 = try Pointer.read(ws, alloc);
    defer pointer2.deinit();

    try expectEq(Op.Delete, pointer1.op);
    try std.testing.expectEqualDeep(record1.pointer, pointer1);
    try std.testing.expectEqualDeep(record2.pointer, pointer2);

    // read second then first, to ensure offseting is working as expected
    var record2_ = try pointer2.readValue(ws, alloc);
    defer record2_.deinit();

    var record1_ = try pointer1.readValue(ws, alloc);
    defer record1_.deinit();

    try std.testing.expectEqualDeep(record2, record2_);
    try std.testing.expectEqualDeep(record1, record1_);
}
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

    const p = try Pointer.init("hello", Op.Upsert, alloc);
    defer p.deinit();

    var buf: [256]u8 = undefined;
    var wss = ReaderWriterSeeker.initBuf(&buf);
    var ws = &wss;

    p.offset = 999;

    const bytes_written = try p.write(ws);

    try expectEq(@as(usize, 16), bytes_written);
    try ws.seekTo(0);

    var p1 = try Pointer.read(ws, alloc);
    defer p1.deinit();

    try expectEq(@as(usize, 5), p1.key.len);
    try std.testing.expectEqualSlices(u8, p.key, p1.key);
    try expectEq(p.offset, p1.offset);
    try expectEqualStrings(p.key, p1.key);
}

test "pointer_readRecord" {
    var alloc = std.testing.allocator;

    const pointer = try Pointer.init("hello", Op.Upsert, alloc);
    defer pointer.deinit();
    pointer.offset = pointer.len();

    const r = try Record.init("hello", "world", Op.Upsert, alloc);
    defer r.deinit();
    r.pointer.deinit();
    r.pointer = try pointer.clone(alloc);

    var buf: [256]u8 = undefined;
    var wss = ReaderWriterSeeker.initBuf(&buf);
    var ws = &wss;

    var bytes_written = try pointer.write(ws);
    bytes_written += try r.write(ws);
    try expectEq(r.len(), bytes_written);

    try ws.seekTo(0);

    var p2 = try Pointer.read(ws, alloc);
    defer p2.deinit();
    const r1 = try p2.readValue(ws, alloc);
    defer r1.deinit();

    try expectEqualSlices(u8, r.getKey(), r1.getKey());
    try expectEqualSlices(u8, r.getVal(), r1.getVal());
    try expectEq(r.getOffset(), r1.getOffset());
}

test "pointer_len" {
    var alloc = std.testing.allocator;

    const p = try Pointer.init("hello", Op.Upsert, alloc);
    defer p.deinit();

    try expectEq(@as(usize, 16), p.len());
}
