const std = @import("std");
const expect = std.testing.expect;
const expectEq = std.testing.expectEqual;
const Op = @import("./ops.zig").Op;

pub const RecordLengthType: type = usize;
pub const KeyLengthType: type = u16;

pub const RecordError = error{ BufferTooSmall, KeyTooBig };

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

        _ = s.len();

        return s;
    }

    pub fn init_string(key: []const u8, value: []const u8, alloc: *std.mem.Allocator) !*Self {
        return Record.init(key[0..], value[0..], alloc);
    }

    /// length of the op + key + the key length type
    fn totalKeyLen(self: *const Self) usize {
        // K bytes to store a number that indicates how many bytes the key has
        const key_length = @sizeOf(KeyLengthType);
        return 1 + key_length + self.key.len;
    }

    /// total size in bytes of the record
    pub fn len(self: *Self) usize {
        if (self.record_size_in_bytes != 0) {
            return self.record_size_in_bytes;
        }

        // total bytes (usually 8 bytes) to store the total bytes in the entire record
        const record_len_type_len = @sizeOf(RecordLengthType);

        // Total
        self.record_size_in_bytes = record_len_type_len + self.totalKeyLen() + self.value.len;
        return self.record_size_in_bytes;
    }

    pub fn minimum_size() usize {
        return @sizeOf(KeyLengthType) + @sizeOf(RecordLengthType) + 2;
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.key);
        self.allocator.free(self.value);
        self.allocator.destroy(self);
    }
};

test "record.init" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hell0", "world1", Op.Update, &alloc);
    defer r.deinit();

    try expectEq(@as(usize, 22), r.record_size_in_bytes);
    try expectEq(@as(usize, 22), r.len());
    try expectEq(@as(usize, 8), r.totalKeyLen());

    try std.testing.expectEqualStrings("hell0", r.key);
    try std.testing.expectEqualStrings("world1", r.value);
    try expectEq(Op.Update, r.op);
}

test "record.size" {
    var alloc = std.testing.allocator;
    var r = try Record.init("hello", "world", Op.Create, &alloc);
    defer r.deinit();

    const size = r.len();
    try expectEq(@as(u64, 21), size);
}

test "record.minimum size" {
    try expectEq(@as(usize, 12), Record.minimum_size());
}
