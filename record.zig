const std = @import("std");
const expect = std.testing.expect;
const expectEq = std.testing.expectEqual;

pub const RecordError = error{ BufferTooSmall, KeyTooBig };

/// A record is an array of contiguous bytes in the following form:
/// N bytes to store the total bytes that the record uses, where N = RecordLengthType (u64 by default)
/// K bytes to store the key length where K = KeyLengthType (u32 by default which uses 4 bytes)
/// K' bytes to store the key, where K' = key.len
/// V bytes to store the value, where V = value.len
///
/// For example the key:'hello' and value:'world' uses 22 bytes with u32 max size keys and u64 max size record
/// 8 (u64) + 4 (u32) + 5 (bytes of 'hello') + 5 (bytes of 'world')
pub fn Record(comptime KeyLengthType: type, comptime RecordLengthType: type) type {
    return struct {
        key: []u8,
        value: []u8,

        record_size_in_bytes: usize = 0,
        allocator: *std.mem.Allocator,

        const Self = @This();

        pub fn minimum_size() usize {
            return @sizeOf(KeyLengthType) + @sizeOf(RecordLengthType) + 2;
        }

        // Call deinit() to deallocate this struct and its values
        pub fn init(key: []const u8, value: []const u8, alloc: *std.mem.Allocator) !*Self {
            var s = try alloc.create(Self);

            s.key = try alloc.alloc(u8, key.len);
            std.mem.copy(u8, s.key, key);

            s.value = try alloc.alloc(u8, value.len);
            std.mem.copy(u8, s.value, value);

            s.allocator = alloc;
            s.record_size_in_bytes = 0;

            _ = s.size();

            return s;
        }

        pub fn init_string(key: []const u8, value: []const u8, alloc: *std.mem.Allocator) !*Self {
            return Record(KeyLengthType, RecordLengthType).init(key[0..], value[0..], alloc);
        }

        fn key_size(self: *const Self) usize {
            // K bytes to store a number that indicates how many bytes the key has
            const key_length = @sizeOf(KeyLengthType);
            return key_length + self.key.len;
        }

        pub fn size(self: *Self) usize {
            if (self.record_size_in_bytes != 0) {
                return self.record_size_in_bytes;
            }

            // N bytes to store a number that indicates how many bytes the record has
            const record_length = @sizeOf(RecordLengthType);

            // Total
            self.record_size_in_bytes = record_length + self.key_size() + self.value.len;
            return self.record_size_in_bytes;
        }

        /// TODO Update comment. Writes into the provided buf the data of the record in a contiguous array as described
        /// in fn Record()
        pub fn bytes(self: *const Self, buf: []u8) RecordError!usize {
            var offset: usize = 0;

            //Abort early if necessary
            if (buf.len < self.record_size_in_bytes) {
                return RecordError.BufferTooSmall;
            }

            if (self.key.len > std.math.maxInt(KeyLengthType)) {
                return RecordError.KeyTooBig;
            }

            // Write N bytes to indicate the total size of the record. N is defined as the number of bytes
            // that a type RecordLengthType can store (8 for u64, 4 for a u32, etc.)
            std.mem.writeIntSliceLittle(RecordLengthType, buf[offset .. offset + @sizeOf(RecordLengthType)], self.record_size_in_bytes);
            offset += @sizeOf(RecordLengthType);

            // We can truncate here because we have already checked that the size will fit above
            const temp = @truncate(KeyLengthType, self.key.len);
            std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + @sizeOf(KeyLengthType)], temp);
            offset += @sizeOf(KeyLengthType);

            // TODO Write a function that "Reads as stream" (alas Read interface) instead of copying values
            std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
            offset += self.key.len;

            std.mem.copy(u8, buf[offset .. offset + self.value.len], self.value);

            return self.record_size_in_bytes;
        }

        /// returns a slice with the pair key_length (KeyLengthType bytes) + key in bytes.
        /// Caller is responsible of freeing the slice
        pub fn read_key_and_length_alloc(self: *Self, allocator: *std.mem.Allocator) ![]u8 {
            var buf = try allocator.alloc(u8, @sizeOf(KeyLengthType) + self.key_size());
            self.read_key_and_length(&buf);
            return buf;
        }

        /// fills the provided buffer with the pair key_length (KeyLengthType bytes) + key in bytes
        pub fn read_key_and_length(self: *Self, buf: []u8) []u8 {
            // read the key length
            const record_length_size = @sizeOf(RecordLengthType);
            const bytes_for_key_length = buf[record_length_size .. record_length_size + @sizeOf(KeyLengthType)];
            const key_length = std.mem.readIntSliceLittle(KeyLengthType, bytes_for_key_length);

            // read as many bytes as defined before to get the key
            const key_start = record_length_size + bytes_for_key_length.len;
            const key = self.key[key_start .. key_start + key_length];

            std.mem.copy(u8, buf, key);

            return key;
        }

        /// Returned record and its contents must be deallocated by caller using deinit()
        pub fn read_record(buf: []u8, allocator: *std.mem.Allocator) ?*Self {
            // Check if there's enough data to read the record size
            if (buf.len < @sizeOf(RecordLengthType)) {
                return null;
            }

            //Read the record length bytes (4 or 8 usually) to get the total length of the record
            const bytes_for_record_length = buf[0..@sizeOf(RecordLengthType)];
            const record_length = std.mem.readIntSliceLittle(RecordLengthType, bytes_for_record_length);

            // check if the buffer actually has the amount of bytes that the record_length says
            if (buf.len < record_length) {
                return null;
            }

            // read the key length
            const bytes_for_key_length = buf[bytes_for_record_length.len .. bytes_for_record_length.len + @sizeOf(KeyLengthType)];
            const key_length = std.mem.readIntSliceLittle(KeyLengthType, bytes_for_key_length);

            // read as many bytes as defined before to get the key
            const key_start = bytes_for_record_length.len + bytes_for_key_length.len;
            const key = buf[key_start .. key_start + key_length];

            // read as many bytes as left to get the value
            const value_start = key_start + key_length;
            const value_length = record_length - value_start;
            const value = buf[value_start .. value_start + value_length];

            var r = Self.init(key, value, allocator) catch return null;
            return r;
        }

        pub fn getPointerBytesForOffset(self: *Self, buf: []u8) usize {
            var offset: usize = 0;

            // key length
            std.mem.writeIntSliceLittle(KeyLengthType, buf[offset .. offset + self.key_size()], @intCast(KeyLengthType, self.key.len));
            offset += self.key_size();

            // key
            std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
            offset += self.key.len;

            //offset
            std.mem.writeIntSliceLittle(usize, buf[offset .. offset + self.key_size()], offset);
            offset += @sizeOf(usize);

            return offset;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.key);
            self.allocator.free(self.value);
            self.allocator.destroy(self);
        }
    };
}

test "record.size" {
    var r = try Record(u32, u64).init("hello", "world", std.testing.allocator);
    defer r.deinit();

    const size = r.size();
    try expectEq(@as(u64, 22), size);
}

test "record.minimum size" {
    try expect(Record(u32, u64).minimum_size() == 14);
}

test "record.bytes returns a contiguous array with the record" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var allocator = &arena.allocator;
    var r = try Record(u32, u64).init("hello", "world", allocator);

    var buf = try allocator.alloc(u8, r.size());

    const total_bytes = try r.bytes(buf);
    try expectEq(@as(usize, 22), total_bytes);
    try std.testing.expectStringEndsWith(buf, "helloworld");
    try expect(!std.mem.eql(u8, buf, "helloworld"));
    try expectEq(@as(usize, 22), r.size());
}

test "record.having an slice, read a record starting at an offset" {
    // var offset = 0;
    var record_bytes = [_]u8{
        22, 0, 0, 0, 0, 0, 0, 0, //22 bytes
        5, 0, 0, 0, //5 bytes of key
        104, 101, 108, 108, 111, //hello (the key)
        119, 111, 114, 108, 100, //world (the value)
    };

    const RecordType = Record(u32, u64);
    const r = RecordType.read_record(record_bytes[0..], std.testing.allocator).?;
    defer r.deinit();

    try expect(std.mem.eql(u8, r.key, "hello"));
    try expect(std.mem.eql(u8, r.value, "world"));

    const key = RecordType.read_key_and_length(record_bytes[0..]);
    try expect(std.mem.eql(u8, key, "hello"));

    // return none if there's not enough data for a record in the buffer
    // starting from 20, there's not enough data to read a potential record size
    const r2 = RecordType.read_record(record_bytes[20..], std.testing.allocator);

    try expect(r2 == null);

    // return none in case of some corruption where I can read the record
    // size but there's not enough data. For example if record size says that
    // the record has 30 bytes but the buffer actually has 10
    const r3 = RecordType.read_record(record_bytes[0..10], std.testing.allocator);
    try expect(r3 == null);
}
