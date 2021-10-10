const std = @import("std");

/// TODO Update this comment
/// A record is an array of contiguous bytes in the following form:
/// N bytes to store the total bytes that the record uses, where N = record_size_byte_length
/// K bytes to store the key length where K = key_byte_size
/// K' bytes to store the key, where K' = key.len
/// V bytes to store the value, where V = value.len
fn Record() type {
    return struct {
        key: []u8,
        value: []u8,

        const Self = @This();

        pub fn bytes(self: *const Self) !usize {
            const key_size = std.mem.toBytes(self.key.len); // The size of the key serialized to 8 bytes
            const value_size = std.mem.toBytes(self.value.len); //The size of the value serialized to 8 bytes
            const kv_size = key_size.len + self.key.len + value_size.len + self.value.len;
            try std.testing.expect(kv_size == 8 + 8 + 5 + 5);
            const kv_size_bytes = std.mem.toBytes(kv_size);
            const buf_size = kv_size_bytes.len + kv_size;

            return buf_size;

            // var total_size = record_size + key_byte_size + self.key.len + self.value.len;
            // var buf: [total_size]u8 = undefined;

            // var offset: usize = 0;

            // //TODO check record size
            // std.mem.copy(u8, buf[offset..record_size_byte_length], record_size[0..]);
            // offset = record_size_byte_length;

            // std.mem.copy(u8, buf[offset .. offset + key_byte_size], key_size[0..]);
            // offset += key_size.len;

            // std.mem.copy(u8, buf[offset .. offset + self.key.len], self.key);
            // offset += self.key.len;

            // std.mem.copy(u8, buf[offset .. offset + self.value.len], self.value);
            // offset += self.value.len;

            // const record_size: []u8 = std.mem.toBytes(total_size);

            // if (offset != total_size) {
            //     std.debug.print("offset: {d}, total size: {d}\n", .{ offset, total_size });
            //     unreachable;
            // }

            // return buf[0..];
        }
    };
}

test "bytes" {
    var key = std.base64;
    std.debug.print("{d}\n", .{key});
    var value = std.mem.toBytes("world");

    const r = Record(){
        .key = key[0..],
        .value = value[0..],
    };

    const res = try r.bytes();
    std.debug.print("{d}\n", .{res});
    try std.testing.expect(res == 8 + 8 + 5 + 5);
}
