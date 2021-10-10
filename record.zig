/// A record is an array of contiguous bytes in the following form:
/// N bytes to store the total bytes that the record uses, where N = record_size_byte_length
/// K bytes to store the key, where K = key_byte_size
/// V bytes to store the value, where V = value.len
/// So a record is always `record_size_byte_length+key_byte_size+value.len`
fn Record(comptime record_size_byte_length: usize, comptime key_byte_size: usize) type {
    return struct {
        key: [key_byte_size]u8,
        value: []u8,

        const Self: @This();

        pub fn bytes(self: Self) []u8 {
            // Total size of the record including key, length bytes and the value
            const record_size_bytes = record_size_byte_length + key_byte_size + value.len;
            const record_size_part = std.mem.ToBytes(record_size_bytes);

            return std.mem.ToBytes(self.key) + std.mem.ToBytes(self.value);
        }
    };
}
