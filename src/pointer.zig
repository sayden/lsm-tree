const std = @import("std");
const KeyLengthType = @import("./record.zig").KeyLengthType;
const Op = @import("ops.zig").Op;

// A pointer contains an Operation, a key and a offset to find the Value of the record.
// The pointer is stored as follows:
// 1 byte: Operation
// 2 bytes: Key size
// X bytes: Key
// 8 bytes: Offset in the data
pub const Pointer = struct {
    op: Op,
    key: []const u8,
    byte_offset: usize = 0,

    const Self = @This();

    pub fn bytesLen(self: *const Self) usize {
        return 1 + @sizeOf(KeyLengthType) + self.key.len + @sizeOf(@TypeOf(self.byte_offset));
    }
};

test "pointer.bytesLen" {
    var p = Pointer{
        .op = Op.Create,
        .key = "hello",
        .byte_offset = 100,
    };

    var len = p.bytesLen();
    try std.testing.expectEqual(@as(usize, 16), len);
}
