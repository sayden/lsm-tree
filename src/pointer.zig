const std = @import("std");
const KeyLengthType = @import("./record.zig").KeyLengthType;
const Op = @import("ops.zig").Op;

pub const Pointer = struct {
    const key_size: usize = @sizeOf(KeyLengthType);

    key: []const u8,
    byte_offset: usize = 0,
    op: Op,

    const Self = @This();

    pub fn bytesLen(self: *const Self) usize {
        return 1 + Self.key_size + self.key.len + @sizeOf(@TypeOf(self.byte_offset));
    }
};