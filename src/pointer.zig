const std = @import("std");
const KeyLengthType = @import("./record.zig").KeyLengthType;
const Op = @import("ops.zig").Op;

pub const Pointer = struct {
    key: []const u8,
    byte_offset: usize = 0,
    op: Op,

    pub const key_size: usize = @sizeOf(KeyLengthType);
    const Self = @This();

    pub fn bytesLen(self: *const Self) usize {
        return 1 + Self.key_size + self.key.len + @sizeOf(@TypeOf(self.byte_offset));
    }
};