const std = @import("std");

pub const Op = enum(u8) {
    Create,
    Delete,
    Update,
};

const expectEqual = std.testing.expectEqual;

test "op" {
    try expectEqual(@as(u8, 0), @intFromEnum(Op.Create));
    try expectEqual(@as(u8, 1), @intFromEnum(Op.Delete));
    try expectEqual(@as(u8, 2), @intFromEnum(Op.Update));
}
