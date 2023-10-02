const std = @import("std");

pub const Op = enum(u8) {
    Create,
    Delete,
    Update,
    Skip,
};
