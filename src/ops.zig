const std = @import("std");

pub const Op = enum(u8) {
    Upsert,

    Delete,
    Skip,
};
