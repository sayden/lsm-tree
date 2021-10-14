const std = @import("std");

const Header = struct {
    pointer_byte_offset: usize,
    first_key: []u8,
    last_key: []u8,
};
