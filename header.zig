const std = @import("std");

pub const Header = struct {
    magic_number: u32 = 0,
    pointer_byte_offset: usize,
    first_key_offset: usize,
    last_key_offset: usize,

    pub fn header_size(self: *const Header) usize {
        return @sizeOf(@TypeOf(self.magic_number))
        + (@sizeOf(usize)*3);
    }
};

pub fn toBytes(h: *const Header, buf: []u8) void {
    const magic_number_size = @sizeOf(@TypeOf(h.magic_number));
    
    std.mem.writeIntLittle(@TypeOf(h.magic_number), buf[0..magic_number_size], h.magic_number);
    std.mem.writeIntLittle(usize, buf[magic_number_size..magic_number_size+8], h.first_key_offset);
    std.mem.writeIntLittle(usize, buf[magic_number_size+8..magic_number_size+16], h.last_key_offset);
    std.mem.writeIntLittle(usize, buf[magic_number_size+16..magic_number_size+24], h.pointer_byte_offset);
}
