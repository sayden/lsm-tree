const std = @import("std");

const Pointer = struct {
    key_size: usize,
    key: []u8,
    byte_offset: usize,
};

pub fn readPointer(bytes: []u8)*Pointer{
    
}

pub fn toPointer(r: *Record)Pointer {

}

pub fn toPointerAlloc(r: *Record, allocator: *std.mem.Allocator)*Pointer{

}