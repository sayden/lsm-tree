const std = @import("std");

pub const Pointer = @import("./pointer.zig").Pointer;
pub const Record = @import("./record.zig").Record;
pub const RecordError = @import("./record.zig").RecordError;
pub const Op = @import("./ops.zig").Op;
pub const KeyLengthType = @import("./record.zig").KeyLengthType;
pub const Header = @import("./header.zig").Header;
pub const RecordLengthType = @import("./record.zig").RecordLengthType;

pub const serialize = @import("./serialize/main.zig");

pub fn main() void {}
