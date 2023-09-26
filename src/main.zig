const std = @import("std");
pub const log_level: std.log.Level = .debug;

pub const Pointer = @import("./pointer.zig").Pointer;
pub const Record = @import("./record.zig").Record;
pub const RecordError = @import("./record.zig").RecordError;
pub const Op = @import("./ops.zig").Op;
pub const KeyLengthType = @import("./record.zig").KeyLengthType;
pub const Header = @import("./header.zig").Header;
pub const headerSize = @import("./header.zig").headerSize;
pub const RecordLengthType = @import("./record.zig").RecordLengthType;

pub fn main() void {}
