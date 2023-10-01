const std = @import("std");
pub const log_level: std.log.log_level = .debug;

comptime {
    _ = @import("disk_manager.zig");
    _ = @import("header.zig");
    _ = @import("pointer.zig");
    _ = @import("record.zig");
    _ = @import("sst.zig");
    _ = @import("sst_manager.zig");
    _ = @import("wal_handler.zig");
    _ = @import("wal.zig");
}
