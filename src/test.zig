comptime {
    _ = @import("disk_manager.zig");
    _ = @import("header.zig");
    _ = @import("pointer.zig");
    _ = @import("record.zig");
    _ = @import("sst.zig");
    _ = @import("wal.zig");
    _ = @import("serialize/test.zig");
}
