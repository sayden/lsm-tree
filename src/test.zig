const std = @import("std");

pub const log_level: std.log.log_level = .debug;
comptime {
    _ = @import("disk_manager.zig");
    _ = @import("header.zig");
    _ = @import("record.zig");
    _ = @import("sst.zig");
    _ = @import("sst_manager.zig");
    _ = @import("wal_handler.zig");
    _ = @import("wal.zig");
}

const DiskManager = @import("./disk_manager.zig").DiskManager;
const WalNs = @import("./wal.zig");
const WalHandler = @import("./wal_handler.zig").WalHandler;
const SstNs = @import("./sst_manager.zig");
const SstManager = SstNs.SstManager;
const Op = @import("./ops.zig").Op;
const RecordNs = @import("./record.zig");
const Record = RecordNs.Record;

// test "end_to_end_1" {
//     var alloc = std.testing.allocator;
//     std.testing.log_level = .info;

//     const path = "/tmp/e2e";

//     std.fs.makeDirAbsolute(path) catch {};
//     var dm = try DiskManager.init(path, alloc);
//     defer dm.deinit();

//     var wh = try WalHandler.init(dm, alloc);
//     defer wh.deinit();

//     var s = try SstManager.init(wh, dm, alloc);
//     defer s.deinit();

//     for (0..1_000) |i| {
//         const key = try std.fmt.allocPrint(alloc, "hello{}", .{i});
//         defer alloc.free(key);
//         const value = try std.fmt.allocPrint(alloc, "world{}", .{i});
//         defer alloc.free(value);

//         var r = try Record.init(key, value, Op.Upsert, alloc);
//         errdefer r.deinit();

//         try s.appendOwn(r);
//     }
// }
