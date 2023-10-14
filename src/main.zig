const std = @import("std");
pub const log_level: std.log.Level = .debug;
const os = std.os;

pub fn newHandler(_: c_int) align(1) callconv(.C) void {
    std.debug.print("\nBye!\n", .{});
    std.os.exit(0);
}

pub fn main() !void {
    const new_action = os.Sigaction{
        .handler = .{ .handler = newHandler },
        .mask = std.os.empty_sigset,
        .flags = 0,
    };
    var old_action: os.Sigaction = undefined;
    try os.sigaction(os.SIG.INT, &new_action, &old_action);

    std.time.sleep(10000000000000);
}
