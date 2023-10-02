const std = @import("std");
const clap = @import("./pkg/zig-clap/clap.zig");
const HeaderNs = @import("./header.zig");
const Header = HeaderNs.Header;
const Record = @import("./record.zig").Record;
const Pointer = @import("./pointer.zig").Pointer;
const ReadWriteSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;

pub fn println(s: anytype) void {
    std.debug.print("{}\n", .{s});
}

pub fn printlns(s: anytype) void {
    std.debug.print("{s}\n", .{s});
}

pub fn prints(s: anytype) void {
    std.debug.print("'{s}'\n", .{s});
}

pub fn main() !void {
    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-e, --header <str>   An option parameter, which takes a value.
        \\-w, --wal <str>      An option parameter, which takes a value.
        \\<str>...
        \\
    );

    // Initialize our diagnostics, which can be used for reporting useful errors.
    // This is optional. You can also pass `.{}` to `clap.parse` if you don't
    // care about the extra information `Diagnostics` provides.
    var diag = clap.Diagnostic{};
    var res = clap.parse(clap.Help, &params, clap.parsers.default, .{
        .diagnostic = &diag,
    }) catch |err| {
        // Report useful error and exit
        diag.report(std.io.getStdErr().writer(), err) catch {};
        return err;
    };
    defer res.deinit();

    // var header: []u8 = null;
    var path: []const u8 = "";

    if (res.args.help != 0)
        std.debug.print("--help\n", .{});
    for (res.positionals) |pos| {
        path = pos;
        break;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var alloc = gpa.allocator();

    var buf = try alloc.alloc(u8, 500);
    defer alloc.free(buf);

    const abs_path = try std.fs.cwd().realpath(path, buf);
    var file = try std.fs.openFileAbsolute(abs_path, std.fs.File.OpenFlags{ .mode = .read_only });
    defer file.close();

    var rs = ReadWriteSeeker.initFile(file);
    var h = try Header.read(&rs);

    h.debug();

    const pointer = try Pointer.read(&rs, alloc);

    const record = try pointer.readValue(&rs, alloc);
    defer record.deinit();

    pointer.debug();
    record.debug();
}
