const std = @import("std");
const clap = @import("./pkg/zig-clap/clap.zig");
const Header = @import("./header.zig").Header;

pub fn main() !void {
    // First we specify what parameters our program can take.
    // We can use `parseParamsComptime` to parse a string into an array of `Param(Help)`
    const params = comptime clap.parseParamsComptime(
        \\-h, --help             Display this help and exit.
        \\-e, --header <str>   An option parameter, which takes a value.
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
    // if (res.args.header) |e|
    //     bytes = e;
    // for (res.args.offset) |o|
    //     offset = o;
    for (res.positionals) |pos| {
        path = pos;
        break;
    }

    var f = try std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{ .mode = .read_only });
    defer f.close();

    var reader = f.reader();
    var h = try Header.fromReader(reader);

    std.debug.print("Header\n------\n", .{});
    std.debug.print("Magic number:\t\t{}\nTotal records:\t\t{}\nFirst pointer offset:\t{}\n", .{ h.magic_number, h.total_records, h.first_pointer_offset });
    std.debug.print("Last pointer offset:\t{}\nRecords size:\t\t{}\n", .{ h.last_pointer_offset, h.records_size });
    std.debug.print("Reserved: {s}\n\nFirst Record:\n-------------\n", .{h.reserved});

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    var allocator = gpa.allocator();
    var r = try @import("./record.zig").Record.fromBytesReader(&allocator, reader);
    defer r.deinit();
    r.str();
}
