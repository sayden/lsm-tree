const std = @import("std");
const File = std.fs.File;
const math = std.math;
const Op = @import("./ops.zig").Op;
const Allocator = std.mem.Allocator;

const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const Kv = @import("./kv.zig").Kv;
const Column = @import("./columnar.zig").Column;

pub const Data = union(enum) {
    kv: Kv,
    col: Column,

    pub fn new(data: anytype) Data {
        return switch (@TypeOf(data)) {
            Kv => return Data{ .kv = data },
            inline else => return Data{ .col = data },
        };
    }

    pub fn deinit(self: Data) void {
        return switch (self) {
            inline else => |case| case.deinit(),
        };
    }

    pub fn read(comptime T: type, reader: *ReaderWriterSeeker, alloc: Allocator) !Data {
        var result: T = try T.read(reader, alloc);
        return switch (T) {
            Kv => Data{ .kv = result },
            inline else => Data{ .col = result },
        };
    }

    pub fn write(self: Data, writer: *ReaderWriterSeeker) !usize {
        return switch (self) {
            inline else => |case| case.write(writer),
        };
    }

    /// Returns true if self < other
    pub fn compare(self: Data, other: Data) bool {
        return switch (self) {
            inline else => |case| case.compare(other),
        };
    }

    pub fn writeIndexingValue(self: Data, writer: *ReaderWriterSeeker) !void {
        return switch (self) {
            inline else => |case| case.writeIndexingValue(writer),
        };
    }

    pub fn sortFn(_: void, self: Data, other: Data) bool {
        return switch (self) {
            inline else => |case| case.sortFn(self, other),
        };
    }

    pub fn clone(self: Data, alloc: Allocator) !Data {
        return switch (self) {
            inline else => |case| case.clone(alloc),
        };
    }

    pub fn cloneTo(self: Data, other: *Data, alloc: Allocator) !void {
        return switch (self) {
            inline else => |case| case.cloneTo(other, alloc),
        };
    }

    pub fn equals(self: Data, other: Data) bool {
        return switch (self) {
            inline else => |case| case.equals(other),
        };
    }

    pub fn debug(self: Data, log: anytype) void {
        return switch (self) {
            inline else => |case| case.debug(log),
        };
    }
};

test "Data_Row" {
    var alloc = std.testing.allocator;

    var row = Kv.new("hello", "world", Op.Upsert);

    var data = Data{ .kv = row };
    defer data.deinit();

    var buf: [64]u8 = undefined;
    var rws = ReaderWriterSeeker.initBuf(&buf);
    const bytes_written = try data.write(&rws);
    _ = bytes_written;

    try rws.seekTo(0);

    var row2 = try Data.read(Kv, &rws, alloc);
    defer row2.deinit();

    try std.testing.expectEqualStrings("hello", row2.kv.key);
}
