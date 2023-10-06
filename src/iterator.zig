const std = @import("std");
const ReaderWriterSeeker = @import("./read_writer_seeker.zig").ReaderWriterSeeker;
const headerSize = @import("./header.zig").headerSize;
const Record = @import("./record.zig").Record;
const Pointer = @import("./record.zig").Pointer;

pub fn Iterator(comptime T: anytype) type {
    return struct {
        const Self = @This();

        pos: usize = 0,
        items: []T,

        pub fn init(items: []T) Self {
            return Self{
                .items = items,
            };
        }

        pub fn next(self: *Self) ?T {
            if (self.pos == self.items.len) {
                return null;
            }

            const r = self.items[self.pos];
            self.pos += 1;
            return r;
        }
    };
}

pub fn MutableIterator(comptime T: anytype) type {
    return struct {
        const Self = @This();

        pos: usize = 0,
        items: []T,

        pub fn init(items: []T) Self {
            return Self{
                .items = items,
            };
        }

        pub fn next(self: *Self) ?*T {
            if (self.pos == self.items.len) {
                return null;
            }

            var ptr = &self.items[self.pos];
            self.pos += 1;
            return ptr;
        }
    };
}

pub const BytesIterator = struct {
    reader: *ReaderWriterSeeker,
    alloc: std.mem.Allocator,

    pub fn init(rs: *ReaderWriterSeeker, alloc: std.mem.Allocator) !BytesIterator {
        try rs.seekTo(headerSize());
        return BytesIterator{ .reader = rs, .alloc = alloc };
    }

    pub fn next(self: *BytesIterator) ?*Record {
        const pointer = Pointer.read(self.rs, self.alloc) catch |err| {
            std.log.err("{}", .{err});
            return null;
        };
        errdefer pointer.deinit();

        const record = pointer.readValue(self.reader, self.alloc) catch |err| {
            std.log.err("{}", .{err});
            return null;
        };

        return record;
    }
};

pub fn IteratorBackwards(comptime T: anytype) type {
    return struct {
        const Self = @This();

        pos: usize = 0,
        items: []T,
        finished: bool = false,

        pub fn init(items: []T) Self {
            const tuple = @subWithOverflow(items.len, 1);
            if (tuple[1] != 0) {
                //empty

                return Self{
                    .items = items,
                    .pos = 0,
                    .finished = true,
                };
            }
            return Self{
                .items = items,
                .pos = items.len - 1,
            };
        }

        pub fn next(self: *Self) ?T {
            if (self.pos == 0 and self.finished) {
                return null;
            }

            const r = self.items[self.pos];
            if (self.pos != 0) {
                self.pos -= 1;
            } else {
                self.finished = true;
            }

            return r;
        }
    };
}
