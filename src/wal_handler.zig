const std = @import("std");
const MemoryWal = @import("./memory_wal.zig").MemoryWal;
const Record = @import("./record.zig").Record;
const DiskManager = @import("./disk_manager.zig").DiskManager;

pub const Result = enum {
    Ok,
    WalSwitched,
};

pub fn WalHandler(comptime WalType: type) type {
    return struct {
        const Self = @This();

        disk_manager: *DiskManager,

        old: ?*WalType,
        current: *WalType,
        next: *WalType,

        alloc: std.mem.Allocator,

        pub fn init(dm: *DiskManager, alloc: std.mem.Allocator) !*Self {
            var wh: *Self = try alloc.create(Self);

            wh.alloc = alloc;
            wh.disk_manager = dm;

            wh.old = null;

            // Create a WAL to use as current
            wh.current = try WalType.init(alloc);

            // Create also "next" WAL to switch when 'current' is full
            wh.next = try WalType.init(alloc);

            return wh;
        }

        pub fn deinit(self: *Self) void {
            self.persist(self.current) catch |err| {
                std.debug.print("Unkonwn error persisting current WAL: {}\n", .{err});
            };
            self.current.deinit();

            self.next.deinit();

            if (self.old) |old| {
                old.deinit();
            }

            self.alloc.destroy(self);
        }

        fn persist(self: *Self, wal: *WalType) !void {
            if (wal.header.total_records == 0) {
                return;
            }

            //Get a new file to persist the wal
            var f = try self.disk_manager.getNewFile(self.alloc);
            defer f.deinit();

            _ = try wal.persist(&f.file);
        }

        pub fn append(self: *Self, r: *Record) !Result {
            if (self.hasEnoughCapacity(r.len())) {
                try self.current.append(r);
                return Result.Ok;
            }

            try self.switchWal(r);
            try self.current.append(r);

            return Result.WalSwitched;
        }

        fn switchWal(self: *Self, r: *Record) !void {
            try self.next.append(r);

            //Get a new file to persist the wal
            var fileData = try self.disk_manager.getNewFile(self.alloc);
            defer fileData.deinit();
            var f = &fileData.file;

            _ = try self.current.persist(f);
            self.old = self.current;
            self.current = self.next;

            self.next = try WalType.init(self.alloc);
        }

        pub fn find(self: *Self, key: []const u8) ?*Record {
            return self.current.find(key);
        }

        pub fn hasEnoughCapacity(self: *Self, size: usize) bool {
            return self.current.availableBytes() >= size;
        }
    };
}

test "wal_handler_init" {
    var alloc = std.testing.allocator;

    var path = "/tmp";

    var dm = try DiskManager.init(path, alloc);
    defer dm.deinit();

    var wh = try WalHandler(MemoryWal(4096)).init(dm, alloc);
    wh.deinit();
}
