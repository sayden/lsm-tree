const std = @import("std");
const expect = std.testing.expect;
const print = std.debug.print;
const record = @import("record.zig");
const Record = record.Record;

test "size of a struct" {
    var key = "helslo".*;
    var value = "world".*;
    const r = Record(u32, u64){
        .key = key[0..],
        .value = value[0..],
    };

    print("\nsize {d}\n", .{@sizeOf(@TypeOf(r))});
    print("size {d}\n", .{@sizeOf(@TypeOf(key))});
    print("size {d}\n", .{@sizeOf(@TypeOf(r.key))});
    print("size {d}\n", .{@sizeOf(@TypeOf(r.value))});
}
