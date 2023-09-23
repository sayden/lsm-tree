const std = @import("std");
const Order = std.math.Order;
const Math = std.math;
const expectEqual = std.testing.expectEqual;

/// Compare a and b, returning less than, equal to or
/// greater than zero if a is lexicographically less than,
/// equal to or greater than b.
pub fn strcmp(a: []const u8, b: []const u8) Order {
    const min_len = @min(a.len, b.len);
    var i: usize = 0;

    while (i < min_len) : (i += 1) {
        const diff: i32 = @as(i32, a[i]) - b[i];
        if (diff != 0) {
            return Math.order(a[i], b[i]);
        }
    }

    return Math.order(a.len, b.len);
}

test "strcmp" {
    const hella = "hella";
    const hello = "hello";
    const n = strcmp(hello, hella);
    try expectEqual(Order.gt, n);

    const str1: []const u8 = "apple";
    const str2: []const u8 = "banana";
    const n1 = strcmp(str1, str2);
    try expectEqual(Order.lt, n1);

    const n2 = strcmp("hello", "hello");
    try expectEqual(Order.eq, n2);

    const n3 = strcmp("hello", "hello");
    try expectEqual(Order.eq, n3);

    const n4 = strcmp("hello", "helloa");
    try expectEqual(Order.lt, n4);

    const n5 = strcmp("hello10", "hello9");
    try expectEqual(Order.lt, n5);
}
