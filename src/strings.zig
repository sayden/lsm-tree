const std = @import("std");

/// Compare a and b, returning less than, equal to or
/// greater than zero if a is lexicographically less than,
/// equal to or greater than b.
pub fn strcmp(a: []const u8, b: []const u8) i32 {
    const min_len = @min(a.len, b.len);
    var i: usize = 0;

    while (i < min_len) : (i += 1) {
        const diff: i32 = @as(i32, a[i]) - b[i];
        if (diff != 0) {
            return diff;
        }
    }

    if (a.len == b.len) {
        return 0;
    } else if (a.len < b.len) {
        return -1;
    } else {
        return 1;
    }
}

test "strcmp" {
    const hella = "hella";
    const hello = "hello";
    const n = strcmp(hello, hella);
    try std.testing.expect(n > 0);

    const str1: []const u8 = "apple";
    const str2: []const u8 = "banana";
    const n1 = strcmp(str1, str2);
    try std.testing.expect(n1 < 0);

    const n2 = strcmp("hello", "hello");
    try std.testing.expect(n2 == 0);

    const n3 = strcmp("hello", "hello");
    try std.testing.expect(n3 == 0);

    const n4 = strcmp("hello", "helloa");
    try std.testing.expect(n4 < 0);
}
