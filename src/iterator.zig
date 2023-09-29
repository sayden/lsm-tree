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
