pub fn ItemIterator(comptime T: anytype) type {
    return struct {
        const Self: type = @This();

        pos: usize = 0,
        items: []T,
        size: usize = 0,

        pub fn init(items: []T, size: usize) Self {
            return Self{
                .items = items,
                .size = size,
            };
        }

        pub fn next(self: *Self) ?T {
            if (self.pos == self.size) {
                return null;
            }

            const r = self.items[self.pos];
            self.pos += 1;
            return r;
        }
    };
}

pub fn ItemBackwardIterator(comptime T: anytype) type {
    return struct {
        const Self: type = @This();

        pos: usize = 0,
        items: []T,
        finished: bool = false,
        size: usize = 0,

        pub fn init(items: []T, size: usize) Self {
            const tuple = @subWithOverflow(size, 1);
            if (tuple[1] != 0) {
                //empty

                return Self{
                    .items = items,
                    .size = size,
                };
            }
            return Self{
                .items = items,
                .pos = size - 1,
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
