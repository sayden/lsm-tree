pub fn Iterator(comptime T: anytype) type {
    return struct {
        const Self = @This();

        pos: usize = 0,
        records: []T,

        pub fn init(records: []T) Self {
            return Self{
                .records = records,
            };
        }

        pub fn next(self: *Self) ?T {
            if (self.pos == self.records.len) {
                return null;
            }

            const r = self.records[self.pos];
            self.pos += 1;
            return r;
        }
    };
}

pub fn IteratorBackwards(comptime T: anytype) type {
    return struct {
        const Self = @This();

        pos: usize = 0,
        records: []T,
        finished: bool = false,

        pub fn init(records: []T) Self {
            const tuple = @subWithOverflow(records.len, 1);
            if (tuple[1] != 0) {
                //empty

                return Self{
                    .records = records,
                    .pos = 0,
                    .finished = true,
                };
            }
            return Self{
                .records = records,
                .pos = records.len - 1,
            };
        }

        pub fn next(self: *Self) ?T {
            if (self.pos == 0 and self.finished) {
                return null;
            }

            const r = self.records[self.pos];
            if (self.pos != 0) {
                self.pos -= 1;
            } else {
                self.finished = true;
            }

            return r;
        }
    };
}
