const std = @import("std");
const pointer = @import("./pointer.zig");
const Pointer = pointer.Pointer;

/// A SST or Sorted String Table is created from a Wal object. The structure is the following:
/// 
/// HEADER:
/// 8 bytes with the offset of the first key in the "data" chunk.
/// 8 bytes with the offset of the last key in the "data" chunk.
/// 8 bytes with the offset of the beginning of the "keys" chunk.
/// 
/// DATA CHUNK:
/// Contiguous array of records
/// 
/// KEYS CHUNK
/// Contiguous array of keys only with pointers to values in the data chunk
pub fn Sst(comptime WalType: type, comptime RecordType: type, comptime KeyLengthType: type) type {
    return struct {
        const header_size: usize = 8 + 8 + 8;
        head_offset: usize = 0,
        tail_offset: usize,
        file: *std.fs.File,
        wal: *WalType,

        pub fn init(w: *WalType, f: *std.fs.File) Sst {
            return Sst{
                .tail_offset = wal.current_size,
                .wal = w,
                .file = f,
            };
        }

        pub fn persist(self: *self) !usize {
            var iter = self.wal.iterator();

            var written: usize = 0;
            var total_record_bytes: usize = 0;
            var total_pointer_bytes: usize = 0;
            var buf: [2048]u8 = undefined;
            var PointerType = Pointer(KeyLengthType);

            //Write the header
            var ubuf: [8]u8 = undefined;

            // position of the first data chunk
            std.mem.writeIntSliceLittle(usize, ubuf, header_size);
            written += try self.file.pwrite(ubuf, 0);
            self.head_offset += 8;

            //position of the last data chunk. total_pointer_bytes has the size of the last record
            const last_record_offset = self.wal.current_size - total_pointer_bytes;
            std.mem.writeIntSliceLittle(usize, ubuf, last_record_offset);
            written += try self.file.pwrite(ubuf, self.head_offset);
            self.head_offset += 8;

            //position of the beginning of the keys
            std.mem.writeIntSliceLittle(usize, ubuf, self.wal.current_size);
            written += try self.file.pwrite(self.head_offset, ubuf);
            self.head_offset += 8;

            // Write the data chunkss
            while (iter.next()) |record| {
                var p = PointerType.toPointer(&record);
                p.byte_offset = self.head_offset;

                // Write the record at the beginning of the file (head offset)
                total_record_bytes = try record.bytes(&buf);
                written += try self.file.pwrite(buf[0..total_record_bytes], self.head_offset);
                self.head_offset += total_record_bytes;

                //Write pointer on the end of the file (tail offset)
                total_pointer_bytes = p.bytes(buf, self.head_offset);
                written += try self.file.pwrite(buf[0..total_pointer_bytes], self.tail_offset);
                self.tail_offset += total_pointer_bytes;
            }

            self.file.close();
        }
    };
}
