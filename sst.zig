const std = @import("std");

/// A SST or Sorted String Table is created from a Wal object. The structure is the following:
/// 
/// HEADER:
/// 8 bytes to the offset of the first key in the "data" chunk.
/// 8 bytes to the offset of the last key in the "data" chunk.
/// 8 bytes to the offset of the beginning of the "keys" chunk.
/// 
/// DATA CHUNK:
/// Contiguous array of records
/// 
/// KEYS CHUNK
/// Contiguous array of keys only with pointers to values in the data chunk