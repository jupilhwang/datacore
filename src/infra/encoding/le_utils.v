// Little-endian read/write helper functions.
// Eliminates duplicated LE encoding patterns across parquet_encoder.v,
// parquet_decoder.v, and parquet_metadata.v.
module encoding

// read_le_u32 reads a 4-byte little-endian u32 from data at position pos.
fn read_le_u32(data []u8, pos int) u32 {
	return u32(data[pos]) | u32(data[pos + 1]) << 8 | u32(data[pos + 2]) << 16 | u32(data[pos + 3]) << 24
}

// read_le_i32 reads a 4-byte little-endian i32 from data at position pos.
fn read_le_i32(data []u8, pos int) i32 {
	return i32(read_le_u32(data, pos))
}

// read_le_u64 reads an 8-byte little-endian u64 from data at position pos.
fn read_le_u64(data []u8, pos int) u64 {
	lo := u64(read_le_u32(data, pos))
	hi := u64(read_le_u32(data, pos + 4))
	return lo | hi << 32
}

// read_le_i64 reads an 8-byte little-endian i64 from data at position pos.
fn read_le_i64(data []u8, pos int) i64 {
	return i64(read_le_u64(data, pos))
}

// write_le_u32 appends a 4-byte little-endian u32 to buf.
fn write_le_u32(mut buf []u8, v u32) {
	buf << u8(v & 0xFF)
	buf << u8((v >> 8) & 0xFF)
	buf << u8((v >> 16) & 0xFF)
	buf << u8((v >> 24) & 0xFF)
}

// write_le_u64 appends an 8-byte little-endian u64 to buf.
fn write_le_u64(mut buf []u8, v u64) {
	buf << u8(v & 0xFF)
	buf << u8((v >> 8) & 0xFF)
	buf << u8((v >> 16) & 0xFF)
	buf << u8((v >> 24) & 0xFF)
	buf << u8((v >> 32) & 0xFF)
	buf << u8((v >> 40) & 0xFF)
	buf << u8((v >> 48) & 0xFF)
	buf << u8((v >> 56) & 0xFF)
}
