// Tests for little-endian read helper functions.
module encoding

fn test_read_le_u32_basic() {
	data := [u8(0x01), 0x02, 0x03, 0x04]
	result := read_le_u32(data, 0)
	// 0x04030201 = 67305985
	assert result == u32(0x04030201), 'read_le_u32 at pos=0 failed: got ${result}'
}

fn test_read_le_u32_with_offset() {
	data := [u8(0x00), 0x00, 0x78, 0x56, 0x34, 0x12]
	result := read_le_u32(data, 2)
	assert result == u32(0x12345678), 'read_le_u32 at pos=2 failed: got ${result}'
}

fn test_read_le_u32_zero() {
	data := [u8(0x00), 0x00, 0x00, 0x00]
	result := read_le_u32(data, 0)
	assert result == u32(0), 'read_le_u32 of zeros failed'
}

fn test_read_le_u32_max() {
	data := [u8(0xFF), 0xFF, 0xFF, 0xFF]
	result := read_le_u32(data, 0)
	assert result == u32(0xFFFFFFFF), 'read_le_u32 max value failed'
}

fn test_read_le_i32_positive() {
	// 42 = 0x0000002A in LE: 2A 00 00 00
	data := [u8(0x2A), 0x00, 0x00, 0x00]
	result := read_le_i32(data, 0)
	assert result == i32(42), 'read_le_i32 positive failed: got ${result}'
}

fn test_read_le_i32_negative() {
	// -1 = 0xFFFFFFFF in LE: FF FF FF FF
	data := [u8(0xFF), 0xFF, 0xFF, 0xFF]
	result := read_le_i32(data, 0)
	assert result == i32(-1), 'read_le_i32 negative failed: got ${result}'
}

fn test_read_le_u64_basic() {
	// 0x0807060504030201
	data := [u8(0x01), 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
	result := read_le_u64(data, 0)
	assert result == u64(0x0807060504030201), 'read_le_u64 failed: got ${result}'
}

fn test_read_le_u64_with_offset() {
	data := [u8(0x00), 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
	result := read_le_u64(data, 2)
	assert result == u64(1), 'read_le_u64 with offset failed: got ${result}'
}

fn test_read_le_i64_positive() {
	// 100 = 0x0000000000000064 LE: 64 00 00 00 00 00 00 00
	data := [u8(0x64), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
	result := read_le_i64(data, 0)
	assert result == i64(100), 'read_le_i64 positive failed: got ${result}'
}

fn test_read_le_i64_negative() {
	// -1 = 0xFFFFFFFFFFFFFFFF LE: FF FF FF FF FF FF FF FF
	data := [u8(0xFF), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
	result := read_le_i64(data, 0)
	assert result == i64(-1), 'read_le_i64 negative failed: got ${result}'
}

fn test_write_le_u32_basic() {
	mut buf := []u8{cap: 4}
	write_le_u32(mut buf, u32(0x12345678))
	assert buf == [u8(0x78), 0x56, 0x34, 0x12], 'write_le_u32 failed: got ${buf}'
}

fn test_write_le_u32_zero() {
	mut buf := []u8{cap: 4}
	write_le_u32(mut buf, u32(0))
	assert buf == [u8(0x00), 0x00, 0x00, 0x00], 'write_le_u32 zero failed'
}

fn test_write_le_u64_basic() {
	mut buf := []u8{cap: 8}
	write_le_u64(mut buf, u64(0x0807060504030201))
	assert buf == [u8(0x01), 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08], 'write_le_u64 failed: got ${buf}'
}

fn test_le_roundtrip_u32() {
	original := u32(12345678)
	mut buf := []u8{cap: 4}
	write_le_u32(mut buf, original)
	result := read_le_u32(buf, 0)
	assert result == original, 'u32 roundtrip failed: ${original} -> ${result}'
}

fn test_le_roundtrip_u64() {
	original := u64(9876543210123456789)
	mut buf := []u8{cap: 8}
	write_le_u64(mut buf, original)
	result := read_le_u64(buf, 0)
	assert result == original, 'u64 roundtrip failed: ${original} -> ${result}'
}
