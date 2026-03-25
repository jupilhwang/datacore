/// Common binary utility functions shared across architecture layers.
/// Pure functions for byte-order conversions, varint encoding, and string escaping.
/// These have no external dependencies, safe to use from Domain, Service, and Infra layers.
module common

// Big-endian write helpers

/// write_i16_be writes a 16-bit integer in big-endian order to a buffer.
pub fn write_i16_be(mut buf []u8, val i16) {
	buf << [u8(val >> 8), u8(val)]
}

/// write_i32_be writes a 32-bit integer in big-endian order to a buffer.
pub fn write_i32_be(mut buf []u8, val i32) {
	buf << [u8(val >> 24), u8(val >> 16), u8(val >> 8), u8(val)]
}

/// write_i64_be writes a 64-bit integer in big-endian order to a buffer.
pub fn write_i64_be(mut buf []u8, val i64) {
	buf << [
		u8(val >> 56),
		u8(val >> 48),
		u8(val >> 40),
		u8(val >> 32),
		u8(val >> 24),
		u8(val >> 16),
		u8(val >> 8),
		u8(val),
	]
}

// Big-endian read helpers

/// read_i16_be reads a 16-bit integer in big-endian order from a buffer.
/// Returns error if the buffer has fewer than 2 bytes.
pub fn read_i16_be(data []u8) !i16 {
	if data.len < 2 {
		return error('insufficient data: need 2 bytes, have ${data.len}')
	}
	return i16(u16(data[0]) << 8 | u16(data[1]))
}

/// read_i32_be reads a 32-bit integer in big-endian order from a buffer.
/// Returns error if the buffer has fewer than 4 bytes.
pub fn read_i32_be(data []u8) !i32 {
	if data.len < 4 {
		return error('insufficient data: need 4 bytes, have ${data.len}')
	}
	return i32(u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3]))
}

/// read_i64_be reads a 64-bit integer in big-endian order from a buffer.
/// Returns error if the buffer has fewer than 8 bytes.
pub fn read_i64_be(data []u8) !i64 {
	if data.len < 8 {
		return error('insufficient data: need 8 bytes, have ${data.len}')
	}
	return i64(u64(data[0]) << 56 | u64(data[1]) << 48 | u64(data[2]) << 40 | u64(data[3]) << 32 | u64(data[4]) << 24 | u64(data[5]) << 16 | u64(data[6]) << 8 | u64(data[7]))
}

// Little-endian write helpers

/// write_i32_le writes a 32-bit integer in little-endian order to a buffer.
pub fn write_i32_le(mut buf []u8, val i32) {
	buf << [u8(val), u8(val >> 8), u8(val >> 16), u8(val >> 24)]
}

/// write_i64_le writes a 64-bit integer in little-endian order to a buffer.
pub fn write_i64_le(mut buf []u8, val i64) {
	buf << [
		u8(val),
		u8(val >> 8),
		u8(val >> 16),
		u8(val >> 24),
		u8(val >> 32),
		u8(val >> 40),
		u8(val >> 48),
		u8(val >> 56),
	]
}

// Little-endian read helpers

/// read_i32_le reads a 32-bit integer in little-endian order from a buffer.
pub fn read_i32_le(data []u8) i32 {
	return i32(u32(data[0]) | u32(data[1]) << 8 | u32(data[2]) << 16 | u32(data[3]) << 24)
}

/// read_i64_le reads a 64-bit integer in little-endian order from a buffer.
pub fn read_i64_le(data []u8) i64 {
	return i64(u64(data[0]) | u64(data[1]) << 8 | u64(data[2]) << 16 | u64(data[3]) << 24 | u64(data[4]) << 32 | u64(data[5]) << 40 | u64(data[6]) << 48 | u64(data[7]) << 56)
}

// Varint encoding - Kafka protocol uses signed varints

/// encode_varint encodes a signed integer as a varint.
pub fn encode_varint(value i64) []u8 {
	shift_v := if value < 0 { u64(0xffffffffffffffff) } else { u64(0) }
	zigzag := (u64(value) << 1) ^ shift_v
	return encode_uvarint(u64(zigzag))
}

/// encode_uvarint encodes an unsigned integer as a varint.
pub fn encode_uvarint(value u64) []u8 {
	mut result := []u8{cap: 10}
	mut v := value

	for v >= 0x80 {
		result << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	result << u8(v)

	return result
}

/// decode_varint decodes a signed varint.
pub fn decode_varint(data []u8) (i64, int) {
	uval, n := decode_uvarint(data)
	if n <= 0 {
		return 0, n
	}
	val := i64(uval >> 1) ^ -(i64(uval) & 1)
	return val, n
}

/// decode_uvarint decodes an unsigned varint.
pub fn decode_uvarint(data []u8) (u64, int) {
	mut result := u64(0)
	mut shift := u64(0)

	for i, b in data {
		if i >= 10 {
			return 0, -1
		}

		result |= u64(b & 0x7f) << shift

		if b & 0x80 == 0 {
			return result, i + 1
		}

		shift += 7
	}

	return 0, 0
}

// Varint size calculation

/// varint_size returns the encoded size of a signed varint value.
pub fn varint_size(value i64) int {
	shift_v := if value < 0 { u64(0xffffffffffffffff) } else { u64(0) }
	zigzag := (u64(value) << 1) ^ shift_v
	return uvarint_size(zigzag)
}

/// uvarint_size returns the encoded size of an unsigned varint value.
pub fn uvarint_size(value u64) int {
	mut size := 1
	mut v := value
	for v >= 0x80 {
		size += 1
		v >>= 7
	}
	return size
}

// String utility functions

/// escape_json_string escapes a string for use in a JSON value.
pub fn escape_json_string(s string) string {
	mut result := []u8{cap: s.len + 10}
	for c in s.bytes() {
		match c {
			`"` {
				result << [u8(`\\`), u8(`"`)]
			}
			`\\` {
				result << [u8(`\\`), u8(`\\`)]
			}
			`\n` {
				result << [u8(`\\`), u8(`n`)]
			}
			`\r` {
				result << [u8(`\\`), u8(`r`)]
			}
			`\t` {
				result << [u8(`\\`), u8(`t`)]
			}
			else {
				result << c
			}
		}
	}
	return result.bytestr()
}

// Hex utility functions

/// hex_char_to_nibble converts a hexadecimal character to a 4-bit value.
/// Returns -1 if the character is not a valid hex digit.
pub fn hex_char_to_nibble(c u8) int {
	if c >= `0` && c <= `9` {
		return int(c - `0`)
	} else if c >= `a` && c <= `f` {
		return int(c - `a` + 10)
	} else if c >= `A` && c <= `F` {
		return int(c - `A` + 10)
	}
	return -1
}
