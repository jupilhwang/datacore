// Service Layer - 스키마 인코더 프리미티브
// Avro 프리미티브 타입을 위한 저수준 바이너리 인코딩/디코딩
module schema

// ============================================================================
// Varint Encoding/Decoding
// ============================================================================

// encode_varint_zigzag encodes an i64 using variable-length zigzag encoding
fn encode_varint_zigzag(val i64) []u8 {
	// ZigZag encoding: (n << 1) ^ (n >> 63)
	// Cast to u64 first to avoid signed shift warning
	zigzag := u64(val) << 1 ^ u64(val >> 63)
	return encode_varint(zigzag)
}

// encode_varint encodes a u64 using variable-length encoding
fn encode_varint(val u64) []u8 {
	mut result := []u8{}
	mut n := val

	for {
		mut b := u8(n & 0x7F)
		n = n >> 7
		if n != 0 {
			b |= 0x80 // More bytes to follow
		}
		result << b
		if n == 0 {
			break
		}
	}

	return result
}

// decode_varint_zigzag decodes a zigzag-encoded varint to i64
fn decode_varint_zigzag(mut reader AvroReader) !i64 {
	val := decode_varint(mut reader)!
	// ZigZag decoding: (n >> 1) ^ -(n & 1)
	return i64((val >> 1) ^ u64(-i64(val & 1)))
}

// decode_varint_zigzag_int decodes a zigzag-encoded varint to i32
fn decode_varint_zigzag_int(mut reader AvroReader) !int {
	val := decode_varint_zigzag(mut reader)!
	return int(val)
}

// decode_varint decodes a variable-length u64
fn decode_varint(mut reader AvroReader) !u64 {
	mut result := u64(0)
	mut shift := 0

	for {
		if reader.pos >= reader.data.len {
			return error('unexpected end of varint')
		}

		b := reader.data[reader.pos]
		reader.pos += 1

		result |= u64(b & 0x7F) << shift

		if (b & 0x80) == 0 {
			break
		}

		shift += 7
		if shift >= 64 {
			return error('varint too long')
		}
	}

	return result
}

// ============================================================================
// Float/Double Encoding/Decoding
// ============================================================================

// encode_float encodes a f32 in little-endian IEEE 754 format
fn encode_float(val f32) []u8 {
	bits := *unsafe { &u32(&val) }
	return [
		u8(bits & 0xFF),
		u8((bits >> 8) & 0xFF),
		u8((bits >> 16) & 0xFF),
		u8((bits >> 24) & 0xFF),
	]
}

// encode_double encodes a f64 in little-endian IEEE 754 format
fn encode_double(val f64) []u8 {
	bits := *unsafe { &u64(&val) }
	return [
		u8(bits & 0xFF),
		u8((bits >> 8) & 0xFF),
		u8((bits >> 16) & 0xFF),
		u8((bits >> 24) & 0xFF),
		u8((bits >> 32) & 0xFF),
		u8((bits >> 40) & 0xFF),
		u8((bits >> 48) & 0xFF),
		u8((bits >> 56) & 0xFF),
	]
}

// decode_float decodes a f32 from little-endian IEEE 754
fn decode_float(mut reader AvroReader) !f32 {
	if reader.pos + 4 > reader.data.len {
		return error('unexpected end of float')
	}

	bits := u32(reader.data[reader.pos]) | (u32(reader.data[reader.pos + 1]) << 8) | (u32(reader.data[
		reader.pos + 2]) << 16) | (u32(reader.data[reader.pos + 3]) << 24)
	reader.pos += 4

	return *unsafe { &f32(&bits) }
}

// decode_double decodes a f64 from little-endian IEEE 754
fn decode_double(mut reader AvroReader) !f64 {
	if reader.pos + 8 > reader.data.len {
		return error('unexpected end of double')
	}

	bits := u64(reader.data[reader.pos]) | (u64(reader.data[reader.pos + 1]) << 8) | (u64(reader.data[
		reader.pos + 2]) << 16) | (u64(reader.data[reader.pos + 3]) << 24) | (u64(reader.data[
		reader.pos + 4]) << 32) | (u64(reader.data[reader.pos + 5]) << 40) | (u64(reader.data[
		reader.pos + 6]) << 48) | (u64(reader.data[reader.pos + 7]) << 56)
	reader.pos += 8

	return *unsafe { &f64(&bits) }
}

// ============================================================================
// Bytes/String Encoding/Decoding
// ============================================================================

// encode_bytes encodes bytes with length prefix
fn encode_bytes(data []u8) []u8 {
	mut result := encode_varint_zigzag(i64(data.len))
	result << data
	return result
}

// encode_string encodes a string as bytes
fn encode_string(s string) []u8 {
	return encode_bytes(s.bytes())
}

// decode_bytes decodes length-prefixed bytes
fn decode_bytes(mut reader AvroReader) ![]u8 {
	length := decode_varint_zigzag(mut reader)!
	if length < 0 {
		return error('negative bytes length')
	}

	len_int := int(length)
	if reader.pos + len_int > reader.data.len {
		return error('unexpected end of bytes')
	}

	result := reader.data[reader.pos..reader.pos + len_int].clone()
	reader.pos += len_int

	return result
}

// decode_string decodes a string from bytes
fn decode_string(mut reader AvroReader) !string {
	bytes := decode_bytes(mut reader)!
	return bytes.bytestr()
}
