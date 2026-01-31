// Service Layer - 스키마 인코더 프리미티브
// Avro 프리미티브 타입을 위한 저수준 바이너리 인코딩/디코딩
module schema

// Varint Encoding/Decoding

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

// Float/Double Encoding/Decoding

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

// Bytes/String Encoding/Decoding

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
