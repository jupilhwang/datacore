// Service Layer - Schema encoder primitives
// Low-level binary encoding/decoding for Avro primitive types
module schema

import infra.performance.core

// Varint Encoding/Decoding

// encode_varint_zigzag encodes an i64 using variable-length zigzag encoding
fn encode_varint_zigzag(val i64) []u8 {
	return core.encode_varint(val)
}

// encode_varint encodes a u64 using variable-length encoding
fn encode_varint(val u64) []u8 {
	return core.encode_uvarint(val)
}

// Float/Double Encoding/Decoding

// encode_float encodes a f32 in little-endian IEEE 754 format
fn encode_float(val f32) []u8 {
	mut buf := []u8{}
	bits := *unsafe { &u32(&val) }
	core.write_i32_le(mut buf, i32(bits))
	return buf
}

// encode_double encodes a f64 in little-endian IEEE 754 format
fn encode_double(val f64) []u8 {
	mut buf := []u8{}
	bits := *unsafe { &u64(&val) }
	core.write_i64_le(mut buf, i64(bits))
	return buf
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
