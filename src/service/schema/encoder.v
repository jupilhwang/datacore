// Avro, JSON Schema, Protobuf를 위한 바이너리 인코딩/디코딩을 제공합니다.
module schema

// Format represents the encoding format type
pub enum Format {
	avro
	protobuf
	json
}

// Encoder interface for schema-based encoding/decoding
pub interface Encoder {
	encode(data []u8, schema string) ![]u8
	decode(data []u8, schema string) ![]u8
	format() Format
}

// Encoder errors
pub const err_invalid_schema = error('invalid schema')
pub const err_encoding_failed = error('encoding failed')
pub const err_decoding_failed = error('decoding failed')
