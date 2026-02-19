// Provides binary encoding/decoding for Avro, JSON Schema, and Protobuf.
module schema

// Format represents the encoding format type
/// Format represents the encoding format type.
pub enum Format {
	avro
	protobuf
	json
}

// Encoder interface for schema-based encoding/decoding
/// Encoder is the encoder interface.
pub interface Encoder {
	encode(data []u8, schema string) ![]u8
	decode(data []u8, schema string) ![]u8
	format() Format
}

// Encoder errors
/// err_invalid_schema is an error constant for invalid schema.
pub const err_invalid_schema = error('invalid schema')
/// err_encoding_failed is an error constant for encoding failure.
pub const err_encoding_failed = error('encoding failed')
/// err_decoding_failed is an error constant for decoding failure.
pub const err_decoding_failed = error('decoding failed')
