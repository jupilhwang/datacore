// Schema encoder interface following DIP.
// Abstracts schema-based encoding/decoding from concrete encoder implementations.
// Follows ISP: only encode/decode, excludes format() and other concerns.
module port

/// SchemaEncoderPort abstracts schema-based data encoding and decoding.
/// Implemented by AvroEncoder, JsonEncoder, and ProtobufEncoder via structural typing.
pub interface SchemaEncoderPort {
mut:
	/// Encodes data using the specified schema definition string.
	encode(data []u8, schema_str string) ![]u8
	/// Decodes data using the specified schema definition string.
	decode(data []u8, schema_str string) ![]u8
}
