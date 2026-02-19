// Common response headers and response builder functions
module kafka

/// Response header v0 (non-flexible)
/// Used by: ApiVersions (always), non-flexible APIs
pub struct ResponseHeader {
pub:
	correlation_id i32
}

/// Response header v1 (flexible)
/// Used by: flexible APIs
/// Includes tag_buffer after correlation_id
pub struct ResponseHeaderV1 {
pub:
	correlation_id i32
	// tag_buffer is written separately (minimum 1 byte: 0x00 when empty)
}

/// Builds a response with a header (non-flexible, Response Header v0).
/// Used by: ApiVersions (always), SaslHandshake, non-flexible API versions
/// Note: no tag_buffer in the header!
pub fn build_response(correlation_id i32, body []u8) []u8 {
	mut writer := new_writer_with_capacity(4 + 4 + body.len)

	// Size (total length excluding the size field itself)
	writer.write_i32(i32(4 + body.len))
	// Correlation ID
	writer.write_i32(correlation_id)
	// No tag_buffer in non-flexible response header!
	// Body
	writer.write_raw(body)

	return writer.bytes()
}

/// Builds a flexible response (Response Header v1, includes tag_buffer).
/// Used by: flexible API versions (except ApiVersions which is always non-flexible)
/// Important: tag_buffer is at least 1 byte (0x00 for empty tags)
pub fn build_flexible_response(correlation_id i32, body []u8) []u8 {
	mut writer := new_writer_with_capacity(4 + 4 + 1 + body.len)

	// Size (total length excluding the size field itself)
	// = correlation_id(4) + tag_buffer(1, minimum) + body
	writer.write_i32(i32(4 + 1 + body.len))
	// Correlation ID
	writer.write_i32(correlation_id)
	// Tag buffer (0x00 when empty, meaning num_tags=0)
	writer.write_uvarint(0)
	// Body
	writer.write_raw(body)

	return writer.bytes()
}

/// Builds a response with the appropriate header based on API key and version.
/// This is the recommended function for building responses.
pub fn build_response_auto(api_key ApiKey, api_version i16, correlation_id i32, body []u8) []u8 {
	response_header_version := get_response_header_version(api_key, api_version)
	if response_header_version >= 1 {
		return build_flexible_response(correlation_id, body)
	}
	return build_response(correlation_id, body)
}
