// Input validation for S3 key construction.
// Prevents path traversal attacks by rejecting dangerous characters
// in identifiers used to build S3 object keys (group_id, topic, etc.).
module s3

/// max_identifier_length is the maximum allowed length for an identifier
/// used in S3 key construction. Consistent with max_topic_name_length.
const max_identifier_length = 255

/// validate_identifier validates a string used in S3 key construction.
/// Rejects empty strings, path traversal sequences, null bytes,
/// non-printable characters, and characters outside the allowed set.
/// Allowed characters: alphanumeric, underscore, hyphen, dot (but not ".." substring).
fn validate_identifier(value string, field_name string) ! {
	if value.len == 0 {
		return error('${field_name} cannot be empty')
	}
	if value.len > max_identifier_length {
		return error('${field_name} too long: ${value.len} > ${max_identifier_length}')
	}
	if value.contains('..') {
		return error('${field_name} contains path traversal sequence: ".."')
	}
	for ch in value {
		if ch == 0 {
			return error('${field_name} contains null byte')
		}
		if ch < 0x20 {
			return error('${field_name} contains non-printable character: 0x${ch:02x}')
		}
		if !ch.is_alnum() && ch != `_` && ch != `-` && ch != `.` {
			return error('${field_name} contains invalid character: ${[ch].bytestr()}')
		}
	}
}
