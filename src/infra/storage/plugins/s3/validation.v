// Input validation for S3 configuration.
// Prevents path traversal attacks in S3 key identifiers.
// SSRF endpoint validation has been moved to config/endpoint_validation.v
// to break the circular dependency between config and s3 modules.
module s3

/// max_identifier_length is the maximum allowed length for an identifier
/// used in S3 key construction. Consistent with max_topic_name_length.
const max_identifier_length = 255

/// parse_partition_key splits a "topic:partition" key into topic name and partition number.
fn parse_partition_key(partition_key string) !(string, int) {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key: ${partition_key}')
	}
	partition := parts[1].int()
	return parts[0], partition
}

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
