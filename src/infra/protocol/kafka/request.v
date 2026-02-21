// Common request structs and parsing functions
//
// This module handles request header parsing for the Kafka protocol.
// Supports different formats based on request header version (v0, v1, v2),
// and also processes tagged fields for flexible versions (Kafka 2.4+).
module kafka

/// Request header v0 (used by most requests)
///
/// Note: v0 uses STRING (non-null) for client_id,
/// while v1+ uses NULLABLE_STRING.
pub struct RequestHeader {
pub:
	api_key        i16
	api_version    i16
	correlation_id i32
	client_id      string
}

/// Tagged field - carries extension data in flexible versions
pub struct TaggedField {
pub:
	tag  u64
	data []u8
}

/// General request wrapper - contains header and body
pub struct Request {
pub:
	header RequestHeader
	body   []u8
}

/// Returns the request header version for the given API.
///
/// Returns: 0 = v0 (legacy), 1 = v1 (non-flexible), 2 = v2 (flexible, includes tag_buffer)
///
/// Note: ApiVersions requests follow the normal rule (v3+ uses Header v2)
///       Only ApiVersions responses are special (always Header v0, KIP-511)
pub fn get_request_header_version(api_key ApiKey, api_version i16) i16 {
	// SaslHandshake always uses Header v1 (non-flexible header)
	// For ApiVersions, this ensures backward compatibility per KIP-511
	if api_key == .sasl_handshake {
		return 1
	}
	// For ApiVersions, v3+ uses Header v2 (Flexible)
	// v0-v2 use Header v1 (Non-flexible)
	if api_key == .api_versions {
		return if api_version >= 3 { i16(2) } else { i16(1) }
	}
	// All other APIs follow the general rule:
	// - Flexible API versions use Header v2
	// - Non-flexible API versions use Header v1
	return if is_flexible_version(api_key, api_version) { i16(2) } else { i16(1) }
}

/// Returns the response header version for the given API.
///
/// Returns: 0 = v0 (no tag_buffer), 1 = v1 (includes tag_buffer)
///
/// Important: ApiVersions responses always use Header v0! (KIP-511 special rule)
/// This ensures clients can always parse ApiVersions responses
/// before knowing the server's capabilities.
pub fn get_response_header_version(api_key ApiKey, api_version i16) i16 {
	// ApiVersions responses always use Header v0 (KIP-511)
	// This is the only API to which this special rule applies
	if api_key == .api_versions {
		return 0
	}
	// All other APIs: flexible versions use Header v1, non-flexible use Header v0
	return if is_flexible_version(api_key, api_version) { i16(1) } else { i16(0) }
}

/// Parses a request from raw bytes.
///
/// Format: [header][body] (the size field has already been stripped by the TCP layer)
pub fn parse_request(data []u8) !Request {
	if data.len < 8 {
		return error('request too short')
	}

	mut reader := new_reader(data)

	// Parse header
	api_key := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!

	// Check whether this is a flexible version (v2 header)
	api_key_enum := unsafe { ApiKey(api_key) }
	header_version := get_request_header_version(api_key_enum, api_version)
	is_flexible_header := header_version >= 2

	// Even in Request Header v2 (flexible), client_id is still NULLABLE_STRING (not compact!)
	client_id := reader.read_nullable_string()!

	if is_flexible_header {
		// Skip tagged fields (tag_buffer) in the header
		reader.skip_tagged_fields()!
	}

	// Remaining data is the request body
	body := reader.data[reader.pos..].clone()

	return Request{
		header: RequestHeader{
			api_key:        api_key
			api_version:    api_version
			correlation_id: correlation_id
			client_id:      client_id
		}
		body:   body
	}
}

/// Checks whether the given API version uses flexible encoding.
///
/// Flexible versions were introduced in Kafka 2.4,
/// and each API has its own threshold version.
pub fn is_flexible_version(api_key ApiKey, version i16) bool {
	return match api_key {
		.produce { version >= 9 }
		.fetch { version >= 12 }
		.list_offsets { version >= 6 }
		.metadata { version >= 9 }
		.offset_commit { version >= 8 }
		.offset_fetch { version >= 6 }
		.find_coordinator { version >= 3 }
		.join_group { version >= 6 }
		.heartbeat { version >= 4 }
		.leave_group { version >= 4 }
		.sync_group { version >= 4 }
		.describe_groups { version >= 5 }
		.list_groups { version >= 3 }
		.sasl_handshake { false }
		.api_versions { version >= 3 }
		.create_topics { version >= 5 }
		.delete_topics { version >= 4 }
		.delete_records { version >= 2 }
		.init_producer_id { version >= 2 }
		.describe_configs { version >= 4 }
		.alter_configs { version >= 2 }
		.create_partitions { version >= 2 }
		.add_partitions_to_txn { version >= 3 }
		.add_offsets_to_txn { version >= 3 }
		.end_txn { version >= 3 }
		.write_txn_markers { version >= 1 }
		.txn_offset_commit { version >= 3 }
		.sasl_authenticate { version >= 2 }
		.delete_groups { version >= 2 }
		.describe_acls { version >= 2 }
		.create_acls { version >= 2 }
		.delete_acls { version >= 2 }
		.describe_cluster { version >= 0 }
		.consumer_group_heartbeat { version >= 0 }
		.consumer_group_describe { version >= 0 }
		.share_group_heartbeat { version >= 0 }
		.share_group_describe { version >= 0 }
		.share_fetch { version >= 0 }
		.share_acknowledge { version >= 0 }
		else { false }
	}
}
