// Adapter Layer - Kafka Request Parsing
// Common request structures and parsing functions
module kafka

// Request Header v0 (used for most requests)
// Note: v0 uses STRING (non-null) for client_id, v1+ uses NULLABLE_STRING
pub struct RequestHeader {
pub:
	api_key        i16
	api_version    i16
	correlation_id i32
	client_id      string // Empty string means null/no client id
}

// Request Header v2 (flexible versions, Kafka 2.4+)
// Note: client_id is still NULLABLE_STRING, NOT compact! Only tag_buffer is compact.
pub struct RequestHeaderV2 {
pub:
	api_key        i16
	api_version    i16
	correlation_id i32
	client_id      string
	tagged_fields  []TaggedField
}

pub struct TaggedField {
pub:
	tag  u64
	data []u8
}

// Generic Request wrapper
pub struct Request {
pub:
	header RequestHeader
	body   []u8
}

// Get Request Header version for a given API
// Returns: 0 = v0 (legacy), 1 = v1 (non-flexible), 2 = v2 (flexible with tag_buffer)
// Note: ApiVersions Request follows NORMAL rules (v3+ uses Header v2)
//       Only ApiVersions RESPONSE is special (always Header v0, KIP-511)
pub fn get_request_header_version(api_key ApiKey, api_version i16) i16 {
	// SaslHandshake and ApiVersions always use header v1 (non-flexible header)
	// For ApiVersions, this is per KIP-511 to ensure backward compatibility.
	if api_key == .sasl_handshake {
		return 1
	}
	// For ApiVersions, v3+ uses Header v2 (Flexible)
	// v0-v2 uses Header v1 (Non-flexible)
	if api_key == .api_versions {
		return if api_version >= 3 { i16(2) } else { i16(1) }
	}
	// All other APIs follow normal rules:
	// - Flexible API versions use Header v2
	// - Non-flexible API versions use Header v1
	return if is_flexible_version(api_key, api_version) { i16(2) } else { i16(1) }
}

// Get Response Header version for a given API
// Returns: 0 = v0 (no tag_buffer), 1 = v1 (with tag_buffer)
// CRITICAL: ApiVersions Response is ALWAYS Header v0! (KIP-511 special rule)
// This ensures clients can always parse ApiVersions response even before
// knowing the server's capabilities
pub fn get_response_header_version(api_key ApiKey, api_version i16) i16 {
	// ApiVersions Response ALWAYS uses Header v0 (KIP-511)
	// This is the ONLY API with this special rule
	if api_key == .api_versions {
		return 0
	}
	// All other APIs: flexible versions use Header v1, non-flexible use Header v0
	return if is_flexible_version(api_key, api_version) { i16(1) } else { i16(0) }
}

// Parse request from raw bytes
// Format: [size: 4 bytes][header][body]
pub fn parse_request(data []u8) !Request {
	if data.len < 8 {
		return error('request too short')
	}

	mut reader := new_reader(data)

	// Read size field (first 4 bytes)
	reader.read_i32()!

	// Parse header
	api_key := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!

	// Check if this is a flexible version (v2 header)
	api_key_enum := unsafe { ApiKey(api_key) }
	header_version := get_request_header_version(api_key_enum, api_version)
	is_flexible_header := header_version >= 2

	// In Request Header v2 (flexible), client_id is still NULLABLE_STRING (NOT compact!)
	client_id := reader.read_nullable_string()!

	if is_flexible_header {
		// Skip tagged fields (tag_buffer) in header
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

// Check if API version uses flexible encoding
pub fn is_flexible_version(api_key ApiKey, version i16) bool {
	// Flexible versions were introduced in Kafka 2.4
	// Each API has its own threshold
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
		.sasl_handshake { false } // v0-v1 are never flexible
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
		.sasl_authenticate { version >= 2 } // v2+ is flexible
		.delete_groups { version >= 2 }
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
