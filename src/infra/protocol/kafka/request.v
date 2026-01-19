// Adapter Layer - Kafka Request Parsing
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

	// Parse header
	api_key := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!

	// Check if this is a flexible version (v2 header)
	// Note: ApiVersions is ALWAYS non-flexible in header (client doesn't know server version yet)
	api_key_enum := unsafe { ApiKey(api_key) }
	header_version := get_request_header_version(api_key_enum, api_version)
	is_flexible_header := header_version >= 2

	// In Request Header v2 (flexible), client_id is still a regular NULLABLE_STRING (2-byte length prefix)
	// NOT a compact string! Only the tag_buffer at the end is compact-encoded
	client_id := reader.read_nullable_string()!

	if is_flexible_header {
		// Skip tagged fields (tag_buffer) in header
		reader.skip_tagged_fields()!
	}

	// Remaining data is the request body
	body := data[reader.pos..].clone()

	// Debug: print header parsing position for FETCH requests
	if api_key_enum == .fetch {
		eprintln('[DEBUG] parse_request FETCH: total_len=${data.len} header_end_pos=${reader.pos} body.len=${body.len}')
		eprintln('[DEBUG] parse_request FETCH: api_key=${api_key} api_version=${api_version} correlation_id=${correlation_id}')
		eprintln('[DEBUG] parse_request FETCH: client_id=${client_id} is_flexible=${is_flexible_header}')
		if data.len >= 40 {
			eprintln('[DEBUG] parse_request FETCH: first 40 bytes of full data: ${data[..40].hex()}')
		}
		if body.len >= 40 {
			eprintln('[DEBUG] parse_request FETCH: first 40 bytes of BODY: ${body[..40].hex()}')
		}
	}

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

// ApiVersions Request
pub struct ApiVersionsRequest {
pub:
	client_software_name    string
	client_software_version string
}

fn parse_api_versions_request(mut reader BinaryReader, version i16, is_flexible bool) !ApiVersionsRequest {
	mut req := ApiVersionsRequest{}

	if version >= 3 {
		if is_flexible {
			req = ApiVersionsRequest{
				client_software_name:    reader.read_compact_string()!
				client_software_version: reader.read_compact_string()!
			}
			reader.skip_tagged_fields()!
		}
	}

	return req
}

// Metadata Request
pub struct MetadataRequest {
pub:
	topics                         []MetadataRequestTopic
	allow_auto_topic_creation      bool
	include_cluster_authorized_ops bool
	include_topic_authorized_ops   bool
}

pub struct MetadataRequestTopic {
pub:
	topic_id []u8
	name     ?string
}

fn parse_metadata_request(mut reader BinaryReader, version i16, is_flexible bool) !MetadataRequest {
	mut topics := []MetadataRequestTopic{}

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			// v10+: Read topic_id (UUID, 16 bytes)
			mut topic_id := []u8{}
			if version >= 10 {
				topic_id = reader.read_uuid()!
			}

			mut topic := MetadataRequestTopic{}
			if is_flexible {
				name := reader.read_compact_string()!
				topic = MetadataRequestTopic{
					topic_id: topic_id
					name:     if name.len > 0 { name } else { none }
				}
			} else {
				topic = MetadataRequestTopic{
					name: reader.read_nullable_string()!
				}
			}

			if is_flexible && reader.remaining() > 0 {
				reader.skip_tagged_fields()!
			}

			topics << topic
		}
	}

	mut allow_auto_topic_creation := true
	if version >= 4 {
		allow_auto_topic_creation = reader.read_i8()! != 0
	}

	mut include_cluster_authorized_ops := false
	mut include_topic_authorized_ops := false
	// v8-v10: both fields present
	// v11+: include_cluster_authorized_operations was removed (moved to DescribeCluster API)
	if version >= 8 && version <= 10 {
		include_cluster_authorized_ops = reader.read_i8()! != 0
		include_topic_authorized_ops = reader.read_i8()! != 0
	} else if version >= 11 {
		// Only include_topic_authorized_operations remains
		include_topic_authorized_ops = reader.read_i8()! != 0
	}

	return MetadataRequest{
		topics:                         topics
		allow_auto_topic_creation:      allow_auto_topic_creation
		include_cluster_authorized_ops: include_cluster_authorized_ops
		include_topic_authorized_ops:   include_topic_authorized_ops
	}
}

// Produce Request (simplified)
pub struct ProduceRequest {
pub:
	transactional_id ?string
	acks             i16
	timeout_ms       i32
	topic_data       []ProduceRequestTopic
}

pub struct ProduceRequestTopic {
pub:
	name           string
	topic_id       []u8 // v13+ (16 bytes UUID)
	partition_data []ProduceRequestPartition
}

pub struct ProduceRequestPartition {
pub:
	index   i32
	records []u8 // RecordBatch or MessageSet
}

fn parse_produce_request(mut reader BinaryReader, version i16, is_flexible bool) !ProduceRequest {
	mut transactional_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			transactional_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			transactional_id = if str.len > 0 { str } else { none }
		}
	}

	acks := reader.read_i16()!
	timeout_ms := reader.read_i32()!

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut topic_data := []ProduceRequestTopic{}
	for _ in 0 .. topic_count {
		mut name := ''
		mut topic_id := []u8{}

		if version >= 13 {
			// v13+: uses topic_id instead of name
			topic_id = reader.read_uuid()!
			// name needs to be resolved from topic_id later
		} else if is_flexible {
			name = reader.read_compact_string()!
		} else {
			name = reader.read_string()!
		}

		partition_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		mut partition_data := []ProduceRequestPartition{}
		for _ in 0 .. partition_count {
			index := reader.read_i32()!
			records := if is_flexible {
				reader.read_compact_bytes()!
			} else {
				reader.read_bytes()!
			}

			partition_data << ProduceRequestPartition{
				index:   index
				records: records
			}

			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topic_data << ProduceRequestTopic{
			name:           name
			topic_id:       topic_id
			partition_data: partition_data
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	return ProduceRequest{
		transactional_id: transactional_id
		acks:             acks
		timeout_ms:       timeout_ms
		topic_data:       topic_data
	}
}

// Fetch Request (simplified)
pub struct FetchRequest {
pub:
	replica_id            i32
	max_wait_ms           i32
	min_bytes             i32
	max_bytes             i32
	isolation_level       i8
	topics                []FetchRequestTopic
	forgotten_topics_data []FetchRequestForgottenTopic
}

pub struct FetchRequestForgottenTopic {
	name       string
	topic_id   []u8
	partitions []i32
}

pub struct FetchRequestTopic {
pub:
	name       string
	topic_id   []u8 // UUID for v13+
	partitions []FetchRequestPartition
}

pub struct FetchRequestPartition {
pub:
	partition           i32
	fetch_offset        i64
	partition_max_bytes i32
}

fn parse_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequest {
	// Kafka 공식 스키마 (FetchRequest.json) 기준:
	// - v0-v14: ReplicaId가 첫 번째 필드
	// - v15+ (KIP-903): ReplicaId 제거, ReplicaState가 tagged field(tag=1)로 이동
	//                   Body가 MaxWaitMs로 시작!

	mut replica_id := i32(-1) // Default: consumer
	if version < 15 {
		// v0-v14: replica_id는 첫 번째 필드
		replica_id = reader.read_i32()!
	}
	// v15+: replica_id는 tagged field에서 읽음 (파싱 끝에서 처리)

	max_wait_ms := reader.read_i32()! // v15+에서 첫 번째 필드!
	min_bytes := reader.read_i32()!

	mut max_bytes := i32(0x7fffffff)
	if version >= 3 {
		max_bytes = reader.read_i32()!
	}

	mut isolation_level := i8(0)
	if version >= 4 {
		isolation_level = reader.read_i8()!
	}

	// session_id and session_epoch for version >= 7
	if version >= 7 {
		_ = reader.read_i32()! // session_id
		_ = reader.read_i32()! // session_epoch
	}

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut topics := []FetchRequestTopic{}
	for _ in 0 .. topic_count {
		// v13+: topic_id (UUID) instead of name
		mut name := ''
		mut topic_id := []u8{}
		if version >= 13 {
			topic_id = reader.read_uuid()! // UUID is 16 bytes
			// We need to look up the topic name by topic_id
			// For now, we'll store the topic_id and handle it in the handler
			name = '' // Empty name, will use topic_id
		} else if is_flexible {
			name = reader.read_compact_string()!
		} else {
			name = reader.read_string()!
		}

		partition_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		mut partitions := []FetchRequestPartition{}
		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			// current_leader_epoch for version >= 9
			if version >= 9 {
				_ = reader.read_i32()!
			}

			fetch_offset := reader.read_i64()!

			// last_fetched_epoch for version >= 12
			if version >= 12 {
				_ = reader.read_i32()!
			}

			// log_start_offset for version >= 5
			if version >= 5 {
				_ = reader.read_i64()!
			}

			partition_max_bytes := reader.read_i32()!

			partitions << FetchRequestPartition{
				partition:           partition
				fetch_offset:        fetch_offset
				partition_max_bytes: partition_max_bytes
			}

			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topics << FetchRequestTopic{
			name:       name
			topic_id:   topic_id
			partitions: partitions
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	// v7+: forgotten_topics_data
	mut forgotten_topics_data := []FetchRequestForgottenTopic{}
	if version >= 7 {
		forgotten_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		for _ in 0 .. forgotten_count {
			mut forgotten_name := ''
			mut forgotten_topic_id := []u8{}
			if version >= 13 {
				forgotten_topic_id = reader.read_uuid()! // topic_id UUID
			} else if is_flexible {
				forgotten_name = reader.read_compact_string()! // topic name
			} else {
				forgotten_name = reader.read_string()!
			}
			// partitions array
			forgotten_partitions_count := if is_flexible {
				reader.read_compact_array_len()!
			} else {
				reader.read_array_len()!
			}
			mut forgotten_partitions := []i32{}
			for _ in 0 .. forgotten_partitions_count {
				forgotten_partitions << reader.read_i32()! // partition
			}

			forgotten_topics_data << FetchRequestForgottenTopic{
				name:       forgotten_name
				topic_id:   forgotten_topic_id
				partitions: forgotten_partitions
			}

			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
	}

	// v11+: rack_id
	if version >= 11 {
		if is_flexible {
			_ = reader.read_compact_string()!
		} else {
			_ = reader.read_string()!
		}
	}

	// v15+ (KIP-903): ReplicaState in tagged field (tag=1)
	// Parse tagged fields to extract replica_id if present
	if version >= 15 && is_flexible {
		num_tags := reader.read_uvarint() or { 0 }
		for _ in 0 .. num_tags {
			tag := reader.read_uvarint() or { break }
			size := reader.read_uvarint() or { break }
			if tag == 1 {
				// ReplicaState: { ReplicaId: INT32, ReplicaEpoch: INT64 }
				replica_id = reader.read_i32() or { -1 }
				_ = reader.read_i64() or { -1 } // replica_epoch (unused for now)
			} else {
				// Skip unknown tags
				if reader.remaining() >= int(size) {
					reader.pos += int(size)
				}
			}
		}
	} else if is_flexible {
		reader.skip_tagged_fields() or {}
	}

	return FetchRequest{
		replica_id:            replica_id
		max_wait_ms:           max_wait_ms
		min_bytes:             min_bytes
		max_bytes:             max_bytes
		isolation_level:       isolation_level
		topics:                topics
		forgotten_topics_data: forgotten_topics_data
	}
}

// FindCoordinator Request
pub struct FindCoordinatorRequest {
pub:
	key              string
	key_type         i8
	coordinator_keys []string
}

fn parse_find_coordinator_request(mut reader BinaryReader, version i16, is_flexible bool) !FindCoordinatorRequest {
	mut key := ''
	mut coordinator_keys := []string{}
	mut key_type := i8(0) // 0 = GROUP, 1 = TRANSACTION

	// v0-v3: 'key' is the first field
	if version <= 3 {
		key = if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
	}

	if version >= 1 {
		key_type = reader.read_i8()!
	}

	if version >= 4 {
		// v4+: CoordinatorKeys array (optional)
		keys_len := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if keys_len > 0 {
			for _ in 0 .. keys_len {
				k := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				// Note: Array of strings (primitives) does not have tagged fields per element
				coordinator_keys << k
			}
		}
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return FindCoordinatorRequest{
		key:              key
		key_type:         key_type
		coordinator_keys: coordinator_keys
	}
}

// JoinGroup Request
pub struct JoinGroupRequest {
pub:
	group_id             string
	session_timeout_ms   i32
	rebalance_timeout_ms i32
	member_id            string
	group_instance_id    ?string
	protocol_type        string
	protocols            []JoinGroupRequestProtocol
}

pub struct JoinGroupRequestProtocol {
pub:
	name     string
	metadata []u8
}

fn parse_join_group_request(mut reader BinaryReader, version i16, is_flexible bool) !JoinGroupRequest {
	group_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	session_timeout_ms := reader.read_i32()!

	mut rebalance_timeout_ms := session_timeout_ms
	if version >= 1 {
		rebalance_timeout_ms = reader.read_i32()!
	}

	member_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	mut group_instance_id := ?string(none)
	if version >= 5 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}

	protocol_type := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	protocol_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut protocols := []JoinGroupRequestProtocol{}
	for _ in 0 .. protocol_count {
		name := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
		metadata := if is_flexible {
			reader.read_compact_bytes()!
		} else {
			reader.read_bytes()!
		}

		protocols << JoinGroupRequestProtocol{
			name:     name
			metadata: metadata
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	return JoinGroupRequest{
		group_id:             group_id
		session_timeout_ms:   session_timeout_ms
		rebalance_timeout_ms: rebalance_timeout_ms
		member_id:            member_id
		group_instance_id:    group_instance_id
		protocol_type:        protocol_type
		protocols:            protocols
	}
}

// Placeholder structs for other requests
pub struct SyncGroupRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
	protocol_type     ?string
	protocol_name     ?string
	assignments       []SyncGroupRequestAssignment
}

pub struct SyncGroupRequestAssignment {
pub:
	member_id  string
	assignment []u8
}

fn parse_sync_group_request(mut reader BinaryReader, version i16, is_flexible bool) !SyncGroupRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	generation_id := reader.read_i32()!
	member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }

	mut group_instance_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}

	mut protocol_type := ?string(none)
	mut protocol_name := ?string(none)
	if version >= 5 {
		if is_flexible {
			pt := reader.read_compact_string()!
			pn := reader.read_compact_string()!
			protocol_type = if pt.len > 0 { pt } else { none }
			protocol_name = if pn.len > 0 { pn } else { none }
		} else {
			pt := reader.read_nullable_string()!
			pn := reader.read_nullable_string()!
			protocol_type = if pt.len > 0 { pt } else { none }
			protocol_name = if pn.len > 0 { pn } else { none }
		}
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut assignments := []SyncGroupRequestAssignment{}
	for _ in 0 .. count {
		mid := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		assignment := if is_flexible { reader.read_compact_bytes()! } else { reader.read_bytes()! }
		assignments << SyncGroupRequestAssignment{
			member_id:  mid
			assignment: assignment
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	return SyncGroupRequest{
		group_id:          group_id
		generation_id:     generation_id
		member_id:         member_id
		group_instance_id: group_instance_id
		protocol_type:     protocol_type
		protocol_name:     protocol_name
		assignments:       assignments
	}
}

pub struct HeartbeatRequest {
pub:
	group_id      string
	generation_id i32
	member_id     string
}

fn parse_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !HeartbeatRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	generation_id := reader.read_i32()!
	member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	return HeartbeatRequest{
		group_id:      group_id
		generation_id: generation_id
		member_id:     member_id
	}
}

pub struct LeaveGroupRequest {
pub:
	group_id  string
	member_id string
}

fn parse_leave_group_request(mut reader BinaryReader, version i16, is_flexible bool) !LeaveGroupRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	member_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	return LeaveGroupRequest{
		group_id:  group_id
		member_id: member_id
	}
}

pub struct OffsetCommitRequest {
pub:
	group_id string
	topics   []OffsetCommitRequestTopic
}

pub struct OffsetCommitRequestTopic {
pub:
	name       string
	partitions []OffsetCommitRequestPartition
}

pub struct OffsetCommitRequestPartition {
pub:
	partition_index    i32
	committed_offset   i64
	committed_metadata string
}

fn parse_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetCommitRequest {
	group_id := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	// v1+: generation_id
	if version >= 1 {
		_ = reader.read_i32()!
	}
	// v1+: member_id
	if version >= 1 {
		_ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}
	// v7+: group_instance_id
	if version >= 7 {
		_ = if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
	}
	// v2-v4: retention_time_ms (deprecated, removed in v5)
	if version >= 2 && version <= 4 {
		_ = reader.read_i64()!
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []OffsetCommitRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		pcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut partitions := []OffsetCommitRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			co := reader.read_i64()!
			// v6+: committed_leader_epoch
			if version >= 6 {
				_ = reader.read_i32()!
			}
			// v1-v4: commit_timestamp (deprecated, removed in v5)
			if version >= 1 && version <= 4 {
				_ = reader.read_i64()!
			}
			cm := if is_flexible { reader.read_compact_nullable_string() or { '' } } else { reader.read_nullable_string() or {
					''} }
			partitions << OffsetCommitRequestPartition{
				partition_index:    pi
				committed_offset:   co
				committed_metadata: cm
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
		topics << OffsetCommitRequestTopic{
			name:       name
			partitions: partitions
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	return OffsetCommitRequest{
		group_id: group_id
		topics:   topics
	}
}

pub struct OffsetFetchRequest {
pub:
	group_id       string
	topics         []OffsetFetchRequestTopic
	groups         []OffsetFetchRequestGroup
	require_stable bool
}

pub struct OffsetFetchRequestTopic {
pub:
	name       string
	partitions []i32
}

pub struct OffsetFetchRequestGroup {
pub:
	group_id     string
	member_id    ?string
	member_epoch i32
	topics       []OffsetFetchRequestGroupTopic
}

pub struct OffsetFetchRequestGroupTopic {
pub:
	name       string
	partitions []i32
}

fn parse_offset_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !OffsetFetchRequest {
	mut group_id := ''
	mut topics := []OffsetFetchRequestTopic{}
	mut groups := []OffsetFetchRequestGroup{}
	mut require_stable := false

	if version <= 7 {
		group_id = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if count >= 0 {
			for _ in 0 .. count {
				name := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				pcount := if is_flexible {
					reader.read_compact_array_len()!
				} else {
					reader.read_array_len()!
				}
				mut partitions := []i32{}
				for _ in 0 .. pcount {
					partitions << reader.read_i32()!
				}
				topics << OffsetFetchRequestTopic{
					name:       name
					partitions: partitions
				}
				if is_flexible {
					reader.skip_tagged_fields()!
				}
			}
		}
	} else {
		gcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		if gcount >= 0 {
			for _ in 0 .. gcount {
				gid := if is_flexible {
					reader.read_compact_string()!
				} else {
					reader.read_string()!
				}
				mut member_id := ?string(none)
				mut member_epoch := i32(-1)
				if version >= 9 {
					if is_flexible {
						mid := reader.read_compact_nullable_string()!
						member_id = if mid.len > 0 { mid } else { none }
					} else {
						mid := reader.read_nullable_string()!
						member_id = if mid.len > 0 { mid } else { none }
					}
					member_epoch = reader.read_i32()!
				}
				tcount := if is_flexible {
					reader.read_compact_array_len()!
				} else {
					reader.read_array_len()!
				}
				mut gtopics := []OffsetFetchRequestGroupTopic{}
				if tcount >= 0 {
					for _ in 0 .. tcount {
						name := if is_flexible {
							reader.read_compact_string()!
						} else {
							reader.read_string()!
						}
						pcount := if is_flexible {
							reader.read_compact_array_len()!
						} else {
							reader.read_array_len()!
						}
						mut partitions := []i32{}
						for _ in 0 .. pcount {
							partitions << reader.read_i32()!
						}
						gtopics << OffsetFetchRequestGroupTopic{
							name:       name
							partitions: partitions
						}
						if is_flexible {
							reader.skip_tagged_fields()!
						}
					}
				}
				groups << OffsetFetchRequestGroup{
					group_id:     gid
					member_id:    member_id
					member_epoch: member_epoch
					topics:       gtopics
				}
				if is_flexible {
					reader.skip_tagged_fields()!
				}
			}
		}
	}

	if version >= 7 {
		require_stable = reader.read_i8()! != 0
	}

	return OffsetFetchRequest{
		group_id:       group_id
		topics:         topics
		groups:         groups
		require_stable: require_stable
	}
}

pub struct ListOffsetsRequest {
pub:
	replica_id      i32
	isolation_level i8
	topics          []ListOffsetsRequestTopic
}

pub struct ListOffsetsRequestTopic {
pub:
	name       string
	partitions []ListOffsetsRequestPartition
}

pub struct ListOffsetsRequestPartition {
pub:
	partition_index i32
	timestamp       i64
}

fn parse_list_offsets_request(mut reader BinaryReader, version i16, is_flexible bool) !ListOffsetsRequest {
	replica_id := reader.read_i32()!
	mut isolation_level := i8(0)
	if version >= 2 {
		isolation_level = reader.read_i8()!
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []ListOffsetsRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		pcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut partitions := []ListOffsetsRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			if version >= 4 {
				_ = reader.read_i32()!
			}
			// current_leader_epoch
			ts := reader.read_i64()!
			partitions << ListOffsetsRequestPartition{
				partition_index: pi
				timestamp:       ts
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
		topics << ListOffsetsRequestTopic{
			name:       name
			partitions: partitions
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	return ListOffsetsRequest{
		replica_id:      replica_id
		isolation_level: isolation_level
		topics:          topics
	}
}

pub struct CreateTopicsRequest {
pub:
	topics        []CreateTopicsRequestTopic
	timeout_ms    i32
	validate_only bool // v4+
}

pub struct CreateTopicsRequestTopic {
pub:
	name               string
	num_partitions     i32
	replication_factor i16
	configs            map[string]string
}

fn parse_create_topics_request(mut reader BinaryReader, version i16, is_flexible bool) !CreateTopicsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []CreateTopicsRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		num_partitions := reader.read_i32()!
		replication_factor := reader.read_i16()!

		// Skip replica assignments
		acount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		for _ in 0 .. acount {
			_ = reader.read_i32()! // partition
			rcount := if is_flexible {
				reader.read_compact_array_len()!
			} else {
				reader.read_array_len()!
			}
			for _ in 0 .. rcount {
				_ = reader.read_i32()!
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		// Parse configs
		ccount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut configs := map[string]string{}
		for _ in 0 .. ccount {
			cname := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
			cvalue := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
			configs[cname] = cvalue
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topics << CreateTopicsRequestTopic{
			name:               name
			num_partitions:     num_partitions
			replication_factor: replication_factor
			configs:            configs
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	timeout_ms := reader.read_i32()!
	// v4+: validate_only field
	validate_only := if version >= 4 { reader.read_i8()! != 0 } else { false }
	return CreateTopicsRequest{
		topics:        topics
		timeout_ms:    timeout_ms
		validate_only: validate_only
	}
}

pub struct DeleteTopicsRequest {
pub:
	topics     []DeleteTopicsRequestTopic
	timeout_ms i32
}

pub struct DeleteTopicsRequestTopic {
pub:
	name     string // v0-v5, or empty for v6+
	topic_id []u8   // v6+: UUID (16 bytes)
}

fn parse_delete_topics_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteTopicsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []DeleteTopicsRequestTopic{}
	for _ in 0 .. count {
		mut name := ''
		mut topic_id := []u8{}

		if version >= 6 {
			// v6+: topics array contains { name: compact_nullable_string, topic_id: uuid }
			name = reader.read_compact_nullable_string() or { '' }
			topic_id = reader.read_uuid()!
		} else {
			// v0-v5: topics is just string array
			name = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		}

		topics << DeleteTopicsRequestTopic{
			name:     name
			topic_id: topic_id
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	timeout_ms := reader.read_i32()!
	return DeleteTopicsRequest{
		topics:     topics
		timeout_ms: timeout_ms
	}
}

pub struct ListGroupsRequest {
pub:
	states_filter []string
}

fn parse_list_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !ListGroupsRequest {
	mut states_filter := []string{}
	if version >= 4 {
		count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		for _ in 0 .. count {
			states_filter << if is_flexible {
				reader.read_compact_string()!
			} else {
				reader.read_string()!
			}
		}
	}
	return ListGroupsRequest{
		states_filter: states_filter
	}
}

pub struct DescribeGroupsRequest {
pub:
	groups                        []string
	include_authorized_operations bool
}

fn parse_describe_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeGroupsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut groups := []string{}
	for _ in 0 .. count {
		groups << if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}
	mut include_authorized_operations := false
	if version >= 3 {
		include_authorized_operations = reader.read_i8()! != 0
	}
	return DescribeGroupsRequest{
		groups:                        groups
		include_authorized_operations: include_authorized_operations
	}
}

// InitProducerId Request (API Key 22)
// Used by idempotent/transactional producers to obtain a producer ID
pub struct InitProducerIdRequest {
pub:
	transactional_id       ?string // Nullable - null for non-transactional producer
	transaction_timeout_ms i32     // Timeout for transactions
	producer_id            i64     // Existing producer ID or -1 for new
	producer_epoch         i16     // Existing epoch or -1 for new
}

fn parse_init_producer_id_request(mut reader BinaryReader, version i16, is_flexible bool) !InitProducerIdRequest {
	// transactional_id: NULLABLE_STRING (v0-v1) / COMPACT_NULLABLE_STRING (v2+)
	raw_transactional_id := if is_flexible {
		reader.read_compact_nullable_string()!
	} else {
		reader.read_nullable_string()!
	}
	// Convert empty string to none for optional type
	transactional_id := if raw_transactional_id.len > 0 {
		?string(raw_transactional_id)
	} else {
		?string(none)
	}

	// transaction_timeout_ms: INT32
	transaction_timeout_ms := reader.read_i32()!

	// producer_id and producer_epoch added in v3
	mut producer_id := i64(-1)
	mut producer_epoch := i16(-1)
	if version >= 3 {
		producer_id = reader.read_i64()!
		producer_epoch = reader.read_i16()!
	}

	return InitProducerIdRequest{
		transactional_id:       transactional_id
		transaction_timeout_ms: transaction_timeout_ms
		producer_id:            producer_id
		producer_epoch:         producer_epoch
	}
}

// ConsumerGroupHeartbeat Request (API Key 68) - KIP-848
// Used for the new Consumer Rebalance Protocol
pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	instance_id            ?string // Static membership (group.instance.id)
	rack_id                ?string
	rebalance_timeout_ms   i32
	subscribed_topic_names []string
	server_assignor        ?string
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition // Current assignment
}

pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

fn parse_consumer_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ConsumerGroupHeartbeatRequest {
	// ConsumerGroupHeartbeat is always flexible (v0+)

	// group_id: COMPACT_STRING
	group_id := reader.read_compact_string()!

	// member_id: COMPACT_STRING
	member_id := reader.read_compact_string()!

	// member_epoch: INT32
	member_epoch := reader.read_i32()!

	// instance_id: COMPACT_NULLABLE_STRING
	raw_instance_id := reader.read_compact_nullable_string()!
	instance_id := if raw_instance_id.len > 0 { ?string(raw_instance_id) } else { ?string(none) }

	// rack_id: COMPACT_NULLABLE_STRING
	raw_rack_id := reader.read_compact_nullable_string()!
	rack_id := if raw_rack_id.len > 0 { ?string(raw_rack_id) } else { ?string(none) }

	// rebalance_timeout_ms: INT32
	rebalance_timeout_ms := reader.read_i32()!

	// subscribed_topic_names: COMPACT_ARRAY[COMPACT_STRING]
	topic_count := reader.read_compact_array_len()!
	mut subscribed_topic_names := []string{}
	for _ in 0 .. topic_count {
		subscribed_topic_names << reader.read_compact_string()!
	}

	// server_assignor: COMPACT_NULLABLE_STRING
	raw_server_assignor := reader.read_compact_nullable_string()!
	server_assignor := if raw_server_assignor.len > 0 {
		?string(raw_server_assignor)
	} else {
		?string(none)
	}

	// topic_partitions: COMPACT_ARRAY[TopicPartition]
	tp_count := reader.read_compact_array_len()!
	mut topic_partitions := []ConsumerGroupHeartbeatTopicPartition{}
	for _ in 0 .. tp_count {
		// topic_id: UUID (16 bytes)
		topic_id := reader.read_uuid()!

		// partitions: COMPACT_ARRAY[INT32]
		part_count := reader.read_compact_array_len()!
		mut partitions := []i32{}
		for _ in 0 .. part_count {
			partitions << reader.read_i32()!
		}

		topic_partitions << ConsumerGroupHeartbeatTopicPartition{
			topic_id:   topic_id
			partitions: partitions
		}

		// Skip tagged fields for each topic partition
		reader.skip_tagged_fields()!
	}

	return ConsumerGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              member_id
		member_epoch:           member_epoch
		instance_id:            instance_id
		rack_id:                rack_id
		rebalance_timeout_ms:   rebalance_timeout_ms
		subscribed_topic_names: subscribed_topic_names
		server_assignor:        server_assignor
		topic_partitions:       topic_partitions
	}
}

// ============================================================================
// SaslHandshake Request (API Key 17)
// ============================================================================
// Used to negotiate SASL mechanism between client and broker
// v0: Basic mechanism negotiation
// v1: Adds mechanism enable/disable flags

pub struct SaslHandshakeRequest {
pub:
	mechanism string // The SASL mechanism chosen by the client
}

fn parse_sasl_handshake_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslHandshakeRequest {
	// SaslHandshake is never flexible (v0-v1 only)
	mechanism := reader.read_string()!

	return SaslHandshakeRequest{
		mechanism: mechanism
	}
}

// ============================================================================
// SaslAuthenticate Request (API Key 36)
// ============================================================================
// Used to perform SASL authentication after mechanism handshake
// v0: Basic authentication
// v1: Adds session lifetime
// v2: Flexible versions

pub struct SaslAuthenticateRequest {
pub:
	auth_bytes []u8 // The SASL authentication bytes from the client
}

fn parse_sasl_authenticate_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslAuthenticateRequest {
	auth_bytes := if is_flexible {
		reader.read_compact_bytes()!
	} else {
		reader.read_bytes()!
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return SaslAuthenticateRequest{
		auth_bytes: auth_bytes
	}
}

// ============================================================================
// DescribeCluster Request (API Key 48)
// ============================================================================

pub struct DescribeClusterRequest {
pub:
	include_cluster_authorized_operations bool
}

fn parse_describe_cluster_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeClusterRequest {
	// DescribeCluster is always flexible (v0+)
	include_cluster_authorized_operations := reader.read_i8()! != 0
	
	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return DescribeClusterRequest{
		include_cluster_authorized_operations: include_cluster_authorized_operations
	}
}

// ============================================================================
// DescribeConfigs Request (API Key 32)
// ============================================================================

pub struct DescribeConfigsRequest {
pub:
	resources        []DescribeConfigsResource
	include_synonyms bool
}

pub struct DescribeConfigsResource {
pub:
	resource_type i8
	resource_name string
	config_names  ?[]string
}

fn parse_describe_configs_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeConfigsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut resources := []DescribeConfigsResource{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		
		mut config_names := ?[]string(none)
		
		// For config_names
		n_count := if is_flexible { 
			reader.read_compact_array_len()! 
		} else { 
			reader.read_array_len()! 
		}

		if n_count >= 0 {
			mut names := []string{}
			for _ in 0 .. n_count {
				names << if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
			}
			config_names = names.clone()
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}

		resources << DescribeConfigsResource{
			resource_type: resource_type
			resource_name: resource_name
			config_names:  config_names
		}
	}

	mut include_synonyms := false
	if version >= 1 {
		include_synonyms = reader.read_i8()! != 0
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return DescribeConfigsRequest{
		resources:        resources
		include_synonyms: include_synonyms
	}
}
