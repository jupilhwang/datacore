// Adapter Layer - Kafka Response Building
module kafka

// Response Header v0 (non-flexible)
// Used for: ApiVersions (always), non-flexible APIs
pub struct ResponseHeader {
pub:
	correlation_id i32
}

// Response Header v1 (flexible)
// Used for: flexible APIs
// Includes tag_buffer after correlation_id
pub struct ResponseHeaderV1 {
pub:
	correlation_id i32
	// tag_buffer is written separately (min 1 byte: 0x00 for empty)
}

// Build response with header (non-flexible, Response Header v0)
// Used for: ApiVersions (ALWAYS), SaslHandshake, and non-flexible API versions
// Note: NO tag_buffer in header!
pub fn build_response(correlation_id i32, body []u8) []u8 {
	mut writer := new_writer_with_capacity(4 + 4 + body.len)

	// Size (total length excluding size field itself)
	writer.write_i32(i32(4 + body.len))
	// Correlation ID
	writer.write_i32(correlation_id)
	// NO tag_buffer for non-flexible response header!
	// Body
	writer.write_raw(body)

	return writer.bytes()
}

// Build flexible response (Response Header v1, with tag_buffer)
// Used for: flexible API versions (except ApiVersions which is always non-flexible)
// Important: tag_buffer is minimum 1 byte (0x00 for empty tags)
pub fn build_flexible_response(correlation_id i32, body []u8) []u8 {
	mut writer := new_writer_with_capacity(4 + 4 + 1 + body.len)

	// Size (total length excluding size field itself)
	// = correlation_id(4) + tag_buffer(1, minimum) + body
	writer.write_i32(i32(4 + 1 + body.len))
	// Correlation ID
	writer.write_i32(correlation_id)
	// Tag buffer (empty = 0x00, which means num_tags=0)
	writer.write_uvarint(0)
	// Body
	writer.write_raw(body)

	return writer.bytes()
}

// Build response with appropriate header based on API key and version
// This is the recommended function to use for building responses
pub fn build_response_auto(api_key ApiKey, api_version i16, correlation_id i32, body []u8) []u8 {
	response_header_version := get_response_header_version(api_key, api_version)
	if response_header_version >= 1 {
		return build_flexible_response(correlation_id, body)
	}
	return build_response(correlation_id, body)
}

// ApiVersions Response
// Note: ApiVersions ALWAYS uses non-flexible response header (v0), even for v3+ body!
// ⚠️ v3+ 응답에는 supported_features, finalized_features 등 추가 필드가 필요합니다.
// 이를 누락하면 클라이언트가 뒤에 더 읽을 바이트가 있다고 가정하여 파싱이 깨질 수 있습니다.
pub struct ApiVersionsResponse {
pub:
	error_code       i16
	api_versions     []ApiVersionsResponseKey
	throttle_time_ms i32
	// v3+ fields (KRaft features)
	supported_features       []ApiVersionsSupportedFeature // v3+
	finalized_features_epoch i64 // v3+, -1 if not initialized
	finalized_features       []ApiVersionsFinalizedFeature // v3+
	zk_migration_ready       bool // v3.4+ (optional)
}

pub struct ApiVersionsResponseKey {
pub:
	api_key     i16
	min_version i16
	max_version i16
}

pub struct ApiVersionsSupportedFeature {
pub:
	name        string
	min_version i16
	max_version i16
}

pub struct ApiVersionsFinalizedFeature {
pub:
	name              string
	max_version_level i16
	min_version_level i16
}

pub fn (r ApiVersionsResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_array_len(r.api_versions.len)
	} else {
		writer.write_array_len(r.api_versions.len)
	}

	for v in r.api_versions {
		writer.write_i16(v.api_key)
		writer.write_i16(v.min_version)
		writer.write_i16(v.max_version)
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	// v3+ KRaft feature fields
	// Note: supported_features and finalized_features are likely Tagged Fields in v3
	// Since we don't support them yet, we just send an empty tag buffer.
	// Writing them as explicit fields (CompactArray) caused parsing errors if they are indeed Tagged Fields.
	if is_flexible {
		// Just write empty tag buffer (0)
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// Create default ApiVersions response
// v3+에서는 feature 필드들을 빈 배열/기본값으로 설정 (KRaft 미지원 시)
pub fn new_api_versions_response() ApiVersionsResponse {
	supported := get_supported_api_versions()
	mut api_versions := []ApiVersionsResponseKey{}

	for v in supported {
		api_versions << ApiVersionsResponseKey{
			api_key:     i16(v.api_key)
			min_version: v.min_version
			max_version: v.max_version
		}
	}

	return ApiVersionsResponse{
		error_code:       0
		api_versions:     api_versions
		throttle_time_ms: 0
		// v3+ fields: empty arrays and default values
		supported_features:       []    // No KRaft features supported yet
		finalized_features_epoch: -1    // Not initialized (-1 means not finalized)
		finalized_features:       []    // No finalized features
		zk_migration_ready:       false // Not in ZK migration mode
	}
}

// Metadata Response
pub struct MetadataResponse {
pub:
	throttle_time_ms       i32
	brokers                []MetadataResponseBroker
	cluster_id             ?string
	controller_id          i32
	topics                 []MetadataResponseTopic
	cluster_authorized_ops i32 // v8-v10 only
}

pub struct MetadataResponseBroker {
pub:
	node_id i32
	host    string
	port    i32
	rack    ?string
}

pub struct MetadataResponseTopic {
pub:
	error_code           i16
	name                 string
	topic_id             []u8 // UUID, 16 bytes (v10+)
	is_internal          bool
	partitions           []MetadataResponsePartition
	topic_authorized_ops i32
}

pub struct MetadataResponsePartition {
pub:
	error_code       i16
	partition_index  i32
	leader_id        i32
	leader_epoch     i32
	replica_nodes    []i32
	isr_nodes        []i32
	offline_replicas []i32 // v5+
}

pub fn (r MetadataResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
		eprintln('[DEBUG] MetadataResponse.encode: after throttle_time_ms, len=${writer.data.len} hex=${writer.data.hex()}')
	}

	// Brokers
	if is_flexible {
		writer.write_compact_array_len(r.brokers.len)
		eprintln('[DEBUG] MetadataResponse.encode: after brokers compact_array_len(${r.brokers.len}), len=${writer.data.len} hex=${writer.data.hex()}')
	} else {
		writer.write_array_len(r.brokers.len)
	}

	for b in r.brokers {
		eprintln('[DEBUG] MetadataResponse.encode: encoding broker node_id=${b.node_id} host="${b.host}" port=${b.port}')
		writer.write_i32(b.node_id)
		eprintln('[DEBUG] MetadataResponse.encode: after write_i32(node_id), len=${writer.data.len} hex=${writer.data[writer.data.len - 4..].hex()}')
		if is_flexible {
			writer.write_compact_string(b.host)
			eprintln('[DEBUG] MetadataResponse.encode: after write_compact_string("${b.host}"), len=${writer.data.len}')
		} else {
			writer.write_string(b.host)
		}
		writer.write_i32(b.port)
		eprintln('[DEBUG] MetadataResponse.encode: after write_i32(port), len=${writer.data.len}')
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(b.rack)
			} else {
				writer.write_nullable_string(b.rack)
			}
		}
		if is_flexible {
			// Debug: log per-broker encoding details to help trace wire format
			eprintln('[DEBUG] MetadataResponse: encoded broker node_id=${b.node_id} host_len=${b.host.len} port=${b.port}')
			writer.write_tagged_fields()
		}
	}

	// Cluster ID
	if version >= 2 {
		if is_flexible {
			writer.write_compact_nullable_string(r.cluster_id)
		} else {
			writer.write_nullable_string(r.cluster_id)
		}
	}

	// Controller ID
	if version >= 1 {
		writer.write_i32(r.controller_id)
	}

	// Topics
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		writer.write_i16(t.error_code)
		if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		// topic_id (UUID, 16 bytes) - v10+
		if version >= 10 {
			writer.write_uuid(t.topic_id)
		}

		if version >= 1 {
			writer.write_i8(if t.is_internal { i8(1) } else { i8(0) })
		}

		// Partitions
		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i16(p.error_code)
			writer.write_i32(p.partition_index)
			writer.write_i32(p.leader_id)
			if version >= 7 {
				writer.write_i32(p.leader_epoch)
			}

			// Replica nodes
			if is_flexible {
				writer.write_compact_array_len(p.replica_nodes.len)
			} else {
				writer.write_array_len(p.replica_nodes.len)
			}
			for n in p.replica_nodes {
				writer.write_i32(n)
			}

			// ISR nodes
			if is_flexible {
				writer.write_compact_array_len(p.isr_nodes.len)
			} else {
				writer.write_array_len(p.isr_nodes.len)
			}
			for n in p.isr_nodes {
				writer.write_i32(n)
			}

			// Offline replicas - v5+
			if version >= 5 {
				if is_flexible {
					writer.write_compact_array_len(p.offline_replicas.len)
				} else {
					writer.write_array_len(p.offline_replicas.len)
				}
				for n in p.offline_replicas {
					writer.write_i32(n)
				}
			}

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if version >= 8 {
			writer.write_i32(t.topic_authorized_ops)
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// v8-v10: cluster_authorized_operations (removed in v11, moved to DescribeCluster API)
	if version >= 8 && version <= 10 {
		writer.write_i32(r.cluster_authorized_ops)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}
	// Debug: print encoded metadata body to help diagnose client parsing errors
	resp_bytes := writer.bytes()
	// Print full body hex for deep debugging
	eprintln('[DEBUG] MetadataResponse.encode version=${version} flexible=${is_flexible} len=${resp_bytes.len} body_hex=${resp_bytes.hex()}')
	return resp_bytes
}

// Produce Response
pub struct ProduceResponse {
pub:
	topics           []ProduceResponseTopic
	throttle_time_ms i32
}

pub struct ProduceResponseTopic {
pub:
	name       string
	topic_id   []u8 // v13+ UUID
	partitions []ProduceResponsePartition
}

pub struct ProduceResponsePartition {
pub:
	index            i32
	error_code       i16
	base_offset      i64
	log_append_time  i64
	log_start_offset i64
}

pub fn (r ProduceResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.base_offset)
			if version >= 2 {
				writer.write_i64(p.log_append_time)
			}
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// v8+: record_errors (empty array for success)
			if version >= 8 {
				if is_flexible {
					writer.write_compact_array_len(0) // No record errors
				} else {
					writer.write_array_len(0)
				}
			}
			// v8+: error_message (null for success)
			if version >= 8 {
				if is_flexible {
					writer.write_compact_nullable_string(none)
				} else {
					writer.write_nullable_string(none)
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// Fetch Response
pub struct FetchResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	session_id       i32
	topics           []FetchResponseTopic
}

pub struct FetchResponseTopic {
pub:
	name       string
	topic_id   []u8 // UUID for v13+
	partitions []FetchResponsePartition
}

pub struct FetchResponsePartition {
pub:
	partition_index    i32
	error_code         i16
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
	records            []u8
}

pub fn (r FetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 12
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	if version >= 7 {
		writer.write_i16(r.error_code)
		writer.write_i32(r.session_id)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		// v13+: topic_id (UUID) instead of name
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.high_watermark)
			if version >= 4 {
				writer.write_i64(p.last_stable_offset)
			}
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// v12+: diverging_epoch, current_leader, snapshot_id are optional tagged fields
			// When None, they are omitted entirely. We don't support them yet, so skip.
			// Aborted transactions (v4+) - empty array or null
			if version >= 4 {
				if is_flexible {
					// COMPACT_NULLABLE_ARRAY: 0 = null, 1 = empty array (len 0)
					writer.write_uvarint(1) // empty array (length 0 + 1)
				} else {
					writer.write_array_len(0)
				}
			}
			// v11+: preferred_read_replica
			if version >= 11 {
				writer.write_i32(-1) // No preferred replica
			}
			// Records - RECORDS type (not COMPACT_BYTES!)
			// RECORDS type uses i32 length prefix in non-flexible versions
			// In flexible versions (v12+), it uses COMPACT_RECORDS (varint length + 1)
			if is_flexible {
				writer.write_compact_bytes(p.records)
			} else {
				// -1 = null (no records), 0 = empty records, N = N bytes of record data
				if p.records.len == 0 {
					// Write length = 0 for empty records (not -1 for null)
					writer.write_i32(0)
				} else {
					writer.write_i32(i32(p.records.len))
					writer.write_raw(p.records)
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// FindCoordinator Response
pub struct FindCoordinatorResponse {
pub:
	throttle_time_ms i32
	// v0-v3: top-level error and single coordinator
	error_code    i16
	error_message ?string
	node_id       i32
	host          string
	port          i32
	// v4+: coordinators array
	coordinators []FindCoordinatorResponseNode
}

pub struct FindCoordinatorResponseNode {
pub:
	key           string
	node_id       i32
	host          string
	port          i32
	error_code    i16
	error_message ?string
}

pub fn (r FindCoordinatorResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if version <= 3 {
		writer.write_i16(r.error_code)
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(r.error_message)
			} else {
				writer.write_nullable_string(r.error_message)
			}
		}

		// v0-v3: single coordinator fields
		writer.write_i32(r.node_id)
		if is_flexible {
			writer.write_compact_string(r.host)
		} else {
			writer.write_string(r.host)
		}
		writer.write_i32(r.port)
	} else {
		// v4+: coordinators array
		if is_flexible {
			writer.write_compact_array_len(r.coordinators.len)
		} else {
			writer.write_array_len(r.coordinators.len)
		}
		for node in r.coordinators {
			if is_flexible {
				writer.write_compact_string(node.key)
			} else {
				writer.write_string(node.key)
			}
			writer.write_i32(node.node_id)
			if is_flexible {
				writer.write_compact_string(node.host)
			} else {
				writer.write_string(node.host)
			}
			writer.write_i32(node.port)
			writer.write_i16(node.error_code)
			if is_flexible {
				writer.write_compact_nullable_string(node.error_message)
			} else {
				writer.write_nullable_string(node.error_message)
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// JoinGroup Response
pub struct JoinGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	generation_id    i32
	protocol_type    ?string
	protocol_name    ?string
	leader           string
	skip_assignment  bool
	member_id        string
	members          []JoinGroupResponseMember
}

pub struct JoinGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string // v5+
	metadata          []u8
}

pub fn (r JoinGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)
	writer.write_i32(r.generation_id)

	if version >= 7 {
		if is_flexible {
			writer.write_compact_nullable_string(r.protocol_type)
			writer.write_compact_nullable_string(r.protocol_name)
		} else {
			writer.write_nullable_string(r.protocol_type)
			writer.write_nullable_string(r.protocol_name)
		}
	} else {
		if is_flexible {
			writer.write_compact_string(r.protocol_name or { '' })
		} else {
			writer.write_string(r.protocol_name or { '' })
		}
	}

	if is_flexible {
		writer.write_compact_string(r.leader)
		if version >= 9 {
			writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
		}
		writer.write_compact_string(r.member_id)
		writer.write_compact_array_len(r.members.len)
	} else {
		writer.write_string(r.leader)
		if version >= 9 {
			writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
		}
		writer.write_string(r.member_id)
		writer.write_array_len(r.members.len)
	}

	for m in r.members {
		if is_flexible {
			writer.write_compact_string(m.member_id)
			// v5+: group_instance_id
			if version >= 5 {
				writer.write_compact_nullable_string(m.group_instance_id)
			}
			writer.write_compact_bytes(m.metadata)
			writer.write_tagged_fields()
		} else {
			writer.write_string(m.member_id)
			// v5+: group_instance_id
			if version >= 5 {
				writer.write_nullable_string(m.group_instance_id)
			}
			writer.write_bytes(m.metadata)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// SyncGroup Response
pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	protocol_type    ?string
	protocol_name    ?string
	assignment       []u8
}

pub fn (r SyncGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if version >= 5 {
		if is_flexible {
			writer.write_compact_nullable_string(r.protocol_type)
			writer.write_compact_nullable_string(r.protocol_name)
		} else {
			writer.write_nullable_string(r.protocol_type)
			writer.write_nullable_string(r.protocol_name)
		}
	}

	if is_flexible {
		writer.write_compact_bytes(r.assignment)
		writer.write_tagged_fields()
	} else {
		writer.write_bytes(r.assignment)
	}

	return writer.bytes()
}

// Heartbeat Response
pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

pub fn (r HeartbeatResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// LeaveGroup Response
pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	members          []LeaveGroupResponseMember
}

pub struct LeaveGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	error_code        i16
}

pub fn (r LeaveGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if version >= 3 {
		if is_flexible {
			writer.write_compact_array_len(r.members.len)
		} else {
			writer.write_array_len(r.members.len)
		}
		for m in r.members {
			if is_flexible {
				writer.write_compact_string(m.member_id)
				writer.write_compact_nullable_string(m.group_instance_id)
			} else {
				writer.write_string(m.member_id)
				writer.write_nullable_string(m.group_instance_id)
			}
			writer.write_i16(m.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// OffsetCommit Response
pub struct OffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetCommitResponseTopic
}

pub struct OffsetCommitResponseTopic {
pub:
	name       string
	partitions []OffsetCommitResponsePartition
}

pub struct OffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

pub fn (r OffsetCommitResponse) encode(version i16) []u8 {
	is_flexible := version >= 8
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_string(t.name)
			writer.write_array_len(t.partitions.len)
		}
		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// OffsetFetch Response
pub struct OffsetFetchResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetFetchResponseTopic
	error_code       i16
	groups           []OffsetFetchResponseGroup
}

pub struct OffsetFetchResponseTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

pub struct OffsetFetchResponsePartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     ?string
	error_code             i16
}

pub struct OffsetFetchResponseGroup {
pub:
	group_id   string
	topics     []OffsetFetchResponseGroupTopic
	error_code i16
}

pub struct OffsetFetchResponseGroupTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

pub fn (r OffsetFetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	if version >= 8 {
		if is_flexible {
			writer.write_compact_array_len(r.groups.len)
		} else {
			writer.write_array_len(r.groups.len)
		}
		for g in r.groups {
			if is_flexible {
				writer.write_compact_string(g.group_id)
				writer.write_compact_array_len(g.topics.len)
			} else {
				writer.write_string(g.group_id)
				writer.write_array_len(g.topics.len)
			}
			for t in g.topics {
				if is_flexible {
					writer.write_compact_string(t.name)
					writer.write_compact_array_len(t.partitions.len)
				} else {
					writer.write_string(t.name)
					writer.write_array_len(t.partitions.len)
				}
				for p in t.partitions {
					writer.write_i32(p.partition_index)
					writer.write_i64(p.committed_offset)
					writer.write_i32(p.committed_leader_epoch)
					if is_flexible {
						writer.write_compact_nullable_string(p.committed_metadata)
					} else {
						writer.write_nullable_string(p.committed_metadata)
					}
					writer.write_i16(p.error_code)
					if is_flexible {
						writer.write_tagged_fields()
					}
				}
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			writer.write_i16(g.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	} else {
		if is_flexible {
			writer.write_compact_array_len(r.topics.len)
		} else {
			writer.write_array_len(r.topics.len)
		}
		for t in r.topics {
			if is_flexible {
				writer.write_compact_string(t.name)
			} else {
				writer.write_string(t.name)
			}

			if is_flexible {
				writer.write_compact_array_len(t.partitions.len)
			} else {
				writer.write_array_len(t.partitions.len)
			}
			for p in t.partitions {
				writer.write_i32(p.partition_index)
				writer.write_i64(p.committed_offset)
				if version >= 5 {
					writer.write_i32(p.committed_leader_epoch)
				}
				if is_flexible {
					writer.write_compact_nullable_string(p.committed_metadata)
				} else {
					writer.write_nullable_string(p.committed_metadata)
				}
				writer.write_i16(p.error_code)
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if version >= 2 {
			writer.write_i16(r.error_code)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ListOffsets Response
pub struct ListOffsetsResponse {
pub:
	throttle_time_ms i32
	topics           []ListOffsetsResponseTopic
}

pub struct ListOffsetsResponseTopic {
pub:
	name       string
	partitions []ListOffsetsResponsePartition
}

pub struct ListOffsetsResponsePartition {
pub:
	partition_index i32
	error_code      i16
	timestamp       i64
	offset          i64
	leader_epoch    i32 // v4+
}

pub fn (r ListOffsetsResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_string(t.name)
			writer.write_array_len(t.partitions.len)
		}
		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			if version >= 1 {
				writer.write_i64(p.timestamp)
				writer.write_i64(p.offset)
			}
			// v4+: leader_epoch
			if version >= 4 {
				writer.write_i32(p.leader_epoch)
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// CreateTopics Response
pub struct CreateTopicsResponse {
pub:
	throttle_time_ms i32
	topics           []CreateTopicsResponseTopic
}

pub struct CreateTopicsResponseTopic {
pub:
	name               string
	topic_id           []u8 // UUID, 16 bytes (v7+)
	error_code         i16
	error_message      ?string
	num_partitions     i32
	replication_factor i16
}

pub fn (r CreateTopicsResponse) encode(version i16) []u8 {
	is_flexible := version >= 5
	mut writer := new_writer()

	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		// topic_id (UUID, 16 bytes) - v7+
		if version >= 7 {
			writer.write_uuid(t.topic_id)
		}

		writer.write_i16(t.error_code)
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(t.error_message)
			} else {
				writer.write_nullable_string(t.error_message)
			}
		}
		if version >= 5 {
			writer.write_i32(t.num_partitions)
			writer.write_i16(t.replication_factor)
			// configs array (empty)
			writer.write_compact_array_len(0)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// DeleteTopics Response
pub struct DeleteTopicsResponse {
pub:
	throttle_time_ms i32
	topics           []DeleteTopicsResponseTopic
}

pub struct DeleteTopicsResponseTopic {
pub:
	name       string
	topic_id   []u8 // v6+: UUID (16 bytes)
	error_code i16
}

pub fn (r DeleteTopicsResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}
		// v6+: topic_id (UUID, 16 bytes)
		if version >= 6 {
			writer.write_uuid(t.topic_id)
		}
		writer.write_i16(t.error_code)
		// v5+: error_message (nullable string, we send null for now)
		if version >= 5 {
			if is_flexible {
				writer.write_compact_nullable_string(none)
			} else {
				writer.write_nullable_string(none)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ListGroups Response
pub struct ListGroupsResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	groups           []ListGroupsResponseGroup
}

pub struct ListGroupsResponseGroup {
pub:
	group_id      string
	protocol_type string
	group_state   string
}

pub fn (r ListGroupsResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_array_len(r.groups.len)
	} else {
		writer.write_array_len(r.groups.len)
	}

	for g in r.groups {
		if is_flexible {
			writer.write_compact_string(g.group_id)
			writer.write_compact_string(g.protocol_type)
		} else {
			writer.write_string(g.group_id)
			writer.write_string(g.protocol_type)
		}
		if version >= 4 {
			if is_flexible {
				writer.write_compact_string(g.group_state)
			} else {
				writer.write_string(g.group_state)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// DescribeGroups Response
pub struct DescribeGroupsResponse {
pub:
	throttle_time_ms i32
	groups           []DescribeGroupsResponseGroup
}

pub struct DescribeGroupsResponseGroup {
pub:
	error_code    i16
	group_id      string
	group_state   string
	protocol_type string
	protocol_data string
	members       []DescribeGroupsResponseMember
}

pub struct DescribeGroupsResponseMember {
pub:
	member_id         string
	client_id         string
	client_host       string
	member_metadata   []u8
	member_assignment []u8
}

pub fn (r DescribeGroupsResponse) encode(version i16) []u8 {
	is_flexible := version >= 5
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.groups.len)
	} else {
		writer.write_array_len(r.groups.len)
	}

	for g in r.groups {
		writer.write_i16(g.error_code)
		if is_flexible {
			writer.write_compact_string(g.group_id)
			writer.write_compact_string(g.group_state)
			writer.write_compact_string(g.protocol_type)
			writer.write_compact_string(g.protocol_data)
			writer.write_compact_array_len(g.members.len)
		} else {
			writer.write_string(g.group_id)
			writer.write_string(g.group_state)
			writer.write_string(g.protocol_type)
			writer.write_string(g.protocol_data)
			writer.write_array_len(g.members.len)
		}

		for m in g.members {
			if is_flexible {
				writer.write_compact_string(m.member_id)
				writer.write_compact_string(m.client_id)
				writer.write_compact_string(m.client_host)
				writer.write_compact_bytes(m.member_metadata)
				writer.write_compact_bytes(m.member_assignment)
				writer.write_tagged_fields()
			} else {
				writer.write_string(m.member_id)
				writer.write_string(m.client_id)
				writer.write_string(m.client_host)
				writer.write_bytes(m.member_metadata)
				writer.write_bytes(m.member_assignment)
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// InitProducerId Response (API Key 22)
// Returns a producer ID for idempotent/transactional producers
pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32 // Throttle time in milliseconds
	error_code       i16 // Error code (0 = success)
	producer_id      i64 // Assigned producer ID
	producer_epoch   i16 // Producer epoch
}

pub fn (r InitProducerIdResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms: INT32 (v0+)
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16 (v0+)
	writer.write_i16(r.error_code)

	// producer_id: INT64 (v0+)
	writer.write_i64(r.producer_id)

	// producer_epoch: INT16 (v0+)
	writer.write_i16(r.producer_epoch)

	// Tagged fields for flexible versions
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ConsumerGroupHeartbeat Response (API Key 68) - KIP-848
// Response for the new Consumer Rebalance Protocol
pub struct ConsumerGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         ?string
	member_id             ?string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ConsumerGroupHeartbeatAssignment
}

pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition
}

pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

pub fn (r ConsumerGroupHeartbeatResponse) encode(version i16) []u8 {
	// ConsumerGroupHeartbeat is always flexible (v0+)
	mut writer := new_writer()

	// throttle_time_ms: INT32
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16
	writer.write_i16(r.error_code)

	// error_message: COMPACT_NULLABLE_STRING
	writer.write_compact_nullable_string(r.error_message)

	// member_id: COMPACT_NULLABLE_STRING
	writer.write_compact_nullable_string(r.member_id)

	// member_epoch: INT32
	writer.write_i32(r.member_epoch)

	// heartbeat_interval_ms: INT32
	writer.write_i32(r.heartbeat_interval_ms)

	// assignment: Assignment (nullable)
	if assignment := r.assignment {
		// Write topic_partitions array
		writer.write_compact_array_len(assignment.topic_partitions.len)

		for tp in assignment.topic_partitions {
			// topic_id: UUID (16 bytes)
			writer.write_uuid(tp.topic_id)

			// partitions: COMPACT_ARRAY[INT32]
			writer.write_compact_array_len(tp.partitions.len)
			for p in tp.partitions {
				writer.write_i32(p)
			}

			// Tagged fields for each topic partition
			writer.write_tagged_fields()
		}

		// Tagged fields for assignment
		writer.write_tagged_fields()
	} else {
		// Write -1 to indicate null assignment
		// For compact nullable structs, we use 0 to indicate null (length = 0 - 1 = -1)
		writer.write_uvarint(0)
	}

	// Tagged fields at the end
	writer.write_tagged_fields()

	return writer.bytes()
}

// ============================================================================
// SaslHandshake Response (API Key 17)
// ============================================================================
// Returns the list of SASL mechanisms supported by the broker

pub struct SaslHandshakeResponse {
pub:
	error_code i16      // Error code (0 = no error, 33 = unsupported mechanism)
	mechanisms []string // List of SASL mechanisms enabled by the broker
}

pub fn (r SaslHandshakeResponse) encode(version i16) []u8 {
	// SaslHandshake is never flexible (v0-v1 only)
	mut writer := new_writer()

	// error_code: INT16
	writer.write_i16(r.error_code)

	// mechanisms: ARRAY[STRING]
	writer.write_array_len(r.mechanisms.len)
	for m in r.mechanisms {
		writer.write_string(m)
	}

	return writer.bytes()
}

// ============================================================================
// SaslAuthenticate Response (API Key 36)
// ============================================================================
// Returns the result of SASL authentication

pub struct SaslAuthenticateResponse {
pub:
	error_code          i16     // Error code (0 = success, 58 = SASL_AUTHENTICATION_FAILED)
	error_message       ?string // Error message if authentication failed
	auth_bytes          []u8    // SASL authentication bytes from server (for multi-step)
	session_lifetime_ms i64     // v1+: Session lifetime in milliseconds (0 = no lifetime)
}

pub fn (r SaslAuthenticateResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// error_code: INT16
	writer.write_i16(r.error_code)

	// error_message: NULLABLE_STRING / COMPACT_NULLABLE_STRING
	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
	} else {
		writer.write_nullable_string(r.error_message)
	}

	// auth_bytes: BYTES / COMPACT_BYTES
	if is_flexible {
		writer.write_compact_bytes(r.auth_bytes)
	} else {
		writer.write_bytes(r.auth_bytes)
	}

	// session_lifetime_ms: INT64 (v1+)
	if version >= 1 {
		writer.write_i64(r.session_lifetime_ms)
	}

	// Tagged fields for flexible versions
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// DescribeCluster Response (API Key 48)
// ============================================================================

pub struct DescribeClusterResponse {
pub:
	throttle_time_ms              i32
	error_code                    i16
	error_message                 ?string
	cluster_id                    string
	controller_id                 i32
	brokers                       []DescribeClusterBroker
	cluster_authorized_operations i32
}

pub struct DescribeClusterBroker {
pub:
	broker_id i32
	host      string
	port      i32
	rack      ?string
}

pub fn (r DescribeClusterResponse) encode(version i16) []u8 {
	is_flexible := version >= 0 // DescribeCluster is always flexible (v0+)
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
		writer.write_compact_string(r.cluster_id)
	} else {
		// Should not happen as v0 is flexible
		writer.write_nullable_string(r.error_message)
		writer.write_string(r.cluster_id)
	}

	writer.write_i32(r.controller_id)

	if is_flexible {
		writer.write_compact_array_len(r.brokers.len)
	} else {
		writer.write_array_len(r.brokers.len)
	}

	for b in r.brokers {
		writer.write_i32(b.broker_id)
		if is_flexible {
			writer.write_compact_string(b.host)
		} else {
			writer.write_string(b.host)
		}
		writer.write_i32(b.port)
		if is_flexible {
			writer.write_compact_nullable_string(b.rack)
			writer.write_tagged_fields()
		} else {
			writer.write_nullable_string(b.rack)
		}
	}

	writer.write_i32(r.cluster_authorized_operations)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// DescribeConfigs Response (API Key 32)
// ============================================================================

pub struct DescribeConfigsResponse {
pub:
	throttle_time_ms i32
	results          []DescribeConfigsResult
}

pub struct DescribeConfigsResult {
pub:
	error_code    i16
	error_message ?string
	resource_type i8
	resource_name string
	configs       []DescribeConfigsEntry
}

pub struct DescribeConfigsEntry {
pub:
	name          string
	value         ?string
	read_only     bool
	is_default    bool // v0 (deprecated in v1+)
	config_source i8   // v1+ (replaces is_default)
	is_sensitive  bool
	synonyms      []DescribeConfigsSynonym // v1+
	config_type   i8                       // v3+
	documentation ?string                  // v3+
}

pub struct DescribeConfigsSynonym {
pub:
	name          string
	value         ?string
	config_source i8
}

pub fn (r DescribeConfigsResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		writer.write_i16(res.error_code)
		if is_flexible {
			writer.write_compact_nullable_string(res.error_message)
		} else {
			writer.write_nullable_string(res.error_message)
		}
		writer.write_i8(res.resource_type)
		if is_flexible {
			writer.write_compact_string(res.resource_name)
			writer.write_compact_array_len(res.configs.len)
		} else {
			writer.write_string(res.resource_name)
			writer.write_array_len(res.configs.len)
		}

		for c in res.configs {
			if is_flexible {
				writer.write_compact_string(c.name)
				writer.write_compact_nullable_string(c.value)
			} else {
				writer.write_string(c.name)
				writer.write_nullable_string(c.value)
			}
			writer.write_i8(if c.read_only { i8(1) } else { i8(0) })

			if version == 0 {
				writer.write_i8(if c.is_default { i8(1) } else { i8(0) })
			} else {
				// v1+ uses config_source instead of is_default
				writer.write_i8(c.config_source)
			}

			writer.write_i8(if c.is_sensitive { i8(1) } else { i8(0) })

			if version >= 1 {
				// synonyms
				if is_flexible {
					writer.write_compact_array_len(c.synonyms.len)
				} else {
					writer.write_array_len(c.synonyms.len)
				}
				for s in c.synonyms {
					if is_flexible {
						writer.write_compact_string(s.name)
						writer.write_compact_nullable_string(s.value)
					} else {
						writer.write_string(s.name)
						writer.write_nullable_string(s.value)
					}
					writer.write_i8(s.config_source)
					if is_flexible {
						writer.write_tagged_fields()
					}
				}
			}

			if version >= 3 {
				writer.write_i8(c.config_type)
				if is_flexible {
					writer.write_compact_nullable_string(c.documentation)
				} else {
					writer.write_nullable_string(c.documentation)
				}
			}

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
