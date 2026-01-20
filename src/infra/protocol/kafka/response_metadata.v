// Adapter Layer - Kafka Metadata Response Building
// ApiVersions, Metadata, FindCoordinator, DescribeCluster responses
module kafka

// ============================================================================
// ApiVersions Response (API Key 18)
// ============================================================================
// Note: ApiVersions ALWAYS uses non-flexible response header (v0), even for v3+ body!
// v3+ 응답에는 supported_features, finalized_features 등 추가 필드가 필요합니다.
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

// ============================================================================
// Metadata Response (API Key 3)
// ============================================================================

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

// ============================================================================
// FindCoordinator Response (API Key 10)
// ============================================================================

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
