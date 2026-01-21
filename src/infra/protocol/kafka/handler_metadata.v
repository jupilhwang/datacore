// Kafka Protocol - ApiVersions, Metadata, FindCoordinator, DescribeCluster, DescribeConfigs
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain

// ============================================================================
// ApiVersions (API Key 18)
// ============================================================================

pub struct ApiVersionsRequest {
pub:
	client_software_name    string
	client_software_version string
}

pub struct ApiVersionsResponse {
pub:
	error_code               i16
	api_versions             []ApiVersionsResponseKey
	throttle_time_ms         i32
	supported_features       []ApiVersionsSupportedFeature
	finalized_features_epoch i64
	finalized_features       []ApiVersionsFinalizedFeature
	zk_migration_ready       bool
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

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

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
		error_code:               0
		api_versions:             api_versions
		throttle_time_ms:         0
		supported_features:       []
		finalized_features_epoch: -1
		finalized_features:       []
		zk_migration_ready:       false
	}
}

fn (h Handler) handle_api_versions(version i16) []u8 {
	safe_version := if version > 3 { i16(3) } else { version }
	resp := new_api_versions_response()
	return resp.encode(safe_version)
}

// ============================================================================
// Metadata (API Key 3)
// ============================================================================

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

pub struct MetadataResponse {
pub:
	throttle_time_ms       i32
	brokers                []MetadataResponseBroker
	cluster_id             ?string
	controller_id          i32
	topics                 []MetadataResponseTopic
	cluster_authorized_ops i32
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
	topic_id             []u8
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
	offline_replicas []i32
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
	if version >= 8 && version <= 10 {
		include_cluster_authorized_ops = reader.read_i8()! != 0
		include_topic_authorized_ops = reader.read_i8()! != 0
	} else if version >= 11 {
		include_topic_authorized_ops = reader.read_i8()! != 0
	}

	return MetadataRequest{
		topics:                         topics
		allow_auto_topic_creation:      allow_auto_topic_creation
		include_cluster_authorized_ops: include_cluster_authorized_ops
		include_topic_authorized_ops:   include_topic_authorized_ops
	}
}

pub fn (r MetadataResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
		eprintln('[DEBUG] MetadataResponse.encode: after throttle_time_ms, len=${writer.data.len} hex=${writer.data.hex()}')
	}

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
			eprintln('[DEBUG] MetadataResponse: encoded broker node_id=${b.node_id} host_len=${b.host.len} port=${b.port}')
			writer.write_tagged_fields()
		}
	}

	if version >= 2 {
		if is_flexible {
			writer.write_compact_nullable_string(r.cluster_id)
		} else {
			writer.write_nullable_string(r.cluster_id)
		}
	}

	if version >= 1 {
		writer.write_i32(r.controller_id)
	}

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

		if version >= 10 {
			writer.write_uuid(t.topic_id)
		}

		if version >= 1 {
			writer.write_i8(if t.is_internal { i8(1) } else { i8(0) })
		}

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

			if is_flexible {
				writer.write_compact_array_len(p.replica_nodes.len)
			} else {
				writer.write_array_len(p.replica_nodes.len)
			}
			for n in p.replica_nodes {
				writer.write_i32(n)
			}

			if is_flexible {
				writer.write_compact_array_len(p.isr_nodes.len)
			} else {
				writer.write_array_len(p.isr_nodes.len)
			}
			for n in p.isr_nodes {
				writer.write_i32(n)
			}

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

	if version >= 8 && version <= 10 {
		writer.write_i32(r.cluster_authorized_ops)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	resp_bytes := writer.bytes()
	eprintln('[DEBUG] MetadataResponse.encode version=${version} flexible=${is_flexible} len=${resp_bytes.len} body_hex=${resp_bytes.hex()}')
	return resp_bytes
}

fn (mut h Handler) handle_metadata(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_metadata_request(mut reader, version, is_flexible_version(.metadata,
		version))!
	resp := h.process_metadata(req, version)!
	eprintln('[DEBUG] handle_metadata: Building response with broker node_id=${h.broker_id} host="${h.host}" (len=${h.host.len}) port=${h.broker_port}')
	return resp.encode(version)
}

fn (mut h Handler) process_metadata(req MetadataRequest, version i16) !MetadataResponse {
	mut resp_topics := []MetadataResponseTopic{}

	// Get active brokers for multi-broker mode
	mut brokers := []MetadataResponseBroker{}
	mut active_broker_ids := []i32{}

	if mut registry := h.broker_registry {
		// Multi-broker mode: get all active brokers from registry
		active_brokers := registry.list_active_brokers() or { []domain.BrokerInfo{} }
		for broker in active_brokers {
			brokers << MetadataResponseBroker{
				node_id: broker.broker_id
				host:    broker.host
				port:    broker.port
				rack:    if broker.rack.len > 0 { broker.rack } else { none }
			}
			active_broker_ids << broker.broker_id
		}
	}

	// Fallback to single broker if no brokers found or single-broker mode
	if brokers.len == 0 {
		brokers << MetadataResponseBroker{
			node_id: h.broker_id
			host:    h.host
			port:    h.broker_port
			rack:    none
		}
		active_broker_ids << h.broker_id
	}

	if req.topics.len > 0 {
		for req_topic in req.topics {
			topic_name := req_topic.name or { '' }
			if topic_name.len == 0 {
				continue
			}

			topic := h.storage.get_topic(topic_name) or {
				if req.allow_auto_topic_creation {
					h.storage.create_topic(topic_name, 1, domain.TopicConfig{}) or {
						resp_topics << MetadataResponseTopic{
							error_code:           i16(ErrorCode.unknown_topic_or_partition)
							name:                 topic_name
							topic_id:             []u8{len: 16}
							is_internal:          false
							partitions:           []
							topic_authorized_ops: -2147483648
						}
						continue
					}
					h.storage.get_topic(topic_name) or {
						resp_topics << MetadataResponseTopic{
							error_code:           i16(ErrorCode.unknown_topic_or_partition)
							name:                 topic_name
							topic_id:             []u8{len: 16}
							is_internal:          false
							partitions:           []
							topic_authorized_ops: -2147483648
						}
						continue
					}
				} else {
					resp_topics << MetadataResponseTopic{
						error_code:           i16(ErrorCode.unknown_topic_or_partition)
						name:                 topic_name
						topic_id:             []u8{len: 16}
						is_internal:          false
						partitions:           []
						topic_authorized_ops: -2147483648
					}
					continue
				}
			}

			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				// In stateless multi-broker mode, any broker can serve any partition
				// Use round-robin or hash-based assignment for load balancing
				leader_id := if active_broker_ids.len > 1 {
					active_broker_ids[p % active_broker_ids.len]
				} else {
					h.broker_id
				}
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  i32(p)
					leader_id:        leader_id
					leader_epoch:     0
					replica_nodes:    active_broker_ids.clone()
					isr_nodes:        active_broker_ids.clone()
					offline_replicas: []
				}
			}
			resp_topics << MetadataResponseTopic{
				error_code:           0
				name:                 topic.name
				topic_id:             topic.topic_id
				is_internal:          topic.is_internal
				partitions:           partitions
				topic_authorized_ops: -2147483648
			}
		}
	} else {
		topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }

		for topic in topic_list {
			mut partitions := []MetadataResponsePartition{}
			for p in 0 .. topic.partition_count {
				// In stateless multi-broker mode, any broker can serve any partition
				leader_id := if active_broker_ids.len > 1 {
					active_broker_ids[p % active_broker_ids.len]
				} else {
					h.broker_id
				}
				partitions << MetadataResponsePartition{
					error_code:       0
					partition_index:  i32(p)
					leader_id:        leader_id
					leader_epoch:     0
					replica_nodes:    active_broker_ids.clone()
					isr_nodes:        active_broker_ids.clone()
					offline_replicas: []
				}
			}
			resp_topics << MetadataResponseTopic{
				error_code:           0
				name:                 topic.name
				topic_id:             topic.topic_id
				is_internal:          topic.is_internal
				partitions:           partitions
				topic_authorized_ops: -2147483648
			}
		}
	}

	// Controller is the first active broker in multi-broker mode, or self in single-broker mode
	controller_id := if active_broker_ids.len > 0 { active_broker_ids[0] } else { h.broker_id }

	return MetadataResponse{
		throttle_time_ms:       0
		brokers:                brokers
		cluster_id:             h.cluster_id
		controller_id:          controller_id
		topics:                 resp_topics
		cluster_authorized_ops: -2147483648
	}
}

// ============================================================================
// FindCoordinator (API Key 10)
// ============================================================================

pub struct FindCoordinatorRequest {
pub:
	key              string
	key_type         i8
	coordinator_keys []string
}

pub struct FindCoordinatorResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    ?string
	node_id          i32
	host             string
	port             i32
	coordinators     []FindCoordinatorResponseNode
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

fn parse_find_coordinator_request(mut reader BinaryReader, version i16, is_flexible bool) !FindCoordinatorRequest {
	mut key := ''
	mut coordinator_keys := []string{}
	mut key_type := i8(0)

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

		writer.write_i32(r.node_id)
		if is_flexible {
			writer.write_compact_string(r.host)
		} else {
			writer.write_string(r.host)
		}
		writer.write_i32(r.port)
	} else {
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

fn (h Handler) handle_find_coordinator(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator,
		version))!
	resp := h.process_find_coordinator(req, version)!
	return resp.encode(version)
}

fn (h Handler) process_find_coordinator(req FindCoordinatorRequest, version i16) !FindCoordinatorResponse {
	if version >= 4 {
		mut keys := req.coordinator_keys.clone()
		if keys.len == 0 && req.key.len > 0 {
			keys << req.key
		}

		mut coordinators := []FindCoordinatorResponseNode{}
		for key in keys {
			coordinators << FindCoordinatorResponseNode{
				key:           key
				node_id:       h.broker_id
				host:          h.host
				port:          h.broker_port
				error_code:    0
				error_message: none
			}
		}

		return FindCoordinatorResponse{
			throttle_time_ms: 0
			coordinators:     coordinators
		}
	}

	return FindCoordinatorResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    none
		node_id:          h.broker_id
		host:             h.host
		port:             h.broker_port
		coordinators:     []FindCoordinatorResponseNode{}
	}
}

// ============================================================================
// DescribeCluster (API Key 60)
// ============================================================================

pub struct DescribeClusterRequest {
pub:
	include_cluster_authorized_operations bool
}

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

fn parse_describe_cluster_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeClusterRequest {
	include_cluster_authorized_operations := reader.read_i8()! != 0

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return DescribeClusterRequest{
		include_cluster_authorized_operations: include_cluster_authorized_operations
	}
}

pub fn (r DescribeClusterResponse) encode(version i16) []u8 {
	is_flexible := version >= 0
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
		writer.write_compact_string(r.cluster_id)
	} else {
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

fn (mut h Handler) handle_describe_cluster(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_describe_cluster_request(mut reader, version, is_flexible_version(.describe_cluster,
		version))!
	resp := h.process_describe_cluster(req, version)!
	return resp.encode(version)
}

fn (mut h Handler) process_describe_cluster(req DescribeClusterRequest, version i16) !DescribeClusterResponse {
	mut brokers := []DescribeClusterBroker{}
	mut controller_id := h.broker_id

	if mut registry := h.broker_registry {
		// Multi-broker mode: get all active brokers from registry
		active_brokers := registry.list_active_brokers() or { []domain.BrokerInfo{} }
		for broker in active_brokers {
			brokers << DescribeClusterBroker{
				broker_id: broker.broker_id
				host:      broker.host
				port:      broker.port
				rack:      if broker.rack.len > 0 { broker.rack } else { none }
			}
		}
		// Controller is the first active broker
		if active_brokers.len > 0 {
			controller_id = active_brokers[0].broker_id
		}
	}

	// Fallback to single broker if no brokers found
	if brokers.len == 0 {
		brokers << DescribeClusterBroker{
			broker_id: h.broker_id
			host:      h.host
			port:      h.broker_port
			rack:      none
		}
	}

	return DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    0
		error_message:                 none
		cluster_id:                    h.cluster_id
		controller_id:                 controller_id
		brokers:                       brokers
		cluster_authorized_operations: -2147483648
	}
}
