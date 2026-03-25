// Metadata request/response types, parsing, encoding, and handler implementation
//
// This module implements the Kafka Metadata API.
// Used by clients to query broker information, topic metadata,
// and partition leader information from the cluster.
module kafka

import domain
import infra.observability
import time

/// MetadataRequest queries cluster and topic metadata.
///
/// If topics is empty, metadata for all topics is returned.
/// If allow_auto_topic_creation is true, non-existent topics are created automatically.
pub struct MetadataRequest {
pub:
	topics                         []MetadataRequestTopic
	allow_auto_topic_creation      bool
	include_cluster_authorized_ops bool
	include_topic_authorized_ops   bool
}

/// MetadataRequestTopic identifies a topic to query.
pub struct MetadataRequestTopic {
pub:
	topic_id []u8 // Topic UUID (v10+)
	name     ?string
}

/// MetadataResponse contains cluster and topic metadata.
pub struct MetadataResponse {
pub:
	throttle_time_ms       i32
	brokers                []MetadataResponseBroker
	cluster_id             ?string
	controller_id          i32
	topics                 []MetadataResponseTopic
	cluster_authorized_ops i32
}

/// MetadataResponseBroker holds broker information.
pub struct MetadataResponseBroker {
pub:
	node_id i32
	host    string
	port    i32
	rack    ?string
}

/// MetadataResponseTopic holds topic metadata.
pub struct MetadataResponseTopic {
pub:
	error_code           i16
	name                 string
	topic_id             []u8 // Topic UUID (v10+)
	is_internal          bool
	partitions           []MetadataResponsePartition
	topic_authorized_ops i32
}

/// MetadataResponsePartition holds partition metadata.
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

// parse_metadata_request parses a Metadata request.
// Reads different fields depending on the version to build the MetadataRequest struct.
fn parse_metadata_request(mut reader BinaryReader, version i16, is_flexible bool) !MetadataRequest {
	mut topics := []MetadataRequestTopic{}

	// Parse topics array
	topic_count := reader.read_flex_array_len(is_flexible)!

	// topic_count >= 0: query specific topics; -1: query all topics
	if topic_count >= 0 {
		for _ in 0 .. topic_count {
			// Topic UUID supported in v10+
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

			// Skip tagged fields in flexible versions
			if is_flexible && reader.remaining() > 0 {
				reader.skip_tagged_fields()!
			}

			topics << topic
		}
	}

	// allow_auto_topic_creation field added in v4+
	mut allow_auto_topic_creation := true
	if version >= 4 {
		allow_auto_topic_creation = reader.read_i8()! != 0
	}

	// v8–10: include_cluster_authorized_ops and include_topic_authorized_ops fields
	// v11+: only include_topic_authorized_ops exists
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

/// encode serializes the MetadataResponse to bytes.
/// Uses flexible or non-flexible format depending on the version.
pub fn (r MetadataResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	// throttle_time_ms field added in v3+
	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	encode_brokers(r.brokers, version, is_flexible, mut writer)

	// cluster_id field added in v2+
	if version >= 2 {
		if is_flexible {
			writer.write_compact_nullable_string(r.cluster_id)
		} else {
			writer.write_nullable_string(r.cluster_id)
		}
	}

	// controller_id field added in v1+
	if version >= 1 {
		writer.write_i32(r.controller_id)
	}

	// Encode topic array
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		encode_topic(t, version, is_flexible, mut writer)
	}

	// cluster_authorized_ops field added in v8-10
	if version >= 8 && version <= 10 {
		writer.write_i32(r.cluster_authorized_ops)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// Legacy handler — byte-array based request processing.
fn (mut h Handler) handle_metadata(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_metadata_request(mut reader, version, is_flexible_version(.metadata,
		version))!
	resp := h.process_metadata(req, version)!
	return resp.encode(version)
}

// process_metadata handles a Metadata request.
// Looks up broker information and metadata for the requested topics and builds the response.
fn (mut h Handler) process_metadata(req MetadataRequest, version i16) !MetadataResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing metadata request', observability.field_int('topics', req.topics.len),
		observability.field_bool('allow_auto_create', req.allow_auto_topic_creation))

	mut brokers := []MetadataResponseBroker{}
	mut active_broker_ids := []i32{}
	h.build_broker_list(mut brokers, mut active_broker_ids)

	// Pre-compute replica/isr lists once to avoid per-partition clone overhead
	replica_nodes := active_broker_ids.clone()
	isr_nodes := active_broker_ids.clone()

	mut resp_topics := []MetadataResponseTopic{}

	if req.topics.len > 0 {
		for req_topic in req.topics {
			topic_name := req_topic.name or { '' }
			if topic_name.len == 0 {
				continue
			}

			topic := h.resolve_or_create_topic(topic_name, req.allow_auto_topic_creation) or {
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

			resp_topics << h.build_topic_metadata(topic, active_broker_ids, replica_nodes,
				isr_nodes)
		}
	} else {
		topic_list := h.storage.list_topics() or { []domain.TopicMetadata{} }
		for topic in topic_list {
			resp_topics << h.build_topic_metadata(topic, active_broker_ids, replica_nodes,
				isr_nodes)
		}
	}

	controller_id := if active_broker_ids.len > 0 { active_broker_ids[0] } else { h.broker_id }

	elapsed := time.since(start_time)
	h.logger.debug('Metadata request completed', observability.field_int('brokers', brokers.len),
		observability.field_int('topics', resp_topics.len), observability.field_duration('latency',
		elapsed))

	return MetadataResponse{
		throttle_time_ms:       0
		brokers:                brokers
		cluster_id:             h.cluster_id
		controller_id:          controller_id
		topics:                 resp_topics
		cluster_authorized_ops: -2147483648
	}
}

// build_broker_list populates broker metadata from the registry or falls back to local broker.
fn (mut h Handler) build_broker_list(mut brokers []MetadataResponseBroker, mut active_broker_ids []i32) {
	if mut registry := h.broker_registry {
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

	if brokers.len == 0 {
		if h.broker_registry == none {
			h.logger.debug('Metadata response: broker_registry not available, returning local broker only',
				observability.field_int('broker_id', h.broker_id), observability.field_string('host',
				h.host), observability.field_int('port', h.broker_port))
		} else {
			h.logger.debug('Metadata response: broker_registry returned empty active brokers, returning local broker only',
				observability.field_int('broker_id', h.broker_id))
		}
		brokers << MetadataResponseBroker{
			node_id: h.broker_id
			host:    h.host
			port:    h.broker_port
			rack:    none
		}
		active_broker_ids << h.broker_id
	}
}

// resolve_or_create_topic looks up a topic by name, optionally auto-creating it.
fn (mut h Handler) resolve_or_create_topic(topic_name string, allow_auto_create bool) !domain.TopicMetadata {
	return h.storage.get_topic(topic_name) or {
		if allow_auto_create {
			h.storage.create_topic(topic_name, 1, domain.TopicConfig{}) or {
				return error('failed to auto-create topic ${topic_name}')
			}
			new_topic := h.storage.get_topic(topic_name) or {
				return error('topic ${topic_name} not found after auto-create')
			}
			return new_topic
		}
		return error('topic ${topic_name} not found')
	}
}

// build_topic_metadata constructs MetadataResponseTopic with partition info for a single topic.
fn (mut h Handler) build_topic_metadata(topic domain.TopicMetadata, active_broker_ids []i32, replica_nodes []i32, isr_nodes []i32) MetadataResponseTopic {
	mut partitions := []MetadataResponsePartition{}
	for p in 0 .. topic.partition_count {
		mut leader_id := h.broker_id
		if mut assigner := h.partition_assigner {
			assigned_leader := assigner.get_partition_leader(topic.name, i32(p)) or { h.broker_id }
			leader_id = assigned_leader
		} else if active_broker_ids.len > 1 {
			leader_id = active_broker_ids[p % active_broker_ids.len]
		}
		partitions << MetadataResponsePartition{
			error_code:       0
			partition_index:  i32(p)
			leader_id:        leader_id
			leader_epoch:     0
			replica_nodes:    replica_nodes.clone()
			isr_nodes:        isr_nodes.clone()
			offline_replicas: []
		}
	}
	return MetadataResponseTopic{
		error_code:           0
		name:                 topic.name
		topic_id:             topic.topic_id
		is_internal:          topic.is_internal
		partitions:           partitions
		topic_authorized_ops: -2147483648
	}
}

// encode_brokers writes broker metadata to the writer.
fn encode_brokers(brokers []MetadataResponseBroker, version i16, is_flexible bool, mut writer BinaryWriter) {
	if is_flexible {
		writer.write_compact_array_len(brokers.len)
	} else {
		writer.write_array_len(brokers.len)
	}

	for b in brokers {
		writer.write_i32(b.node_id)
		if is_flexible {
			writer.write_compact_string(b.host)
		} else {
			writer.write_string(b.host)
		}
		writer.write_i32(b.port)
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(b.rack)
			} else {
				writer.write_nullable_string(b.rack)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}
}

// encode_topic writes a single topic's metadata (including its partitions) to the writer.
fn encode_topic(t MetadataResponseTopic, version i16, is_flexible bool, mut writer BinaryWriter) {
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
		encode_partition(p, version, is_flexible, mut writer)
	}

	if version >= 8 {
		writer.write_i32(t.topic_authorized_ops)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}
}

// encode_partition writes a single partition's metadata to the writer.
fn encode_partition(p MetadataResponsePartition, version i16, is_flexible bool, mut writer BinaryWriter) {
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
