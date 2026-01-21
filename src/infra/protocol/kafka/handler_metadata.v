// Kafka Protocol - Metadata (API Key 3)
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain
import infra.observability
import time

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

	topic_count := reader.read_flex_array_len(is_flexible)!

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
	}

	if is_flexible {
		writer.write_compact_array_len(r.brokers.len)
	} else {
		writer.write_array_len(r.brokers.len)
	}

	for b in r.brokers {
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
	return resp_bytes
}

fn (mut h Handler) handle_metadata(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_metadata_request(mut reader, version, is_flexible_version(.metadata,
		version))!
	resp := h.process_metadata(req, version)!
	return resp.encode(version)
}

fn (mut h Handler) process_metadata(req MetadataRequest, version i16) !MetadataResponse {
	start_time := time.now()

	h.logger.debug('Processing metadata request',
		observability.field_int('topics', req.topics.len),
		observability.field_bool('allow_auto_create', req.allow_auto_topic_creation))

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

	elapsed := time.since(start_time)
	h.logger.debug('Metadata request completed',
		observability.field_int('brokers', brokers.len),
		observability.field_int('topics', resp_topics.len),
		observability.field_duration('latency', elapsed))

	return MetadataResponse{
		throttle_time_ms:       0
		brokers:                brokers
		cluster_id:             h.cluster_id
		controller_id:          controller_id
		topics:                 resp_topics
		cluster_authorized_ops: -2147483648
	}
}
