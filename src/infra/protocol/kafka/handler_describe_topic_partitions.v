// Kafka protocol - DescribeTopicPartitions (API Key 75)
// Request/response types, parsing, encoding, and handlers
module kafka

import infra.observability
import time

// DescribeTopicPartitions (API Key 75, flexible version 0+)

/// DescribeTopicPartitionsRequest holds the request data for DescribeTopicPartitions (API Key 75).
pub struct DescribeTopicPartitionsRequest {
pub:
	topics                   []DescribeTopicPartitionsTopicReq
	response_partition_limit i32 = 2000
	cursor                   ?DescribeTopicPartitionsCursor
}

/// DescribeTopicPartitionsTopicReq holds a single topic name in the request.
pub struct DescribeTopicPartitionsTopicReq {
pub:
	name string
}

/// DescribeTopicPartitionsCursor holds the cursor for pagination.
pub struct DescribeTopicPartitionsCursor {
pub:
	topic_name      string
	partition_index i32
}

/// DescribeTopicPartitionsResponse holds the response data for DescribeTopicPartitions.
pub struct DescribeTopicPartitionsResponse {
pub:
	throttle_time_ms i32
	topics           []DescribeTopicPartitionsTopicResp
	next_cursor      ?DescribeTopicPartitionsCursor
}

/// DescribeTopicPartitionsTopicResp holds per-topic data in the response.
pub struct DescribeTopicPartitionsTopicResp {
pub:
	error_code                  i16
	name                        ?string
	topic_id                    []u8 // UUID, 16 bytes
	is_internal                 bool
	partitions                  []DescribeTopicPartitionsPartition
	topic_authorized_operations i32 = -2147483648
}

/// DescribeTopicPartitionsPartition holds per-partition data in the response.
pub struct DescribeTopicPartitionsPartition {
pub:
	error_code               i16
	partition_index          i32
	leader_id                i32
	leader_epoch             i32 = -1
	replica_nodes            []i32
	isr_nodes                []i32
	eligible_leader_replicas ?[]i32
	last_known_elr           ?[]i32
	offline_replicas         []i32
}

fn parse_describe_topic_partitions_request(mut reader BinaryReader, version i16) !DescribeTopicPartitionsRequest {
	// DescribeTopicPartitions is always flexible (version 0+)
	topics_count := reader.read_compact_array_len()!
	mut topics := []DescribeTopicPartitionsTopicReq{}

	for _ in 0 .. topics_count {
		name := reader.read_compact_string()!
		reader.skip_tagged_fields()!
		topics << DescribeTopicPartitionsTopicReq{
			name: name
		}
	}

	response_partition_limit := reader.read_i32()!

	// Cursor (nullable struct): Kafka nullable struct 컨벤션
	// -1 (0xFF) = null, 1 (0x01) = present + 필드
	cursor_indicator := reader.read_i8()!
	mut cursor := ?DescribeTopicPartitionsCursor(none)
	if cursor_indicator >= 0 {
		topic_name := reader.read_compact_string()!
		partition_index := reader.read_i32()!
		reader.skip_tagged_fields()!
		cursor = DescribeTopicPartitionsCursor{
			topic_name:      topic_name
			partition_index: partition_index
		}
	}

	reader.skip_tagged_fields()!

	return DescribeTopicPartitionsRequest{
		topics:                   topics
		response_partition_limit: response_partition_limit
		cursor:                   cursor
	}
}

/// encode serializes the DescribeTopicPartitionsResponse into bytes.
pub fn (r DescribeTopicPartitionsResponse) encode(version i16) []u8 {
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	// topics array (compact)
	writer.write_compact_array_len(r.topics.len)

	for t in r.topics {
		writer.write_i16(t.error_code)
		// name (nullable compact string)
		writer.write_compact_nullable_string(t.name)
		// topic_id (UUID 16 bytes)
		writer.write_uuid(t.topic_id)
		// is_internal
		writer.write_i8(if t.is_internal { i8(1) } else { i8(0) })

		// partitions array (compact)
		writer.write_compact_array_len(t.partitions.len)
		for p in t.partitions {
			writer.write_i16(p.error_code)
			writer.write_i32(p.partition_index)
			writer.write_i32(p.leader_id)
			writer.write_i32(p.leader_epoch)
			// replica_nodes (compact array of int32)
			writer.write_compact_array_len(p.replica_nodes.len)
			for r_id in p.replica_nodes {
				writer.write_i32(r_id)
			}
			// isr_nodes
			writer.write_compact_array_len(p.isr_nodes.len)
			for isr in p.isr_nodes {
				writer.write_i32(isr)
			}
			// eligible_leader_replicas (nullable compact array)
			if elr := p.eligible_leader_replicas {
				writer.write_compact_array_len(elr.len)
				for e in elr {
					writer.write_i32(e)
				}
			} else {
				// null compact array: uvarint(0) = 0x00 (Kafka compact 프로토콜 표준)
				writer.write_compact_array_len(-1)
			}
			// last_known_elr (nullable compact array)
			if lke := p.last_known_elr {
				writer.write_compact_array_len(lke.len)
				for e in lke {
					writer.write_i32(e)
				}
			} else {
				// null compact array: uvarint(0) = 0x00 (Kafka compact 프로토콜 표준)
				writer.write_compact_array_len(-1)
			}
			// offline_replicas
			writer.write_compact_array_len(p.offline_replicas.len)
			for o in p.offline_replicas {
				writer.write_i32(o)
			}
			writer.write_tagged_fields()
		}

		writer.write_i32(t.topic_authorized_operations)
		writer.write_tagged_fields()
	}

	// next_cursor (nullable struct):
	// null → 1바이트 -1 (0xFF), present → 1바이트 +1 (0x01) + 필드
	if nc := r.next_cursor {
		writer.write_i8(1) // nullable struct present indicator: 1 (0x01)
		writer.write_compact_string(nc.topic_name)
		writer.write_i32(nc.partition_index)
		writer.write_tagged_fields()
	} else {
		writer.write_i8(-1) // nullable struct null indicator: -1 (0xFF)
	}

	writer.write_tagged_fields()

	return writer.bytes()
}

fn (mut h Handler) handle_describe_topic_partitions(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_describe_topic_partitions_request(mut reader, version)!

	h.logger.debug('Processing describe topic partitions', observability.field_int('topics',
		req.topics.len), observability.field_int('limit', req.response_partition_limit))

	resp := h.process_describe_topic_partitions(req, version)!

	elapsed := time.since(start_time)
	h.logger.debug('Describe topic partitions completed', observability.field_int('topics',
		resp.topics.len), observability.field_duration('latency', elapsed))

	return resp.encode(version)
}

fn (mut h Handler) process_describe_topic_partitions(req DescribeTopicPartitionsRequest, version i16) !DescribeTopicPartitionsResponse {
	_ = version
	mut resp_topics := []DescribeTopicPartitionsTopicResp{}
	mut total_partitions := 0

	for t in req.topics {
		// Check partition limit
		if total_partitions >= int(req.response_partition_limit) {
			break
		}

		topic_meta := h.storage.get_topic(t.name) or {
			// Topic not found
			resp_topics << DescribeTopicPartitionsTopicResp{
				error_code:  i16(ErrorCode.unknown_topic_or_partition)
				name:        t.name
				topic_id:    []u8{len: 16}
				is_internal: false
				partitions:  []
			}
			continue
		}

		mut partitions := []DescribeTopicPartitionsPartition{}

		// Determine partition range considering cursor and limit
		start_partition := if cursor := req.cursor {
			if cursor.topic_name == t.name {
				int(cursor.partition_index)
			} else {
				0
			}
		} else {
			0
		}

		for i in start_partition .. topic_meta.partition_count {
			if total_partitions >= int(req.response_partition_limit) {
				break
			}

			partitions << DescribeTopicPartitionsPartition{
				error_code:      0
				partition_index: i32(i)
				leader_id:       h.broker_id
				leader_epoch:    0
				replica_nodes:   [h.broker_id]
				isr_nodes:       [h.broker_id]
				// 빈 배열로 설정: Kafka Java 클라이언트가 null 대신 빈 리스트로 역직렬화하도록
				eligible_leader_replicas: []i32{}
				last_known_elr:           []i32{}
				offline_replicas:         []
			}
			total_partitions += 1
		}

		resp_topics << DescribeTopicPartitionsTopicResp{
			error_code:                  0
			name:                        topic_meta.name
			topic_id:                    topic_meta.topic_id
			is_internal:                 topic_meta.is_internal
			partitions:                  partitions
			topic_authorized_operations: -2147483648
		}
	}

	// Determine next cursor if we hit the partition limit
	mut next_cursor := ?DescribeTopicPartitionsCursor(none)
	if total_partitions >= int(req.response_partition_limit) {
		// Find the next partition to continue from
		if resp_topics.len > 0 {
			last_topic := resp_topics[resp_topics.len - 1]
			if last_topic.partitions.len > 0 {
				last_partition := last_topic.partitions[last_topic.partitions.len - 1]
				next_cursor = DescribeTopicPartitionsCursor{
					topic_name:      last_topic.name or { '' }
					partition_index: last_partition.partition_index + 1
				}
			}
		}
	}

	return DescribeTopicPartitionsResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           resp_topics
		next_cursor:      next_cursor
	}
}
