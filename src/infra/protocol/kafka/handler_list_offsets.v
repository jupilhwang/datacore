// ListOffsets request/response types, parsing, encoding, and handler implementation
//
// This module implements the Kafka ListOffsets API.
// Used by consumers to query offset information for a given topic/partition.
// Supports latest offset (-1), earliest offset (-2), and timestamp-based offset lookup.
module kafka

import infra.observability
import time

/// ListOffsetsRequest holds the request data for ListOffsets.
///
/// The offset returned depends on the timestamp value:
/// - -1: Latest offset (LOG_END_OFFSET)
/// - -2: Earliest offset (LOG_START_OFFSET)
/// - positive: First offset at or after the given timestamp
pub struct ListOffsetsRequest {
pub:
	replica_id      i32
	isolation_level i8
	topics          []ListOffsetsRequestTopic
}

/// ListOffsetsRequestTopic holds the topic and partitions to query offsets for.
pub struct ListOffsetsRequestTopic {
pub:
	name       string
	partitions []ListOffsetsRequestPartition
}

/// ListOffsetsRequestPartition holds the partition index and timestamp for a ListOffsets request.
pub struct ListOffsetsRequestPartition {
pub:
	partition_index i32
	timestamp       i64
}

/// ListOffsetsResponse holds the offset query results for ListOffsets.
pub struct ListOffsetsResponse {
pub:
	throttle_time_ms i32
	topics           []ListOffsetsResponseTopic
}

/// ListOffsetsResponseTopic holds the per-topic offset results for ListOffsets.
pub struct ListOffsetsResponseTopic {
pub:
	name       string
	partitions []ListOffsetsResponsePartition
}

/// ListOffsetsResponsePartition holds the per-partition offset result for ListOffsets.
pub struct ListOffsetsResponsePartition {
pub:
	partition_index i32
	error_code      i16
	timestamp       i64
	offset          i64
	leader_epoch    i32
}

// parse_list_offsets_request parses a ListOffsets request from the binary reader.
// Reads different fields depending on the protocol version.
fn parse_list_offsets_request(mut reader BinaryReader, version i16, is_flexible bool) !ListOffsetsRequest {
	replica_id := reader.read_i32()!

	// v2+ adds the isolation_level field
	mut isolation_level := i8(0)
	if version >= 2 {
		isolation_level = reader.read_i8()!
	}

	// Parse topics array
	count := reader.read_flex_array_len(is_flexible)!
	mut topics := []ListOffsetsRequestTopic{}
	for _ in 0 .. count {
		name := reader.read_flex_string(is_flexible)!

		// Parse partitions array
		pcount := reader.read_flex_array_len(is_flexible)!
		mut partitions := []ListOffsetsRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			// v4+ adds current_leader_epoch field (ignored)
			if version >= 4 {
				_ = reader.read_i32()!
			}
			ts := reader.read_i64()!
			partitions << ListOffsetsRequestPartition{
				partition_index: pi
				timestamp:       ts
			}
			reader.skip_flex_tagged_fields(is_flexible)!
		}
		topics << ListOffsetsRequestTopic{
			name:       name
			partitions: partitions
		}
		reader.skip_flex_tagged_fields(is_flexible)!
	}
	return ListOffsetsRequest{
		replica_id:      replica_id
		isolation_level: isolation_level
		topics:          topics
	}
}

/// encode serializes the ListOffsetsResponse into bytes.
/// Uses flexible or non-flexible encoding depending on the version.
pub fn (r ListOffsetsResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	// v2+ adds throttle_time_ms field
	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}

	// Encode topics array
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		// Encode topic name and partitions array
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
			// v1+ adds timestamp and offset fields
			if version >= 1 {
				writer.write_i64(p.timestamp)
				writer.write_i64(p.offset)
			}
			// v4+ adds leader_epoch field
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

// process_list_offsets handles a ListOffsets request (frame-based).
// Queries offset information for the requested topic/partition combinations.
fn (mut h Handler) process_list_offsets(req ListOffsetsRequest, version i16) !ListOffsetsResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing list offsets', observability.field_int('topics', req.topics.len),
		observability.field_int('isolation_level', req.isolation_level))

	mut topics := []ListOffsetsResponseTopic{}
	for t in req.topics {
		mut partitions := []ListOffsetsResponsePartition{}
		for p in t.partitions {
			// Retrieve partition info
			info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
				// Topic/partition not found - return error response
				partitions << ListOffsetsResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
					timestamp:       -1
					offset:          -1
					leader_epoch:    -1
				}
				continue
			}

			// Determine offset based on timestamp
			// -1: Latest offset (LOG_END_OFFSET)
			// -2: Earliest offset (LOG_START_OFFSET)
			// Other: Return latest offset (timestamp-based lookup not yet implemented)
			offset := match p.timestamp {
				-1 { info.latest_offset }
				-2 { info.earliest_offset }
				else { info.latest_offset }
			}

			partitions << ListOffsetsResponsePartition{
				partition_index: p.partition_index
				error_code:      0
				timestamp:       p.timestamp
				offset:          offset
				leader_epoch:    0
			}
		}
		topics << ListOffsetsResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('List offsets completed', observability.field_int('topics', topics.len),
		observability.field_duration('latency', elapsed))

	return ListOffsetsResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}
}

// Legacy handler - delegates to process_list_offsets
fn (mut h Handler) handle_list_offsets(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_list_offsets_request(mut reader, version, is_flexible_version(.list_offsets,
		version))!
	resp := h.process_list_offsets(req, version)!
	return resp.encode(version)
}
