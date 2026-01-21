// Kafka Protocol - ListOffsets (API Key 2)
// Request/Response types, parsing, encoding, and handlers
module kafka

// ============================================================================
// ListOffsets (API Key 2)
// ============================================================================

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
	leader_epoch    i32
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
				_ = reader.read_i32()! // current_leader_epoch
			}
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

// Process list offsets request (Frame-based)
fn (mut h Handler) process_list_offsets(req ListOffsetsRequest, version i16) !ListOffsetsResponse {
	mut topics := []ListOffsetsResponseTopic{}
	for t in req.topics {
		mut partitions := []ListOffsetsResponsePartition{}
		for p in t.partitions {
			info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
				partitions << ListOffsetsResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
					timestamp:       -1
					offset:          -1
					leader_epoch:    -1
				}
				continue
			}

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

	return ListOffsetsResponse{
		throttle_time_ms: 0
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
