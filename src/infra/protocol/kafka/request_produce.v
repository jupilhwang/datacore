// Infra Layer - Kafka Request Parsing - Produce Operations
// Produce, Fetch, ListOffsets request parsing
module kafka

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
		mut name := ''
		mut topic_id := []u8{}

		// v13+: topic_id (UUID) instead of name
		if version >= 13 {
			topic_id = reader.read_uuid()!
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

			// v9+: current_leader_epoch (skip)
			if version >= 9 {
				_ = reader.read_i32()!
			}

			fetch_offset := reader.read_i64()!

			// v5+: log_start_offset (skip)
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
			mut fname := ''
			mut ftopic_id := []u8{}
			if version >= 13 {
				ftopic_id = reader.read_uuid()!
			} else if is_flexible {
				fname = reader.read_compact_string()!
			} else {
				fname = reader.read_string()!
			}
			fpartition_count := if is_flexible {
				reader.read_compact_array_len()!
			} else {
				reader.read_array_len()!
			}
			mut fpartitions := []i32{}
			for _ in 0 .. fpartition_count {
				fpartitions << reader.read_i32()!
			}
			forgotten_topics_data << FetchRequestForgottenTopic{
				name:       fname
				topic_id:   ftopic_id
				partitions: fpartitions
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
	}

	// v11+: rack_id (skip)
	if version >= 11 {
		_ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
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

// ListOffsets Request
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
