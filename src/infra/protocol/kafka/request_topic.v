// Infra Layer - Kafka Request Parsing - Topic Operations
// CreateTopics, DeleteTopics request parsing
module kafka

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
