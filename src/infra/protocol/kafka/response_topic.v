// Adapter Layer - Kafka Topic Response Building
// CreateTopics, DeleteTopics responses
module kafka

// ============================================================================
// CreateTopics Response (API Key 19)
// ============================================================================

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

// ============================================================================
// DeleteTopics Response (API Key 20)
// ============================================================================

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
