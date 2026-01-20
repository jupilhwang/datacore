// Adapter Layer - Kafka Produce/Fetch Response Building
// Produce, Fetch, ListOffsets responses
module kafka

// ============================================================================
// Produce Response (API Key 0)
// ============================================================================

pub struct ProduceResponse {
pub:
	topics           []ProduceResponseTopic
	throttle_time_ms i32
}

pub struct ProduceResponseTopic {
pub:
	name       string
	topic_id   []u8 // v13+ UUID
	partitions []ProduceResponsePartition
}

pub struct ProduceResponsePartition {
pub:
	index            i32
	error_code       i16
	base_offset      i64
	log_append_time  i64
	log_start_offset i64
}

pub fn (r ProduceResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.base_offset)
			if version >= 2 {
				writer.write_i64(p.log_append_time)
			}
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// v8+: record_errors (empty array for success)
			if version >= 8 {
				if is_flexible {
					writer.write_compact_array_len(0) // No record errors
				} else {
					writer.write_array_len(0)
				}
			}
			// v8+: error_message (null for success)
			if version >= 8 {
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
	}

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// Fetch Response (API Key 1)
// ============================================================================

pub struct FetchResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	session_id       i32
	topics           []FetchResponseTopic
}

pub struct FetchResponseTopic {
pub:
	name       string
	topic_id   []u8 // UUID for v13+
	partitions []FetchResponsePartition
}

pub struct FetchResponsePartition {
pub:
	partition_index    i32
	error_code         i16
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
	records            []u8
}

pub fn (r FetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 12
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	if version >= 7 {
		writer.write_i16(r.error_code)
		writer.write_i32(r.session_id)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		// v13+: topic_id (UUID) instead of name
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.high_watermark)
			if version >= 4 {
				writer.write_i64(p.last_stable_offset)
			}
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// v12+: diverging_epoch, current_leader, snapshot_id are optional tagged fields
			// When None, they are omitted entirely. We don't support them yet, so skip.
			// Aborted transactions (v4+) - empty array or null
			if version >= 4 {
				if is_flexible {
					// COMPACT_NULLABLE_ARRAY: 0 = null, 1 = empty array (len 0)
					writer.write_uvarint(1) // empty array (length 0 + 1)
				} else {
					writer.write_array_len(0)
				}
			}
			// v11+: preferred_read_replica
			if version >= 11 {
				writer.write_i32(-1) // No preferred replica
			}
			// Records - RECORDS type (not COMPACT_BYTES!)
			// RECORDS type uses i32 length prefix in non-flexible versions
			// In flexible versions (v12+), it uses COMPACT_RECORDS (varint length + 1)
			if is_flexible {
				writer.write_compact_bytes(p.records)
			} else {
				// -1 = null (no records), 0 = empty records, N = N bytes of record data
				if p.records.len == 0 {
					// Write length = 0 for empty records (not -1 for null)
					writer.write_i32(0)
				} else {
					writer.write_i32(i32(p.records.len))
					writer.write_raw(p.records)
				}
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

// ============================================================================
// ListOffsets Response (API Key 2)
// ============================================================================

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
	leader_epoch    i32 // v4+
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
			// v4+: leader_epoch
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
