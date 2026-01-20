// Adapter Layer - Kafka Offset Response Building
// OffsetCommit, OffsetFetch responses
module kafka

// ============================================================================
// OffsetCommit Response (API Key 8)
// ============================================================================

pub struct OffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetCommitResponseTopic
}

pub struct OffsetCommitResponseTopic {
pub:
	name       string
	partitions []OffsetCommitResponsePartition
}

pub struct OffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

pub fn (r OffsetCommitResponse) encode(version i16) []u8 {
	is_flexible := version >= 8
	mut writer := new_writer()

	if version >= 3 {
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
// OffsetFetch Response (API Key 9)
// ============================================================================

pub struct OffsetFetchResponse {
pub:
	throttle_time_ms i32
	topics           []OffsetFetchResponseTopic
	error_code       i16
	groups           []OffsetFetchResponseGroup
}

pub struct OffsetFetchResponseTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

pub struct OffsetFetchResponsePartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     ?string
	error_code             i16
}

pub struct OffsetFetchResponseGroup {
pub:
	group_id   string
	topics     []OffsetFetchResponseGroupTopic
	error_code i16
}

pub struct OffsetFetchResponseGroupTopic {
pub:
	name       string
	partitions []OffsetFetchResponsePartition
}

pub fn (r OffsetFetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 3 {
		writer.write_i32(r.throttle_time_ms)
	}

	if version >= 8 {
		if is_flexible {
			writer.write_compact_array_len(r.groups.len)
		} else {
			writer.write_array_len(r.groups.len)
		}
		for g in r.groups {
			if is_flexible {
				writer.write_compact_string(g.group_id)
				writer.write_compact_array_len(g.topics.len)
			} else {
				writer.write_string(g.group_id)
				writer.write_array_len(g.topics.len)
			}
			for t in g.topics {
				if is_flexible {
					writer.write_compact_string(t.name)
					writer.write_compact_array_len(t.partitions.len)
				} else {
					writer.write_string(t.name)
					writer.write_array_len(t.partitions.len)
				}
				for p in t.partitions {
					writer.write_i32(p.partition_index)
					writer.write_i64(p.committed_offset)
					writer.write_i32(p.committed_leader_epoch)
					if is_flexible {
						writer.write_compact_nullable_string(p.committed_metadata)
					} else {
						writer.write_nullable_string(p.committed_metadata)
					}
					writer.write_i16(p.error_code)
					if is_flexible {
						writer.write_tagged_fields()
					}
				}
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			writer.write_i16(g.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	} else {
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

			if is_flexible {
				writer.write_compact_array_len(t.partitions.len)
			} else {
				writer.write_array_len(t.partitions.len)
			}
			for p in t.partitions {
				writer.write_i32(p.partition_index)
				writer.write_i64(p.committed_offset)
				if version >= 5 {
					writer.write_i32(p.committed_leader_epoch)
				}
				if is_flexible {
					writer.write_compact_nullable_string(p.committed_metadata)
				} else {
					writer.write_nullable_string(p.committed_metadata)
				}
				writer.write_i16(p.error_code)
				if is_flexible {
					writer.write_tagged_fields()
				}
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
		if version >= 2 {
			writer.write_i16(r.error_code)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
