// Kafka protocol - Share Group State encoder (KIP-932)
// InitializeShareGroupState (API Key 83), ReadShareGroupState (API Key 84),
// WriteShareGroupState (API Key 85), DeleteShareGroupState (API Key 86)
// Response encoding methods.
module kafka

/// encode encodes InitializeShareGroupStateResponse to bytes.
pub fn (r InitializeShareGroupStateResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// Results array
	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}
	for result in r.results {
		// Partition
		writer.write_i32(result.partition)
		// ErrorCode
		writer.write_i16(result.error_code)
		// ErrorMessage (nullable)
		if is_flexible {
			if result.error_message.len > 0 {
				writer.write_compact_string(result.error_message)
			} else {
				writer.write_compact_nullable_string(none)
			}
		} else {
			writer.write_nullable_string(result.error_message)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// Tagged fields
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode encodes ReadShareGroupStateResponse to bytes.
pub fn (r ReadShareGroupStateResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// Results array (per-topic)
	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}
	for topic_result in r.results {
		// TopicId (UUID)
		writer.write_uuid(topic_result.topic_id)

		// Partitions array
		if is_flexible {
			writer.write_compact_array_len(topic_result.partitions.len)
		} else {
			writer.write_array_len(topic_result.partitions.len)
		}
		for part_result in topic_result.partitions {
			// Partition
			writer.write_i32(part_result.partition)
			// StateEpoch
			writer.write_i32(part_result.state_epoch)
			// StartOffset
			writer.write_i64(part_result.start_offset)
			// ErrorCode
			writer.write_i16(part_result.error_code)
			// ErrorMessage (nullable)
			if is_flexible {
				if part_result.error_message.len > 0 {
					writer.write_compact_string(part_result.error_message)
				} else {
					writer.write_compact_nullable_string(none)
				}
			} else {
				writer.write_nullable_string(part_result.error_message)
			}
			// StateBatches array
			if is_flexible {
				writer.write_compact_array_len(part_result.state_batches.len)
			} else {
				writer.write_array_len(part_result.state_batches.len)
			}
			for batch in part_result.state_batches {
				writer.write_i64(batch.first_offset)
				writer.write_i64(batch.last_offset)
				writer.write_i8(batch.delivery_state)
				writer.write_i16(batch.delivery_count)
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
	}

	// Tagged fields
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode encodes WriteShareGroupStateResponse to bytes.
pub fn (r WriteShareGroupStateResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// Results array
	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}
	for result in r.results {
		// Partition
		writer.write_i32(result.partition)
		// ErrorCode
		writer.write_i16(result.error_code)
		// ErrorMessage (nullable)
		if is_flexible {
			if result.error_message.len > 0 {
				writer.write_compact_string(result.error_message)
			} else {
				writer.write_compact_nullable_string(none)
			}
		} else {
			writer.write_nullable_string(result.error_message)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// Tagged fields
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// encode encodes DeleteShareGroupStateResponse to bytes.
pub fn (r DeleteShareGroupStateResponse) encode(version i16) []u8 {
	is_flexible := true
	mut writer := new_writer()

	// ThrottleTimeMs
	writer.write_i32(r.throttle_time_ms)

	// Results array
	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}
	for result in r.results {
		// Partition
		writer.write_i32(result.partition)
		// ErrorCode
		writer.write_i16(result.error_code)
		// ErrorMessage (nullable)
		if is_flexible {
			if result.error_message.len > 0 {
				writer.write_compact_string(result.error_message)
			} else {
				writer.write_compact_nullable_string(none)
			}
		} else {
			writer.write_nullable_string(result.error_message)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	// Tagged fields
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
