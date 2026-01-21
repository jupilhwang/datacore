// Kafka Protocol - Transaction Encoders
// Encode methods for transaction-related responses
module kafka

// encode encodes InitProducerIdResponse (API Key 22)
pub fn (r InitProducerIdResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms: INT32 (v0+)
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16 (v0+)
	writer.write_i16(r.error_code)

	// producer_id: INT64 (v0+)
	writer.write_i64(r.producer_id)

	// producer_epoch: INT16 (v0+)
	writer.write_i16(r.producer_epoch)

	// Tagged fields for flexible versions
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// encode encodes AddPartitionsToTxnResponse (API Key 24)
pub fn (r AddPartitionsToTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		if is_flexible {
			writer.write_compact_string(res.name)
			writer.write_compact_array_len(res.partitions.len)
		} else {
			writer.write_string(res.name)
			writer.write_array_len(res.partitions.len)
		}

		for p in res.partitions {
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

// encode encodes AddOffsetsToTxnResponse (API Key 25)
pub fn (r AddOffsetsToTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// encode encodes EndTxnResponse (API Key 26)
pub fn (r EndTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// encode encodes WriteTxnMarkersResponse (API Key 27)
pub fn (r WriteTxnMarkersResponse) encode(version i16) []u8 {
	// v1 is always flexible
	is_flexible := version >= 1
	mut writer := new_writer()

	if is_flexible {
		writer.write_compact_array_len(r.markers.len)
	} else {
		writer.write_array_len(r.markers.len)
	}

	for marker in r.markers {
		writer.write_i64(marker.producer_id)

		if is_flexible {
			writer.write_compact_array_len(marker.topics.len)
		} else {
			writer.write_array_len(marker.topics.len)
		}

		for t in marker.topics {
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
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// encode encodes TxnOffsetCommitResponse (API Key 28)
pub fn (r TxnOffsetCommitResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

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
