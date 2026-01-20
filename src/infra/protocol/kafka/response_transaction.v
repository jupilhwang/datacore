// Adapter Layer - Kafka Transaction Response Building
// InitProducerId, AddPartitionsToTxn, EndTxn, AddOffsetsToTxn, TxnOffsetCommit responses
module kafka

// ============================================================================
// InitProducerId Response (API Key 22)
// ============================================================================
// Returns a producer ID for idempotent/transactional producers

pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32 // Throttle time in milliseconds
	error_code       i16 // Error code (0 = success)
	producer_id      i64 // Assigned producer ID
	producer_epoch   i16 // Producer epoch
}

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

// ============================================================================
// AddPartitionsToTxn Response (API Key 24)
// ============================================================================

pub struct AddPartitionsToTxnResponse {
pub:
	throttle_time_ms i32
	results          []AddPartitionsToTxnResult
}

pub struct AddPartitionsToTxnResult {
pub:
	name       string
	partitions []AddPartitionsToTxnPartitionResult
}

pub struct AddPartitionsToTxnPartitionResult {
pub:
	partition_index i32
	error_code      i16
}

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

// ============================================================================
// AddOffsetsToTxn Response (API Key 25)
// ============================================================================

pub struct AddOffsetsToTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

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

// ============================================================================
// EndTxn Response (API Key 26)
// ============================================================================

pub struct EndTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

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

// ============================================================================
// TxnOffsetCommit Response (API Key 28)
// ============================================================================

pub struct TxnOffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []TxnOffsetCommitResponseTopic
}

pub struct TxnOffsetCommitResponseTopic {
pub:
	name       string
	partitions []TxnOffsetCommitResponsePartition
}

pub struct TxnOffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

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
