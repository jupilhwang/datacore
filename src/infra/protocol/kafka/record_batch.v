// Kafka protocol - RecordBatch encoding
// Provides RecordBatch format encoding and CRC32-C checksum calculation
module kafka

import domain
import infra.protocol.kafka.crc32c

/// varint_size calculates the encoded size of a varint.
/// Uses the same ZigZag encoding as write_varint in codec.v for correctness.
fn varint_size(val i64) int {
	// ZigZag encoding: (n << 1) ^ (n >> 63)
	// Cast to unsigned first to avoid signed-shift warnings
	v := (u64(val) << 1) ^ u64(val >> 63)
	mut size := 0
	mut value := v
	for {
		size++
		value >>= 7
		if value == 0 {
			break
		}
	}
	return size
}

/// calculate_record_size calculates the size of an encoded record without actually encoding it.
fn calculate_record_size(timestamp_delta i64, offset_delta i32, record &domain.Record) int {
	mut size := 0

	// attributes (1 byte)
	size += 1

	// timestamp_delta (varint)
	size += varint_size(timestamp_delta)

	// offset_delta (varint)
	size += varint_size(i64(offset_delta))

	// key length + key
	if record.key.len > 0 {
		size += varint_size(i64(record.key.len))
		size += record.key.len
	} else {
		size += varint_size(-1)
	}

	// value length + value
	if record.value.len > 0 {
		size += varint_size(i64(record.value.len))
		size += record.value.len
	} else {
		size += varint_size(-1)
	}

	// headers count (varint, 0 for no headers)
	size += varint_size(0)

	return size
}

// RecordBatch encoding

/// encode_record_batch_zerocopy encodes records into Kafka RecordBatch format.
/// RecordBatch format (v2):
/// - Base Offset: i64
/// - Batch Length: i32
/// - Partition Leader Epoch: i32
/// - Magic: i8 (2 for v2)
/// - CRC: u32 (CRC32-C of data after the CRC field)
/// - Attributes: i16
/// - Last Offset Delta: i32
/// - First Timestamp: i64
/// - Max Timestamp: i64
/// - Producer ID: i64
/// - Producer Epoch: i16
/// - Base Sequence: i32
/// - Records Count: i32
/// - Records: variable
pub fn encode_record_batch_zerocopy(records []domain.Record, base_offset i64) []u8 {
	if records.len == 0 {
		return []u8{}
	}

	// Use optimized encoding path with pre-allocated buffer
	estimated_size := estimate_batch_size(records)

	// RecordBatch header - use pre-allocated writer
	mut writer := new_writer_with_capacity(estimated_size)

	// Base offset (8 bytes)
	writer.write_i64(base_offset)

	// Batch length placeholder (4 bytes) - filled in later
	_ = writer.data.len
	writer.write_i32(0)

	// Partition leader epoch (4 bytes)
	writer.write_i32(-1)

	// Magic byte (1 byte) - version 2
	writer.write_i8(2)

	// CRC placeholder (4 bytes)
	crc_pos := writer.data.len
	writer.write_i32(0)

	// Attributes (2 bytes) - no compression, no timestamp type override
	writer.write_i16(0)

	// Last offset delta
	writer.write_i32(i32(records.len - 1))

	// First timestamp (use first record)
	first_timestamp := if records.len > 0 { records[0].timestamp.unix_milli() } else { i64(0) }
	writer.write_i64(first_timestamp)

	// Max timestamp (use last record)
	max_timestamp := if records.len > 0 {
		records[records.len - 1].timestamp.unix_milli()
	} else {
		first_timestamp
	}
	writer.write_i64(max_timestamp)

	// Producer ID (-1 for non-idempotent)
	writer.write_i64(-1)

	// Producer epoch (-1 for non-idempotent)
	writer.write_i16(-1)

	// Base sequence (-1 for non-idempotent)
	writer.write_i32(-1)

	// Record count
	writer.write_i32(i32(records.len))

	// Encode records
	for i, record in records {
		offset_delta := i32(i)
		timestamp_delta := record.timestamp.unix_milli() - first_timestamp

		// Calculate record size without full encoding
		record_size := calculate_record_size(timestamp_delta, offset_delta, record)

		// Write record only once
		writer.write_varint(i64(record_size))
		writer.write_i8(0)
		writer.write_varint(timestamp_delta)
		writer.write_varint(i64(offset_delta))

		// Key
		if record.key.len > 0 {
			writer.write_varint(i64(record.key.len))
			writer.write_raw(record.key)
		} else {
			writer.write_varint(-1)
		}

		// Value
		if record.value.len > 0 {
			writer.write_varint(i64(record.value.len))
			writer.write_raw(record.value)
		} else {
			writer.write_varint(-1)
		}

		// Headers (none)
		writer.write_varint(0)
	}

	// Obtain final data
	mut batch_data := writer.bytes()

	// Calculate and fill in batch length (total - base_offset - batch_length_field)
	batch_length := batch_data.len - 12
	batch_data[8] = u8(batch_length >> 24)
	batch_data[9] = u8(batch_length >> 16)
	batch_data[10] = u8(batch_length >> 8)
	batch_data[11] = u8(batch_length)

	// Calculate CRC32c of the batch (from attributes to end)
	crc := calculate_crc32c(batch_data[crc_pos + 4..])
	batch_data[crc_pos] = u8(crc >> 24)
	batch_data[crc_pos + 1] = u8(crc >> 16)
	batch_data[crc_pos + 2] = u8(crc >> 8)
	batch_data[crc_pos + 3] = u8(crc)

	return batch_data
}

/// estimate_batch_size estimates the size of an encoded record batch.
/// Used for pre-allocation optimization.
fn estimate_batch_size(records []domain.Record) int {
	// Base batch overhead: 61 bytes (header)
	mut size := 61

	for record in records {
		// Per-record overhead: ~20 bytes (varints, attributes)
		size += 20
		size += record.key.len
		size += record.value.len
	}

	return size
}

// CRC32-C checksum (using the crc32c module)

/// calculate_crc32c calculates a CRC32-C checksum using the Castagnoli polynomial.
/// Required for Kafka RecordBatch validation.
/// Delegates to the crc32c module which uses an optimized Slicing-by-8 algorithm.
/// data: byte array to compute the checksum over
/// Returns: 32-bit CRC32-C checksum value
pub fn calculate_crc32c(data []u8) u32 {
	return crc32c.calculate(data)
}
