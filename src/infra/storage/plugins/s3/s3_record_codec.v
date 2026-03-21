// Infra Layer - S3 Record Codec
// Encoding and decoding logic for stored records
module s3

import time
import infra.performance.core

/// StoredRecord is the internal storage representation with an offset.
struct StoredRecord {
	offset           i64
	timestamp        time.Time
	key              []u8
	value            []u8
	headers          map[string][]u8
	compression_type u8
}

/// encode_stored_records encodes a list of StoredRecords to binary format.
/// Pre-allocates buffer capacity to avoid repeated reallocations.
fn encode_stored_records(records []StoredRecord) []u8 {
	// Buffer size estimate: 4 (count) + records * (8 offset + 8 timestamp + 4 key_len + 4 value_len + 4 header_count + avg data)
	mut estimated_size := 4
	for rec in records {
		// Fixed overhead: 8 (offset) + 8 (timestamp) + 4 (key_len) + 4 (value_len) + 4 (header_count) + 1 (compression_type) = 29
		estimated_size += 29 + rec.key.len + rec.value.len
		// Headers: 2 (key_len) + key + 2 (value_len) + value per header
		for h_key, h_val in rec.headers {
			estimated_size += 4 + h_key.len + h_val.len
		}
	}

	mut buf := []u8{cap: estimated_size}
	core.write_i32_be(mut buf, i32(records.len))

	for rec in records {
		// Offset (8 bytes)
		core.write_i64_be(mut buf, rec.offset)

		// Timestamp (8 bytes)
		core.write_i64_be(mut buf, rec.timestamp.unix_milli())

		// Key
		core.write_i32_be(mut buf, i32(rec.key.len))
		buf << rec.key

		// Value
		core.write_i32_be(mut buf, i32(rec.value.len))
		buf << rec.value

		// Headers (map[string][]u8)
		core.write_i32_be(mut buf, i32(rec.headers.len))

		for h_key, h_val in rec.headers {
			// Header key length and value
			core.write_i16_be(mut buf, i16(h_key.len))
			buf << h_key.bytes()

			// Header value length and value
			core.write_i16_be(mut buf, i16(h_val.len))
			buf << h_val
		}

		// Compression type (1 byte)
		buf << rec.compression_type
	}

	return buf
}

/// encode_stored_records_with_index encodes records to binary and builds
/// a RecordIndex mapping each record offset to its byte position.
/// The binary format is identical to encode_stored_records.
fn encode_stored_records_with_index(records []StoredRecord) ([]u8, []RecordIndex) {
	mut estimated_size := 4
	for rec in records {
		estimated_size += 29 + rec.key.len + rec.value.len
		for h_key, h_val in rec.headers {
			estimated_size += 4 + h_key.len + h_val.len
		}
	}

	mut buf := []u8{cap: estimated_size}
	mut index := []RecordIndex{cap: records.len}

	core.write_i32_be(mut buf, i32(records.len))

	for rec in records {
		index << RecordIndex{
			offset:        rec.offset
			byte_position: i64(buf.len)
		}
		core.write_i64_be(mut buf, rec.offset)
		core.write_i64_be(mut buf, rec.timestamp.unix_milli())
		core.write_i32_be(mut buf, i32(rec.key.len))
		buf << rec.key
		core.write_i32_be(mut buf, i32(rec.value.len))
		buf << rec.value
		core.write_i32_be(mut buf, i32(rec.headers.len))
		for h_key, h_val in rec.headers {
			core.write_i16_be(mut buf, i16(h_key.len))
			buf << h_key.bytes()
			core.write_i16_be(mut buf, i16(h_val.len))
			buf << h_val
		}
		buf << rec.compression_type
	}

	return buf, index
}

/// decode_stored_records decodes binary data into a list of StoredRecords.
/// Supports concatenated segments: loops until EOF reading each
/// (count, records...) tuple produced by encode_stored_records.
fn decode_stored_records(data []u8) []StoredRecord {
	mut records := []StoredRecord{}
	mut pos := 0

	for pos + 4 <= data.len {
		record_count := core.read_u32_be(data[pos..])
		pos += 4

		if record_count == 0 {
			break
		}

		for _ in 0 .. record_count {
			if pos + 20 > data.len {
				return records
			}

			// Offset
			offset := core.read_i64_be(data[pos..])
			pos += 8

			ts := core.read_i64_be(data[pos..])
			pos += 8

			// Key
			key_len := core.read_u32_be(data[pos..])
			pos += 4
			key := data[pos..pos + int(key_len)].clone()
			pos += int(key_len)

			// Value
			value_len := core.read_u32_be(data[pos..])
			pos += 4
			value := data[pos..pos + int(value_len)].clone()
			pos += int(value_len)

			// Headers
			headers_count := core.read_u32_be(data[pos..])
			pos += 4

			mut headers := map[string][]u8{}
			for _ in 0 .. headers_count {
				h_key_len := core.read_u16_be(data[pos..])
				pos += 2
				h_key := data[pos..pos + int(h_key_len)].bytestr()
				pos += int(h_key_len)

				h_val_len := core.read_u16_be(data[pos..])
				pos += 2
				h_val := data[pos..pos + int(h_val_len)].clone()
				pos += int(h_val_len)

				headers[h_key] = h_val
			}

			// Compression type (1 byte; default 0 for backward compatibility if missing)
			mut compression_type := u8(0)
			if pos < data.len {
				compression_type = data[pos]
				pos += 1
			}

			records << StoredRecord{
				offset:           offset
				timestamp:        time.unix_milli(ts)
				key:              key
				value:            value
				headers:          headers
				compression_type: compression_type
			}
		}
	}

	return records
}
