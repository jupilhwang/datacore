// S3 Record Codec
// Encoding and decoding logic for stored records
module s3

import time

// StoredRecord is the internal representation with offset for storage
struct StoredRecord {
	offset    i64
	timestamp time.Time
	key       []u8
	value     []u8
	headers   map[string][]u8
}

// encode_stored_records encodes a list of StoredRecord to binary format
// Pre-allocates buffer capacity to avoid repeated reallocations
fn encode_stored_records(records []StoredRecord) []u8 {
	// Estimate buffer size: 4 (count) + records * (8 offset + 8 ts + 4 key_len + 4 val_len + 4 headers_count + avg data)
	mut estimated_size := 4
	for rec in records {
		// Fixed overhead: 8 (offset) + 8 (timestamp) + 4 (key_len) + 4 (value_len) + 4 (headers_count) = 28
		estimated_size += 28 + rec.key.len + rec.value.len
		// Headers: 2 (key_len) + key + 2 (val_len) + val per header
		for h_key, h_val in rec.headers {
			estimated_size += 4 + h_key.len + h_val.len
		}
	}

	mut buf := []u8{cap: estimated_size}
	record_count := records.len
	buf << u8(record_count >> 24)
	buf << u8(record_count >> 16)
	buf << u8(record_count >> 8)
	buf << u8(record_count)

	for rec in records {
		// Offset (8 bytes)
		for i := 7; i >= 0; i-- {
			buf << u8(rec.offset >> (i * 8))
		}

		// Timestamp (8 bytes)
		ts := rec.timestamp.unix_milli()
		for i := 7; i >= 0; i-- {
			buf << u8(ts >> (i * 8))
		}

		// Key
		key_len := rec.key.len
		buf << u8(key_len >> 24)
		buf << u8(key_len >> 16)
		buf << u8(key_len >> 8)
		buf << u8(key_len)
		buf << rec.key

		// Value
		value_len := rec.value.len
		buf << u8(value_len >> 24)
		buf << u8(value_len >> 16)
		buf << u8(value_len >> 8)
		buf << u8(value_len)
		buf << rec.value

		// Headers (map[string][]u8)
		headers_count := rec.headers.len
		buf << u8(headers_count >> 24)
		buf << u8(headers_count >> 16)
		buf << u8(headers_count >> 8)
		buf << u8(headers_count)

		for h_key, h_val in rec.headers {
			// Header key length and value
			buf << u8(h_key.len >> 8)
			buf << u8(h_key.len)
			buf << h_key.bytes()

			// Header value length and value
			buf << u8(h_val.len >> 8)
			buf << u8(h_val.len)
			buf << h_val
		}
	}

	return buf
}

// decode_stored_records decodes binary data to a list of StoredRecord
fn decode_stored_records(data []u8) []StoredRecord {
	if data.len < 4 {
		return []
	}

	mut pos := 0
	record_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
		pos + 3])
	pos += 4

	mut records := []StoredRecord{}

	for _ in 0 .. record_count {
		if pos + 20 > data.len {
			break
		}

		// Offset
		mut offset := i64(0)
		for i := 0; i < 8; i++ {
			offset = i64((u64(offset) << 8) | u64(data[pos + i]))
		}
		pos += 8

		// Timestamp
		mut ts := i64(0)
		for i := 0; i < 8; i++ {
			ts = i64((u64(ts) << 8) | u64(data[pos + i]))
		}
		pos += 8

		// Key
		key_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4
		key := data[pos..pos + int(key_len)].clone()
		pos += int(key_len)

		// Value
		value_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4
		value := data[pos..pos + int(value_len)].clone()
		pos += int(value_len)

		// Headers
		headers_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4

		mut headers := map[string][]u8{}
		for _ in 0 .. headers_count {
			h_key_len := (u32(data[pos]) << 8) | u32(data[pos + 1])
			pos += 2
			h_key := data[pos..pos + int(h_key_len)].bytestr()
			pos += int(h_key_len)

			h_val_len := (u32(data[pos]) << 8) | u32(data[pos + 1])
			pos += 2
			h_val := data[pos..pos + int(h_val_len)].clone()
			pos += int(h_val_len)

			headers[h_key] = h_val
		}

		records << StoredRecord{
			offset:    offset
			timestamp: time.unix_milli(ts)
			key:       key
			value:     value
			headers:   headers
		}
	}

	return records
}
