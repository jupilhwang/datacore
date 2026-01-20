// Adapter Layer - Kafka Binary Codec
// Binary Reader/Writer in Big Endian format (Kafka wire format)
module kafka

import domain
import time

// Binary Reader
pub struct BinaryReader {
pub mut:
	data []u8
	pos  int
}

pub fn new_reader(data []u8) BinaryReader {
	return BinaryReader{
		data: data
		pos:  0
	}
}

pub fn (r &BinaryReader) remaining() int {
	return r.data.len - r.pos
}

pub fn (r &BinaryReader) position() int {
	return r.pos
}

pub fn (mut r BinaryReader) read_i8() !i8 {
	if r.remaining() < 1 {
		return error('not enough data for i8')
	}
	val := i8(r.data[r.pos])
	r.pos += 1
	return val
}

pub fn (mut r BinaryReader) read_i16() !i16 {
	if r.remaining() < 2 {
		return error('not enough data for i16')
	}
	val := i16(u16(r.data[r.pos]) << 8 | u16(r.data[r.pos + 1]))
	r.pos += 2
	return val
}

pub fn (mut r BinaryReader) read_i32() !i32 {
	if r.remaining() < 4 {
		return error('not enough data for i32')
	}
	val := i32(u32(r.data[r.pos]) << 24 | u32(r.data[r.pos + 1]) << 16 | u32(r.data[r.pos + 2]) << 8 | u32(r.data[
		r.pos + 3]))
	r.pos += 4
	return val
}

pub fn (mut r BinaryReader) read_i64() !i64 {
	if r.remaining() < 8 {
		return error('not enough data for i64')
	}
	val := i64(u64(r.data[r.pos]) << 56 | u64(r.data[r.pos + 1]) << 48 | u64(r.data[r.pos + 2]) << 40 | u64(r.data[
		r.pos + 3]) << 32 | u64(r.data[r.pos + 4]) << 24 | u64(r.data[r.pos + 5]) << 16 | u64(r.data[
		r.pos + 6]) << 8 | u64(r.data[r.pos + 7]))
	r.pos += 8
	return val
}

pub fn (mut r BinaryReader) read_uvarint() !u64 {
	mut result := u64(0)
	mut shift := u32(0)

	for {
		if r.remaining() < 1 {
			return error('not enough data for uvarint')
		}
		b := r.data[r.pos]
		r.pos += 1

		result |= u64(b & 0x7F) << shift
		if b & 0x80 == 0 {
			break
		}
		shift += 7
		if shift >= 64 {
			return error('uvarint overflow')
		}
	}
	return result
}

pub fn (mut r BinaryReader) read_varint() !i64 {
	uval := r.read_uvarint()!
	return i64((uval >> 1) ^ (-(uval & 1)))
}

pub fn (mut r BinaryReader) read_string() !string {
	len := r.read_i16()!
	if len < 0 {
		return ''
	}
	if r.remaining() < int(len) {
		return error('not enough data for string')
	}
	str := r.data[r.pos..r.pos + int(len)].bytestr()
	r.pos += int(len)
	return str
}

pub fn (mut r BinaryReader) read_nullable_string() !string {
	len := r.read_i16()!
	if len < 0 {
		return ''
	}
	if r.remaining() < int(len) {
		return error('not enough data for nullable string')
	}
	str := r.data[r.pos..r.pos + int(len)].bytestr()
	r.pos += int(len)
	return str
}

pub fn (mut r BinaryReader) read_compact_string() !string {
	len := r.read_uvarint()!
	if len == 0 {
		return ''
	}
	actual_len := int(len) - 1
	if r.remaining() < actual_len {
		return error('not enough data for compact string')
	}
	str := r.data[r.pos..r.pos + actual_len].bytestr()
	r.pos += actual_len
	return str
}

pub fn (mut r BinaryReader) read_compact_nullable_string() !string {
	// Compact nullable string: length 0 means null, length N means N-1 bytes
	len := r.read_uvarint()!
	if len == 0 {
		return '' // null
	}
	actual_len := int(len) - 1
	if actual_len == 0 {
		return '' // empty string
	}
	if r.remaining() < actual_len {
		return error('not enough data for compact nullable string')
	}
	str := r.data[r.pos..r.pos + actual_len].bytestr()
	r.pos += actual_len
	return str
}

// Read bytes with specified length
pub fn (mut r BinaryReader) read_bytes_len(len int) ![]u8 {
	if len < 0 {
		return []u8{}
	}
	if r.remaining() < len {
		return error('not enough data for bytes')
	}
	bytes := r.data[r.pos..r.pos + len].clone()
	r.pos += len
	return bytes
}

// Read bytes with i32 length prefix
pub fn (mut r BinaryReader) read_bytes() ![]u8 {
	len := r.read_i32()!
	return r.read_bytes_len(int(len))!
}

// Read UUID (16 bytes, fixed length)
pub fn (mut r BinaryReader) read_uuid() ![]u8 {
	if r.remaining() < 16 {
		return error('not enough data for UUID')
	}
	uuid := r.data[r.pos..r.pos + 16].clone()
	r.pos += 16
	return uuid
}

pub fn (mut r BinaryReader) read_compact_bytes() ![]u8 {
	len := r.read_uvarint()!
	if len == 0 {
		return []u8{}
	}
	actual_len := int(len) - 1
	if r.remaining() < actual_len {
		return error('not enough data for compact bytes')
	}
	bytes := r.data[r.pos..r.pos + actual_len].clone()
	r.pos += actual_len
	return bytes
}

pub fn (mut r BinaryReader) read_array_len() !int {
	len := r.read_i32()!
	return int(len)
}

pub fn (mut r BinaryReader) read_compact_array_len() !int {
	len := r.read_uvarint()!
	if len == 0 {
		return -1
	}
	return int(len) - 1
}

pub fn (mut r BinaryReader) skip(n int) ! {
	if r.remaining() < n {
		return error('not enough data to skip')
	}
	r.pos += n
}

pub fn (mut r BinaryReader) read_i32_bytes() ![]u8 {
	length := r.read_i32()!
	return r.read_bytes_len(int(length))!
}

pub fn (mut r BinaryReader) skip_tagged_fields() ! {
	num_fields := r.read_uvarint()!
	for _ in 0 .. num_fields {
		_ = r.read_uvarint()!
		size := r.read_uvarint()!
		if r.remaining() < int(size) {
			return error('not enough data for tagged field')
		}
		r.pos += int(size)
	}
}

// Binary Writer
pub struct BinaryWriter {
pub mut:
	data []u8
}

pub fn new_writer() BinaryWriter {
	return BinaryWriter{
		data: []u8{}
	}
}

pub fn new_writer_with_capacity(capacity int) BinaryWriter {
	return BinaryWriter{
		data: []u8{cap: capacity}
	}
}

pub fn (w &BinaryWriter) bytes() []u8 {
	return w.data
}

pub fn (w &BinaryWriter) len() int {
	return w.data.len
}

pub fn (mut w BinaryWriter) write_i8(val i8) {
	w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_i16(val i16) {
	w.data << u8(val >> 8)
	w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_i32(val i32) {
	w.data << u8(val >> 24)
	w.data << u8(val >> 16)
	w.data << u8(val >> 8)
	w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_u32(val u32) {
	w.data << u8(val >> 24)
	w.data << u8(val >> 16)
	w.data << u8(val >> 8)
	w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_i64(val i64) {
	w.data << u8(val >> 56)
	w.data << u8(val >> 48)
	w.data << u8(val >> 40)
	w.data << u8(val >> 32)
	w.data << u8(val >> 24)
	w.data << u8(val >> 16)
	w.data << u8(val >> 8)
	w.data << u8(val)
}

pub fn (mut w BinaryWriter) write_uvarint(val u64) {
	mut v := val
	for v >= 0x80 {
		w.data << u8(v | 0x80)
		v >>= 7
	}
	w.data << u8(v)
}

pub fn (mut w BinaryWriter) write_varint(val i64) {
	// ZigZag encoding: convert signed to unsigned
	// Maps negative numbers to odd positive numbers, positive to even
	// -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
	uval := (u64(val) << 1) ^ u64(val >> 63)
	w.write_uvarint(uval)
}

pub fn (mut w BinaryWriter) write_string(s string) {
	w.write_i16(i16(s.len))
	w.data << s.bytes()
}

pub fn (mut w BinaryWriter) write_nullable_string(s ?string) {
	if str := s {
		w.write_i16(i16(str.len))
		w.data << str.bytes()
	} else {
		w.write_i16(-1)
	}
}

pub fn (mut w BinaryWriter) write_compact_string(s string) {
	w.write_uvarint(u64(s.len) + 1)
	w.data << s.bytes()
}

pub fn (mut w BinaryWriter) write_compact_nullable_string(s ?string) {
	if str := s {
		w.write_uvarint(u64(str.len) + 1)
		w.data << str.bytes()
	} else {
		w.write_uvarint(0)
	}
}

pub fn (mut w BinaryWriter) write_bytes(b []u8) {
	w.write_i32(i32(b.len))
	w.data << b
}

pub fn (mut w BinaryWriter) write_compact_bytes(b []u8) {
	w.write_uvarint(u64(b.len) + 1)
	w.data << b
}

// Write UUID (16 bytes, fixed length)
// If uuid is empty or less than 16 bytes, writes zero UUID
pub fn (mut w BinaryWriter) write_uuid(uuid []u8) {
	if uuid.len >= 16 {
		w.data << uuid[0..16]
	} else {
		// Write zero UUID (16 bytes of zeros)
		for _ in 0 .. 16 {
			w.data << u8(0)
		}
	}
}

pub fn (mut w BinaryWriter) write_array_len(len int) {
	w.write_i32(i32(len))
}

pub fn (mut w BinaryWriter) write_compact_array_len(len int) {
	if len < 0 {
		w.write_uvarint(0)
	} else {
		w.write_uvarint(u64(len) + 1)
	}
}

pub fn (mut w BinaryWriter) write_tagged_fields() {
	w.write_uvarint(0)
}

pub fn (mut w BinaryWriter) write_raw(b []u8) {
	w.data << b
}

// ============================================================
// RecordBatch Parsing (Kafka Message Format v2)
// ============================================================

// ParsedRecordBatch represents a parsed Kafka RecordBatch
pub struct ParsedRecordBatch {
pub:
	base_offset            i64
	partition_leader_epoch i32
	magic                  i8
	attributes             i16
	last_offset_delta      i32
	first_timestamp        i64
	max_timestamp          i64
	producer_id            i64
	producer_epoch         i16
	base_sequence          i32
	records                []domain.Record
}

// parse_record_batch parses a Kafka RecordBatch (v2 format, magic=2)
// Returns parsed records or empty array if parsing fails
pub fn parse_record_batch(data []u8) !ParsedRecordBatch {
	if data.len < 12 {
		return error('data too small')
	}

	mut reader := new_reader(data)

	// RecordBatch header
	base_offset := reader.read_i64()!
	batch_length := reader.read_i32()!

	if data.len < 12 + int(batch_length) {
		return error('incomplete record batch')
	}

	partition_leader_epoch := reader.read_i32()!
	magic := reader.read_i8()!

	if magic != 2 {
		// Support MessageSet v0, v1 (magic 0, 1)
		// Parse as legacy MessageSet format
		reader.pos = 0 // Reset to beginning
		return parse_message_set(data)!
	}

	_ = reader.read_i32()! // crc (skip validation for now)
	attributes := reader.read_i16()!
	last_offset_delta := reader.read_i32()!
	first_timestamp := reader.read_i64()!
	max_timestamp := reader.read_i64()!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!
	base_sequence := reader.read_i32()!
	record_count := reader.read_i32()!

	// Parse records
	mut records := []domain.Record{cap: int(record_count)}

	for _ in 0 .. record_count {
		record := parse_record(mut reader, first_timestamp) or { break }
		records << record
	}

	return ParsedRecordBatch{
		base_offset:            base_offset
		partition_leader_epoch: partition_leader_epoch
		magic:                  magic
		attributes:             attributes
		last_offset_delta:      last_offset_delta
		first_timestamp:        first_timestamp
		max_timestamp:          max_timestamp
		producer_id:            producer_id
		producer_epoch:         producer_epoch
		base_sequence:          base_sequence
		records:                records
	}
}

// parse_message_set parses legacy MessageSet format (v0, v1 - magic 0, 1)
fn parse_message_set(data []u8) !ParsedRecordBatch {
	mut reader := new_reader(data)
	mut records := []domain.Record{}

	// MessageSet is array of [offset, size, message]
	for reader.remaining() > 12 {
		_ := reader.read_i64() or { break } // offset (unused in MessageSet parsing)
		message_size := reader.read_i32() or { break }

		if reader.remaining() < int(message_size) {
			break
		}

		// Parse Message: [crc, magic, attributes, key, value]
		start_pos := reader.pos
		_ = reader.read_i32() or { break } // crc
		magic := reader.read_i8() or { break }
		_ = reader.read_i8() or { break } // attributes

		// v1 adds timestamp
		mut timestamp := time.now()
		if magic >= 1 {
			ts_millis := reader.read_i64() or { break }
			timestamp = time.unix(ts_millis / 1000)
		}

		// Key
		key := reader.read_bytes() or { break }
		// Value
		value := reader.read_bytes() or { break }

		records << domain.Record{
			key:       key
			value:     value
			headers:   map[string][]u8{}
			timestamp: timestamp
		}

		// Ensure we consumed exactly message_size bytes
		reader.pos = start_pos + int(message_size)
	}

	return ParsedRecordBatch{
		base_offset:            0
		partition_leader_epoch: -1
		magic:                  0
		attributes:             0
		last_offset_delta:      i32(records.len - 1)
		first_timestamp:        0
		max_timestamp:          0
		producer_id:            -1
		producer_epoch:         -1
		base_sequence:          -1
		records:                records
	}
}

// parse_record parses a single Record from RecordBatch v2
fn parse_record(mut reader BinaryReader, base_timestamp i64) !domain.Record {
	_ = reader.read_varint()! // length (already included in batch)
	_ = reader.read_i8()! // attributes (unused in v2)
	timestamp_delta := reader.read_varint()!
	_ = reader.read_varint()! // offset_delta

	// Key
	key_length := reader.read_varint()!
	mut key := []u8{}
	if key_length > 0 {
		if reader.remaining() < int(key_length) {
			return error('not enough data for key')
		}
		key = reader.data[reader.pos..reader.pos + int(key_length)].clone()
		reader.pos += int(key_length)
	}

	// Value
	value_length := reader.read_varint()!
	mut value := []u8{}
	if value_length > 0 {
		if reader.remaining() < int(value_length) {
			return error('not enough data for value')
		}
		value = reader.data[reader.pos..reader.pos + int(value_length)].clone()
		reader.pos += int(value_length)
	}

	// Headers
	headers_count := reader.read_varint()!
	mut headers := map[string][]u8{}

	for _ in 0 .. headers_count {
		header_key_len := reader.read_varint()!
		mut header_key := ''
		if header_key_len > 0 {
			if reader.remaining() < int(header_key_len) {
				return error('not enough data for header key')
			}
			header_key = reader.data[reader.pos..reader.pos + int(header_key_len)].bytestr()
			reader.pos += int(header_key_len)
		}

		header_value_len := reader.read_varint()!
		mut header_value := []u8{}
		if header_value_len > 0 {
			if reader.remaining() < int(header_value_len) {
				return error('not enough data for header value')
			}
			header_value = reader.data[reader.pos..reader.pos + int(header_value_len)].clone()
			reader.pos += int(header_value_len)
		}

		if header_key.len > 0 {
			headers[header_key] = header_value
		}
	}

	// Calculate timestamp
	ts_millis := base_timestamp + timestamp_delta
	ts := time.unix(ts_millis / 1000)

	return domain.Record{
		key:       key
		value:     value
		headers:   headers
		timestamp: ts
	}
}

// ============================================================
// RecordBatch Encoding (for Fetch responses)
// ============================================================

// crc32c computes CRC-32C (Castagnoli) checksum
fn crc32c(data []u8) u32 {
	mut crc := u32(0xffffffff)
	for b in data {
		crc ^= u32(b)
		for _ in 0 .. 8 {
			if (crc & 1) == 1 {
				crc = (crc >> 1) ^ 0x82f63b78
			} else {
				crc >>= 1
			}
		}
	}
	return ~crc
}

// encode_record_batch encodes records into Kafka RecordBatch v2 format
pub fn encode_record_batch(records []domain.Record, base_offset i64) []u8 {
	if records.len == 0 {
		return []u8{}
	}

	mut writer := new_writer()

	// Calculate timestamps from records
	// Use first record's timestamp as base (or current time if no timestamp)
	first_timestamp := if records[0].timestamp.unix() > 0 {
		records[0].timestamp.unix() * 1000
	} else {
		time.now().unix() * 1000
	}

	// Find max timestamp
	mut max_timestamp := first_timestamp
	for record in records {
		ts := record.timestamp.unix() * 1000
		if ts > max_timestamp {
			max_timestamp = ts
		}
	}

	// Encode records first to calculate batch size
	mut records_data := new_writer()
	for i, record in records {
		encode_record(mut records_data, record, i, first_timestamp)
	}

	records_bytes := records_data.bytes()

	// Build data for CRC calculation
	// IMPORTANT: CRC-32C covers from attributes to end of batch (NOT from partitionLeaderEpoch!)
	// Per Kafka spec: CRC = crc32c(attributes...end_of_batch)
	mut crc_data := new_writer()
	crc_data.write_i16(0) // attributes
	crc_data.write_i32(i32(records.len - 1)) // lastOffsetDelta
	crc_data.write_i64(first_timestamp) // baseTimestamp
	crc_data.write_i64(max_timestamp) // maxTimestamp
	crc_data.write_i64(-1) // producerId
	crc_data.write_i16(-1) // producerEpoch
	crc_data.write_i32(-1) // baseSequence
	crc_data.write_i32(i32(records.len)) // recordCount
	// Note: If delete_horizon_ms is supported (attributes bit 5 for control batch),
	// it should be inserted here after baseSequence, before recordCount.
	// Currently we don't support delete_horizon_ms.
	crc_data.write_raw(records_bytes) // records

	crc_bytes := crc_data.bytes()
	// CRC-32C (Castagnoli) over crc_data
	crc := crc32c(crc_bytes)

	// RecordBatch header fields
	// batchLength = from partitionLeaderEpoch to end
	batch_length := 4 + 1 + 4 + crc_bytes.len // epoch + magic + crc + crc_data

	writer.write_i64(base_offset) // baseOffset
	writer.write_i32(i32(batch_length)) // batchLength
	writer.write_i32(0) // partitionLeaderEpoch
	writer.write_i8(2) // magic (v2)
	writer.write_u32(crc) // crc (CRC-32C)
	writer.write_raw(crc_bytes) // attributes to records

	return writer.bytes()
}

// encode_record encodes a single record in RecordBatch v2 format
fn encode_record(mut writer BinaryWriter, record domain.Record, offset_delta int, base_timestamp i64) {
	// Build record body first to calculate length
	mut body := new_writer()

	body.write_i8(0) // attributes

	// timestamp delta
	ts_millis := record.timestamp.unix() * 1000
	body.write_varint(ts_millis - base_timestamp)

	// offset delta
	body.write_varint(i64(offset_delta))

	// key
	if record.key.len > 0 {
		body.write_varint(i64(record.key.len))
		body.write_raw(record.key)
	} else {
		body.write_varint(-1) // null key
	}

	// value
	if record.value.len > 0 {
		body.write_varint(i64(record.value.len))
		body.write_raw(record.value)
	} else {
		body.write_varint(-1) // null value
	}

	// Record headers (key-value pairs)
	// Note: Both header key and value are treated as raw bytes with VARINT length prefix
	// Header key format: VARINT(key_length) + BYTES(key)
	// Header value format: VARINT(value_length) + BYTES(value)
	body.write_varint(i64(record.headers.len))
	for key, value in record.headers {
		// Header key as bytes (NOT a STRING type - no null marker)
		body.write_varint(i64(key.len))
		body.write_raw(key.bytes())
		// Header value as bytes
		body.write_varint(i64(value.len))
		body.write_raw(value)
	}

	// Write length + body
	body_bytes := body.bytes()
	writer.write_varint(i64(body_bytes.len))
	writer.write_raw(body_bytes)
}
