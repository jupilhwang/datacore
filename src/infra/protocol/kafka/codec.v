// Big-endian binary reader/writer (Kafka wire protocol)
module kafka

import domain
import time
import infra.protocol.kafka.crc32c
import infra.performance.core

/// ByteView - Zero-copy view over a byte slice (lightweight alternative)
/// Used internally for high-performance parsing without memory allocation.
pub struct ByteView {
pub:
	data   []u8
	offset int
	length int
}

/// new creates a new ByteView from a byte slice.
pub fn ByteView.new(data []u8) ByteView {
	return ByteView{
		data:   data
		offset: 0
		length: data.len
	}
}

/// slice creates a sub-view without copying data.
pub fn (v ByteView) slice(start int, end int) !ByteView {
	if start < 0 || end > v.length || start > end {
		return error('slice bounds out of range')
	}
	return ByteView{
		data:   v.data
		offset: v.offset + start
		length: end - start
	}
}

/// bytes returns the underlying bytes (zero-copy when offset is 0).
pub fn (v ByteView) bytes() []u8 {
	if v.length == 0 {
		return []u8{}
	}
	return v.data[v.offset..v.offset + v.length]
}

/// to_owned returns an owned copy of the data.
pub fn (v ByteView) to_owned() []u8 {
	if v.length == 0 {
		return []u8{}
	}
	return v.data[v.offset..v.offset + v.length].clone()
}

/// to_string converts the view to a string (zero-copy when possible).
pub fn (v ByteView) to_string() string {
	if v.length == 0 {
		return ''
	}
	return v.data[v.offset..v.offset + v.length].bytestr()
}

/// len returns the length of the view.
pub fn (v ByteView) len() int {
	return v.length
}

/// is_empty returns true if the view has zero length.
pub fn (v ByteView) is_empty() bool {
	return v.length == 0
}

/// BinaryReader - Reader for parsing the Kafka binary protocol
pub struct BinaryReader {
pub mut:
	data []u8
	pos  int
}

/// new_reader creates a new BinaryReader from a byte slice.
pub fn new_reader(data []u8) BinaryReader {
	return BinaryReader{
		data: data
		pos:  0
	}
}

/// remaining returns the number of unread bytes.
pub fn (r &BinaryReader) remaining() int {
	return r.data.len - r.pos
}

/// position returns the current read position.
pub fn (r &BinaryReader) position() int {
	return r.pos
}

/// read_i8 reads a signed 8-bit integer from the buffer.
pub fn (mut r BinaryReader) read_i8() !i8 {
	if r.remaining() < 1 {
		return error('not enough data for i8')
	}
	val := i8(r.data[r.pos])
	r.pos += 1
	return val
}

/// read_i16 reads a signed 16-bit integer in big-endian format.
pub fn (mut r BinaryReader) read_i16() !i16 {
	if r.remaining() < 2 {
		return error('not enough data for i16')
	}
	val := core.read_i16_be(r.data[r.pos..r.pos + 2])
	r.pos += 2
	return val
}

/// read_i32 reads a signed 32-bit integer in big-endian format.
pub fn (mut r BinaryReader) read_i32() !i32 {
	if r.remaining() < 4 {
		return error('not enough data for i32')
	}
	val := core.read_i32_be(r.data[r.pos..r.pos + 4])
	r.pos += 4
	return val
}

/// read_i64 reads a signed 64-bit integer in big-endian format.
pub fn (mut r BinaryReader) read_i64() !i64 {
	if r.remaining() < 8 {
		return error('not enough data for i64')
	}
	val := core.read_i64_be(r.data[r.pos..r.pos + 8])
	r.pos += 8
	return val
}

/// read_uvarint reads an unsigned variable-length integer.
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

/// read_varint reads a signed variable-length integer (zigzag encoded).
pub fn (mut r BinaryReader) read_varint() !i64 {
	uval := r.read_uvarint()!
	return i64((uval >> 1) ^ (-(uval & 1)))
}

/// read_string reads a length-prefixed string (i16 length).
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

/// read_nullable_string reads a nullable length-prefixed string (i16 length; -1 means null).
// Delegates to read_string since both formats share identical wire encoding.
pub fn (mut r BinaryReader) read_nullable_string() !string {
	return r.read_string()!
}

/// read_compact_string reads a compact string (unsigned varint length + 1).
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

/// read_compact_nullable_string reads a compact nullable string (unsigned varint length; 0 means null).
pub fn (mut r BinaryReader) read_compact_nullable_string() !string {
	// Compact nullable string: length 0 means null; length N means N-1 bytes of content
	len := r.read_uvarint()!
	if len == 0 {
		return ''
	}
	actual_len := int(len) - 1
	if actual_len == 0 {
		return ''
	}
	if r.remaining() < actual_len {
		return error('not enough data for compact nullable string')
	}
	str := r.data[r.pos..r.pos + actual_len].bytestr()
	r.pos += actual_len
	return str
}

/// read_bytes_len reads exactly n bytes from the buffer.
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

/// read_bytes reads a byte slice prefixed with an i32 length.
pub fn (mut r BinaryReader) read_bytes() ![]u8 {
	len := r.read_i32()!
	return r.read_bytes_len(int(len))!
}

/// read_uuid reads a 16-byte fixed-length UUID.
pub fn (mut r BinaryReader) read_uuid() ![]u8 {
	if r.remaining() < 16 {
		return error('not enough data for UUID')
	}
	uuid := r.data[r.pos..r.pos + 16].clone()
	r.pos += 16
	return uuid
}

/// read_compact_bytes reads a compact byte slice (unsigned varint length + 1).
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

/// read_array_len reads an array length (i32; -1 for null array).
pub fn (mut r BinaryReader) read_array_len() !int {
	len := r.read_i32()!
	return int(len)
}

/// read_compact_array_len reads a compact array length (unsigned varint; 0 means null).
pub fn (mut r BinaryReader) read_compact_array_len() !int {
	len := r.read_uvarint()!
	if len == 0 {
		return -1
	}
	return int(len) - 1
}

/// skip advances the read position by n bytes.
pub fn (mut r BinaryReader) skip(n int) ! {
	if r.remaining() < n {
		return error('not enough data to skip')
	}
	r.pos += n
}

/// read_bytes_view reads n bytes as a zero-copy ByteView.
pub fn (mut r BinaryReader) read_bytes_view(len int) !ByteView {
	if len < 0 {
		return ByteView{}
	}
	if r.remaining() < len {
		return error('not enough data for bytes view')
	}
	view := ByteView{
		data:   r.data
		offset: r.pos
		length: len
	}
	r.pos += len
	return view
}

/// read_bytes_as_view reads an i32-length-prefixed byte slice as a zero-copy ByteView.
pub fn (mut r BinaryReader) read_bytes_as_view() !ByteView {
	len := r.read_i32()!
	return r.read_bytes_view(int(len))!
}

/// read_compact_bytes_view reads compact bytes as a zero-copy ByteView.
pub fn (mut r BinaryReader) read_compact_bytes_view() !ByteView {
	len := r.read_uvarint()!
	if len == 0 {
		return ByteView{}
	}
	actual_len := int(len) - 1
	return r.read_bytes_view(actual_len)!
}

/// peek_bytes_view returns the next n bytes as a zero-copy ByteView without advancing the position.
pub fn (r &BinaryReader) peek_bytes_view(len int) !ByteView {
	if r.remaining() < len {
		return error('not enough data to peek')
	}
	return ByteView{
		data:   r.data
		offset: r.pos
		length: len
	}
}

/// remaining_view returns the remaining unread data as a ByteView.
pub fn (r &BinaryReader) remaining_view() ByteView {
	return ByteView{
		data:   r.data
		offset: r.pos
		length: r.remaining()
	}
}

/// read_i32_bytes reads an i32-length-prefixed byte slice.
pub fn (mut r BinaryReader) read_i32_bytes() ![]u8 {
	length := r.read_i32()!
	return r.read_bytes_len(int(length))!
}

/// skip_tagged_fields skips Kafka tagged fields (flexible message format).
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

// Flexible version helpers
// These reduce code duplication when parsing flexible vs. non-flexible messages.

/// read_flex_string reads a string in compact format if flexible, otherwise in standard format.
pub fn (mut r BinaryReader) read_flex_string(flexible bool) !string {
	return if flexible { r.read_compact_string()! } else { r.read_string()! }
}

/// read_flex_nullable_string reads a nullable string in compact format if flexible.
pub fn (mut r BinaryReader) read_flex_nullable_string(flexible bool) !string {
	return if flexible { r.read_compact_nullable_string()! } else { r.read_nullable_string()! }
}

/// read_flex_bytes reads bytes in compact format if flexible, otherwise in standard format.
pub fn (mut r BinaryReader) read_flex_bytes(flexible bool) ![]u8 {
	return if flexible { r.read_compact_bytes()! } else { r.read_bytes()! }
}

/// read_flex_array_len reads an array length in compact format if flexible.
pub fn (mut r BinaryReader) read_flex_array_len(flexible bool) !int {
	return if flexible { r.read_compact_array_len()! } else { r.read_array_len()! }
}

/// skip_flex_tagged_fields skips tagged fields only when in flexible format.
pub fn (mut r BinaryReader) skip_flex_tagged_fields(flexible bool) ! {
	if flexible {
		r.skip_tagged_fields()!
	}
}

/// BinaryWriter - Writer for encoding the Kafka binary protocol
pub struct BinaryWriter {
pub mut:
	data []u8
}

/// new_writer creates a new BinaryWriter.
pub fn new_writer() BinaryWriter {
	return BinaryWriter{
		data: []u8{}
	}
}

/// new_writer_with_capacity creates a new BinaryWriter with the given initial capacity.
pub fn new_writer_with_capacity(capacity int) BinaryWriter {
	return BinaryWriter{
		data: []u8{cap: capacity}
	}
}

/// bytes returns the written bytes.
pub fn (w &BinaryWriter) bytes() []u8 {
	return w.data
}

/// len returns the number of bytes written.
pub fn (w &BinaryWriter) len() int {
	return w.data.len
}

/// write_i8 writes a signed 8-bit integer.
pub fn (mut w BinaryWriter) write_i8(val i8) {
	w.data << u8(val)
}

/// write_i16 writes a signed 16-bit integer in big-endian format.
pub fn (mut w BinaryWriter) write_i16(val i16) {
	core.write_i16_be(mut w.data, val)
}

/// write_i32 writes a signed 32-bit integer in big-endian format.
pub fn (mut w BinaryWriter) write_i32(val i32) {
	core.write_i32_be(mut w.data, val)
}

/// write_u32 writes an unsigned 32-bit integer in big-endian format.
pub fn (mut w BinaryWriter) write_u32(val u32) {
	core.write_u32_be(mut w.data, val)
}

/// write_i64 writes a signed 64-bit integer in big-endian format.
pub fn (mut w BinaryWriter) write_i64(val i64) {
	core.write_i64_be(mut w.data, val)
}

/// write_uvarint writes an unsigned variable-length integer.
pub fn (mut w BinaryWriter) write_uvarint(val u64) {
	mut v := val
	for v >= 0x80 {
		w.data << u8(v | 0x80)
		v >>= 7
	}
	w.data << u8(v)
}

/// write_varint writes a signed variable-length integer (zigzag encoded).
pub fn (mut w BinaryWriter) write_varint(val i64) {
	// ZigZag encoding: maps signed values to unsigned values
	// Negative numbers map to odd positives; non-negative to even positives
	// e.g. -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4
	uval := (u64(val) << 1) ^ u64(val >> 63)
	w.write_uvarint(uval)
}

/// write_string writes a length-prefixed string (i16 length).
pub fn (mut w BinaryWriter) write_string(s string) {
	w.write_i16(i16(s.len))
	w.data << s.bytes()
}

/// write_nullable_string writes a nullable string (i16 length; -1 for null).
pub fn (mut w BinaryWriter) write_nullable_string(s ?string) {
	if str := s {
		w.write_i16(i16(str.len))
		w.data << str.bytes()
	} else {
		w.write_i16(-1)
	}
}

/// write_compact_string writes a compact string (unsigned varint length + 1).
pub fn (mut w BinaryWriter) write_compact_string(s string) {
	w.write_uvarint(u64(s.len) + 1)
	w.data << s.bytes()
}

/// write_compact_nullable_string writes a compact nullable string.
pub fn (mut w BinaryWriter) write_compact_nullable_string(s ?string) {
	if str := s {
		w.write_uvarint(u64(str.len) + 1)
		w.data << str.bytes()
	} else {
		w.write_uvarint(0)
	}
}

/// write_bytes writes a byte slice prefixed with an i32 length.
pub fn (mut w BinaryWriter) write_bytes(b []u8) {
	w.write_i32(i32(b.len))
	w.data << b
}

/// write_compact_bytes writes a compact byte slice.
pub fn (mut w BinaryWriter) write_compact_bytes(b []u8) {
	w.write_uvarint(u64(b.len) + 1)
	w.data << b
}

/// write_uuid writes a 16-byte fixed-length UUID.
/// Writes a zero UUID if the slice is empty or shorter than 16 bytes.
pub fn (mut w BinaryWriter) write_uuid(uuid []u8) {
	if uuid.len >= 16 {
		w.data << uuid[0..16]
	} else {
		// Write zero UUID (16 zero bytes) in a single append
		w.data << []u8{len: 16, init: 0}
	}
}

/// write_array_len writes an array length as i32.
pub fn (mut w BinaryWriter) write_array_len(len int) {
	w.write_i32(i32(len))
}

/// write_compact_array_len writes a compact array length.
pub fn (mut w BinaryWriter) write_compact_array_len(len int) {
	if len < 0 {
		w.write_uvarint(0)
	} else {
		w.write_uvarint(u64(len) + 1)
	}
}

/// write_tagged_fields writes an empty tagged fields section.
pub fn (mut w BinaryWriter) write_tagged_fields() {
	w.write_uvarint(0)
}

/// write_tagged_field_header writes a tagged field header (tag and size).
pub fn (mut w BinaryWriter) write_tagged_field_header(tag int, size int) {
	w.write_uvarint(u64(tag))
	w.write_uvarint(u64(size))
}

/// write_raw appends raw bytes without any length prefix.
pub fn (mut w BinaryWriter) write_raw(b []u8) {
	w.data << b
}

/// ParsedRecordBatch represents a parsed Kafka RecordBatch.
pub struct ParsedRecordBatch {
pub mut:
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

/// parse_record_batch parses a Kafka RecordBatch (v2 format, magic=2).
/// Returns the parsed records, or an empty slice on failure.
pub fn parse_record_batch(data []u8) !ParsedRecordBatch {
	if data.len < 12 {
		return error('data too small')
	}

	mut reader := new_reader(data)

	// RecordBatch header fields
	base_offset := reader.read_i64()!
	batch_length := reader.read_i32()!

	if data.len < 12 + int(batch_length) {
		return error('incomplete record batch')
	}

	partition_leader_epoch := reader.read_i32()!
	magic := reader.read_i8()!

	if magic != 2 {
		// Fall back to legacy MessageSet format for magic 0 or 1
		reader.pos = 0
		return parse_message_set(data)!
	}

	// CRC32-C validation: verifies RecordBatch integrity.
	// CRC covers from the attributes field to the end of the batch.
	stored_crc := u32(reader.read_i32()!)
	crc_start_pos := reader.pos
	crc_data := data[crc_start_pos..12 + int(batch_length)]
	calculated_crc := crc32c_checksum(crc_data)
	if stored_crc != calculated_crc {
		return error('CRC32-C validation failed: stored=${stored_crc}, calculated=${calculated_crc}')
	}

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

/// parse_nested_record_batch parses records from decompressed Kafka RecordBatch data.
///
/// Kafka compresses the records field of a RecordBatch, NOT an inner RecordBatch.
/// After decompression, the data is a sequence of Record v2 entries:
///
///   [varint: record_length][i8: attributes][varint: timestamp_delta]
///   [varint: offset_delta][varint: key_length][bytes: key]
///   [varint: value_length][bytes: value][varint: headers_count]...
///
/// There is NO nested RecordBatch header (no base_offset, batch_length, magic, CRC, etc.).
pub fn parse_nested_record_batch(data []u8) !ParsedRecordBatch {
	if data.len == 0 {
		return ParsedRecordBatch{
			records: []
		}
	}

	mut reader := new_reader(data)
	mut records := []domain.Record{}

	// Parse records until the buffer is exhausted.
	// base_timestamp=0 because compressed records use relative timestamp deltas.
	for reader.remaining() > 0 {
		record := parse_record(mut reader, i64(0)) or { break }
		records << record
	}

	last_offset_delta := if records.len > 0 { i32(records.len - 1) } else { i32(0) }

	return ParsedRecordBatch{
		base_offset:            0
		partition_leader_epoch: 0
		magic:                  2
		attributes:             0
		last_offset_delta:      last_offset_delta
		first_timestamp:        0
		max_timestamp:          0
		producer_id:            -1
		producer_epoch:         -1
		base_sequence:          -1
		records:                records
	}
}

/// parse_message_set parses the legacy MessageSet format (v0/v1, magic 0 or 1).
fn parse_message_set(data []u8) !ParsedRecordBatch {
	mut reader := new_reader(data)
	mut records := []domain.Record{}

	// MessageSet is an array of [offset, size, message] entries
	for reader.remaining() > 12 {
		_ := reader.read_i64() or { break }
		message_size := reader.read_i32() or { break }

		if reader.remaining() < int(message_size) {
			break
		}

		// Parse message: [crc, magic, attributes, key, value]
		start_pos := reader.pos
		_ = reader.read_i32() or { break }
		magic := reader.read_i8() or { break }
		_ = reader.read_i8() or { break }

		// v1 adds a timestamp field
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

		// Advance by exactly message_size bytes
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

/// parse_record parses a single Record from a RecordBatch v2.
/// Uses ByteView for zero-copy reads where possible.
fn parse_record(mut reader BinaryReader, base_timestamp i64) !domain.Record {
	_ = reader.read_varint()!
	_ = reader.read_i8()!
	timestamp_delta := reader.read_varint()!
	_ = reader.read_varint()!

	// Key - use a view and copy to owned only when storing
	key_length := reader.read_varint()!
	mut key := []u8{}
	if key_length > 0 {
		key_view := reader.read_bytes_view(int(key_length))!
		key = key_view.to_owned()
	}

	// Value - use a view and copy to owned only when storing
	value_length := reader.read_varint()!
	mut value := []u8{}
	if value_length > 0 {
		value_view := reader.read_bytes_view(int(value_length))!
		value = value_view.to_owned()
	}

	// Headers
	headers_count := reader.read_varint()!
	mut headers := map[string][]u8{}

	for _ in 0 .. headers_count {
		header_key_len := reader.read_varint()!
		mut header_key := ''
		if header_key_len > 0 {
			key_view := reader.read_bytes_view(int(header_key_len))!
			header_key = key_view.to_string()
		}

		header_value_len := reader.read_varint()!
		mut header_value := []u8{}
		if header_value_len > 0 {
			val_view := reader.read_bytes_view(int(header_value_len))!
			header_value = val_view.to_owned()
		}

		if header_key.len > 0 {
			headers[header_key] = header_value
		}
	}

	// Compute timestamp
	ts_millis := base_timestamp + timestamp_delta
	ts := time.unix(ts_millis / 1000)

	return domain.Record{
		key:       key
		value:     value
		headers:   headers
		timestamp: ts
	}
}

/// crc32c_checksum computes a CRC-32C (Castagnoli) checksum using the optimized crc32c module.
fn crc32c_checksum(data []u8) u32 {
	return crc32c.calculate(data)
}

/// encode_record_batch encodes records into the Kafka RecordBatch v2 format.
pub fn encode_record_batch(records []domain.Record, base_offset i64) []u8 {
	if records.len == 0 {
		return []u8{}
	}

	// Estimate per-record size: ~100 bytes overhead + key + value + headers
	// Pre-allocating avoids repeated []u8 growth during encoding.
	mut estimated_record_size := 0
	for r in records {
		estimated_record_size += 30 + r.key.len + r.value.len
	}

	// RecordBatch fixed header = 12 (baseOffset + batchLength) + 9 (epoch + magic + CRC)
	// CRC-covered section = 2+4+8+8+8+2+4+4 = 40 bytes of fixed fields
	estimated_total := 12 + 9 + 40 + estimated_record_size
	mut writer := new_writer_with_capacity(estimated_total)

	// Compute timestamps from records.
	// Use the first record's timestamp as the base; fall back to now if unset.
	first_timestamp := if records[0].timestamp.unix() > 0 {
		records[0].timestamp.unix() * 1000
	} else {
		time.now().unix() * 1000
	}

	// Find the maximum timestamp across all records
	mut max_timestamp := first_timestamp
	for record in records {
		ts := record.timestamp.unix() * 1000
		if ts > max_timestamp {
			max_timestamp = ts
		}
	}

	// Encode records first to determine batch size
	mut records_data := new_writer_with_capacity(estimated_record_size)
	for i, record in records {
		encode_record(mut records_data, record, i, first_timestamp)
	}

	records_bytes := records_data.bytes()

	// Build the data covered by the CRC.
	// Per the Kafka spec, CRC-32C covers from attributes to the end of the batch
	// (not from partitionLeaderEpoch).
	// CRC = crc32c(attributes...end_of_batch)
	crc_fixed_size := 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 // 40 bytes of fixed fields
	mut crc_data := new_writer_with_capacity(crc_fixed_size + records_bytes.len)
	crc_data.write_i16(0)
	crc_data.write_i32(i32(records.len - 1))
	crc_data.write_i64(first_timestamp)
	crc_data.write_i64(max_timestamp)
	crc_data.write_i64(-1)
	crc_data.write_i16(-1)
	crc_data.write_i32(-1)
	crc_data.write_i32(i32(records.len))
	// Note: if delete_horizon_ms is supported (attributes bit 5 = control batch),
	// it must be inserted after baseSequence and before recordCount.
	// delete_horizon_ms is not supported at this time.
	crc_data.write_raw(records_bytes)

	crc_bytes := crc_data.bytes()
	// Compute CRC-32C (Castagnoli) over crc_data
	crc := crc32c_checksum(crc_bytes)

	// RecordBatch header fields
	// batchLength covers from partitionLeaderEpoch to the end of the batch
	batch_length := 4 + 1 + 4 + crc_bytes.len

	writer.write_i64(base_offset)
	writer.write_i32(i32(batch_length))
	writer.write_i32(0)
	writer.write_i8(2)
	writer.write_u32(crc)
	writer.write_raw(crc_bytes)

	return writer.bytes()
}

/// encode_record encodes a single record in RecordBatch v2 format.
fn encode_record(mut writer BinaryWriter, record domain.Record, offset_delta int, base_timestamp i64) {
	// Pre-allocate body capacity: fixed overhead (attributes + timestamps + varint sizes) +
	// actual key/value/header data to avoid incremental []u8 growth.
	estimated_body := 20 + record.key.len + record.value.len
	mut body := new_writer_with_capacity(estimated_body)

	body.write_i8(0)

	// Timestamp delta
	ts_millis := record.timestamp.unix() * 1000
	body.write_varint(ts_millis - base_timestamp)

	// Offset delta
	body.write_varint(i64(offset_delta))

	// Key
	if record.key.len > 0 {
		body.write_varint(i64(record.key.len))
		body.write_raw(record.key)
	} else {
		body.write_varint(-1)
	}

	// Value
	if record.value.len > 0 {
		body.write_varint(i64(record.value.len))
		body.write_raw(record.value)
	} else {
		body.write_varint(-1)
	}

	// Record headers (key-value pairs)
	// Both header key and value are encoded as raw bytes with a VARINT length prefix
	// (not as STRING type — no null marker).
	// Header key format:   VARINT(key_length)   + BYTES(key)
	// Header value format: VARINT(value_length) + BYTES(value)
	body.write_varint(i64(record.headers.len))
	for key, value in record.headers {
		// Header key as bytes (not STRING type — no null marker)
		body.write_varint(i64(key.len))
		body.write_raw(key.bytes())
		// Header value as bytes
		body.write_varint(i64(value.len))
		body.write_raw(value)
	}

	// Write length prefix followed by the record body
	body_bytes := body.bytes()
	writer.write_varint(i64(body_bytes.len))
	writer.write_raw(body_bytes)
}
