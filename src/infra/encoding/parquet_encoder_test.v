// Tests for the real Parquet encoder implementation.
module encoding

import time
import domain

// verify_parquet_magic checks that a byte slice starts and ends with PAR1.
fn verify_parquet_magic(data []u8) bool {
	if data.len < 8 {
		return false
	}
	start := data[0] == u8(`P`) && data[1] == u8(`A`) && data[2] == u8(`R`) && data[3] == u8(`1`)
	end := data[data.len - 4] == u8(`P`) && data[data.len - 3] == u8(`A`)
		&& data[data.len - 2] == u8(`R`) && data[data.len - 1] == u8(`1`)
	return start && end
}

// make_test_record creates a domain.Record for testing.
fn make_test_record(key string, value string, ts i64) domain.Record {
	return domain.Record{
		key:       key.bytes()
		value:     value.bytes()
		headers:   {}
		timestamp: time.unix_milli(ts)
	}
}

fn test_thrift_compact_varint() {
	mut w := new_thrift_writer()
	w.write_varint32(0)
	assert w.bytes() == [u8(0x00)], 'varint32(0) should be 0x00'

	mut w2 := new_thrift_writer()
	w2.write_varint32(127)
	assert w2.bytes() == [u8(0x7F)], 'varint32(127) should be 0x7F'

	mut w3 := new_thrift_writer()
	w3.write_varint32(128)
	assert w3.bytes() == [u8(0x80), u8(0x01)], 'varint32(128) should be 0x80 0x01'

	mut w4 := new_thrift_writer()
	w4.write_varint32(300)
	// 300 = 0b100101100 -> 7-bit groups: 0b0000010 0b0101100 -> 0xAC 0x02
	assert w4.bytes() == [u8(0xAC), u8(0x02)], 'varint32(300) should be 0xAC 0x02'
}

fn test_thrift_compact_zigzag() {
	// zigzag32: 0->0, -1->1, 1->2, -2->3
	assert zigzag32(0) == 0
	assert zigzag32(-1) == 1
	assert zigzag32(1) == 2
	assert zigzag32(-2) == 3
	assert zigzag32(2147483647) == 4294967294

	// zigzag64
	assert zigzag64(i64(0)) == u64(0)
	assert zigzag64(i64(-1)) == u64(1)
	assert zigzag64(i64(1)) == u64(2)
}

fn test_thrift_compact_i32_field() {
	mut w := new_thrift_writer()
	w.write_struct_begin()
	w.write_i32(1, 42)
	w.write_struct_end()
	b := w.bytes()
	// Field header for field_id=1, delta=1 from 0: upper nibble=1, lower nibble=i32_type(5) -> 0x15
	// zigzag(42) = 84 = 0x54
	// stop = 0x00
	assert b.len >= 3, 'i32 field encoding should be at least 3 bytes'
	assert b[0] == 0x15, 'field header should be 0x15 (delta=1, type=i32)'
	assert b[1] == 0x54, 'zigzag(42)=84=0x54'
	assert b[b.len - 1] == 0x00, 'stop byte should be 0x00'
}

fn test_thrift_compact_string_field() {
	mut w := new_thrift_writer()
	w.write_struct_begin()
	w.write_string(1, 'hi')
	w.write_struct_end()
	b := w.bytes()
	// Field header: delta=1, type=binary(8) -> 0x18
	// varint length=2 -> 0x02
	// bytes: 'h'=0x68, 'i'=0x69
	// stop: 0x00
	assert b == [u8(0x18), u8(0x02), u8(0x68), u8(0x69), u8(0x00)], 'string field encoding mismatch'
}

fn test_parquet_encoder_empty_records() {
	mut enc := new_parquet_encoder('uncompressed', 128) or { panic(err) }
	if _, _ := enc.encode() {
		assert false, 'empty encoder should return error but succeeded'
	} else {
		assert err == error('no records to encode'), 'empty encoder should return error'
	}
}

fn test_parquet_encoder_single_record() {
	mut enc := new_parquet_encoder('uncompressed', 128) or { panic(err) }
	rec := make_test_record('key1', 'value1', 1700000000000)
	enc.add_record('test-topic', 0, rec, 0) or { panic(err) }

	data, meta := enc.encode() or { panic(err) }

	// Validate magic bytes
	assert verify_parquet_magic(data), 'file should start and end with PAR1'
	assert data.len >= 12, 'parquet file must be at least 12 bytes'
	assert meta.num_rows == 1, 'num_rows should be 1'
	assert meta.row_groups.len == 1, 'should have 1 row group'
}

fn test_parquet_encoder_multiple_records() {
	mut enc := new_parquet_encoder('uncompressed', 128) or { panic(err) }

	records := [
		make_test_record('k1', 'v1', 1700000001000),
		make_test_record('k2', 'v2', 1700000002000),
		make_test_record('k3', 'v3', 1700000003000),
	]
	enc.add_records('my-topic', 1, records, 100) or { panic(err) }

	data, meta := enc.encode() or { panic(err) }

	assert verify_parquet_magic(data), 'should have valid PAR1 magic'
	assert meta.num_rows == 3, 'num_rows should be 3'
	assert meta.schema.columns.len == 7, 'schema should have 7 columns'
}

fn test_parquet_encoder_footer_length() {
	mut enc := new_parquet_encoder('uncompressed', 128) or { panic(err) }
	rec := make_test_record('key', 'value', 1700000000000)
	enc.add_record('topic', 0, rec, 5) or { panic(err) }

	data, _ := enc.encode() or { panic(err) }

	// Footer length is stored at bytes [len-8..len-4] as little-endian u32
	footer_len := u32(data[data.len - 8]) | (u32(data[data.len - 7]) << 8) | (u32(data[data.len - 6]) << 16) | (u32(data[data.len - 5]) << 24)

	// Validate footer length is consistent with file size
	// file = magic(4) + column_data + footer + footer_len(4) + magic(4)
	computed_footer_end := data.len - 8 // footer ends before footer_len bytes + magic
	computed_footer_start := computed_footer_end - int(footer_len)
	assert computed_footer_start > 4, 'footer should start after magic'
	assert footer_len > 0, 'footer length should be non-zero'
}

fn test_parquet_encoder_reset() {
	mut enc := new_parquet_encoder('uncompressed', 128) or { panic(err) }
	rec := make_test_record('k', 'v', 1700000000000)
	enc.add_record('t', 0, rec, 0) or { panic(err) }
	assert enc.record_count() == 1

	enc.reset()
	assert enc.record_count() == 0

	if _, _ := enc.encode() {
		assert false, 'after reset, encode should fail but succeeded'
	} else {
		assert err == error('no records to encode'), 'after reset, encode should fail'
	}
}

fn test_parquet_file_structure_integrity() {
	mut enc := new_parquet_encoder('uncompressed', 128) or { panic(err) }

	// Add records with various field values
	records := [
		domain.Record{
			key:       'kafka-key'.bytes()
			value:     '{"event":"click","user":123}'.bytes()
			headers:   {
				'content-type': 'application/json'.bytes()
			}
			timestamp: time.unix_milli(1700000000000)
		},
		domain.Record{
			key:       []u8{}
			value:     'plain text value'.bytes()
			headers:   {}
			timestamp: time.unix_milli(1700000001000)
		},
	]

	enc.add_records('events', 0, records, 0) or { panic(err) }
	data, meta := enc.encode() or { panic(err) }

	// File size sanity check
	assert data.len > 20, 'file should be larger than minimal parquet file'
	assert verify_parquet_magic(data), 'magic bytes must be valid'
	assert meta.compression == .uncompressed
	assert meta.num_rows == 2

	// Last 4 bytes = magic
	assert data[data.len - 4] == u8(`P`)
	assert data[data.len - 3] == u8(`A`)
	assert data[data.len - 2] == u8(`R`)
	assert data[data.len - 1] == u8(`1`)

	// First 4 bytes = magic
	assert data[0] == u8(`P`)
	assert data[1] == u8(`A`)
	assert data[2] == u8(`R`)
	assert data[3] == u8(`1`)
}

fn test_encode_batch() {
	records := [
		ParquetRecord{
			offset:    0
			timestamp: 1700000000000
			topic:     'test'
			partition: 0
			key:       'k'.bytes()
			value:     'v'.bytes()
			headers:   '{}'
		},
		ParquetRecord{
			offset:    1
			timestamp: 1700000001000
			topic:     'test'
			partition: 0
			key:       'k2'.bytes()
			value:     'v2'.bytes()
			headers:   '{}'
		},
	]

	data, meta := encode_batch(records, .uncompressed) or { panic(err) }
	assert verify_parquet_magic(data), 'batch encode should produce valid parquet'
	assert meta.num_rows == 2
}

fn test_plain_int64_encoding() {
	values := [i64(100), i64(200), i64(300)]
	page := encode_plain_int64_page(values, 0, parquet_compression_uncompressed)

	// Page must be non-empty and contain the values
	assert page.len > 0, 'page should have data'

	// Extract the raw data portion (after PageHeader thrift encoding)
	// We can't easily parse the header, but we can verify the values are present
	// by searching for known byte patterns of the first value (100 LE = 64 00 00 00 00 00 00 00)
	mut found := false
	for i := 0; i < page.len - 7; i++ {
		if page[i] == 0x64 && page[i + 1] == 0x00 && page[i + 2] == 0x00 && page[i + 3] == 0x00
			&& page[i + 4] == 0x00 && page[i + 5] == 0x00 && page[i + 6] == 0x00
			&& page[i + 7] == 0x00 {
			found = true
			break
		}
	}
	assert found, 'value 100 (0x64) should appear in page data'
}

fn test_default_parquet_schema() {
	schema := default_parquet_schema()
	assert schema.columns.len == 7
	assert schema.columns[0].name == 'offset'
	assert schema.columns[0].typ == .int64
	assert schema.columns[0].required == true
	assert schema.columns[1].name == 'timestamp'
	assert schema.columns[1].typ == .timestamp_millis
	assert schema.columns[4].name == 'key'
	assert schema.columns[4].required == false
}
