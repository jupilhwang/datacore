// Tests for RecordIndex generation, Range-request support, and compaction index preservation
module s3

import time

// test_encode_with_index_byte_positions_enable_single_record_decode verifies
// that byte positions from encode_stored_records_with_index allow
// decode_single_record to locate each record precisely.
fn test_encode_with_index_byte_positions_enable_single_record_decode() {
	records := [
		StoredRecord{
			offset:    0
			timestamp: time.unix_milli(1000)
			key:       'key0'.bytes()
			value:     'value0'.bytes()
			headers:   map[string][]u8{}
		},
		StoredRecord{
			offset:    1
			timestamp: time.unix_milli(2000)
			key:       'key1'.bytes()
			value:     'value1'.bytes()
			headers:   map[string][]u8{}
		},
		StoredRecord{
			offset:    2
			timestamp: time.unix_milli(3000)
			key:       'key2'.bytes()
			value:     'value2'.bytes()
			headers:   {
				'h1': 'v1'.bytes()
			}
		},
	]

	data, index := encode_stored_records_with_index(records)

	assert index.len == 3, 'expected 3 index entries, got ${index.len}'
	for i, ri in index {
		assert ri.offset == records[i].offset
		rec, next_pos := decode_single_record(data, int(ri.byte_position))
		assert next_pos >= 0, 'decode failed at byte_position ${ri.byte_position}'
		assert rec.offset == records[i].offset
		assert rec.key == records[i].key
	}
}

// test_log_segment_find_byte_range_with_record_index verifies that
// a LogSegment with record_index enables precise Range Request targeting.
fn test_log_segment_find_byte_range_with_record_index() {
	records := [
		StoredRecord{
			offset:    100
			timestamp: time.unix_milli(1000)
			key:       'k100'.bytes()
			value:     'v100'.bytes()
			headers:   map[string][]u8{}
		},
		StoredRecord{
			offset:    101
			timestamp: time.unix_milli(2000)
			key:       'k101'.bytes()
			value:     'v101'.bytes()
			headers:   map[string][]u8{}
		},
		StoredRecord{
			offset:    102
			timestamp: time.unix_milli(3000)
			key:       'k102'.bytes()
			value:     'v102'.bytes()
			headers:   map[string][]u8{}
		},
	]

	data, index := encode_stored_records_with_index(records)

	seg := LogSegment{
		start_offset: 100
		end_offset:   102
		key:          'test-segment.bin'
		size_bytes:   i64(data.len)
		record_index: index
	}

	byte_start, byte_end := seg.find_byte_range(101, 4096)
	assert byte_start >= 0, 'byte_start should be non-negative'
	assert byte_end >= byte_start, 'byte_end should be >= byte_start'

	rec, _ := decode_single_record(data, int(byte_start))
	assert rec.offset == 101, 'expected offset 101, got ${rec.offset}'
}

// test_compaction_reencode_produces_valid_record_index verifies that
// decoding concatenated segments and re-encoding with index produces
// valid byte positions for all records.
fn test_compaction_reencode_produces_valid_record_index() {
	seg1 := [
		StoredRecord{
			offset:    0
			timestamp: time.unix_milli(100)
			key:       'k0'.bytes()
			value:     'v0'.bytes()
			headers:   map[string][]u8{}
		},
	]
	seg2 := [
		StoredRecord{
			offset:    1
			timestamp: time.unix_milli(200)
			key:       'k1'.bytes()
			value:     'v1'.bytes()
			headers:   map[string][]u8{}
		},
	]

	enc1 := encode_stored_records(seg1)
	enc2 := encode_stored_records(seg2)

	mut merged := []u8{}
	merged << enc1
	merged << enc2

	all_records := decode_stored_records(merged)
	assert all_records.len == 2

	final_data, final_index := encode_stored_records_with_index(all_records)
	assert final_index.len == 2, 'expected 2 index entries after re-encode'

	for i, ri in final_index {
		rec, pos := decode_single_record(final_data, int(ri.byte_position))
		assert pos >= 0
		assert rec.offset == all_records[i].offset
	}
}

// test_build_merged_record_index_adjusts_byte_positions verifies that
// build_merged_record_index correctly offsets byte positions by cumulative
// segment sizes for server-side copy compaction.
fn test_build_merged_record_index_adjusts_byte_positions() {
	segments := [
		LogSegment{
			start_offset: 0
			end_offset:   1
			size_bytes:   100
			record_index: [
				RecordIndex{
					offset:        0
					byte_position: 4
				},
				RecordIndex{
					offset:        1
					byte_position: 50
				},
			]
		},
		LogSegment{
			start_offset: 2
			end_offset:   3
			size_bytes:   80
			record_index: [
				RecordIndex{
					offset:        2
					byte_position: 4
				},
				RecordIndex{
					offset:        3
					byte_position: 40
				},
			]
		},
	]

	merged := build_merged_record_index(segments)

	assert merged.len == 4, 'expected 4 merged entries, got ${merged.len}'
	assert merged[0].byte_position == 4
	assert merged[1].byte_position == 50
	assert merged[2].byte_position == 104
	assert merged[3].byte_position == 140
}

// test_build_merged_record_index_empty_segments returns empty for no input.
fn test_build_merged_record_index_empty_segments() {
	merged := build_merged_record_index([]LogSegment{})
	assert merged.len == 0
}

// test_build_merged_record_index_skips_segments_without_index verifies
// that segments without record_index are handled gracefully.
fn test_build_merged_record_index_skips_segments_without_index() {
	segments := [
		LogSegment{
			start_offset: 0
			end_offset:   1
			size_bytes:   100
			record_index: []RecordIndex{}
		},
		LogSegment{
			start_offset: 2
			end_offset:   3
			size_bytes:   80
			record_index: [
				RecordIndex{
					offset:        2
					byte_position: 4
				},
			]
		},
	]

	merged := build_merged_record_index(segments)
	assert merged.len == 1
	assert merged[0].offset == 2
	assert merged[0].byte_position == 104
}
