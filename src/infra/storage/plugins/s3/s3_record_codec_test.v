// Unit tests for S3 record codec - multi-segment decode support
module s3

import time

fn test_decode_multi_segment_concat() {
	// Two segments concatenated should decode all records from both segments.
	// Segment A: 2 records, Segment B: 2 records -> total 4 records.
	seg_a_records := [
		StoredRecord{
			offset:    0
			timestamp: time.unix_milli(1000)
			key:       'key-a0'.bytes()
			value:     'val-a0'.bytes()
			headers:   map[string][]u8{}
		},
		StoredRecord{
			offset:    1
			timestamp: time.unix_milli(2000)
			key:       'key-a1'.bytes()
			value:     'val-a1'.bytes()
			headers:   map[string][]u8{}
		},
	]

	seg_b_records := [
		StoredRecord{
			offset:    2
			timestamp: time.unix_milli(3000)
			key:       'key-b0'.bytes()
			value:     'val-b0'.bytes()
			headers:   map[string][]u8{}
		},
		StoredRecord{
			offset:    3
			timestamp: time.unix_milli(4000)
			key:       'key-b1'.bytes()
			value:     'val-b1'.bytes()
			headers:   map[string][]u8{}
		},
	]

	// Encode each segment separately, then concatenate
	encoded_a := encode_stored_records(seg_a_records)
	encoded_b := encode_stored_records(seg_b_records)

	mut concat := []u8{cap: encoded_a.len + encoded_b.len}
	concat << encoded_a
	concat << encoded_b

	// Decode the concatenated data
	decoded := decode_stored_records(concat)

	// All 4 records must be returned
	assert decoded.len == 4, 'expected 4 records, got ${decoded.len}'
	assert decoded[0].offset == 0
	assert decoded[1].offset == 1
	assert decoded[2].offset == 2
	assert decoded[3].offset == 3
	assert decoded[0].key == 'key-a0'.bytes()
	assert decoded[2].key == 'key-b0'.bytes()
}

fn test_decode_single_segment_still_works() {
	// Backward compatibility: single segment with one count header
	records := [
		StoredRecord{
			offset:    10
			timestamp: time.unix_milli(5000)
			key:       'single-key'.bytes()
			value:     'single-val'.bytes()
			headers:   map[string][]u8{}
		},
	]

	encoded := encode_stored_records(records)
	decoded := decode_stored_records(encoded)

	assert decoded.len == 1
	assert decoded[0].offset == 10
	assert decoded[0].key == 'single-key'.bytes()
	assert decoded[0].value == 'single-val'.bytes()
}

fn test_decode_three_segments_concat() {
	// Three segments concatenated
	seg1 := [
		StoredRecord{
			offset:    0
			timestamp: time.unix_milli(100)
			key:       'k1'.bytes()
			value:     'v1'.bytes()
			headers:   map[string][]u8{}
		},
	]
	seg2 := [
		StoredRecord{
			offset:    1
			timestamp: time.unix_milli(200)
			key:       'k2'.bytes()
			value:     'v2'.bytes()
			headers:   map[string][]u8{}
		},
	]
	seg3 := [
		StoredRecord{
			offset:    2
			timestamp: time.unix_milli(300)
			key:       'k3'.bytes()
			value:     'v3'.bytes()
			headers:   map[string][]u8{}
		},
	]

	mut concat := []u8{}
	concat << encode_stored_records(seg1)
	concat << encode_stored_records(seg2)
	concat << encode_stored_records(seg3)

	decoded := decode_stored_records(concat)
	assert decoded.len == 3
	assert decoded[0].offset == 0
	assert decoded[1].offset == 1
	assert decoded[2].offset == 2
}

fn test_decode_with_headers_multi_segment() {
	// Verify headers survive multi-segment concat decode
	seg_a := [
		StoredRecord{
			offset:    0
			timestamp: time.unix_milli(100)
			key:       'hk'.bytes()
			value:     'hv'.bytes()
			headers:   {
				'content-type': 'application/json'.bytes()
				'x-custom':     'test'.bytes()
			}
		},
	]
	seg_b := [
		StoredRecord{
			offset:    1
			timestamp: time.unix_milli(200)
			key:       'hk2'.bytes()
			value:     'hv2'.bytes()
			headers:   {
				'encoding': 'utf-8'.bytes()
			}
		},
	]

	mut concat := []u8{}
	concat << encode_stored_records(seg_a)
	concat << encode_stored_records(seg_b)

	decoded := decode_stored_records(concat)
	assert decoded.len == 2
	assert decoded[0].headers.len == 2
	assert decoded[0].headers['content-type'] == 'application/json'.bytes()
	assert decoded[1].headers.len == 1
	assert decoded[1].headers['encoding'] == 'utf-8'.bytes()
}

fn test_decode_empty_data() {
	decoded := decode_stored_records([]u8{})
	assert decoded.len == 0
}

fn test_decode_encode_roundtrip() {
	// Encode then decode should produce identical records
	records := [
		StoredRecord{
			offset:           42
			timestamp:        time.unix_milli(99999)
			key:              'roundtrip-key'.bytes()
			value:            'roundtrip-val'.bytes()
			headers:          map[string][]u8{}
			compression_type: 1
		},
	]

	encoded := encode_stored_records(records)
	decoded := decode_stored_records(encoded)

	assert decoded.len == 1
	assert decoded[0].offset == 42
	assert decoded[0].compression_type == 1
	assert decoded[0].key == 'roundtrip-key'.bytes()
}
