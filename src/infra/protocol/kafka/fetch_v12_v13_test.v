module kafka

fn test_fetch_response_v12_v13_encoding() {
	// 16-byte UUID for v13 tests
	uuid := [u8(0xaa), 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
		0x77, 0x88, 0x99]

	resp_v12 := FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           [
			FetchResponseTopic{
				name: 'test-topic-v12'
				// topic_id ignored in v12
				partitions: [
					FetchResponsePartition{
						partition_index:    0
						error_code:         0
						high_watermark:     100
						last_stable_offset: 100
						log_start_offset:   0
						records:            [u8(0x01), 0x02, 0x03] // Dummy records payload
					},
				]
			},
		]
	}

	// Test v12 (Flexible + Name + Compact Records)
	encoded_v12 := resp_v12.encode(12)

	// Manual verification of key fields
	mut offset := 0

	// Header (Response Header v1: CorrelationId + TaggedFields) - NOT handled by encode() but by handler
	// encode() starts with throttle_time_ms
	offset += 4 // throttle_time_ms
	offset += 2 // error_code
	offset += 4 // session_id

	// Topics Array (Compact Array)
	assert encoded_v12[offset] == 2 // Len 1 (1+1)
	offset += 1

	// Topic Name (Compact String)
	assert encoded_v12[offset] == 15 // Len 14 + 1
	offset += 1
	name_bytes := encoded_v12[offset..offset + 14]
	assert name_bytes.bytestr() == 'test-topic-v12'
	offset += 14

	// Partitions Array (Compact Array)
	assert encoded_v12[offset] == 2 // Len 1
	offset += 1

	// Partition Index
	offset += 4
	// Error Code
	offset += 2
	// High Watermark
	offset += 8
	// Last Stable Offset
	offset += 8
	// Log Start Offset
	offset += 8

	// Aborted Transactions (Compact Array) -> Empty (1)
	assert encoded_v12[offset] == 1
	offset += 1

	// Preferred Read Replica (v11+)
	offset += 4

	// Records (COMPACT_RECORDS => Compact Nullable Bytes)
	// Len 3. Compact Len = 4 (3+1). Varint.
	assert encoded_v12[offset] == 4
	offset += 1
	assert encoded_v12[offset] == 0x01
	assert encoded_v12[offset + 1] == 0x02
	assert encoded_v12[offset + 2] == 0x03
	offset += 3

	println('FetchResponse v12 encoding test passed')

	// Test v13 (Flexible + Topic ID + Compact Records)
	resp_v13 := FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           [
			FetchResponseTopic{
				name:       'ignored'
				topic_id:   uuid
				partitions: [
					FetchResponsePartition{
						partition_index: 0
						records:         []u8{} // Empty records
					},
				]
			},
		]
	}

	encoded_v13 := resp_v13.encode(13)
	offset = 0
	offset += 4 + 2 + 4 // throttle, error, session

	// Topics Array
	offset += 1

	// Topic ID (UUID)
	read_uuid := encoded_v13[offset..offset + 16]
	assert read_uuid == uuid
	offset += 16

	// Partitions Array
	offset += 1

	// Partition Index ... to Records
	offset += 4 + 2 + 8 + 8 + 8 + 1 + 4

	// Records (Empty)
	// Compact Nullable Bytes: Len 0 -> Compact Len 1 (0+1)
	assert encoded_v13[offset] == 1
	offset += 1

	println('FetchResponse v13 encoding test passed')
}
