// Tests for Share Group State APIs (KIP-932)
// API Keys 83, 84, 85, 86
module kafka_test

import infra.protocol.kafka

// --- Helper: build a 16-byte mock UUID ---
fn mock_uuid() []u8 {
	return [u8(0x01), 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
		0x0f, 0x10]
}

// --- InitializeShareGroupState (83) ---

fn test_initialize_share_group_state_response_encode_decode() {
	resp := kafka.InitializeShareGroupStateResponse{
		throttle_time_ms: 0
		results:          [
			kafka.InitializeShareGroupStateResult{
				partition:     0
				error_code:    0
				error_message: ''
			},
			kafka.InitializeShareGroupStateResult{
				partition:     1
				error_code:    3
				error_message: 'unknown topic'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	// Verify: throttle_time_ms is first 4 bytes (big-endian, value=0)
	mut reader := kafka.new_reader(encoded)
	throttle := reader.read_i32() or { panic('failed to read throttle') }
	assert throttle == 0

	// Read compact array length
	arr_len := reader.read_compact_array_len() or { panic('failed to read array len') }
	assert arr_len == 2

	// First result
	p0 := reader.read_i32() or { panic('failed') }
	assert p0 == 0
	ec0 := reader.read_i16() or { panic('failed') }
	assert ec0 == 0
}

fn test_initialize_share_group_state_empty_results() {
	resp := kafka.InitializeShareGroupStateResponse{
		throttle_time_ms: 100
		results:          []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := kafka.new_reader(encoded)
	throttle := reader.read_i32() or { panic('failed') }
	assert throttle == 100

	arr_len := reader.read_compact_array_len() or { panic('failed') }
	assert arr_len == 0
}

// --- ReadShareGroupState (84) ---

fn test_read_share_group_state_response_encode() {
	resp := kafka.ReadShareGroupStateResponse{
		throttle_time_ms: 0
		results:          [
			kafka.ReadShareGroupStateTopicResult{
				topic_id:   mock_uuid()
				partitions: [
					kafka.ReadShareGroupStatePartitionResult{
						partition:     0
						state_epoch:   1
						start_offset:  100
						state_batches: [
							kafka.StateBatch{
								first_offset:   100
								last_offset:    109
								delivery_state: 0
								delivery_count: 0
							},
							kafka.StateBatch{
								first_offset:   110
								last_offset:    115
								delivery_state: 1
								delivery_count: 2
							},
						]
						error_code:    0
						error_message: ''
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	// Decode header
	mut reader := kafka.new_reader(encoded)
	throttle := reader.read_i32() or { panic('failed') }
	assert throttle == 0

	// Topics array
	topics_len := reader.read_compact_array_len() or { panic('failed') }
	assert topics_len == 1

	// Topic UUID
	topic_id := reader.read_uuid() or { panic('failed') }
	assert topic_id == mock_uuid()

	// Partitions array
	parts_len := reader.read_compact_array_len() or { panic('failed') }
	assert parts_len == 1

	// Partition
	p := reader.read_i32() or { panic('failed') }
	assert p == 0

	// State epoch
	se := reader.read_i32() or { panic('failed') }
	assert se == 1

	// Start offset
	so := reader.read_i64() or { panic('failed') }
	assert so == 100
}

fn test_read_share_group_state_response_empty_batches() {
	resp := kafka.ReadShareGroupStateResponse{
		throttle_time_ms: 0
		results:          [
			kafka.ReadShareGroupStateTopicResult{
				topic_id:   mock_uuid()
				partitions: [
					kafka.ReadShareGroupStatePartitionResult{
						partition:     0
						state_epoch:   0
						start_offset:  -1
						state_batches: []
						error_code:    69
						error_message: 'not found'
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0
}

// --- WriteShareGroupState (85) ---

fn test_write_share_group_state_response_encode() {
	resp := kafka.WriteShareGroupStateResponse{
		throttle_time_ms: 0
		results:          [
			kafka.WriteShareGroupStateResult{
				partition:     0
				error_code:    0
				error_message: ''
			},
			kafka.WriteShareGroupStateResult{
				partition:     1
				error_code:    56
				error_message: 'storage error'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := kafka.new_reader(encoded)
	throttle := reader.read_i32() or { panic('failed') }
	assert throttle == 0

	arr_len := reader.read_compact_array_len() or { panic('failed') }
	assert arr_len == 2
}

// --- DeleteShareGroupState (86) ---

fn test_delete_share_group_state_response_encode() {
	resp := kafka.DeleteShareGroupStateResponse{
		throttle_time_ms: 0
		results:          [
			kafka.DeleteShareGroupStateResult{
				partition:     0
				error_code:    0
				error_message: ''
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := kafka.new_reader(encoded)
	throttle := reader.read_i32() or { panic('failed') }
	assert throttle == 0

	arr_len := reader.read_compact_array_len() or { panic('failed') }
	assert arr_len == 1

	p := reader.read_i32() or { panic('failed') }
	assert p == 0

	ec := reader.read_i16() or { panic('failed') }
	assert ec == 0
}

// --- Parser roundtrip tests ---

fn build_initialize_request() []u8 {
	mut writer := kafka.new_writer()

	// group_id (compact string)
	writer.write_compact_string('test-share-group')

	// topics array (compact array len = 1)
	writer.write_compact_array_len(1)

	// topic_id (UUID 16 bytes)
	writer.write_uuid(mock_uuid())

	// partitions array (compact array len = 1)
	writer.write_compact_array_len(1)

	// partition (i32)
	writer.write_i32(0)
	// state_epoch (i32)
	writer.write_i32(1)
	// start_offset (i64)
	writer.write_i64(100)
	// tagged fields for partition
	writer.write_tagged_fields()

	// tagged fields for topic
	writer.write_tagged_fields()

	// tagged fields for request
	writer.write_tagged_fields()

	return writer.bytes()
}

fn test_initialize_share_group_state_parser_roundtrip() {
	data := build_initialize_request()
	mut reader := kafka.new_reader(data)

	// Verify the parser can read what we encoded
	// We read group_id
	group_id := reader.read_compact_string() or { panic('failed to read group_id') }
	assert group_id == 'test-share-group'

	// Read topics array
	topics_len := reader.read_compact_array_len() or { panic('failed') }
	assert topics_len == 1

	// Read topic_id
	topic_id := reader.read_uuid() or { panic('failed') }
	assert topic_id == mock_uuid()

	// Read partitions
	parts_len := reader.read_compact_array_len() or { panic('failed') }
	assert parts_len == 1

	p := reader.read_i32() or { panic('failed') }
	assert p == 0

	se := reader.read_i32() or { panic('failed') }
	assert se == 1

	so := reader.read_i64() or { panic('failed') }
	assert so == 100
}

fn build_read_request() []u8 {
	mut writer := kafka.new_writer()

	// group_id
	writer.write_compact_string('test-share-group')

	// topics array (1 topic)
	writer.write_compact_array_len(1)

	// topic_id
	writer.write_uuid(mock_uuid())

	// partitions array (2 partitions)
	writer.write_compact_array_len(2)

	// partition 0
	writer.write_i32(0)
	writer.write_i32(1) // leader_epoch
	writer.write_tagged_fields()

	// partition 1
	writer.write_i32(1)
	writer.write_i32(1) // leader_epoch
	writer.write_tagged_fields()

	// tagged fields for topic
	writer.write_tagged_fields()

	// tagged fields for request
	writer.write_tagged_fields()

	return writer.bytes()
}

fn test_read_share_group_state_parser_roundtrip() {
	data := build_read_request()
	mut reader := kafka.new_reader(data)

	group_id := reader.read_compact_string() or { panic('failed') }
	assert group_id == 'test-share-group'

	topics_len := reader.read_compact_array_len() or { panic('failed') }
	assert topics_len == 1

	topic_id := reader.read_uuid() or { panic('failed') }
	assert topic_id == mock_uuid()

	parts_len := reader.read_compact_array_len() or { panic('failed') }
	assert parts_len == 2

	p0 := reader.read_i32() or { panic('failed') }
	assert p0 == 0

	le0 := reader.read_i32() or { panic('failed') }
	assert le0 == 1
}

fn build_write_request() []u8 {
	mut writer := kafka.new_writer()

	// group_id
	writer.write_compact_string('test-share-group')

	// topics array (1 topic)
	writer.write_compact_array_len(1)

	// topic_id
	writer.write_uuid(mock_uuid())

	// partitions array (1 partition)
	writer.write_compact_array_len(1)

	// partition
	writer.write_i32(0)
	// state_epoch
	writer.write_i32(2)
	// leader_epoch
	writer.write_i32(1)
	// start_offset
	writer.write_i64(50)
	// delivery_complete_count
	writer.write_i32(10)

	// state_batches array (2 batches)
	writer.write_compact_array_len(2)

	// batch 1: offsets 50-59, available (0), delivery count 0
	writer.write_i64(50) // first_offset
	writer.write_i64(59) // last_offset
	writer.write_i8(0) // delivery_state (available)
	writer.write_i16(0) // delivery_count
	writer.write_tagged_fields()

	// batch 2: offsets 60-64, acquired (1), delivery count 1
	writer.write_i64(60)
	writer.write_i64(64)
	writer.write_i8(1) // delivery_state (acquired)
	writer.write_i16(1) // delivery_count
	writer.write_tagged_fields()

	// tagged fields for partition
	writer.write_tagged_fields()

	// tagged fields for topic
	writer.write_tagged_fields()

	// tagged fields for request
	writer.write_tagged_fields()

	return writer.bytes()
}

fn test_write_share_group_state_parser_roundtrip() {
	data := build_write_request()
	mut reader := kafka.new_reader(data)

	group_id := reader.read_compact_string() or { panic('failed') }
	assert group_id == 'test-share-group'

	topics_len := reader.read_compact_array_len() or { panic('failed') }
	assert topics_len == 1

	topic_id := reader.read_uuid() or { panic('failed') }
	assert topic_id == mock_uuid()

	parts_len := reader.read_compact_array_len() or { panic('failed') }
	assert parts_len == 1

	p := reader.read_i32() or { panic('failed') }
	assert p == 0

	se := reader.read_i32() or { panic('failed') }
	assert se == 2

	le := reader.read_i32() or { panic('failed') }
	assert le == 1

	so := reader.read_i64() or { panic('failed') }
	assert so == 50

	dcc := reader.read_i32() or { panic('failed') }
	assert dcc == 10

	batches_len := reader.read_compact_array_len() or { panic('failed') }
	assert batches_len == 2

	// Batch 1
	fo1 := reader.read_i64() or { panic('failed') }
	assert fo1 == 50
	lo1 := reader.read_i64() or { panic('failed') }
	assert lo1 == 59
	ds1 := reader.read_i8() or { panic('failed') }
	assert ds1 == 0
	dc1 := reader.read_i16() or { panic('failed') }
	assert dc1 == 0
}

fn build_delete_request() []u8 {
	mut writer := kafka.new_writer()

	// group_id
	writer.write_compact_string('test-share-group')

	// topics array (1 topic)
	writer.write_compact_array_len(1)

	// topic_id
	writer.write_uuid(mock_uuid())

	// partitions array (3 partitions)
	writer.write_compact_array_len(3)
	writer.write_i32(0)
	writer.write_i32(1)
	writer.write_i32(2)

	// tagged fields for topic
	writer.write_tagged_fields()

	// tagged fields for request
	writer.write_tagged_fields()

	return writer.bytes()
}

fn test_delete_share_group_state_parser_roundtrip() {
	data := build_delete_request()
	mut reader := kafka.new_reader(data)

	group_id := reader.read_compact_string() or { panic('failed') }
	assert group_id == 'test-share-group'

	topics_len := reader.read_compact_array_len() or { panic('failed') }
	assert topics_len == 1

	topic_id := reader.read_uuid() or { panic('failed') }
	assert topic_id == mock_uuid()

	parts_len := reader.read_compact_array_len() or { panic('failed') }
	assert parts_len == 3

	p0 := reader.read_i32() or { panic('failed') }
	assert p0 == 0
	p1 := reader.read_i32() or { panic('failed') }
	assert p1 == 1
	p2 := reader.read_i32() or { panic('failed') }
	assert p2 == 2
}

// --- StateBatch type tests ---

fn test_state_batch_struct() {
	batch := kafka.StateBatch{
		first_offset:   100
		last_offset:    109
		delivery_state: 0
		delivery_count: 0
	}
	assert batch.first_offset == 100
	assert batch.last_offset == 109
	assert batch.delivery_state == 0
	assert batch.delivery_count == 0
}

fn test_state_batch_acquired_with_delivery_count() {
	batch := kafka.StateBatch{
		first_offset:   200
		last_offset:    205
		delivery_state: 1 // acquired
		delivery_count: 3
	}
	assert batch.first_offset == 200
	assert batch.last_offset == 205
	assert batch.delivery_state == 1
	assert batch.delivery_count == 3
}
