module kafka

// 빈 배열 인코딩 검증:
// eligible_leader_replicas와 last_known_elr가 []i32{}(빈 배열)일 때
// uvarint(1) = 0x01 으로 인코딩되어야 함 (Kafka Java 클라이언트 호환)
fn test_describe_topic_partitions_empty_elr_encodes_as_uvarint_one() {
	resp := DescribeTopicPartitionsResponse{
		throttle_time_ms: 0
		topics:           [
			DescribeTopicPartitionsTopicResp{
				error_code:                  0
				name:                        'test-topic'
				topic_id:                    []u8{len: 16}
				is_internal:                 false
				partitions:                  [
					DescribeTopicPartitionsPartition{
						error_code:               0
						partition_index:          0
						leader_id:                1
						leader_epoch:             0
						replica_nodes:            [i32(1)]
						isr_nodes:                [i32(1)]
						eligible_leader_replicas: []i32{} // 빈 배열 → uvarint(1) = 0x01
						last_known_elr:           []i32{} // 빈 배열 → uvarint(1) = 0x01
						offline_replicas:         []
					},
				]
				topic_authorized_operations: -2147483648
			},
		]
		next_cursor:      none
	}

	encoded := resp.encode(0)

	// 인코딩된 바이트를 파싱하여 eligible_leader_replicas 빈 배열 값 검증
	mut reader := new_reader(encoded)

	// throttle_time_ms (4 bytes)
	_ = reader.read_i32()!

	// topics compact array len (uvarint, N+1)
	topics_len := reader.read_compact_array_len()!
	assert topics_len == 1, '토픽 배열 길이가 1이어야 함'

	// error_code (2 bytes)
	_ = reader.read_i16()!

	// name (nullable compact string)
	_ = reader.read_compact_nullable_string()!

	// topic_id (16 bytes UUID)
	_ = reader.read_uuid()!

	// is_internal (1 byte)
	_ = reader.read_i8()!

	// partitions compact array len
	parts_len := reader.read_compact_array_len()!
	assert parts_len == 1, '파티션 배열 길이가 1이어야 함'

	// error_code
	_ = reader.read_i16()!
	// partition_index
	_ = reader.read_i32()!
	// leader_id
	_ = reader.read_i32()!
	// leader_epoch
	_ = reader.read_i32()!

	// replica_nodes (compact array)
	rn_len := reader.read_compact_array_len()!
	assert rn_len == 1
	_ = reader.read_i32()!

	// isr_nodes (compact array)
	isr_len := reader.read_compact_array_len()!
	assert isr_len == 1
	_ = reader.read_i32()!

	// eligible_leader_replicas (compact array) - 빈 배열이므로 uvarint(1) = 0x01
	// read_compact_array_len()이 0(빈 배열)을 반환해야 함
	elr_len := reader.read_compact_array_len()!
	// 빈 compact array는 uvarint(1)으로 인코딩되며 read_compact_array_len()은 0을 반환
	assert elr_len == 0, 'eligible_leader_replicas 빈 배열은 uvarint(1)으로 인코딩되어야 함, read_compact_array_len() == 0 (현재: ${elr_len})'

	// last_known_elr (compact array) - 빈 배열이므로 uvarint(1) = 0x01
	lke_len := reader.read_compact_array_len()!
	assert lke_len == 0, 'last_known_elr 빈 배열은 uvarint(1)으로 인코딩되어야 함, read_compact_array_len() == 0 (현재: ${lke_len})'
}

// next_cursor null 인코딩 검증 (구형 - 삭제 예정):
// 이 테스트는 이전 구현을 검증하는 것으로, 새 테스트로 대체됨
fn test_describe_topic_partitions_null_cursor_encodes_as_uvarint_zero() {
	resp := DescribeTopicPartitionsResponse{
		throttle_time_ms: 0
		topics:           []
		next_cursor:      none
	}

	encoded := resp.encode(0)
	mut reader := new_reader(encoded)

	// throttle_time_ms
	_ = reader.read_i32()!

	// topics (empty compact array, len=0 → uvarint(1))
	topics_len := reader.read_compact_array_len()!
	assert topics_len == 0

	// next_cursor null → nullable struct null indicator: -1 (0xFF)
	cursor_byte := reader.read_i8()!
	assert cursor_byte == i8(-1), 'next_cursor null은 0xFF(-1)로 인코딩되어야 함 (현재: ${cursor_byte})'
}

// next_cursor present 인코딩 검증:
// next_cursor가 present일 때 write_i8(1) → 0x01 이어야 하고, 이후 필드 인코딩이 따라와야 함
fn test_describe_topic_partitions_present_cursor_encodes_correctly() {
	resp := DescribeTopicPartitionsResponse{
		throttle_time_ms: 0
		topics:           []
		next_cursor:      DescribeTopicPartitionsCursor{
			topic_name:      'my-topic'
			partition_index: 3
		}
	}

	encoded := resp.encode(0)
	mut reader := new_reader(encoded)

	// throttle_time_ms
	_ = reader.read_i32()!

	// topics (empty compact array, len=0 → uvarint(1))
	topics_len := reader.read_compact_array_len()!
	assert topics_len == 0

	// next_cursor present → nullable struct present indicator: 1 (0x01)
	cursor_byte := reader.read_i8()!
	assert cursor_byte == i8(1), 'next_cursor present는 0x01(1)로 인코딩되어야 함 (현재: ${cursor_byte})'

	// topic_name compact string
	topic_name := reader.read_compact_string()!
	assert topic_name == 'my-topic', 'topic_name이 my-topic이어야 함 (현재: ${topic_name})'

	// partition_index i32
	partition_index := reader.read_i32()!
	assert partition_index == 3, 'partition_index가 3이어야 함 (현재: ${partition_index})'
}

// [RED] cursor null 파싱 검증:
// Kafka Java 클라이언트가 보내는 null cursor는 0xFF(-1) 1바이트로 인코딩됨.
// read_compact_array_len()이 아닌 read_i8()로 읽어야 올바르게 파싱됨.
fn test_parse_request_with_null_cursor() {
	// Kafka Java 클라이언트가 보내는 cursor null 요청 바이트 시퀀스 구성:
	// - topics array: uvarint(2) = [0x02] → 1개 토픽
	// - topic name: compact string "test" = [0x05, 0x74, 0x65, 0x73, 0x74]
	// - topic tagged_fields: [0x00]
	// - response_partition_limit: i32(100) = [0x00, 0x00, 0x00, 0x64]
	// - cursor null indicator: i8(-1) = [0xFF]
	// - outer tagged_fields: [0x00]
	body := [
		// topics compact array len: 1개 → uvarint(2)
		u8(0x02),
		// topic name "test": compact string (len+1=5, then "test")
		u8(0x05),
		u8(0x74),
		u8(0x65),
		u8(0x73),
		u8(0x74),
		// topic tagged_fields
		u8(0x00),
		// response_partition_limit = 100 (big-endian i32)
		u8(0x00),
		u8(0x00),
		u8(0x00),
		u8(0x64),
		// cursor null indicator: 0xFF (-1)
		u8(0xFF),
		// outer tagged_fields
		u8(0x00),
	]

	mut reader := new_reader(body)
	req := parse_describe_topic_partitions_request(mut reader, 0) or {
		assert false, '파싱 오류: ${err}'
		return
	}

	assert req.topics.len == 1, '토픽 수가 1이어야 함 (현재: ${req.topics.len})'
	assert req.topics[0].name == 'test', '토픽명이 test이어야 함 (현재: ${req.topics[0].name})'
	assert req.response_partition_limit == 100, 'partition_limit이 100이어야 함'
	assert req.cursor == none, 'cursor가 none이어야 함 (null cursor 0xFF)'
}

// [RED] cursor present 파싱 검증:
// Kafka Java 클라이언트가 보내는 present cursor는 0x01 + struct fields로 인코딩됨.
fn test_parse_request_with_present_cursor() {
	// cursor topic_name "my-topic" = [0x6D, 0x79, 0x2D, 0x74, 0x6F, 0x70, 0x69, 0x63]
	body := [
		// topics compact array len: 1개 → uvarint(2)
		u8(0x02),
		// topic name "test": compact string
		u8(0x05),
		u8(0x74),
		u8(0x65),
		u8(0x73),
		u8(0x74),
		// topic tagged_fields
		u8(0x00),
		// response_partition_limit = 50 (big-endian i32)
		u8(0x00),
		u8(0x00),
		u8(0x00),
		u8(0x32),
		// cursor present indicator: 0x01 (+1)
		u8(0x01),
		// cursor topic_name "my-topic": compact string (len+1=9, then 8 bytes)
		u8(0x09),
		u8(0x6D),
		u8(0x79),
		u8(0x2D),
		u8(0x74),
		u8(0x6F),
		u8(0x70),
		u8(0x69),
		u8(0x63),
		// cursor partition_index = 5 (big-endian i32)
		u8(0x00),
		u8(0x00),
		u8(0x00),
		u8(0x05),
		// cursor tagged_fields
		u8(0x00),
		// outer tagged_fields
		u8(0x00),
	]

	mut reader := new_reader(body)
	req := parse_describe_topic_partitions_request(mut reader, 0) or {
		assert false, '파싱 오류: ${err}'
		return
	}

	assert req.topics.len == 1, '토픽 수가 1이어야 함 (현재: ${req.topics.len})'
	assert req.response_partition_limit == 50, 'partition_limit이 50이어야 함'

	if cursor := req.cursor {
		assert cursor.topic_name == 'my-topic', 'cursor.topic_name이 my-topic이어야 함 (현재: ${cursor.topic_name})'
		assert cursor.partition_index == 5, 'cursor.partition_index가 5이어야 함 (현재: ${cursor.partition_index})'
	} else {
		assert false, 'cursor가 none이 아니어야 함 (present cursor 0x01)'
	}
}
