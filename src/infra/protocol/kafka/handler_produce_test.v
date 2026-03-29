// Produce 핸들러 단위 테스트
// process_produce, ProduceResponse.encode, build_produce_error_response_typed 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory
import time

// -- 테스트 헬퍼 --

fn create_produce_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	return storage, handler
}

// make_record_batch_bytes 는 단건 레코드를 포함하는 RecordBatch v2 바이트를 생성한다.
fn make_record_batch_bytes(key []u8, value []u8) []u8 {
	records := [
		domain.Record{
			key:       key
			value:     value
			timestamp: time.now()
		},
	]
	return encode_record_batch(records, 0)
}

// -- process_produce 테스트 --

fn test_process_produce_basic() {
	mut storage, mut handler := create_produce_test_handler()

	// 토픽 사전 생성
	storage.create_topic('test-produce', 3, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	record_bytes := make_record_batch_bytes('key1'.bytes(), 'value1'.bytes())

	req := ProduceRequest{
		acks:       -1
		timeout_ms: 5000
		topic_data: [
			ProduceRequestTopic{
				name:           'test-produce'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: record_bytes
					},
				]
			},
		]
	}

	resp := handler.process_produce(req, 7) or { panic('process_produce 실패: ${err}') }

	assert resp.topics.len == 1, '토픽 수는 1이어야 한다'
	assert resp.topics[0].name == 'test-produce'
	assert resp.topics[0].partitions.len == 1
	assert resp.topics[0].partitions[0].error_code == 0, '에러 코드는 0이어야 한다'
	assert resp.topics[0].partitions[0].base_offset == 0, 'base_offset은 0이어야 한다'
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_produce_auto_create_topic() {
	_, mut handler := create_produce_test_handler()

	// 존재하지 않는 토픽에 produce -- 자동 생성 로직 트리거
	record_bytes := make_record_batch_bytes('k'.bytes(), 'v'.bytes())

	req := ProduceRequest{
		acks:       1
		timeout_ms: 3000
		topic_data: [
			ProduceRequestTopic{
				name:           'auto-created-topic'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: record_bytes
					},
				]
			},
		]
	}

	resp := handler.process_produce(req, 7) or { panic('process_produce 실패: ${err}') }

	assert resp.topics.len == 1
	// 자동 생성 후 재시도 시 성공해야 한다
	assert resp.topics[0].partitions[0].error_code == 0, '자동 생성 후 produce는 성공해야 한다'
}

fn test_process_produce_empty_records() {
	mut storage, mut handler := create_produce_test_handler()

	storage.create_topic('empty-topic', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	// 빈 레코드 바이트 -- 61바이트 미만이므로 RecordBatch 파싱 실패 -> corrupt_message
	req := ProduceRequest{
		acks:       0
		timeout_ms: 1000
		topic_data: [
			ProduceRequestTopic{
				name:           'empty-topic'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: []u8{}
					},
				]
			},
		]
	}

	resp := handler.process_produce(req, 7) or { panic('process_produce 실패: ${err}') }

	assert resp.topics.len == 1
	partition_resp := resp.topics[0].partitions[0]
	// NOTE: 빈 바이트 배열은 parse_record_batch에서 "data too small" 에러를 발생시켜
	// corrupt_message(2)로 처리된다
	assert partition_resp.error_code == i16(ErrorCode.corrupt_message), '빈 레코드 바이트는 corrupt_message를 반환해야 한다'
	assert partition_resp.base_offset == -1
}

fn test_process_produce_multiple_partitions() {
	mut storage, mut handler := create_produce_test_handler()

	storage.create_topic('multi-part', 4, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	batch0 := make_record_batch_bytes('k0'.bytes(), 'v0'.bytes())
	batch2 := make_record_batch_bytes('k2'.bytes(), 'v2'.bytes())

	req := ProduceRequest{
		acks:       -1
		timeout_ms: 5000
		topic_data: [
			ProduceRequestTopic{
				name:           'multi-part'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: batch0
					},
					ProduceRequestPartition{
						index:   2
						records: batch2
					},
				]
			},
		]
	}

	resp := handler.process_produce(req, 7) or { panic('process_produce 실패: ${err}') }

	assert resp.topics[0].partitions.len == 2, '파티션 응답 수는 2이어야 한다'
	for p in resp.topics[0].partitions {
		assert p.error_code == 0
	}
}

// -- ProduceResponse.encode 테스트 --

fn test_produce_response_encode_v0() {
	resp := ProduceResponse{
		topics:           [
			ProduceResponseTopic{
				name:       'topic-a'
				partitions: [
					ProduceResponsePartition{
						index:       0
						error_code:  0
						base_offset: 10
					},
				]
			},
		]
		throttle_time_ms: 0
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩 결과는 비어있으면 안 된다'

	// v0: [topic_count(4)] + [topic_name_len(2) + name] + [partition_count(4)] + [index(4) + error(2) + base_offset(8)]
	mut reader := new_reader(encoded)
	topic_count := reader.read_array_len() or { panic('배열 길이 읽기 실패') }
	assert topic_count == 1

	name := reader.read_string() or { panic('문자열 읽기 실패') }
	assert name == 'topic-a'

	part_count := reader.read_array_len() or { panic('배열 길이 읽기 실패') }
	assert part_count == 1

	idx := reader.read_i32() or { panic('i32 읽기 실패') }
	assert idx == 0

	err_code := reader.read_i16() or { panic('i16 읽기 실패') }
	assert err_code == 0

	base_off := reader.read_i64() or { panic('i64 읽기 실패') }
	assert base_off == 10
}

fn test_produce_response_encode_v9_flexible() {
	resp := ProduceResponse{
		topics:           [
			ProduceResponseTopic{
				name:       'flex-topic'
				partitions: [
					ProduceResponsePartition{
						index:            0
						error_code:       3
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					},
				]
			},
		]
		throttle_time_ms: 100
	}

	encoded := resp.encode(9)
	assert encoded.len > 0, 'v9 flexible 인코딩 결과는 비어있으면 안 된다'

	// v9 는 compact_array + compact_string + tagged_fields 사용
	mut reader := new_reader(encoded)
	topic_count := reader.read_compact_array_len() or { panic('compact 배열 읽기 실패') }
	assert topic_count == 1
}

// -- build_produce_error_response_typed 테스트 --

fn test_build_produce_error_response_typed() {
	_, handler := create_produce_test_handler()

	req := ProduceRequest{
		acks:       -1
		timeout_ms: 5000
		topic_data: [
			ProduceRequestTopic{
				name:           'err-topic'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: []u8{}
					},
					ProduceRequestPartition{
						index:   1
						records: []u8{}
					},
				]
			},
		]
	}

	resp := handler.build_produce_error_response_typed(req, ErrorCode.unknown_topic_or_partition)

	assert resp.topics.len == 1
	assert resp.topics[0].partitions.len == 2, '에러 응답 파티션 수는 2이어야 한다'
	for p in resp.topics[0].partitions {
		assert p.error_code == i16(ErrorCode.unknown_topic_or_partition)
		assert p.base_offset == -1
		assert p.log_append_time == -1
		assert p.log_start_offset == -1
	}
}

// -- build_produce_error_response (legacy, bytes 반환) 테스트 --

fn test_build_produce_error_response_legacy() {
	req := ProduceRequest{
		acks:       1
		timeout_ms: 1000
		topic_data: [
			ProduceRequestTopic{
				name:           'legacy-err'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: []u8{}
					},
				]
			},
		]
	}

	encoded := build_produce_error_response(req, i16(ErrorCode.unknown_server_error),
		7)
	assert encoded.len > 0, 'legacy 에러 응답은 비어있으면 안 된다'
}

// -- store_with_auto_create: partition_not_found vs topic_not_found 구분 테스트 --

fn test_produce_partition_not_found_returns_partition_error() {
	mut storage, mut handler := create_produce_test_handler()

	// 토픽을 1개 파티션으로 생성
	storage.create_topic('partition-test', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	record_bytes := make_record_batch_bytes('k'.bytes(), 'v'.bytes())

	// 존재하지 않는 파티션(5)에 produce -- partition_not_found 에러가 반환되어야 한다
	req := ProduceRequest{
		acks:       -1
		timeout_ms: 3000
		topic_data: [
			ProduceRequestTopic{
				name:           'partition-test'
				partition_data: [
					ProduceRequestPartition{
						index:   5
						records: record_bytes
					},
				]
			},
		]
	}

	resp := handler.process_produce(req, 7) or { panic('process_produce 실패: ${err}') }

	assert resp.topics.len == 1
	partition_resp := resp.topics[0].partitions[0]
	// partition_not_found는 auto-create가 아닌 unknown_topic_or_partition(3) 에러를 반환해야 한다
	assert partition_resp.error_code == i16(ErrorCode.unknown_topic_or_partition), 'partition_not_found는 unknown_topic_or_partition(3)을 반환해야 하지만 실제: ${partition_resp.error_code}'
}

fn test_produce_auto_create_still_works_for_missing_topic() {
	_, mut handler := create_produce_test_handler()

	// 존재하지 않는 토픽에 produce -- 자동 생성 후 성공해야 한다
	record_bytes := make_record_batch_bytes('k'.bytes(), 'v'.bytes())

	req := ProduceRequest{
		acks:       1
		timeout_ms: 3000
		topic_data: [
			ProduceRequestTopic{
				name:           'new-auto-topic'
				partition_data: [
					ProduceRequestPartition{
						index:   0
						records: record_bytes
					},
				]
			},
		]
	}

	resp := handler.process_produce(req, 7) or { panic('process_produce 실패: ${err}') }

	assert resp.topics.len == 1
	assert resp.topics[0].partitions[0].error_code == 0, '존재하지 않는 토픽은 자동 생성 후 성공(0)해야 하지만 실제: ${resp.topics[0].partitions[0].error_code}'
}
