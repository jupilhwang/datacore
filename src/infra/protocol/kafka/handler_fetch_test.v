// Fetch 핸들러 단위 테스트
// process_fetch, FetchResponse.encode, compress_records_for_fetch 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory
import time

// -- 테스트 헬퍼 --

fn create_fetch_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)
	return storage, handler
}

// seed_topic_with_records 는 토픽을 생성하고 레코드를 삽입한다.
fn seed_topic_with_records(mut storage memory.MemoryStorageAdapter, topic_name string, partition_count int, records []domain.Record) {
	storage.create_topic(topic_name, partition_count, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}
	if records.len > 0 {
		storage.append(topic_name, 0, records, -1) or { panic('레코드 추가 실패: ${err}') }
	}
}

// -- process_fetch 테스트 --

fn test_process_fetch_basic() {
	mut storage, mut handler := create_fetch_test_handler()

	test_records := [
		domain.Record{
			key:       'key1'.bytes()
			value:     'value1'.bytes()
			timestamp: time.now()
		},
		domain.Record{
			key:       'key2'.bytes()
			value:     'value2'.bytes()
			timestamp: time.now()
		},
	]
	seed_topic_with_records(mut storage, 'fetch-topic', 2, test_records)

	req := FetchRequest{
		replica_id:  -1
		max_wait_ms: 500
		min_bytes:   1
		max_bytes:   1048576
		topics:      [
			FetchRequestTopic{
				name:       'fetch-topic'
				partitions: [
					FetchRequestPartition{
						partition:           0
						fetch_offset:        0
						partition_max_bytes: 1048576
					},
				]
			},
		]
	}

	resp := handler.process_fetch(req, 7) or { panic('process_fetch 실패: ${err}') }

	assert resp.topics.len == 1, '토픽 수는 1이어야 한다'
	assert resp.topics[0].name == 'fetch-topic'
	assert resp.topics[0].partitions.len == 1

	part := resp.topics[0].partitions[0]
	assert part.error_code == 0, '에러 코드는 0이어야 한다'
	assert part.high_watermark == 2, 'high_watermark는 2이어야 한다'
	assert part.records.len > 0, '레코드 데이터가 비어있으면 안 된다'
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_fetch_topic_not_found() {
	_, mut handler := create_fetch_test_handler()

	req := FetchRequest{
		replica_id:  -1
		max_wait_ms: 100
		min_bytes:   1
		max_bytes:   1048576
		topics:      [
			FetchRequestTopic{
				name:       'nonexistent-topic'
				partitions: [
					FetchRequestPartition{
						partition:           0
						fetch_offset:        0
						partition_max_bytes: 1048576
					},
				]
			},
		]
	}

	resp := handler.process_fetch(req, 7) or { panic('process_fetch 실패: ${err}') }

	assert resp.topics.len == 1
	part := resp.topics[0].partitions[0]
	assert part.error_code == i16(ErrorCode.unknown_topic_or_partition), '존재하지 않는 토픽은 UNKNOWN_TOPIC_OR_PARTITION 에러를 반환해야 한다'
	assert part.records.len == 0
}

fn test_process_fetch_empty_partition() {
	mut storage, mut handler := create_fetch_test_handler()

	// 빈 토픽 생성 (레코드 없음)
	seed_topic_with_records(mut storage, 'empty-fetch', 1, []domain.Record{})

	req := FetchRequest{
		replica_id:  -1
		max_wait_ms: 100
		min_bytes:   1
		max_bytes:   1048576
		topics:      [
			FetchRequestTopic{
				name:       'empty-fetch'
				partitions: [
					FetchRequestPartition{
						partition:           0
						fetch_offset:        0
						partition_max_bytes: 1048576
					},
				]
			},
		]
	}

	resp := handler.process_fetch(req, 7) or { panic('process_fetch 실패: ${err}') }

	assert resp.topics[0].partitions[0].error_code == 0
	assert resp.topics[0].partitions[0].high_watermark == 0
}

fn test_process_fetch_partition_out_of_range() {
	mut storage, mut handler := create_fetch_test_handler()

	seed_topic_with_records(mut storage, 'range-topic', 1, []domain.Record{})

	req := FetchRequest{
		replica_id:  -1
		max_wait_ms: 100
		min_bytes:   1
		max_bytes:   1048576
		topics:      [
			FetchRequestTopic{
				name:       'range-topic'
				partitions: [
					FetchRequestPartition{
						partition:           99
						fetch_offset:        0
						partition_max_bytes: 1048576
					},
				]
			},
		]
	}

	resp := handler.process_fetch(req, 7) or { panic('process_fetch 실패: ${err}') }

	part := resp.topics[0].partitions[0]
	// 범위를 벗어난 파티션은 에러를 반환해야 한다
	assert part.error_code != 0, '범위를 벗어난 파티션은 에러를 반환해야 한다'
}

fn test_process_fetch_multiple_topics() {
	mut storage, mut handler := create_fetch_test_handler()

	records1 := [
		domain.Record{
			key:       'a'.bytes()
			value:     'alpha'.bytes()
			timestamp: time.now()
		},
	]
	records2 := [
		domain.Record{
			key:       'b'.bytes()
			value:     'beta'.bytes()
			timestamp: time.now()
		},
	]
	seed_topic_with_records(mut storage, 'topic-alpha', 1, records1)
	seed_topic_with_records(mut storage, 'topic-beta', 1, records2)

	req := FetchRequest{
		replica_id:  -1
		max_wait_ms: 500
		min_bytes:   1
		max_bytes:   1048576
		topics:      [
			FetchRequestTopic{
				name:       'topic-alpha'
				partitions: [
					FetchRequestPartition{
						partition:           0
						fetch_offset:        0
						partition_max_bytes: 1048576
					},
				]
			},
			FetchRequestTopic{
				name:       'topic-beta'
				partitions: [
					FetchRequestPartition{
						partition:           0
						fetch_offset:        0
						partition_max_bytes: 1048576
					},
				]
			},
		]
	}

	resp := handler.process_fetch(req, 7) or { panic('process_fetch 실패: ${err}') }

	assert resp.topics.len == 2, '2개 토픽의 응답이 반환되어야 한다'
	for t in resp.topics {
		assert t.partitions[0].error_code == 0
		assert t.partitions[0].records.len > 0
	}
}

// -- FetchResponse.encode 테스트 --

fn test_fetch_response_encode_v0() {
	resp := FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           [
			FetchResponseTopic{
				name:       'enc-topic'
				partitions: [
					FetchResponsePartition{
						partition_index: 0
						error_code:      0
						high_watermark:  5
						records:         [u8(0x01), 0x02, 0x03]
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	// v0: topic_count + topic_name + partition_count + partition fields + records
	mut reader := new_reader(encoded)
	topic_count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert topic_count == 1

	name := reader.read_string() or { panic('문자열 읽기 실패') }
	assert name == 'enc-topic'
}

fn test_fetch_response_encode_v12_flexible() {
	resp := FetchResponse{
		throttle_time_ms: 50
		error_code:       0
		session_id:       1234
		topics:           [
			FetchResponseTopic{
				name:       'flex-fetch'
				partitions: [
					FetchResponsePartition{
						partition_index:    0
						error_code:         0
						high_watermark:     10
						last_stable_offset: 10
						log_start_offset:   0
						records:            []u8{}
					},
				]
			},
		]
	}

	encoded := resp.encode(12)
	assert encoded.len > 0, 'v12 flexible 인코딩은 비어있으면 안 된다'

	// v12: throttle_time_ms(4) + error_code(2) + session_id(4) + compact_array + ...
	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 50

	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == 0

	session := reader.read_i32() or { panic('session_id 읽기 실패') }
	assert session == 1234
}

// -- compress_records_for_fetch 테스트 --

fn test_compress_records_no_compression() {
	_, mut handler := create_fetch_test_handler()

	data := []u8{len: 100, init: u8(0x41)}
	result := handler.compress_records_for_fetch(data, compression.CompressionType.none,
		'topic', 0)

	assert result.data == data, '비압축 시 원본 데이터가 반환되어야 한다'
	assert result.bytes_saved == 0
}

fn test_compress_records_empty_data() {
	_, mut handler := create_fetch_test_handler()

	result := handler.compress_records_for_fetch([]u8{}, compression.CompressionType.gzip,
		'topic', 0)

	assert result.data.len == 0, '빈 데이터는 그대로 반환되어야 한다'
	assert result.bytes_saved == 0
}

fn test_compress_records_small_payload_skip() {
	_, mut handler := create_fetch_test_handler()

	// 256 바이트 미만인 경우 압축 건너뛰기
	small_data := []u8{len: 100, init: u8(0x42)}
	result := handler.compress_records_for_fetch(small_data, compression.CompressionType.gzip,
		'topic', 0)

	assert result.data == small_data, '256 바이트 미만은 압축을 건너뛰어야 한다'
	assert result.bytes_saved == 0
}
