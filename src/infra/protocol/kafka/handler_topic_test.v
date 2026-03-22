// Topic 핸들러 단위 테스트
// process_create_topics, process_delete_topics, CreateTopicsResponse.encode, DeleteTopicsResponse.encode 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_topic_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)
	return storage, handler
}

// -- process_create_topics 테스트 --

fn test_process_create_topics_basic() {
	_, mut handler := create_topic_test_handler()

	req := CreateTopicsRequest{
		topics:        [
			CreateTopicsRequestTopic{
				name:               'new-topic'
				num_partitions:     3
				replication_factor: 1
				configs:            {}
			},
		]
		timeout_ms:    5000
		validate_only: false
	}

	resp := handler.process_create_topics(req, 5) or {
		panic('process_create_topics 실패: ${err}')
	}

	assert resp.topics.len == 1, '토픽 응답 수는 1이어야 한다'
	topic := resp.topics[0]
	assert topic.name == 'new-topic'
	assert topic.error_code == 0, '성공 시 에러 코드는 0이어야 한다'
	assert topic.num_partitions == 3
	assert topic.topic_id.len == 16, 'UUID는 16바이트여야 한다'
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_create_topics_already_exists() {
	mut storage, mut handler := create_topic_test_handler()

	// 사전에 동일 이름 토픽 생성
	storage.create_topic('existing-topic', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := CreateTopicsRequest{
		topics:     [
			CreateTopicsRequestTopic{
				name:               'existing-topic'
				num_partitions:     1
				replication_factor: 1
				configs:            {}
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_create_topics(req, 5) or {
		panic('process_create_topics 실패: ${err}')
	}

	assert resp.topics.len == 1
	assert resp.topics[0].error_code == i16(ErrorCode.topic_already_exists), '중복 토픽은 TOPIC_ALREADY_EXISTS 에러를 반환해야 한다'
}

fn test_process_create_topics_multiple() {
	_, mut handler := create_topic_test_handler()

	req := CreateTopicsRequest{
		topics:     [
			CreateTopicsRequestTopic{
				name:               'multi-1'
				num_partitions:     1
				replication_factor: 1
				configs:            {}
			},
			CreateTopicsRequestTopic{
				name:               'multi-2'
				num_partitions:     5
				replication_factor: 1
				configs:            {}
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_create_topics(req, 5) or {
		panic('process_create_topics 실패: ${err}')
	}

	assert resp.topics.len == 2, '2개의 토픽 응답이 반환되어야 한다'
	for t in resp.topics {
		assert t.error_code == 0, '각 토픽 생성은 성공해야 한다'
	}
	assert resp.topics[0].name == 'multi-1'
	assert resp.topics[1].name == 'multi-2'
}

fn test_process_create_topics_with_configs() {
	_, mut handler := create_topic_test_handler()

	req := CreateTopicsRequest{
		topics:     [
			CreateTopicsRequestTopic{
				name:               'config-topic'
				num_partitions:     2
				replication_factor: 1
				configs:            {
					'retention.ms':      '86400000'
					'cleanup.policy':    'compact'
					'max.message.bytes': '2097152'
				}
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_create_topics(req, 5) or {
		panic('process_create_topics 실패: ${err}')
	}

	assert resp.topics[0].error_code == 0
	assert resp.topics[0].name == 'config-topic'
}

fn test_process_create_topics_zero_partitions_defaults_to_one() {
	_, mut handler := create_topic_test_handler()

	// num_partitions <= 0 이면 기본값 1
	req := CreateTopicsRequest{
		topics:     [
			CreateTopicsRequestTopic{
				name:               'zero-part'
				num_partitions:     0
				replication_factor: 1
				configs:            {}
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_create_topics(req, 5) or {
		panic('process_create_topics 실패: ${err}')
	}

	assert resp.topics[0].error_code == 0, 'num_partitions=0일 때도 성공해야 한다'
}

// -- process_delete_topics 테스트 --

fn test_process_delete_topics_basic() {
	mut storage, mut handler := create_topic_test_handler()

	storage.create_topic('delete-me', 2, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DeleteTopicsRequest{
		topics:     [
			DeleteTopicsRequestTopic{
				name: 'delete-me'
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_delete_topics(req, 3) or {
		panic('process_delete_topics 실패: ${err}')
	}

	assert resp.topics.len == 1
	assert resp.topics[0].name == 'delete-me'
	assert resp.topics[0].error_code == 0, '존재하는 토픽 삭제는 성공해야 한다'
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_delete_topics_not_found() {
	_, mut handler := create_topic_test_handler()

	req := DeleteTopicsRequest{
		topics:     [
			DeleteTopicsRequestTopic{
				name: 'ghost-topic'
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_delete_topics(req, 3) or {
		panic('process_delete_topics 실패: ${err}')
	}

	assert resp.topics.len == 1
	assert resp.topics[0].error_code == i16(ErrorCode.unknown_topic_or_partition), '존재하지 않는 토픽 삭제는 UNKNOWN_TOPIC_OR_PARTITION 에러를 반환해야 한다'
}

fn test_process_delete_topics_multiple() {
	mut storage, mut handler := create_topic_test_handler()

	storage.create_topic('del-1', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}
	storage.create_topic('del-2', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DeleteTopicsRequest{
		topics:     [
			DeleteTopicsRequestTopic{
				name: 'del-1'
			},
			DeleteTopicsRequestTopic{
				name: 'del-2'
			},
			DeleteTopicsRequestTopic{
				name: 'del-nonexist'
			},
		]
		timeout_ms: 5000
	}

	resp := handler.process_delete_topics(req, 3) or {
		panic('process_delete_topics 실패: ${err}')
	}

	assert resp.topics.len == 3
	assert resp.topics[0].error_code == 0, 'del-1 삭제 성공'
	assert resp.topics[1].error_code == 0, 'del-2 삭제 성공'
	assert resp.topics[2].error_code != 0, 'del-nonexist 삭제 실패'
}

// -- CreateTopicsResponse.encode 테스트 --

fn test_create_topics_response_encode_v0() {
	resp := CreateTopicsResponse{
		throttle_time_ms: 0
		topics:           [
			CreateTopicsResponseTopic{
				name:               'enc-create'
				topic_id:           []u8{len: 16}
				error_code:         0
				error_message:      none
				num_partitions:     2
				replication_factor: 1
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1

	name := reader.read_string() or { panic('문자열 읽기 실패') }
	assert name == 'enc-create'

	err_code := reader.read_i16() or { panic('i16 읽기 실패') }
	assert err_code == 0
}

fn test_create_topics_response_encode_v5_flexible() {
	resp := CreateTopicsResponse{
		throttle_time_ms: 100
		topics:           [
			CreateTopicsResponseTopic{
				name:               'flex-create'
				topic_id:           []u8{len: 16}
				error_code:         0
				error_message:      none
				num_partitions:     4
				replication_factor: 1
			},
		]
	}

	encoded := resp.encode(5)
	assert encoded.len > 0, 'v5 flexible 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	// v5: throttle_time_ms(4) + compact_array + ...
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 100
}

// -- DeleteTopicsResponse.encode 테스트 --

fn test_delete_topics_response_encode_v0() {
	resp := DeleteTopicsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteTopicsResponseTopic{
				name:          'enc-delete'
				error_code:    0
				error_message: none
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1

	name := reader.read_string() or { panic('문자열 읽기 실패') }
	assert name == 'enc-delete'

	err_code := reader.read_i16() or { panic('i16 읽기 실패') }
	assert err_code == 0
}

fn test_delete_topics_response_encode_v4_flexible() {
	resp := DeleteTopicsResponse{
		throttle_time_ms: 50
		topics:           [
			DeleteTopicsResponseTopic{
				name:          'flex-delete'
				error_code:    3
				error_message: 'topic not found'
			},
		]
	}

	encoded := resp.encode(4)
	assert encoded.len > 0, 'v4 flexible 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	// v4: throttle_time_ms(4) + compact_array + ...
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 50
}
