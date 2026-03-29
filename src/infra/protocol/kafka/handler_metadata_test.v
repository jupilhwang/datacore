// Metadata 핸들러 단위 테스트
// process_metadata, MetadataResponse.encode 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_metadata_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	return storage, handler
}

// -- process_metadata 테스트 --

fn test_process_metadata_specific_topic() {
	mut storage, mut handler := create_metadata_test_handler()

	storage.create_topic('meta-topic', 3, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := MetadataRequest{
		topics:                    [
			MetadataRequestTopic{
				name: 'meta-topic'
			},
		]
		allow_auto_topic_creation: false
	}

	resp := handler.process_metadata(req, 9) or { panic('process_metadata 실패: ${err}') }

	// 브로커 정보 검증
	assert resp.brokers.len >= 1, '브로커 수는 최소 1이어야 한다'
	assert resp.brokers[0].node_id == 1
	assert resp.brokers[0].host == 'localhost'
	assert resp.brokers[0].port == 9092

	// 클러스터 정보 검증
	if cid := resp.cluster_id {
		assert cid == 'test-cluster'
	} else {
		assert false, 'cluster_id는 none이 아니어야 한다'
	}
	assert resp.controller_id == 1

	// 토픽 메타데이터 검증
	assert resp.topics.len == 1, '요청한 토픽 1개의 메타데이터가 반환되어야 한다'
	topic := resp.topics[0]
	assert topic.error_code == 0
	assert topic.name == 'meta-topic'
	assert topic.partitions.len == 3, '파티션 수는 3이어야 한다'

	for i, p in topic.partitions {
		assert p.partition_index == i32(i)
		assert p.leader_id == 1, '단일 브로커에서 leader_id는 1이어야 한다'
		assert p.replica_nodes.len >= 1
		assert p.isr_nodes.len >= 1
	}
}

fn test_process_metadata_all_topics() {
	mut storage, mut handler := create_metadata_test_handler()

	storage.create_topic('topic-1', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}
	storage.create_topic('topic-2', 2, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	// 빈 topics 배열 = 전체 토픽 조회
	req := MetadataRequest{
		topics:                    []MetadataRequestTopic{}
		allow_auto_topic_creation: false
	}

	resp := handler.process_metadata(req, 9) or { panic('process_metadata 실패: ${err}') }

	assert resp.topics.len == 2, '2개 토픽의 메타데이터가 반환되어야 한다'

	mut found_names := []string{}
	for t in resp.topics {
		found_names << t.name
		assert t.error_code == 0
	}
	assert 'topic-1' in found_names
	assert 'topic-2' in found_names
}

fn test_process_metadata_topic_not_found_no_auto_create() {
	_, mut handler := create_metadata_test_handler()

	req := MetadataRequest{
		topics:                    [
			MetadataRequestTopic{
				name: 'nonexistent'
			},
		]
		allow_auto_topic_creation: false
	}

	resp := handler.process_metadata(req, 9) or { panic('process_metadata 실패: ${err}') }

	assert resp.topics.len == 1
	assert resp.topics[0].error_code == i16(ErrorCode.unknown_topic_or_partition), '존재하지 않는 토픽은 UNKNOWN_TOPIC_OR_PARTITION을 반환해야 한다'
	assert resp.topics[0].name == 'nonexistent'
	assert resp.topics[0].partitions.len == 0
}

fn test_process_metadata_auto_create_topic() {
	_, mut handler := create_metadata_test_handler()

	req := MetadataRequest{
		topics:                    [
			MetadataRequestTopic{
				name: 'auto-meta-topic'
			},
		]
		allow_auto_topic_creation: true
	}

	resp := handler.process_metadata(req, 9) or { panic('process_metadata 실패: ${err}') }

	assert resp.topics.len == 1
	// 자동 생성 활성화 시 성공해야 한다
	assert resp.topics[0].error_code == 0, '자동 생성이 활성화된 경우 토픽이 생성되어야 한다'
	assert resp.topics[0].name == 'auto-meta-topic'
	assert resp.topics[0].partitions.len >= 1
}

fn test_process_metadata_empty_topic_name_skipped() {
	_, mut handler := create_metadata_test_handler()

	// 빈 이름의 토픽 요청은 건너뛰어야 한다
	req := MetadataRequest{
		topics:                    [
			MetadataRequestTopic{
				name: ''
			},
		]
		allow_auto_topic_creation: false
	}

	resp := handler.process_metadata(req, 9) or { panic('process_metadata 실패: ${err}') }

	// 빈 이름은 continue 로 건너뛰므로 응답 토픽이 없다
	assert resp.topics.len == 0, '빈 이름의 토픽 요청은 건너뛰어야 한다'
}

fn test_process_metadata_internal_topic() {
	mut storage, mut handler := create_metadata_test_handler()

	// __ 접두사 토픽은 is_internal=true
	storage.create_topic('__consumer_offsets', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := MetadataRequest{
		topics:                    [
			MetadataRequestTopic{
				name: '__consumer_offsets'
			},
		]
		allow_auto_topic_creation: false
	}

	resp := handler.process_metadata(req, 9) or { panic('process_metadata 실패: ${err}') }

	assert resp.topics.len == 1
	assert resp.topics[0].is_internal == true, '__접두사 토픽은 is_internal이 true여야 한다'
}

// -- MetadataResponse.encode 테스트 --

fn test_metadata_response_encode_v0() {
	resp := MetadataResponse{
		brokers:       [
			MetadataResponseBroker{
				node_id: 1
				host:    'localhost'
				port:    9092
			},
		]
		controller_id: 1
		topics:        [
			MetadataResponseTopic{
				error_code: 0
				name:       'enc-meta'
				partitions: [
					MetadataResponsePartition{
						error_code:      0
						partition_index: 0
						leader_id:       1
						replica_nodes:   [i32(1)]
						isr_nodes:       [i32(1)]
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	// v0: broker_count(4) + broker(node_id(4) + host_len(2) + host + port(4))
	broker_count := reader.read_array_len() or { panic('broker 배열 읽기 실패') }
	assert broker_count == 1

	node_id := reader.read_i32() or { panic('node_id 읽기 실패') }
	assert node_id == 1

	host := reader.read_string() or { panic('host 읽기 실패') }
	assert host == 'localhost'

	port := reader.read_i32() or { panic('port 읽기 실패') }
	assert port == 9092
}

fn test_metadata_response_encode_v9_flexible() {
	resp := MetadataResponse{
		throttle_time_ms: 200
		brokers:          [
			MetadataResponseBroker{
				node_id: 2
				host:    '10.0.0.1'
				port:    9093
				rack:    'rack-a'
			},
		]
		cluster_id:       'cluster-xyz'
		controller_id:    2
		topics:           []MetadataResponseTopic{}
	}

	encoded := resp.encode(9)
	assert encoded.len > 0, 'v9 flexible 인코딩은 비어있으면 안 된다'

	// v9: throttle_time_ms(4) + compact_broker_array + ...
	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 200
}
