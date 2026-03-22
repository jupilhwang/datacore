// DescribeCluster 핸들러 단위 테스트
// process_describe_cluster, DescribeClusterResponse.encode 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_describe_cluster_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)
	return storage, handler
}

// -- process_describe_cluster 테스트 --

fn test_process_describe_cluster_basic() {
	_, mut handler := create_describe_cluster_test_handler()

	req := DescribeClusterRequest{
		include_cluster_authorized_operations: false
	}

	resp := handler.process_describe_cluster(req, 0) or {
		panic('process_describe_cluster 실패: ${err}')
	}

	assert resp.error_code == 0, '에러 코드는 0이어야 한다'
	assert resp.cluster_id == 'test-cluster'
	assert resp.controller_id == 1
	assert resp.brokers.len == 1, '단일 브로커 모드에서 브로커 수는 1이어야 한다'
	assert resp.brokers[0].broker_id == 1
	assert resp.brokers[0].host == 'localhost'
	assert resp.brokers[0].port == 9092
}

fn test_process_describe_cluster_with_authorized_operations() {
	_, mut handler := create_describe_cluster_test_handler()

	req := DescribeClusterRequest{
		include_cluster_authorized_operations: true
	}

	resp := handler.process_describe_cluster(req, 0) or {
		panic('process_describe_cluster 실패: ${err}')
	}

	assert resp.error_code == 0
	assert resp.cluster_authorized_operations == -2147483648, 'authorized_operations 기본값 검증'
}

fn test_process_describe_cluster_single_broker_fallback() {
	_, mut handler := create_describe_cluster_test_handler()

	// broker_registry가 설정되지 않은 상태에서 단일 브로커 폴백 검증
	req := DescribeClusterRequest{
		include_cluster_authorized_operations: false
	}

	resp := handler.process_describe_cluster(req, 0) or {
		panic('process_describe_cluster 실패: ${err}')
	}

	assert resp.brokers.len >= 1, '최소 1개 브로커가 있어야 한다'
	assert resp.brokers[0].broker_id == 1
	assert resp.brokers[0].rack == none, '기본 브로커에는 rack이 없어야 한다'
}

fn test_process_describe_cluster_throttle_time() {
	_, mut handler := create_describe_cluster_test_handler()

	req := DescribeClusterRequest{
		include_cluster_authorized_operations: false
	}

	resp := handler.process_describe_cluster(req, 0) or {
		panic('process_describe_cluster 실패: ${err}')
	}

	assert resp.throttle_time_ms == 0
	assert resp.error_message == none
}

// -- DescribeClusterResponse.encode 테스트 --

fn test_describe_cluster_response_encode_v0() {
	resp := DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    0
		error_message:                 none
		cluster_id:                    'test-cluster-id'
		controller_id:                 1
		brokers:                       [
			DescribeClusterBroker{
				broker_id: 1
				host:      'localhost'
				port:      9092
				rack:      none
			},
		]
		cluster_authorized_operations: -2147483648
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == 0
}

fn test_describe_cluster_response_encode_multiple_brokers() {
	resp := DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    0
		error_message:                 none
		cluster_id:                    'multi-broker-cluster'
		controller_id:                 1
		brokers:                       [
			DescribeClusterBroker{
				broker_id: 1
				host:      'broker-1'
				port:      9092
				rack:      'us-east-1a'
			},
			DescribeClusterBroker{
				broker_id: 2
				host:      'broker-2'
				port:      9092
				rack:      'us-east-1b'
			},
			DescribeClusterBroker{
				broker_id: 3
				host:      'broker-3'
				port:      9092
				rack:      none
			},
		]
		cluster_authorized_operations: -2147483648
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, '다중 브로커 인코딩은 비어있으면 안 된다'
}

fn test_describe_cluster_response_encode_with_error() {
	resp := DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    i16(ErrorCode.unknown_server_error)
		error_message:                 'internal error'
		cluster_id:                    'err-cluster'
		controller_id:                 -1
		brokers:                       []
		cluster_authorized_operations: -2147483648
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == i16(ErrorCode.unknown_server_error)
}

fn test_describe_cluster_response_encode_empty_brokers() {
	resp := DescribeClusterResponse{
		throttle_time_ms:              0
		error_code:                    0
		error_message:                 none
		cluster_id:                    'empty-cluster'
		controller_id:                 0
		brokers:                       []
		cluster_authorized_operations: -2147483648
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, '빈 브로커 리스트 인코딩은 비어있으면 안 된다'
}

// -- DescribeTopicPartitions 테스트 --

fn test_process_describe_topic_partitions_basic() {
	mut storage, mut handler := create_describe_cluster_test_handler()

	storage.create_topic('dtp-topic', 3, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DescribeTopicPartitionsRequest{
		topics:                   [
			DescribeTopicPartitionsTopicReq{
				name: 'dtp-topic'
			},
		]
		response_partition_limit: 2000
		cursor:                   none
	}

	resp := handler.process_describe_topic_partitions(req, 0) or {
		panic('process_describe_topic_partitions 실패: ${err}')
	}

	assert resp.topics.len == 1
	assert resp.topics[0].error_code == 0
	assert resp.topics[0].partitions.len == 3
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_describe_topic_partitions_not_found() {
	_, mut handler := create_describe_cluster_test_handler()

	req := DescribeTopicPartitionsRequest{
		topics:                   [
			DescribeTopicPartitionsTopicReq{
				name: 'nonexistent'
			},
		]
		response_partition_limit: 2000
		cursor:                   none
	}

	resp := handler.process_describe_topic_partitions(req, 0) or {
		panic('process_describe_topic_partitions 실패: ${err}')
	}

	assert resp.topics.len == 1
	assert resp.topics[0].error_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_describe_topic_partitions_response_encode() {
	resp := DescribeTopicPartitionsResponse{
		throttle_time_ms: 0
		topics:           [
			DescribeTopicPartitionsTopicResp{
				error_code:  0
				name:        'enc-topic'
				topic_id:    []u8{len: 16}
				is_internal: false
				partitions:  [
					DescribeTopicPartitionsPartition{
						error_code:       0
						partition_index:  0
						leader_id:        1
						leader_epoch:     0
						replica_nodes:    [i32(1)]
						isr_nodes:        [i32(1)]
						offline_replicas: []
					},
				]
			},
		]
		next_cursor:      none
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'DescribeTopicPartitions 인코딩은 비어있으면 안 된다'
}
