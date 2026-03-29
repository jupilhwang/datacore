// LogDirs 핸들러 단위 테스트
// process_describe_log_dirs, DescribeLogDirsResponse.encode,
// AlterReplicaLogDirsResponse.encode 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_log_dirs_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, new_compression_port_adapter(cs))
	return storage, handler
}

// -- process_describe_log_dirs 테스트 --

fn test_process_describe_log_dirs_all_topics() {
	mut storage, mut handler := create_log_dirs_test_handler()

	storage.create_topic('log-dir-topic-1', 2, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}
	storage.create_topic('log-dir-topic-2', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	// topics = none -> 모든 토픽 조회
	req := DescribeLogDirsRequest{
		topics: none
	}

	resp := handler.process_describe_log_dirs(req, 1) or {
		panic('process_describe_log_dirs 실패: ${err}')
	}

	assert resp.results.len == 1, 'virtual log dir 결과는 1개여야 한다'
	assert resp.results[0].error_code == 0
	assert resp.results[0].log_dir == virtual_log_dir
	assert resp.results[0].topics.len >= 2, '2개 이상의 토픽이 반환되어야 한다'
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_describe_log_dirs_specific_topics() {
	mut storage, mut handler := create_log_dirs_test_handler()

	storage.create_topic('specific-log-topic', 3, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DescribeLogDirsRequest{
		topics: [
			DescribeLogDirsTopic{
				topic:      'specific-log-topic'
				partitions: [i32(0), 1, 2]
			},
		]
	}

	resp := handler.process_describe_log_dirs(req, 1) or {
		panic('process_describe_log_dirs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].topics.len == 1
	assert resp.results[0].topics[0].name == 'specific-log-topic'
	assert resp.results[0].topics[0].partitions.len == 3
}

fn test_process_describe_log_dirs_topic_not_found() {
	_, mut handler := create_log_dirs_test_handler()

	req := DescribeLogDirsRequest{
		topics: [
			DescribeLogDirsTopic{
				topic:      'ghost-topic'
				partitions: [i32(0)]
			},
		]
	}

	resp := handler.process_describe_log_dirs(req, 1) or {
		panic('process_describe_log_dirs 실패: ${err}')
	}

	// 존재하지 않는 토픽은 결과에서 건너뜀
	assert resp.results.len == 1
	assert resp.results[0].topics.len == 0, '존재하지 않는 토픽은 결과에 포함되지 않아야 한다'
}

fn test_process_describe_log_dirs_invalid_partition_index() {
	mut storage, mut handler := create_log_dirs_test_handler()

	storage.create_topic('bounded-topic', 2, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DescribeLogDirsRequest{
		topics: [
			DescribeLogDirsTopic{
				topic:      'bounded-topic'
				partitions: [i32(0), 1, 99] // 99번 파티션은 존재하지 않음
			},
		]
	}

	resp := handler.process_describe_log_dirs(req, 1) or {
		panic('process_describe_log_dirs 실패: ${err}')
	}

	assert resp.results[0].topics.len == 1
	// 유효한 파티션만 반환
	assert resp.results[0].topics[0].partitions.len == 2, '유효하지 않은 파티션 인덱스는 건너뛰어야 한다'
}

fn test_process_describe_log_dirs_empty_topics_list() {
	_, mut handler := create_log_dirs_test_handler()

	req := DescribeLogDirsRequest{
		topics: []DescribeLogDirsTopic{}
	}

	resp := handler.process_describe_log_dirs(req, 1) or {
		panic('process_describe_log_dirs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].topics.len == 0
}

fn test_process_describe_log_dirs_virtual_log_dir_path() {
	_, mut handler := create_log_dirs_test_handler()

	req := DescribeLogDirsRequest{
		topics: none
	}

	resp := handler.process_describe_log_dirs(req, 1) or {
		panic('process_describe_log_dirs 실패: ${err}')
	}

	assert resp.results[0].log_dir == '/var/kafka-logs', 'virtual log dir 경로 검증'
}

// -- DescribeLogDirsResponse.encode 테스트 --

fn test_describe_log_dirs_response_encode_v1_non_flexible() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 0
		error_code:       0
		results:          [
			DescribeLogDirsResult{
				error_code:   0
				log_dir:      '/var/kafka-logs'
				topics:       [
					DescribeLogDirsTopicResult{
						name:       'enc-topic'
						partitions: [
							DescribeLogDirsPartitionResult{
								partition_index: 0
								partition_size:  1024
								offset_lag:      0
								is_future_key:   false
							},
						]
					},
				]
				total_bytes:  -1
				usable_bytes: -1
			},
		]
	}

	encoded := resp.encode(1)
	assert encoded.len > 0, 'v1 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0
}

fn test_describe_log_dirs_response_encode_v2_flexible() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 50
		error_code:       0
		results:          [
			DescribeLogDirsResult{
				error_code: 0
				log_dir:    '/var/kafka-logs'
				topics:     []
			},
		]
	}

	encoded := resp.encode(2)
	assert encoded.len > 0, 'v2 flexible 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 50
}

fn test_describe_log_dirs_response_encode_v3_with_error_code() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 0
		error_code:       i16(ErrorCode.unknown_server_error)
		results:          []
	}

	encoded := resp.encode(3)
	assert encoded.len > 0, 'v3 에러 코드 포함 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	error_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert error_code == i16(ErrorCode.unknown_server_error)
}

fn test_describe_log_dirs_response_encode_v4_with_bytes() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 0
		error_code:       0
		results:          [
			DescribeLogDirsResult{
				error_code:   0
				log_dir:      '/var/kafka-logs'
				topics:       []
				total_bytes:  1073741824
				usable_bytes: 536870912
			},
		]
	}

	encoded := resp.encode(4)
	assert encoded.len > 0, 'v4 bytes 포함 인코딩은 비어있으면 안 된다'
}

fn test_describe_log_dirs_response_encode_empty_results() {
	resp := DescribeLogDirsResponse{
		throttle_time_ms: 0
		error_code:       0
		results:          []
	}

	encoded := resp.encode(1)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 0
}

// -- AlterReplicaLogDirsResponse.encode 테스트 --

fn test_alter_replica_log_dirs_response_encode_v1() {
	resp := AlterReplicaLogDirsResponse{
		throttle_time_ms: 0
		results:          [
			AlterReplicaLogDirTopicResult{
				topic_name: 'alter-topic'
				partitions: [
					AlterReplicaLogDirPartitionResult{
						partition_index: 0
						error_code:      0
					},
					AlterReplicaLogDirPartitionResult{
						partition_index: 1
						error_code:      0
					},
				]
			},
		]
	}

	encoded := resp.encode(1)
	assert encoded.len > 0, 'v1 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1
}

fn test_alter_replica_log_dirs_response_encode_v2_flexible() {
	resp := AlterReplicaLogDirsResponse{
		throttle_time_ms: 100
		results:          [
			AlterReplicaLogDirTopicResult{
				topic_name: 'flex-alter-topic'
				partitions: [
					AlterReplicaLogDirPartitionResult{
						partition_index: 0
						error_code:      i16(ErrorCode.unknown_topic_or_partition)
					},
				]
			},
		]
	}

	encoded := resp.encode(2)
	assert encoded.len > 0, 'v2 flexible 인코딩은 비어있으면 안 된다'
}

fn test_alter_replica_log_dirs_response_encode_empty() {
	resp := AlterReplicaLogDirsResponse{
		throttle_time_ms: 0
		results:          []
	}

	encoded := resp.encode(1)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 0
}
