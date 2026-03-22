// Admin 핸들러 단위 테스트
// AlterConfigsResponse.encode, CreatePartitionsResponse.encode,
// DeleteRecordsResponse.encode 및 process 함수 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_admin_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)
	return storage, handler
}

// -- AlterConfigsResponse.encode 테스트 --

fn test_alter_configs_response_encode_v0() {
	resp := AlterConfigsResponse{
		throttle_time_ms: 0
		results:          [
			AlterConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'test-topic'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1

	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == 0
}

fn test_alter_configs_response_encode_v2_flexible() {
	resp := AlterConfigsResponse{
		throttle_time_ms: 50
		results:          [
			AlterConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 4
				resource_name: '1'
			},
		]
	}

	encoded := resp.encode(2)
	assert encoded.len > 0, 'v2 flexible 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 50
}

fn test_alter_configs_response_encode_with_error() {
	resp := AlterConfigsResponse{
		throttle_time_ms: 0
		results:          [
			AlterConfigsResult{
				error_code:    i16(ErrorCode.unknown_topic_or_partition)
				error_message: 'Topic not found'
				resource_type: 2
				resource_name: 'missing-topic'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1

	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_alter_configs_response_encode_multiple_results() {
	resp := AlterConfigsResponse{
		throttle_time_ms: 0
		results:          [
			AlterConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'topic-a'
			},
			AlterConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 4
				resource_name: '1'
			},
			AlterConfigsResult{
				error_code:    i16(ErrorCode.invalid_request)
				error_message: 'Unsupported resource type'
				resource_type: 99
				resource_name: 'unknown'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 3
}

fn test_alter_configs_response_encode_empty() {
	resp := AlterConfigsResponse{
		throttle_time_ms: 0
		results:          []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 0
}

// -- CreatePartitionsResponse.encode 테스트 --

fn test_create_partitions_response_encode_v0() {
	resp := CreatePartitionsResponse{
		throttle_time_ms: 0
		results:          [
			CreatePartitionsResult{
				name:          'create-part-topic'
				error_code:    0
				error_message: none
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1

	name := reader.read_string() or { panic('문자열 읽기 실패') }
	assert name == 'create-part-topic'

	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == 0
}

fn test_create_partitions_response_encode_v2_flexible() {
	resp := CreatePartitionsResponse{
		throttle_time_ms: 100
		results:          [
			CreatePartitionsResult{
				name:          'flex-topic'
				error_code:    0
				error_message: none
			},
		]
	}

	encoded := resp.encode(2)
	assert encoded.len > 0, 'v2 flexible 인코딩은 비어있으면 안 된다'
}

fn test_create_partitions_response_encode_with_error() {
	resp := CreatePartitionsResponse{
		throttle_time_ms: 0
		results:          [
			CreatePartitionsResult{
				name:          'err-topic'
				error_code:    i16(ErrorCode.invalid_partitions)
				error_message: 'New count must be greater'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	_ := reader.read_array_len() or { panic('배열 읽기 실패') }
	_ := reader.read_string() or { panic('문자열 읽기 실패') }
	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == i16(ErrorCode.invalid_partitions)
}

fn test_create_partitions_response_encode_multiple() {
	resp := CreatePartitionsResponse{
		throttle_time_ms: 0
		results:          [
			CreatePartitionsResult{
				name:          'topic-1'
				error_code:    0
				error_message: none
			},
			CreatePartitionsResult{
				name:          'topic-2'
				error_code:    i16(ErrorCode.unknown_topic_or_partition)
				error_message: 'Topic not found'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 2
}

// -- DeleteRecordsResponse.encode 테스트 --

fn test_delete_records_response_encode_v0() {
	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteRecordsResponseTopic{
				name:       'del-rec-topic'
				partitions: [
					DeleteRecordsResponsePartition{
						partition_index: 0
						low_watermark:   10
						error_code:      0
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0, 'v0 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 0

	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 1

	name := reader.read_string() or { panic('문자열 읽기 실패') }
	assert name == 'del-rec-topic'
}

fn test_delete_records_response_encode_v2_flexible() {
	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteRecordsResponseTopic{
				name:       'flex-del-topic'
				partitions: [
					DeleteRecordsResponsePartition{
						partition_index: 0
						low_watermark:   5
						error_code:      0
					},
				]
			},
		]
	}

	encoded := resp.encode(2)
	assert encoded.len > 0, 'v2 flexible 인코딩은 비어있으면 안 된다'
}

fn test_delete_records_response_encode_with_error() {
	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteRecordsResponseTopic{
				name:       'err-del-topic'
				partitions: [
					DeleteRecordsResponsePartition{
						partition_index: 0
						low_watermark:   -1
						error_code:      i16(ErrorCode.unknown_topic_or_partition)
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	_ := reader.read_array_len() or { panic('배열 읽기 실패') }
	_ := reader.read_string() or { panic('문자열 읽기 실패') }
	p_count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert p_count == 1

	_ := reader.read_i32() or { panic('partition_index 읽기 실패') }
	_ := reader.read_i64() or { panic('low_watermark 읽기 실패') }
	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == i16(ErrorCode.unknown_topic_or_partition)
}

fn test_delete_records_response_encode_multiple_partitions() {
	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           [
			DeleteRecordsResponseTopic{
				name:       'multi-part-del'
				partitions: [
					DeleteRecordsResponsePartition{
						partition_index: 0
						low_watermark:   10
						error_code:      0
					},
					DeleteRecordsResponsePartition{
						partition_index: 1
						low_watermark:   20
						error_code:      0
					},
					DeleteRecordsResponsePartition{
						partition_index: 2
						low_watermark:   -1
						error_code:      i16(ErrorCode.offset_out_of_range)
					},
				]
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	_ := reader.read_array_len() or { panic('배열 읽기 실패') }
	_ := reader.read_string() or { panic('문자열 읽기 실패') }
	p_count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert p_count == 3
}

fn test_delete_records_response_encode_empty() {
	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 0
}

// -- Admin Request struct 검증 --

fn test_alter_configs_request_struct() {
	req := AlterConfigsRequest{
		resources:     [
			AlterConfigsResource{
				resource_type: 2
				resource_name: 'my-topic'
				configs:       [
					AlterConfigsEntry{
						name:  'retention.ms'
						value: '86400000'
					},
				]
			},
		]
		validate_only: false
	}

	assert req.resources.len == 1
	assert req.resources[0].configs.len == 1
	assert req.resources[0].configs[0].name == 'retention.ms'
	assert req.validate_only == false
}

fn test_create_partitions_request_struct() {
	req := CreatePartitionsRequest{
		topics:        [
			CreatePartitionsTopic{
				name:        'part-topic'
				count:       6
				assignments: none
			},
		]
		timeout_ms:    5000
		validate_only: false
	}

	assert req.topics.len == 1
	assert req.topics[0].count == 6
	assert req.timeout_ms == 5000
}

fn test_delete_records_request_struct() {
	req := DeleteRecordsRequest{
		topics:     [
			DeleteRecordsTopic{
				name:       'del-topic'
				partitions: [
					DeleteRecordsPartition{
						partition_index: 0
						offset:          100
					},
					DeleteRecordsPartition{
						partition_index: 1
						offset:          200
					},
				]
			},
		]
		timeout_ms: 30000
	}

	assert req.topics.len == 1
	assert req.topics[0].partitions.len == 2
	assert req.topics[0].partitions[0].offset == 100
	assert req.topics[0].partitions[1].offset == 200
}
