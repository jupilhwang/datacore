// IncrementalAlterConfigs 핸들러 단위 테스트
// IncrementalAlterConfigsResponse.encode 및 요청/응답 구조 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_incremental_alter_configs_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)
	return storage, handler
}

// -- IncrementalAlterConfigsResponse.encode 테스트 --

fn test_incremental_alter_configs_response_encode_v0() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        [
			IncrementalAlterConfigsResourceResponse{
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

fn test_incremental_alter_configs_response_encode_v1_flexible() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 100
		responses:        [
			IncrementalAlterConfigsResourceResponse{
				error_code:    0
				error_message: none
				resource_type: 4
				resource_name: '1'
			},
		]
	}

	encoded := resp.encode(1)
	assert encoded.len > 0, 'v1 flexible 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 100
}

fn test_incremental_alter_configs_response_encode_with_error() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        [
			IncrementalAlterConfigsResourceResponse{
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

fn test_incremental_alter_configs_response_encode_multiple() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        [
			IncrementalAlterConfigsResourceResponse{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'topic-a'
			},
			IncrementalAlterConfigsResourceResponse{
				error_code:    0
				error_message: none
				resource_type: 4
				resource_name: '1'
			},
			IncrementalAlterConfigsResourceResponse{
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

fn test_incremental_alter_configs_response_encode_empty() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        []
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	count := reader.read_array_len() or { panic('배열 읽기 실패') }
	assert count == 0
}

fn test_incremental_alter_configs_response_encode_invalid_config_error() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        [
			IncrementalAlterConfigsResourceResponse{
				error_code:    i16(ErrorCode.invalid_config)
				error_message: 'Invalid config operation 5 for retention.ms'
				resource_type: 2
				resource_name: 'bad-config-topic'
			},
		]
	}

	encoded := resp.encode(0)
	assert encoded.len > 0

	mut reader := new_reader(encoded)
	_ := reader.read_i32() or { panic('throttle 읽기 실패') }
	_ := reader.read_array_len() or { panic('배열 읽기 실패') }
	err_code := reader.read_i16() or { panic('error_code 읽기 실패') }
	assert err_code == i16(ErrorCode.invalid_config)
}

// -- IncrementalAlterConfigsRequest 구조 검증 --

fn test_incremental_alter_configs_request_struct() {
	req := IncrementalAlterConfigsRequest{
		resources:     [
			IncrementalAlterConfigsResource{
				resource_type: 2
				resource_name: 'my-topic'
				configs:       [
					IncrementalAlterableConfig{
						name:             'retention.ms'
						config_operation: 0 // SET
						value:            '86400000'
					},
					IncrementalAlterableConfig{
						name:             'cleanup.policy'
						config_operation: 1 // DELETE
						value:            none
					},
				]
			},
		]
		validate_only: true
	}

	assert req.resources.len == 1
	assert req.resources[0].configs.len == 2
	assert req.resources[0].configs[0].config_operation == 0
	assert req.resources[0].configs[1].config_operation == 1
	assert req.validate_only == true
}

fn test_incremental_alter_configs_request_all_operations() {
	// config_operation: 0=SET, 1=DELETE, 2=APPEND, 3=SUBTRACT
	configs := [
		IncrementalAlterableConfig{
			name:             'key1'
			config_operation: 0
			value:            'set-value'
		},
		IncrementalAlterableConfig{
			name:             'key2'
			config_operation: 1
			value:            none
		},
		IncrementalAlterableConfig{
			name:             'key3'
			config_operation: 2
			value:            'append-value'
		},
		IncrementalAlterableConfig{
			name:             'key4'
			config_operation: 3
			value:            'subtract-value'
		},
	]

	assert configs[0].config_operation == 0
	assert configs[1].config_operation == 1
	assert configs[2].config_operation == 2
	assert configs[3].config_operation == 3
}

fn test_incremental_alter_configs_resource_response_struct() {
	resp := IncrementalAlterConfigsResourceResponse{
		error_code:    0
		error_message: none
		resource_type: 2
		resource_name: 'result-topic'
	}

	assert resp.error_code == 0
	assert resp.resource_type == 2
	assert resp.resource_name == 'result-topic'
	assert resp.error_message == none
}

fn test_incremental_alter_configs_response_encode_v0_v1_size_diff() {
	resp := IncrementalAlterConfigsResponse{
		throttle_time_ms: 0
		responses:        [
			IncrementalAlterConfigsResourceResponse{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'size-topic'
			},
		]
	}

	encoded_v0 := resp.encode(0)
	encoded_v1 := resp.encode(1)

	// v1 (flexible) 은 tagged_fields 등으로 인해 v0과 크기가 다를 수 있다
	assert encoded_v0.len > 0
	assert encoded_v1.len > 0
}
