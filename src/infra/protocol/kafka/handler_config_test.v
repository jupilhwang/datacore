// Config 핸들러 단위 테스트
// process_describe_configs, DescribeConfigsResponse.encode 검증
module kafka

import domain
import infra.compression
import infra.storage.plugins.memory

// -- 테스트 헬퍼 --

fn create_config_test_handler() (&memory.MemoryStorageAdapter, Handler) {
	storage := memory.new_memory_adapter()
	cs := compression.new_default_compression_service() or {
		panic('compression service 생성 실패: ${err}')
	}
	handler := new_handler(1, 'localhost', 9092, 'test-cluster', storage, cs)
	return storage, handler
}

// -- process_describe_configs 테스트 --

fn test_process_describe_configs_topic_found() {
	mut storage, mut handler := create_config_test_handler()

	storage.create_topic('config-topic', 3, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 2 // TOPIC
				resource_name: 'config-topic'
				config_names:  none
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == 0
	assert resp.results[0].resource_name == 'config-topic'
	assert resp.results[0].resource_type == 2
	assert resp.results[0].configs.len > 0, '기본 설정이 반환되어야 한다'
	assert resp.throttle_time_ms == default_throttle_time_ms
}

fn test_process_describe_configs_topic_not_found() {
	_, mut handler := create_config_test_handler()

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 2
				resource_name: 'nonexistent-topic'
				config_names:  none
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == i16(ErrorCode.unknown_topic_or_partition)
	assert resp.results[0].configs.len == 0
}

fn test_process_describe_configs_specific_config_names() {
	mut storage, mut handler := create_config_test_handler()

	storage.create_topic('specific-config-topic', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 2
				resource_name: 'specific-config-topic'
				config_names:  ['retention.ms']
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == 0
	assert resp.results[0].configs.len == 1, '요청한 설정만 반환되어야 한다'
	assert resp.results[0].configs[0].name == 'retention.ms'
}

fn test_process_describe_configs_broker_resource() {
	_, mut handler := create_config_test_handler()

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 4   // BROKER
				resource_name: '1' // broker_id = 1
				config_names:  none
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == 0
	assert resp.results[0].resource_type == 4
	assert resp.results[0].resource_name == '1'
	assert resp.results[0].configs.len > 0, '브로커 설정이 반환되어야 한다'
}

fn test_process_describe_configs_broker_not_found() {
	_, mut handler := create_config_test_handler()

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 4
				resource_name: '999' // 존재하지 않는 broker_id
				config_names:  none
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == i16(ErrorCode.resource_not_found)
}

fn test_process_describe_configs_unsupported_resource_type() {
	_, mut handler := create_config_test_handler()

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 99 // 지원하지 않는 타입
				resource_name: 'something'
				config_names:  none
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == i16(ErrorCode.invalid_request)
}

fn test_process_describe_configs_multiple_resources() {
	mut storage, mut handler := create_config_test_handler()

	storage.create_topic('multi-config-1', 1, domain.TopicConfig{}) or {
		panic('토픽 생성 실패: ${err}')
	}

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 2
				resource_name: 'multi-config-1'
				config_names:  none
			},
			DescribeConfigsResource{
				resource_type: 4
				resource_name: '1'
				config_names:  none
			},
			DescribeConfigsResource{
				resource_type: 2
				resource_name: 'nonexistent'
				config_names:  none
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 3
	assert resp.results[0].error_code == 0, '존재하는 토픽은 성공해야 한다'
	assert resp.results[1].error_code == 0, '브로커 설정은 성공해야 한다'
	assert resp.results[2].error_code == i16(ErrorCode.unknown_topic_or_partition), '존재하지 않는 토픽은 에러여야 한다'
}

fn test_process_describe_configs_broker_specific_config() {
	_, mut handler := create_config_test_handler()

	req := DescribeConfigsRequest{
		resources:        [
			DescribeConfigsResource{
				resource_type: 4
				resource_name: '1'
				config_names:  ['num.partitions']
			},
		]
		include_synonyms: false
	}

	resp := handler.process_describe_configs(req, 0) or {
		panic('process_describe_configs 실패: ${err}')
	}

	assert resp.results.len == 1
	assert resp.results[0].error_code == 0
	assert resp.results[0].configs.len == 1
	assert resp.results[0].configs[0].name == 'num.partitions'
	assert resp.results[0].configs[0].value or { '' } == '1'
}

// -- DescribeConfigsResponse.encode 테스트 --

fn test_describe_configs_response_encode_v0() {
	resp := DescribeConfigsResponse{
		throttle_time_ms: 0
		results:          [
			DescribeConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'test-topic'
				configs:       [
					DescribeConfigsEntry{
						name:          'retention.ms'
						value:         '604800000'
						read_only:     false
						is_default:    true
						config_source: 4
						is_sensitive:  false
						synonyms:      []
						config_type:   0
						documentation: none
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
}

fn test_describe_configs_response_encode_v4_flexible() {
	resp := DescribeConfigsResponse{
		throttle_time_ms: 50
		results:          [
			DescribeConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'flex-topic'
				configs:       [
					DescribeConfigsEntry{
						name:          'cleanup.policy'
						value:         'delete'
						read_only:     false
						is_default:    true
						config_source: 4
						is_sensitive:  false
						synonyms:      []
						config_type:   0
						documentation: 'The cleanup policy'
					},
				]
			},
		]
	}

	encoded := resp.encode(4)
	assert encoded.len > 0, 'v4 flexible 인코딩은 비어있으면 안 된다'

	mut reader := new_reader(encoded)
	throttle := reader.read_i32() or { panic('throttle 읽기 실패') }
	assert throttle == 50
}

fn test_describe_configs_response_encode_empty_results() {
	resp := DescribeConfigsResponse{
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

fn test_describe_configs_response_encode_with_error() {
	resp := DescribeConfigsResponse{
		throttle_time_ms: 0
		results:          [
			DescribeConfigsResult{
				error_code:    i16(ErrorCode.unknown_topic_or_partition)
				error_message: 'topic not found'
				resource_type: 2
				resource_name: 'missing-topic'
				configs:       []
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

fn test_describe_configs_response_encode_v1_with_synonyms() {
	resp := DescribeConfigsResponse{
		throttle_time_ms: 0
		results:          [
			DescribeConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'syn-topic'
				configs:       [
					DescribeConfigsEntry{
						name:          'retention.ms'
						value:         '604800000'
						read_only:     false
						is_default:    true
						config_source: 4
						is_sensitive:  false
						synonyms:      [
							DescribeConfigsSynonym{
								name:          'log.retention.ms'
								value:         '604800000'
								config_source: 4
							},
						]
						config_type:   0
						documentation: none
					},
				]
			},
		]
	}

	encoded := resp.encode(1)
	assert encoded.len > 0, 'v1 synonym 포함 인코딩은 비어있으면 안 된다'
}

fn test_describe_configs_response_encode_v3_with_documentation() {
	resp := DescribeConfigsResponse{
		throttle_time_ms: 0
		results:          [
			DescribeConfigsResult{
				error_code:    0
				error_message: none
				resource_type: 2
				resource_name: 'doc-topic'
				configs:       [
					DescribeConfigsEntry{
						name:          'retention.ms'
						value:         '604800000'
						read_only:     false
						is_default:    true
						config_source: 4
						is_sensitive:  false
						synonyms:      []
						config_type:   1
						documentation: 'Time to retain logs'
					},
				]
			},
		]
	}

	encoded := resp.encode(3)
	assert encoded.len > 0, 'v3 documentation 포함 인코딩은 비어있으면 안 된다'
}
