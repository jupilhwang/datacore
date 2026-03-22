// ProduceUseCase 단위 테스트.
// 메시지 생산 요청의 유효성 검증, 토픽 존재 여부 확인, 레코드 저장 로직을 검증한다.
module broker

import domain
import infra.storage.plugins.memory

// 헬퍼: 테스트용 스토리지 생성
fn create_produce_test_storage() &memory.MemoryStorageAdapter {
	return memory.new_memory_adapter()
}

// 헬퍼: 테스트용 ProduceUseCase 생성 (스토리지 공유)
fn create_produce_usecase_with_storage(storage &memory.MemoryStorageAdapter) &ProduceUseCase {
	return new_produce_usecase(storage, storage)
}

// ============================================================
// 성공 케이스 테스트
// ============================================================

fn test_produce_success_to_existing_topic() {
	// 존재하는 토픽/파티션에 레코드를 생산하면 성공해야 한다.
	mut storage := create_produce_test_storage()
	storage.create_topic('test-topic', 3, domain.TopicConfig{}) or {
		assert false, '토픽 생성 실패: ${err}'
		return
	}

	mut usecase := create_produce_usecase_with_storage(storage)

	req := ProduceRequest{
		topic:      'test-topic'
		partition:  0
		records:    [
			domain.Record{
				key:   'key1'.bytes()
				value: 'value1'.bytes()
			},
		]
		acks:       -1
		timeout_ms: 30000
	}

	resp := usecase.execute(req) or {
		assert false, 'produce 실행 실패: ${err}'
		return
	}

	assert resp.error_code == 0, '오류 코드가 0이어야 한다, 실제: ${resp.error_code}'
	assert resp.topic == 'test-topic'
	assert resp.partition == 0
	assert resp.base_offset == 0
}

fn test_produce_multiple_records() {
	// 여러 레코드를 한 번에 생산하면 모두 저장되어야 한다.
	mut storage := create_produce_test_storage()
	storage.create_topic('multi-topic', 2, domain.TopicConfig{}) or {
		assert false, '토픽 생성 실패: ${err}'
		return
	}

	mut usecase := create_produce_usecase_with_storage(storage)

	records := [
		domain.Record{
			key:   'k1'.bytes()
			value: 'v1'.bytes()
		},
		domain.Record{
			key:   'k2'.bytes()
			value: 'v2'.bytes()
		},
		domain.Record{
			key:   'k3'.bytes()
			value: 'v3'.bytes()
		},
	]

	req := ProduceRequest{
		topic:      'multi-topic'
		partition:  1
		records:    records
		acks:       1
		timeout_ms: 5000
	}

	resp := usecase.execute(req) or {
		assert false, 'produce 실행 실패: ${err}'
		return
	}

	assert resp.error_code == 0
	assert resp.base_offset == 0

	// fetch로 저장된 레코드 수 확인
	fetch_result := storage.fetch('multi-topic', 1, 0, 1048576) or {
		assert false, 'fetch 실패: ${err}'
		return
	}
	assert fetch_result.records.len == 3, '저장된 레코드 수가 3이어야 한다, 실제: ${fetch_result.records.len}'
}

fn test_produce_successive_appends_increment_offset() {
	// 연속 produce 호출 시 base_offset이 증가해야 한다.
	mut storage := create_produce_test_storage()
	storage.create_topic('offset-topic', 1, domain.TopicConfig{}) or {
		assert false, '토픽 생성 실패: ${err}'
		return
	}

	mut usecase := create_produce_usecase_with_storage(storage)

	req1 := ProduceRequest{
		topic:      'offset-topic'
		partition:  0
		records:    [
			domain.Record{
				key:   'k1'.bytes()
				value: 'v1'.bytes()
			},
			domain.Record{
				key:   'k2'.bytes()
				value: 'v2'.bytes()
			},
		]
		acks:       -1
		timeout_ms: 5000
	}

	resp1 := usecase.execute(req1) or {
		assert false, '첫 번째 produce 실패: ${err}'
		return
	}
	assert resp1.base_offset == 0

	req2 := ProduceRequest{
		topic:      'offset-topic'
		partition:  0
		records:    [
			domain.Record{
				key:   'k3'.bytes()
				value: 'v3'.bytes()
			},
		]
		acks:       -1
		timeout_ms: 5000
	}

	resp2 := usecase.execute(req2) or {
		assert false, '두 번째 produce 실패: ${err}'
		return
	}
	assert resp2.base_offset == 2, '두 번째 base_offset은 2여야 한다, 실제: ${resp2.base_offset}'
}

// ============================================================
// 실패 케이스 테스트
// ============================================================

fn test_produce_to_nonexistent_topic() {
	// 존재하지 않는 토픽에 produce하면 unknown_topic_or_partition 오류를 반환해야 한다.
	mut usecase := create_produce_usecase_with_storage(create_produce_test_storage())

	req := ProduceRequest{
		topic:      'nonexistent-topic'
		partition:  0
		records:    [
			domain.Record{
				key:   'k1'.bytes()
				value: 'v1'.bytes()
			},
		]
		acks:       -1
		timeout_ms: 5000
	}

	resp := usecase.execute(req) or {
		assert false, 'execute는 error가 아닌 응답을 반환해야 한다'
		return
	}

	assert resp.error_code == i16(domain.ErrorCode.unknown_topic_or_partition), '존재하지 않는 토픽에 대한 오류 코드가 unknown_topic_or_partition이어야 한다, 실제: ${resp.error_code}'
}

fn test_produce_with_empty_records() {
	// 빈 레코드로 produce하면 invalid_request 오류를 반환해야 한다.
	mut storage := create_produce_test_storage()
	storage.create_topic('empty-records-topic', 1, domain.TopicConfig{}) or {
		assert false, '토픽 생성 실패: ${err}'
		return
	}

	mut usecase := create_produce_usecase_with_storage(storage)

	req := ProduceRequest{
		topic:      'empty-records-topic'
		partition:  0
		records:    []domain.Record{}
		acks:       -1
		timeout_ms: 5000
	}

	resp := usecase.execute(req) or {
		assert false, 'execute는 error가 아닌 응답을 반환해야 한다'
		return
	}

	assert resp.error_code == i16(domain.ErrorCode.invalid_request), '빈 레코드에 대한 오류 코드가 invalid_request여야 한다, 실제: ${resp.error_code}'
}

fn test_produce_with_empty_topic_name() {
	// 빈 토픽 이름으로 produce하면 invalid_topic_exception 오류를 반환해야 한다.
	mut usecase := create_produce_usecase_with_storage(create_produce_test_storage())

	req := ProduceRequest{
		topic:      ''
		partition:  0
		records:    [
			domain.Record{
				key:   'k1'.bytes()
				value: 'v1'.bytes()
			},
		]
		acks:       -1
		timeout_ms: 5000
	}

	resp := usecase.execute(req) or {
		assert false, 'execute는 error가 아닌 응답을 반환해야 한다'
		return
	}

	assert resp.error_code == i16(domain.ErrorCode.invalid_topic_exception), '빈 토픽 이름에 대한 오류 코드가 invalid_topic_exception이어야 한다, 실제: ${resp.error_code}'
}
