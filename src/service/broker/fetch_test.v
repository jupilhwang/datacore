// FetchUseCase 단위 테스트.
// 메시지 소비 요청의 순차/병렬 처리, 타임아웃, 오류 처리 로직을 검증한다.
module broker

import domain
import infra.storage.plugins.memory

// 헬퍼: 테스트용 스토리지 생성
fn create_fetch_test_storage() &memory.MemoryStorageAdapter {
	return memory.new_memory_adapter()
}

// 헬퍼: 테스트 토픽 생성 후 레코드 추가
fn setup_topic_with_records(mut storage memory.MemoryStorageAdapter, topic string, partitions int, records_per_partition int) {
	storage.create_topic(topic, partitions, domain.TopicConfig{}) or { return }

	for p in 0 .. partitions {
		mut records := []domain.Record{cap: records_per_partition}
		for i in 0 .. records_per_partition {
			records << domain.Record{
				key:   'key-${p}-${i}'.bytes()
				value: 'value-${p}-${i}'.bytes()
			}
		}
		storage.append(topic, p, records, i16(-1)) or {}
	}
}

// 헬퍼: 테스트용 FetchUseCase 생성
fn create_fetch_usecase_with_storage(storage &memory.MemoryStorageAdapter) &FetchUseCase {
	return new_fetch_usecase(storage, storage)
}

// ============================================================
// 기본 fetch 테스트
// ============================================================

fn test_fetch_basic_from_topic_partition() {
	// 기존 토픽/파티션에서 레코드를 정상적으로 가져올 수 있어야 한다.
	mut storage := create_fetch_test_storage()
	setup_topic_with_records(mut storage, 'fetch-topic', 2, 5)

	mut usecase := create_fetch_usecase_with_storage(storage)

	req := FetchRequest{
		max_wait_ms: 500
		min_bytes:   1
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'fetch-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.error_code == 0
	assert resp.partitions.len == 1
	assert resp.partitions[0].error_code == 0
	assert resp.partitions[0].topic == 'fetch-topic'
	assert resp.partitions[0].partition == 0
	assert resp.partitions[0].records.len == 5, '레코드 수가 5여야 한다, 실제: ${resp.partitions[0].records.len}'
	assert resp.partitions[0].high_watermark == 5
}

fn test_fetch_with_offset() {
	// 특정 오프셋부터 fetch하면 해당 오프셋 이후 레코드만 반환해야 한다.
	mut storage := create_fetch_test_storage()
	setup_topic_with_records(mut storage, 'offset-fetch-topic', 1, 10)

	mut usecase := create_fetch_usecase_with_storage(storage)

	req := FetchRequest{
		max_wait_ms: 500
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'offset-fetch-topic'
				partition:    0
				fetch_offset: 5
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.partitions.len == 1
	assert resp.partitions[0].error_code == 0
	assert resp.partitions[0].records.len == 5, '오프셋 5부터 fetch하면 5개 레코드가 반환되어야 한다'
}

// ============================================================
// 존재하지 않는 토픽 테스트
// ============================================================

fn test_fetch_from_nonexistent_topic() {
	// 존재하지 않는 토픽에서 fetch하면 unknown_topic_or_partition 오류를 반환해야 한다.
	mut usecase := create_fetch_usecase_with_storage(create_fetch_test_storage())

	req := FetchRequest{
		max_wait_ms: 500
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'nonexistent-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.partitions.len == 1
	assert resp.partitions[0].error_code == i16(domain.ErrorCode.unknown_topic_or_partition), '존재하지 않는 토픽의 오류 코드가 unknown_topic_or_partition이어야 한다'
}

// ============================================================
// 빈 파티션 테스트
// ============================================================

fn test_fetch_from_empty_partition() {
	// 빈 파티션에서 fetch하면 빈 레코드 목록을 반환해야 한다.
	mut storage := create_fetch_test_storage()
	storage.create_topic('empty-topic', 1, domain.TopicConfig{}) or {
		assert false, '토픽 생성 실패: ${err}'
		return
	}

	mut usecase := create_fetch_usecase_with_storage(storage)

	req := FetchRequest{
		max_wait_ms: 500
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'empty-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.partitions.len == 1
	assert resp.partitions[0].error_code == 0
	assert resp.partitions[0].records.len == 0, '빈 파티션에서 fetch한 레코드 수가 0이어야 한다'
	assert resp.partitions[0].high_watermark == 0
}

// ============================================================
// 순차 처리 테스트 (파티션 수 <= 2)
// ============================================================

fn test_fetch_sequential_two_partitions() {
	// 2개 이하 파티션 요청은 순차 처리되어야 한다.
	mut storage := create_fetch_test_storage()
	setup_topic_with_records(mut storage, 'seq-topic', 3, 3)

	mut usecase := create_fetch_usecase_with_storage(storage)

	req := FetchRequest{
		max_wait_ms: 500
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'seq-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
			FetchPartitionRequest{
				topic:        'seq-topic'
				partition:    1
				fetch_offset: 0
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.partitions.len == 2
	for part_resp in resp.partitions {
		assert part_resp.error_code == 0
		assert part_resp.records.len == 3
	}
}

// ============================================================
// 병렬 처리 테스트 (파티션 수 > 2)
// ============================================================

fn test_fetch_parallel_multiple_partitions() {
	// 3개 초과 파티션 요청은 병렬로 처리되어야 한다.
	mut storage := create_fetch_test_storage()
	setup_topic_with_records(mut storage, 'parallel-topic', 5, 2)

	mut usecase := create_fetch_usecase_with_storage(storage)

	mut partitions := []FetchPartitionRequest{cap: 4}
	for i in 0 .. 4 {
		partitions << FetchPartitionRequest{
			topic:        'parallel-topic'
			partition:    i
			fetch_offset: 0
			max_bytes:    1048576
		}
	}

	req := FetchRequest{
		max_wait_ms: 5000
		max_bytes:   1048576
		partitions:  partitions
	}

	resp := usecase.execute(req)

	// 모든 파티션에서 응답을 받아야 한다
	assert resp.partitions.len == 4, '응답 파티션 수가 4여야 한다, 실제: ${resp.partitions.len}'

	// 모든 응답이 성공이어야 한다
	for part_resp in resp.partitions {
		assert part_resp.error_code == 0, '파티션 ${part_resp.partition}의 오류 코드가 0이어야 한다'
		assert part_resp.records.len == 2, '파티션 ${part_resp.partition}의 레코드 수가 2여야 한다'
	}
}

fn test_fetch_parallel_mixed_topics() {
	// 병렬 fetch에서 존재하지 않는 토픽이 포함되면 해당 파티션만 오류를 반환해야 한다.
	mut storage := create_fetch_test_storage()
	setup_topic_with_records(mut storage, 'existing-topic', 2, 3)

	mut usecase := create_fetch_usecase_with_storage(storage)

	req := FetchRequest{
		max_wait_ms: 5000
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'existing-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
			FetchPartitionRequest{
				topic:        'existing-topic'
				partition:    1
				fetch_offset: 0
				max_bytes:    1048576
			},
			FetchPartitionRequest{
				topic:        'missing-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.partitions.len == 3, '응답 파티션 수가 3이어야 한다'

	// 결과 분류
	mut success_count := 0
	mut error_count := 0
	for part_resp in resp.partitions {
		if part_resp.error_code == 0 {
			success_count += 1
		} else {
			error_count += 1
			assert part_resp.error_code == i16(domain.ErrorCode.unknown_topic_or_partition)
		}
	}
	assert success_count == 2, '성공 파티션 수가 2여야 한다'
	assert error_count == 1, '오류 파티션 수가 1이어야 한다'
}

// ============================================================
// 다중 파티션 fetch 테스트
// ============================================================

fn test_fetch_multiple_partitions_same_topic() {
	// 같은 토픽의 여러 파티션을 동시에 fetch할 수 있어야 한다.
	mut storage := create_fetch_test_storage()
	setup_topic_with_records(mut storage, 'multi-part-topic', 3, 4)

	mut usecase := create_fetch_usecase_with_storage(storage)

	req := FetchRequest{
		max_wait_ms: 500
		max_bytes:   1048576
		partitions:  [
			FetchPartitionRequest{
				topic:        'multi-part-topic'
				partition:    0
				fetch_offset: 0
				max_bytes:    1048576
			},
			FetchPartitionRequest{
				topic:        'multi-part-topic'
				partition:    2
				fetch_offset: 2
				max_bytes:    1048576
			},
		]
	}

	resp := usecase.execute(req)

	assert resp.partitions.len == 2
	// 파티션 0: 오프셋 0부터 4개
	// 파티션 2: 오프셋 2부터 2개
	for part_resp in resp.partitions {
		assert part_resp.error_code == 0
		if part_resp.partition == 0 {
			assert part_resp.records.len == 4
		} else if part_resp.partition == 2 {
			assert part_resp.records.len == 2, '파티션 2 오프셋 2부터 fetch하면 2개여야 한다, 실제: ${part_resp.records.len}'
		}
	}
}
