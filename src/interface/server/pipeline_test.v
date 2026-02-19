// Request Pipeline 단위 테스트
module server

import time

fn test_pipeline_creation() {
	mut pipeline := new_pipeline(10)

	assert pipeline.pending_count() == 0
	assert !pipeline.is_full()

	stats := pipeline.get_stats()
	assert stats.pending_count == 0
	assert stats.max_pending == 10
	assert stats.total_enqueued == 0
	assert stats.total_completed == 0
}

fn test_pipeline_enqueue() {
	mut pipeline := new_pipeline(10)

	// 요청 큐에 추가
	pipeline.enqueue(1, 0, 0, [u8(1), 2, 3]) or { assert false, 'enqueue should not fail' }

	assert pipeline.pending_count() == 1

	stats := pipeline.get_stats()
	assert stats.total_enqueued == 1
}

fn test_pipeline_enqueue_multiple() {
	mut pipeline := new_pipeline(10)

	// 여러 요청 큐에 추가
	for i in 1 .. 6 {
		pipeline.enqueue(i32(i), 0, 0, [u8(i)]) or { assert false, 'enqueue should not fail' }
	}

	assert pipeline.pending_count() == 5
	assert !pipeline.is_full()
}

fn test_pipeline_full() {
	mut pipeline := new_pipeline(3)

	// 파이프라인 채우기
	for i in 1 .. 4 {
		pipeline.enqueue(i32(i), 0, 0, [u8(i)]) or { assert false, 'enqueue should not fail' }
	}

	assert pipeline.is_full()

	// 가득 찬 상태에서 추가 시도 - 실패해야 함
	pipeline.enqueue(4, 0, 0, [u8(4)]) or {
		assert err.str().contains('pipeline full')
		return
	}
	assert false, 'should have failed due to full pipeline'
}

fn test_pipeline_complete_and_get_ready() {
	mut pipeline := new_pipeline(10)

	// 요청 큐에 추가
	pipeline.enqueue(1, 0, 0, [u8(1)]) or { assert false }
	pipeline.enqueue(2, 0, 0, [u8(2)]) or { assert false }
	pipeline.enqueue(3, 0, 0, [u8(3)]) or { assert false }

	// 첫 번째 요청 완료
	pipeline.complete(1, [u8(10), 11]) or { assert false }

	// 준비된 응답 가져오기 - 첫 번째만 반환되어야 함
	ready := pipeline.get_ready_responses()
	assert ready.len == 1
	assert ready[0].correlation_id == 1
	assert ready[0].response_data == [u8(10), 11]

	assert pipeline.pending_count() == 2
}

fn test_pipeline_order_preservation() {
	mut pipeline := new_pipeline(10)

	// 순서대로 요청 큐에 추가
	pipeline.enqueue(1, 0, 0, []) or { assert false }
	pipeline.enqueue(2, 0, 0, []) or { assert false }
	pipeline.enqueue(3, 0, 0, []) or { assert false }

	// 순서 무관하게 완료
	pipeline.complete(3, [u8(30)]) or { assert false }
	pipeline.complete(2, [u8(20)]) or { assert false }

	// 준비된 응답 가져오기 - 첫 번째가 완료되지 않아 없어야 함
	ready1 := pipeline.get_ready_responses()
	assert ready1.len == 0

	// 첫 번째 완료
	pipeline.complete(1, [u8(10)]) or { assert false }

	// 이제 세 개 모두 순서대로 반환되어야 함
	ready2 := pipeline.get_ready_responses()
	assert ready2.len == 3
	assert ready2[0].correlation_id == 1
	assert ready2[1].correlation_id == 2
	assert ready2[2].correlation_id == 3
}

fn test_pipeline_complete_with_error() {
	mut pipeline := new_pipeline(10)

	pipeline.enqueue(1, 0, 0, []) or { assert false }

	pipeline.complete_with_error(1, 'test error') or { assert false }

	ready := pipeline.get_ready_responses()
	assert ready.len == 1
	assert ready[0].error_msg == 'test error'
}

fn test_pipeline_complete_not_found() {
	mut pipeline := new_pipeline(10)

	pipeline.enqueue(1, 0, 0, []) or { assert false }

	// 존재하지 않는 요청 완료 시도
	pipeline.complete(999, []) or {
		assert err.str().contains('not found')
		return
	}
	assert false, 'should have failed for non-existent correlation_id'
}

fn test_pipeline_peek_first() {
	mut pipeline := new_pipeline(10)

	// 빈 파이프라인
	if _ := pipeline.peek_first() {
		assert false, 'should return none for empty pipeline'
	}

	pipeline.enqueue(42, 3, 2, [u8(1), 2, 3]) or { assert false }

	if req := pipeline.peek_first() {
		assert req.correlation_id == 42
		assert req.api_key == 3
		assert req.api_version == 2
	} else {
		assert false, 'should return first request'
	}

	// peek는 제거하지 않아야 함
	assert pipeline.pending_count() == 1
}

fn test_pipeline_clear() {
	mut pipeline := new_pipeline(10)

	for i in 1 .. 6 {
		pipeline.enqueue(i32(i), 0, 0, []) or { assert false }
	}

	assert pipeline.pending_count() == 5

	pipeline.clear()

	assert pipeline.pending_count() == 0
	assert !pipeline.is_full()
}

fn test_pipeline_oldest_pending_age() {
	mut pipeline := new_pipeline(10)

	// 빈 파이프라인
	assert pipeline.oldest_pending_age() == 0

	pipeline.enqueue(1, 0, 0, []) or { assert false }

	// age > 0 확인을 위한 짧은 지연
	time.sleep(10 * time.millisecond)

	age := pipeline.oldest_pending_age()
	assert age >= 10
}

fn test_pipeline_has_timed_out() {
	mut pipeline := new_pipeline(10)

	// 빈 파이프라인 - 타임아웃 없음
	assert !pipeline.has_timed_out(100)

	pipeline.enqueue(1, 0, 0, []) or { assert false }

	// 아직 타임아웃되지 않아야 함
	assert !pipeline.has_timed_out(10000)

	// 대기 후 짧은 타임아웃으로 확인
	time.sleep(50 * time.millisecond)
	assert pipeline.has_timed_out(10)
}

fn test_pipeline_stats() {
	mut pipeline := new_pipeline(5)

	// 요청 큐에 추가 및 완료
	pipeline.enqueue(1, 0, 0, []) or { assert false }
	pipeline.enqueue(2, 0, 0, []) or { assert false }

	pipeline.complete(1, []) or { assert false }
	pipeline.complete(2, []) or { assert false }

	_ := pipeline.get_ready_responses()

	stats := pipeline.get_stats()
	assert stats.max_pending == 5
	assert stats.total_enqueued == 2
	assert stats.total_completed == 2
	assert stats.pending_count == 0
}

fn test_pending_request_struct() {
	now := time.now()
	req := PendingRequest{
		correlation_id: 123
		api_key:        1
		api_version:    10
		received_at:    now
		request_data:   [u8(1), 2, 3]
		response_data:  [u8(4), 5, 6]
		completed:      true
		error_msg:      ''
	}

	assert req.correlation_id == 123
	assert req.api_key == 1
	assert req.api_version == 10
	assert req.request_data.len == 3
	assert req.response_data.len == 3
	assert req.completed == true
	assert req.error_msg == ''
}
