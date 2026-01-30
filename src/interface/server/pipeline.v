/// Interface Layer - Request Pipelining
/// 인터페이스 레이어 - 요청 파이프라이닝
///
/// Kafka 프로토콜은 요청 파이프라이닝을 지원합니다.
/// 클라이언트는 응답을 받기 전에 여러 요청을 보낼 수 있지만,
/// 응답은 반드시 요청 순서대로 반환되어야 합니다.
///
/// 이 모듈은 요청 순서를 추적하고 응답을 올바른 순서로
/// 반환하는 기능을 제공합니다.
module server

import sync
import time

/// PendingRequest는 응답을 기다리는 요청을 나타냅니다.
pub struct PendingRequest {
pub:
	correlation_id i32       // 상관 ID (요청-응답 매칭용)
	api_key        i16       // API 키
	api_version    i16       // API 버전
	received_at    time.Time // 수신 시간
	request_data   []u8      // 요청 데이터
pub mut:
	response_data []u8   // 응답 데이터
	completed     bool   // 완료 여부
	error_msg     string // 에러 메시지 (있는 경우)
}

/// RequestPipeline은 연결에 대한 파이프라인된 요청을 관리합니다.
/// 응답이 요청 수신 순서대로 전송되도록 보장합니다.
pub struct RequestPipeline {
mut:
	pending         []PendingRequest // 대기 중인 요청 목록 (FIFO 순서)
	max_pending     int              // 최대 대기 요청 수 제한
	lock            sync.Mutex       // 동시 접근 보호용 뮤텍스
	total_enqueued  u64              // 총 큐에 추가된 요청 수 (통계)
	total_completed u64              // 총 완료된 요청 수 (통계)
}

/// new_pipeline - creates a new request pipeline
/// new_pipeline - creates a new request pipeline
pub fn new_pipeline(max_pending int) &RequestPipeline {
	return &RequestPipeline{
		max_pending: max_pending
		pending:     []PendingRequest{cap: max_pending} // 용량 미리 할당
	}
}

/// enqueue - adds a new request to the pipeline
/// enqueue - adds a new request to the pipeline
pub fn (mut p RequestPipeline) enqueue(correlation_id i32, api_key i16, api_version i16, data []u8) ! {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pending.len >= p.max_pending {
		return error('pipeline full: ${p.pending.len}/${p.max_pending} pending requests')
	}

	p.pending << PendingRequest{
		correlation_id: correlation_id
		api_key:        api_key
		api_version:    api_version
		received_at:    time.now()
		request_data:   data
	}
	p.total_enqueued += 1
}

/// complete - completes a request with a response
/// complete - completes a request with a response
pub fn (mut p RequestPipeline) complete(correlation_id i32, response []u8) ! {
	p.lock.@lock()
	defer { p.lock.unlock() }

	for mut req in p.pending {
		if req.correlation_id == correlation_id {
			req.response_data = response
			req.completed = true
			p.total_completed += 1
			return
		}
	}

	return error('correlation_id ${correlation_id} not found in pipeline')
}

/// complete_with_error - completes a request with an error
/// complete_with_error - completes a request with an error
pub fn (mut p RequestPipeline) complete_with_error(correlation_id i32, err_msg string) ! {
	p.lock.@lock()
	defer { p.lock.unlock() }

	for mut req in p.pending {
		if req.correlation_id == correlation_id {
			req.completed = true
			req.error_msg = err_msg
			p.total_completed += 1
			return
		}
	}

	return error('correlation_id ${correlation_id} not found in pipeline')
}

/// get_ready_responses - returns ready responses in order
/// get_ready_responses - returns ready responses in order
pub fn (mut p RequestPipeline) get_ready_responses() []PendingRequest {
	p.lock.@lock()
	defer { p.lock.unlock() }

	// 연속으로 완료된 요청 수 계산
	mut ready_count := 0
	for i in 0 .. p.pending.len {
		if p.pending[i].completed {
			ready_count++
		} else {
			break
		}
	}

	if ready_count == 0 {
		return []PendingRequest{}
	}

	// 완료된 요청들을 복사하고 배열에서 제거
	// 슬라이싱을 사용하여 O(n) 대신 O(ready_count)로 복사
	ready := p.pending[0..ready_count].clone()

	// 나머지 요청들만 유지 (배열 재할당 한 번만 발생)
	p.pending = p.pending[ready_count..].clone()

	return ready
}

/// peek_first - returns the first pending request without removing it
/// peek_first - returns the first pending request without removing it
pub fn (mut p RequestPipeline) peek_first() ?PendingRequest {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pending.len > 0 {
		return p.pending[0]
	}
	return none
}

/// pending_count - returns the number of pending requests
/// pending_count - returns the number of pending requests
pub fn (mut p RequestPipeline) pending_count() int {
	p.lock.@lock()
	defer { p.lock.unlock() }

	return p.pending.len
}

/// is_full - checks if the pipeline cannot accept more requests
/// is_full - checks if the pipeline cannot accept more requests
pub fn (mut p RequestPipeline) is_full() bool {
	p.lock.@lock()
	defer { p.lock.unlock() }

	return p.pending.len >= p.max_pending
}

/// clear - removes all pending requests
/// clear - removes all pending requests
pub fn (mut p RequestPipeline) clear() {
	p.lock.@lock()
	defer { p.lock.unlock() }

	p.pending.clear()
}

/// get_stats - returns pipeline statistics
/// get_stats - returns pipeline statistics
pub fn (mut p RequestPipeline) get_stats() PipelineStats {
	p.lock.@lock()
	defer { p.lock.unlock() }

	return PipelineStats{
		pending_count:   p.pending.len
		max_pending:     p.max_pending
		total_enqueued:  p.total_enqueued
		total_completed: p.total_completed
	}
}

/// PipelineStats는 파이프라인 통계를 담는 구조체입니다.
pub struct PipelineStats {
pub:
	pending_count   int // 현재 대기 중인 요청 수
	max_pending     int // 최대 대기 가능 요청 수
	total_enqueued  u64 // 총 큐에 추가된 요청 수
	total_completed u64 // 총 완료된 요청 수
}

/// oldest_pending_age - returns the age of the oldest pending request in milliseconds
/// oldest_pending_age - returns the age of the oldest pending request in milliseconds
pub fn (mut p RequestPipeline) oldest_pending_age() i64 {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pending.len == 0 {
		return 0
	}

	return (time.now() - p.pending[0].received_at).milliseconds()
}

/// has_timed_out - checks if any pending request has timed out
/// has_timed_out - checks if any pending request has timed out
pub fn (mut p RequestPipeline) has_timed_out(timeout_ms i64) bool {
	p.lock.@lock()
	defer { p.lock.unlock() }

	now := time.now()
	for req in p.pending {
		// 각 요청의 대기 시간을 계산하여 타임아웃 확인
		if (now - req.received_at).milliseconds() > timeout_ms {
			return true
		}
	}
	return false
}
