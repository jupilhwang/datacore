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

/// new_pipeline은 새로운 요청 파이프라인을 생성합니다.
/// max_pending은 동시에 대기할 수 있는 최대 요청 수를 지정합니다.
pub fn new_pipeline(max_pending int) &RequestPipeline {
	return &RequestPipeline{
		max_pending: max_pending
		pending:     []PendingRequest{cap: max_pending} // 용량 미리 할당
	}
}

/// enqueue는 새로운 요청을 파이프라인에 추가합니다.
/// 파이프라인이 가득 차면 에러를 반환합니다.
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

/// complete는 요청을 응답과 함께 완료 처리합니다.
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

/// complete_with_error는 요청을 에러와 함께 완료 처리합니다.
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

/// get_ready_responses는 전송 준비된 응답을 순서대로 반환합니다.
/// 앞에서부터 연속으로 완료된 요청만 반환합니다.
/// 예: [완료, 완료, 미완료, 완료] -> [완료, 완료]만 반환
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

/// peek_first는 첫 번째 대기 요청을 제거하지 않고 반환합니다.
pub fn (mut p RequestPipeline) peek_first() ?PendingRequest {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pending.len > 0 {
		return p.pending[0]
	}
	return none
}

/// pending_count는 대기 중인 요청 수를 반환합니다.
pub fn (mut p RequestPipeline) pending_count() int {
	p.lock.@lock()
	defer { p.lock.unlock() }

	return p.pending.len
}

/// is_full은 파이프라인이 더 이상 요청을 받을 수 없는지 확인합니다.
pub fn (mut p RequestPipeline) is_full() bool {
	p.lock.@lock()
	defer { p.lock.unlock() }

	return p.pending.len >= p.max_pending
}

/// clear는 모든 대기 요청을 제거합니다.
pub fn (mut p RequestPipeline) clear() {
	p.lock.@lock()
	defer { p.lock.unlock() }

	p.pending.clear()
}

/// get_stats는 파이프라인 통계를 반환합니다.
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

/// oldest_pending_age는 가장 오래된 대기 요청의 나이를 밀리초로 반환합니다.
/// 대기 요청이 없으면 0을 반환합니다.
/// 타임아웃 감지에 사용됩니다.
pub fn (mut p RequestPipeline) oldest_pending_age() i64 {
	p.lock.@lock()
	defer { p.lock.unlock() }

	if p.pending.len == 0 {
		return 0
	}

	return (time.now() - p.pending[0].received_at).milliseconds()
}

/// has_timed_out은 대기 요청 중 타임아웃된 것이 있는지 확인합니다.
/// timeout_ms를 초과한 요청이 하나라도 있으면 true를 반환합니다.
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
