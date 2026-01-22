/// Interface Layer - Worker Pool
/// 인터페이스 레이어 - 워커 풀
///
/// 이 모듈은 동시 연결 핸들러 수를 제한하여
/// 고부하 상황에서 고루틴 폭발을 방지합니다.
///
/// 세마포어 패턴을 사용하여 워커 슬롯을 관리하며,
/// 타임아웃과 함께 슬롯 획득을 지원합니다.
module server

import sync
import time

/// WorkerPoolConfig는 워커 풀 설정을 담는 구조체입니다.
pub struct WorkerPoolConfig {
pub:
	max_workers      int = 1000 // 최대 동시 연결 핸들러 수
	queue_size       int = 5000 // 최대 대기 연결 큐 크기
	acquire_timeout  int = 5000 // 워커 슬롯 획득 타임아웃 (ms)
	metrics_interval int = 60   // 메트릭 로깅 간격 (초)
}

/// WorkerPoolMetrics는 워커 풀 통계를 추적하는 구조체입니다.
pub struct WorkerPoolMetrics {
pub mut:
	active_workers     int // 현재 활성 워커 수
	peak_workers       int // 최대 동시 워커 수
	queued_connections int // 현재 대기 중인 연결 수
	total_acquired     u64 // 총 슬롯 획득 성공 수
	total_released     u64 // 총 슬롯 해제 수
	total_timeouts     u64 // 총 획득 타임아웃 수
	total_rejected     u64 // 총 거부 수 (큐 가득 참)
}

/// WorkerPool은 연결 처리를 위한 고정 크기 워커 슬롯 풀을 관리합니다.
/// 세마포어 패턴을 사용하여 동시 실행 수를 제한합니다.
pub struct WorkerPool {
mut:
	config       WorkerPoolConfig  // 워커 풀 설정
	semaphore    chan bool         // 워커 슬롯용 카운팅 세마포어 (채널 기반)
	metrics      WorkerPoolMetrics // 워커 풀 통계
	metrics_lock sync.Mutex        // 메트릭 동기화용 뮤텍스
	running      bool              // 풀 실행 상태 플래그
}

/// new_worker_pool은 새로운 워커 풀을 생성합니다.
/// 세마포어 채널을 max_workers 크기로 생성하고 토큰으로 채웁니다.
pub fn new_worker_pool(config WorkerPoolConfig) &WorkerPool {
	// 용량이 max_workers인 세마포어 채널 생성
	mut sem := chan bool{cap: config.max_workers}
	// 채널을 토큰으로 미리 채움 (사용 가능한 슬롯 표시)
	for _ in 0 .. config.max_workers {
		sem <- true
	}

	return &WorkerPool{
		config:    config
		semaphore: sem
		running:   true // 초기 상태: 실행 중
	}
}

/// acquire는 워커 슬롯 획득을 시도합니다.
/// 성공하면 true, 타임아웃 또는 풀 종료 중이면 false를 반환합니다.
pub fn (mut wp WorkerPool) acquire() bool {
	if !wp.running {
		return false
	}

	// 타임아웃과 함께 획득 시도
	select {
		_ := <-wp.semaphore {
			// 슬롯 획득 성공
			wp.update_metrics_on_acquire()
			return true
		}
		wp.config.acquire_timeout * time.millisecond {
			// 타임아웃
			wp.metrics_lock.@lock()
			wp.metrics.total_timeouts += 1
			wp.metrics_lock.unlock()
			return false
		}
	}

	return false
}

/// try_acquire는 블로킹 없이 워커 슬롯 획득을 시도합니다.
/// 성공하면 true, 사용 가능한 슬롯이 없으면 false를 반환합니다.
pub fn (mut wp WorkerPool) try_acquire() bool {
	if !wp.running {
		return false
	}

	select {
		_ := <-wp.semaphore {
			wp.update_metrics_on_acquire()
			return true
		}
		else {
			wp.metrics_lock.@lock()
			wp.metrics.total_rejected += 1
			wp.metrics_lock.unlock()
			return false
		}
	}

	return false
}

/// release는 워커 슬롯을 풀에 반환합니다.
/// acquire()와 쌍으로 호출되어야 합니다.
pub fn (mut wp WorkerPool) release() {
	// 메트릭 업데이트 (락 보호)
	wp.metrics_lock.@lock()
	wp.metrics.active_workers -= 1
	wp.metrics.total_released += 1
	wp.metrics_lock.unlock()

	// 세마포어에 토큰 반환 (논블로킹)
	select {
		wp.semaphore <- true {}
		else {
			// acquire/release가 쌍으로 호출되면 발생하지 않음
			// 채널이 가득 찬 경우 (비정상 상황)
		}
	}
}

/// update_metrics_on_acquire는 슬롯 획득 시 메트릭을 업데이트합니다.
/// 활성 워커 수 증가, 총 획득 수 증가, 피크 워커 수 갱신을 수행합니다.
fn (mut wp WorkerPool) update_metrics_on_acquire() {
	wp.metrics_lock.@lock()
	wp.metrics.active_workers += 1
	wp.metrics.total_acquired += 1
	if wp.metrics.active_workers > wp.metrics.peak_workers {
		wp.metrics.peak_workers = wp.metrics.active_workers
	}
	wp.metrics_lock.unlock()
}

/// get_metrics는 현재 워커 풀 메트릭을 반환합니다.
pub fn (mut wp WorkerPool) get_metrics() WorkerPoolMetrics {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.metrics
}

/// active_count는 활성 워커 수를 반환합니다.
pub fn (mut wp WorkerPool) active_count() int {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.metrics.active_workers
}

/// available_slots는 사용 가능한 워커 슬롯 수를 반환합니다.
pub fn (mut wp WorkerPool) available_slots() int {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.config.max_workers - wp.metrics.active_workers
}

/// shutdown은 워커 풀을 우아하게 종료합니다.
/// running 플래그를 false로 설정하고 세마포어를 드레인합니다.
pub fn (mut wp WorkerPool) shutdown() {
	wp.running = false
	// 세마포어 드레인 (대기 중인 acquire 호출 해제)
	for {
		select {
			_ := <-wp.semaphore {}
			else {
				break // 더 이상 토큰이 없으면 종료
			}
		}
	}
}

/// is_running은 풀이 아직 실행 중인지 확인합니다.
pub fn (wp &WorkerPool) is_running() bool {
	return wp.running
}

/// WorkerGuard는 RAII 스타일의 워커 슬롯 관리를 제공합니다.
/// defer와 함께 사용하여 슬롯이 항상 해제되도록 보장합니다.
/// 사용 예: defer { guard.release() }
pub struct WorkerGuard {
mut:
	pool     &WorkerPool // 연결된 워커 풀 참조
	released bool        // 해제 여부 (중복 해제 방지)
}

/// new_worker_guard는 새로운 워커 가드를 생성합니다.
/// 워커 슬롯을 획득한 후 이 가드를 생성하여 자동 해제를 보장합니다.
pub fn new_worker_guard(mut pool WorkerPool) WorkerGuard {
	return WorkerGuard{
		pool:     pool
		released: false // 초기 상태: 미해제
	}
}

/// release는 워커 슬롯을 해제합니다.
/// 멱등성을 보장하여 여러 번 호출해도 안전합니다.
pub fn (mut g WorkerGuard) release() {
	if !g.released {
		g.pool.release()
		g.released = true // 중복 해제 방지
	}
}
