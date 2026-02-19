// 분산 락을 사용하여 컨트롤러 선출을 관리
// v0.28.0: 멀티 브로커 클러스터를 위한 리더 선출 구현
module cluster

import service.port
import sync
import time

// 컨트롤러 선출 상수

// 컨트롤러 선출을 위한 락 이름
const controller_lock_name = 'controller-election'

// 컨트롤러 선출 락 TTL (밀리초)
const controller_lock_ttl_ms = i64(30000)

// 컨트롤러 갱신 주기 (TTL보다 짧아야 함)
const controller_refresh_interval_ms = 10000

// 컨트롤러 선출기

/// ControllerElector는 멀티 브로커 모드에서 컨트롤러 선출을 관리한다
pub struct ControllerElector {
	broker_id i32
mut:
	// 분산 락을 위한 클러스터 메타데이터 포트
	metadata_port ?port.ClusterMetadataPort
	// 현재 컨트롤러 상태
	is_controller bool
	controller_id i32
	// 락 상태
	lock_acquired bool
	last_refresh  i64
	// 스레드 안전성
	lock sync.RwMutex
	// 백그라운드 워커 제어
	running bool
	// 콜백 함수들
	on_become_controller  ?fn ()
	on_lose_controller    ?fn ()
	on_controller_changed ?fn (i32)
}

/// ControllerElectorConfig는 컨트롤러 선출을 위한 설정을 담는다
pub struct ControllerElectorConfig {
pub:
	broker_id           i32
	lock_ttl_ms         i64 = controller_lock_ttl_ms
	refresh_interval_ms int = controller_refresh_interval_ms
}

/// new_controller_elector는 새로운 컨트롤러 선출기를 생성한다
pub fn new_controller_elector(config ControllerElectorConfig, metadata_port ?port.ClusterMetadataPort) &ControllerElector {
	return &ControllerElector{
		broker_id:     config.broker_id
		metadata_port: metadata_port
		is_controller: false
		controller_id: -1
		lock_acquired: false
		running:       false
	}
}

// 선출 작업

/// try_become_controller는 컨트롤러가 되기를 시도한다
/// 이 브로커가 컨트롤러가 되면 true를 반환한다
pub fn (mut e ControllerElector) try_become_controller() !bool {
	e.lock.@lock()
	defer { e.lock.unlock() }

	// 이미 컨트롤러라면 true 반환
	if e.is_controller && e.lock_acquired {
		return true
	}

	// 멀티 브로커 선출을 위해 분산 스토리지가 필요함
	if mut mp := e.metadata_port {
		holder_id := 'broker-${e.broker_id}'
		acquired := mp.try_acquire_lock(controller_lock_name, holder_id, controller_lock_ttl_ms)!

		if acquired {
			e.is_controller = true
			e.lock_acquired = true
			e.controller_id = e.broker_id
			e.last_refresh = time.now().unix_milli()

			// 콜백 트리거
			if callback := e.on_become_controller {
				spawn callback()
			}

			println('[Controller] Broker ${e.broker_id} became controller')
			return true
		} else {
			// 다른 브로커가 컨트롤러임 - 누구인지 확인 시도
			e.is_controller = false
			e.lock_acquired = false
			return false
		}
	}

	// 단일 브로커 모드 - 항상 컨트롤러임
	e.is_controller = true
	e.controller_id = e.broker_id
	return true
}

/// refresh_controller_lock은 컨트롤러 락을 보유하고 있다면 갱신한다
pub fn (mut e ControllerElector) refresh_controller_lock() !bool {
	e.lock.@lock()
	defer { e.lock.unlock() }

	if !e.lock_acquired {
		return false
	}

	if mut mp := e.metadata_port {
		holder_id := 'broker-${e.broker_id}'
		refreshed := mp.refresh_lock(controller_lock_name, holder_id, controller_lock_ttl_ms)!

		if refreshed {
			e.last_refresh = time.now().unix_milli()
			return true
		} else {
			// 락을 잃음
			e.lose_controller_internal()
			return false
		}
	}

	// 단일 브로커 모드 - 항상 성공
	return true
}

/// resign_controller는 자발적으로 컨트롤러 역할을 포기한다
pub fn (mut e ControllerElector) resign_controller() ! {
	e.lock.@lock()
	defer { e.lock.unlock() }

	if !e.lock_acquired {
		return
	}

	if mut mp := e.metadata_port {
		holder_id := 'broker-${e.broker_id}'
		mp.release_lock(controller_lock_name, holder_id)!
	}

	e.lose_controller_internal()
	println('[Controller] Broker ${e.broker_id} resigned as controller')
}

/// lose_controller_internal은 컨트롤러 상태 상실을 처리한다 (락을 보유한 상태에서 호출해야 함)
fn (mut e ControllerElector) lose_controller_internal() {
	was_controller := e.is_controller
	e.is_controller = false
	e.lock_acquired = false
	e.controller_id = -1

	if was_controller {
		if callback := e.on_lose_controller {
			spawn callback()
		}
	}
}

// 조회 작업

/// is_controller는 이 브로커가 컨트롤러인지 반환한다
pub fn (e &ControllerElector) is_controller() bool {
	return e.is_controller
}

/// get_controller_id는 현재 컨트롤러 ID를 반환한다 (알 수 없으면 -1)
pub fn (e &ControllerElector) get_controller_id() i32 {
	return e.controller_id
}

/// discover_controller는 현재 컨트롤러를 탐색한다
pub fn (mut e ControllerElector) discover_controller() !i32 {
	e.lock.rlock()
	defer { e.lock.runlock() }

	// 우리가 컨트롤러라면 우리 ID 반환
	if e.is_controller {
		return e.broker_id
	}

	// 클러스터 메타데이터에서 가져오기 시도
	if mut mp := e.metadata_port {
		metadata := mp.get_cluster_metadata()!
		return metadata.controller_id
	}

	// 단일 브로커 모드
	return e.broker_id
}

/// start는 컨트롤러 선출 백그라운드 워커를 시작한다
pub fn (mut e ControllerElector) start() {
	e.running = true
	spawn e.election_loop()
}

/// stop은 컨트롤러 선출 워커를 중지하고 락을 해제한다
pub fn (mut e ControllerElector) stop() {
	e.running = false
	e.resign_controller() or {}
}

fn (mut e ControllerElector) election_loop() {
	// 초기 선출 시도
	e.try_become_controller() or { eprintln('[Controller] Initial election failed: ${err}') }

	interval := time.Duration(controller_refresh_interval_ms * time.millisecond)

	for e.running {
		time.sleep(interval)

		if !e.running {
			break
		}

		if e.lock_acquired {
			// 락 갱신
			e.refresh_controller_lock() or {
				eprintln('[Controller] Lock refresh failed: ${err}')
				// 재획득 시도
				e.try_become_controller() or {}
			}
		} else {
			// 컨트롤러 되기 시도
			e.try_become_controller() or {
				// 다른 브로커가 컨트롤러임, 정상 상황
			}
		}
	}
}

// 콜백

/// set_on_become_controller는 이 브로커가 컨트롤러가 될 때의 콜백을 설정한다
pub fn (mut e ControllerElector) set_on_become_controller(callback fn ()) {
	e.on_become_controller = callback
}

/// set_on_lose_controller는 이 브로커가 컨트롤러 역할을 잃을 때의 콜백을 설정한다
pub fn (mut e ControllerElector) set_on_lose_controller(callback fn ()) {
	e.on_lose_controller = callback
}

/// set_on_controller_changed는 컨트롤러가 변경될 때의 콜백을 설정한다
pub fn (mut e ControllerElector) set_on_controller_changed(callback fn (i32)) {
	e.on_controller_changed = callback
}

// 컨트롤러 태스크 (컨트롤러에서만 실행)

/// ControllerTask는 컨트롤러에서만 실행되어야 하는 태스크를 나타낸다
pub struct ControllerTask {
	name     string
	interval time.Duration
	task     ?fn () !
}

/// ControllerTaskRunner는 이 브로커가 컨트롤러일 때만 태스크를 실행한다
pub struct ControllerTaskRunner {
mut:
	elector &ControllerElector
	tasks   []ControllerTask
	running bool
}

/// new_controller_task_runner는 새로운 태스크 러너를 생성한다
pub fn new_controller_task_runner(elector &ControllerElector) &ControllerTaskRunner {
	return &ControllerTaskRunner{
		elector: elector
		tasks:   []ControllerTask{}
		running: false
	}
}

/// add_task는 컨트롤러일 때 실행할 태스크를 추가한다
pub fn (mut r ControllerTaskRunner) add_task(name string, interval time.Duration, task fn () !) {
	r.tasks << ControllerTask{
		name:     name
		interval: interval
		task:     task
	}
}

/// start는 모든 태스크 러너를 시작한다
pub fn (mut r ControllerTaskRunner) start() {
	r.running = true
	for task in r.tasks {
		spawn r.run_task(task)
	}
}

/// stop은 모든 태스크 러너를 중지한다
pub fn (mut r ControllerTaskRunner) stop() {
	r.running = false
}

fn (mut r ControllerTaskRunner) run_task(task ControllerTask) {
	for r.running {
		if r.elector.is_controller() {
			if task_fn := task.task {
				task_fn() or { eprintln('[ControllerTask] ${task.name} failed: ${err}') }
			}
		}
		time.sleep(task.interval)
	}
}
