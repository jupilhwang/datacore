/// 설정 핫 리로드 감시자 모듈
/// 설정 파일의 변경을 모니터링하고 리로드 콜백을 트리거합니다.
module config

import os
import sync
import time

/// ConfigCallback은 리로드 콜백 함수 타입입니다.
pub type ConfigCallback = fn (Config)

/// ConfigWatcher는 설정 파일의 변경을 모니터링합니다.
/// file_path: 감시할 설정 파일 경로
/// check_interval: 파일 변경 확인 간격
/// last_modified: 마지막 수정 시간
/// running: 감시자 실행 상태
/// callbacks: 등록된 콜백 함수 목록
/// lock: 스레드 안전을 위한 뮤텍스
/// current_config: 현재 로드된 설정
pub struct ConfigWatcher {
	file_path      string
	check_interval time.Duration
mut:
	last_modified  i64
	running        bool
	callbacks      []ConfigCallback
	lock           sync.Mutex
	current_config Config
}

/// WatcherConfig는 설정 감시자의 설정을 담습니다.
/// file_path: 감시할 설정 파일 경로
/// check_interval: 파일 변경 확인 간격 (기본값: 2초)
pub struct WatcherConfig {
pub:
	file_path      string        = 'config.toml'
	check_interval time.Duration = 2 * time.second
}

/// new_config_watcher는 새로운 설정 감시자를 생성합니다.
/// watcher_config: 감시자 설정
/// 반환값: ConfigWatcher 포인터 또는 에러
pub fn new_config_watcher(watcher_config WatcherConfig) !&ConfigWatcher {
	// 초기 설정 로드
	initial_config := load_config(watcher_config.file_path)!

	// 초기 파일 수정 시간 가져오기
	file_info := os.stat(watcher_config.file_path) or {
		return error('Failed to stat config file: ${err}')
	}

	return &ConfigWatcher{
		file_path:      watcher_config.file_path
		check_interval: watcher_config.check_interval
		last_modified:  file_info.mtime
		running:        false
		callbacks:      []ConfigCallback{}
		current_config: initial_config
	}
}

/// on_reload는 설정이 리로드될 때 호출될 콜백을 등록합니다.
/// callback: 리로드 시 호출될 함수
pub fn (mut w ConfigWatcher) on_reload(callback ConfigCallback) {
	w.lock.@lock()
	defer { w.lock.unlock() }
	w.callbacks << callback
}

/// get_config는 현재 설정을 반환합니다. (스레드 안전)
/// 반환값: 현재 Config
pub fn (mut w ConfigWatcher) get_config() Config {
	w.lock.@lock()
	defer { w.lock.unlock() }
	return w.current_config
}

/// start는 백그라운드에서 설정 감시자를 시작합니다. (스레드 안전)
pub fn (mut w ConfigWatcher) start() {
	w.lock.@lock()
	if w.running {
		w.lock.unlock()
		return
	}
	w.running = true
	w.lock.unlock()

	spawn w.watch_loop()
	println('[ConfigWatcher] Started watching ${w.file_path} (interval: ${w.check_interval})')
}

/// stop은 설정 감시자를 중지합니다. (스레드 안전)
pub fn (mut w ConfigWatcher) stop() {
	w.lock.@lock()
	w.running = false
	w.lock.unlock()
	println('[ConfigWatcher] Stopped')
}

/// is_running은 감시자가 실행 중인지 반환합니다. (스레드 안전)
fn (mut w ConfigWatcher) is_running() bool {
	w.lock.@lock()
	defer { w.lock.unlock() }
	return w.running
}

/// watch_loop는 파일 변경을 확인하는 메인 루프입니다.
fn (mut w ConfigWatcher) watch_loop() {
	for w.is_running() {
		time.sleep(w.check_interval)

		if !w.is_running() {
			break
		}

		// 파일 수정 여부 확인
		file_info := os.stat(w.file_path) or {
			// 쓰기 중에 파일이 일시적으로 사용 불가능할 수 있음
			continue
		}

		w.lock.@lock()
		last_mod := w.last_modified
		w.lock.unlock()

		if file_info.mtime != last_mod {
			// 파일이 수정됨
			w.lock.@lock()
			w.last_modified = file_info.mtime
			w.lock.unlock()

			// 설정 리로드 시도
			w.reload_config()
		}
	}
}

/// reload_config는 설정 파일을 리로드하고 콜백들에게 알립니다.
fn (mut w ConfigWatcher) reload_config() {
	// 파일 쓰기가 완료되도록 약간의 지연
	time.sleep(100 * time.millisecond)

	new_config := load_config(w.file_path) or {
		println('[ConfigWatcher] Failed to reload config: ${err}')
		return
	}

	// 변경 사항 감지 및 리로드 가능 여부 확인
	changes := detect_config_changes(w.current_config, new_config)

	if changes.has_non_reloadable {
		println('[ConfigWatcher] Warning: Non-reloadable settings changed. Server restart required for:')
		for item in changes.non_reloadable_items {
			println('[ConfigWatcher]   - ${item}')
		}
	}

	if changes.has_reloadable {
		// 현재 설정 업데이트
		w.lock.@lock()
		w.current_config = new_config
		callbacks := w.callbacks.clone()
		w.lock.unlock()

		println('[ConfigWatcher] Configuration reloaded successfully')
		for item in changes.reloadable_items {
			println('[ConfigWatcher]   - ${item}')
		}

		// 모든 콜백에 알림
		for callback in callbacks {
			callback(new_config)
		}
	}
}

/// ConfigChanges는 감지된 설정 변경 사항을 나타냅니다.
/// has_reloadable: 리로드 가능한 변경 사항 존재 여부
/// has_non_reloadable: 리로드 불가능한 변경 사항 존재 여부
/// reloadable_items: 리로드 가능한 변경 항목 목록
/// non_reloadable_items: 리로드 불가능한 변경 항목 목록
struct ConfigChanges {
mut:
	has_reloadable       bool
	has_non_reloadable   bool
	reloadable_items     []string
	non_reloadable_items []string
}

/// detect_config_changes는 두 설정을 비교하여 변경 사항을 감지합니다.
/// old_config: 이전 설정
/// new_config: 새 설정
/// 반환값: 감지된 변경 사항
fn detect_config_changes(old_config Config, new_config Config) ConfigChanges {
	mut changes := ConfigChanges{}

	// 리로드 불가능한 설정 (재시작 필요)
	if old_config.broker.port != new_config.broker.port {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'broker.port'
	}
	if old_config.broker.broker_id != new_config.broker.broker_id {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'broker.broker_id'
	}
	if old_config.broker.host != new_config.broker.host {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'broker.host'
	}
	if old_config.broker.cluster_id != new_config.broker.cluster_id {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'broker.cluster_id'
	}
	if old_config.storage.engine != new_config.storage.engine {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'storage.engine'
	}
	if old_config.rest.host != new_config.rest.host {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'rest.host'
	}
	if old_config.rest.port != new_config.rest.port {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'rest.port'
	}
	if old_config.observability.metrics.prometheus_port != new_config.observability.metrics.prometheus_port {
		changes.has_non_reloadable = true
		changes.non_reloadable_items << 'observability.metrics.prometheus_port'
	}

	// 리로드 가능한 설정 (런타임에 변경 가능)
	if old_config.broker.max_connections != new_config.broker.max_connections {
		changes.has_reloadable = true
		changes.reloadable_items << 'broker.max_connections: ${old_config.broker.max_connections} -> ${new_config.broker.max_connections}'
	}
	if old_config.broker.request_timeout_ms != new_config.broker.request_timeout_ms {
		changes.has_reloadable = true
		changes.reloadable_items << 'broker.request_timeout_ms: ${old_config.broker.request_timeout_ms} -> ${new_config.broker.request_timeout_ms}'
	}
	if old_config.broker.idle_timeout_ms != new_config.broker.idle_timeout_ms {
		changes.has_reloadable = true
		changes.reloadable_items << 'broker.idle_timeout_ms: ${old_config.broker.idle_timeout_ms} -> ${new_config.broker.idle_timeout_ms}'
	}
	if old_config.broker.max_request_size != new_config.broker.max_request_size {
		changes.has_reloadable = true
		changes.reloadable_items << 'broker.max_request_size: ${old_config.broker.max_request_size} -> ${new_config.broker.max_request_size}'
	}
	if old_config.observability.logging.level != new_config.observability.logging.level {
		changes.has_reloadable = true
		changes.reloadable_items << 'observability.logging.level: ${old_config.observability.logging.level} -> ${new_config.observability.logging.level}'
	}
	if old_config.observability.metrics.enabled != new_config.observability.metrics.enabled {
		changes.has_reloadable = true
		changes.reloadable_items << 'observability.metrics.enabled: ${old_config.observability.metrics.enabled} -> ${new_config.observability.metrics.enabled}'
	}
	if old_config.observability.metrics.collection_interval != new_config.observability.metrics.collection_interval {
		changes.has_reloadable = true
		changes.reloadable_items << 'observability.metrics.collection_interval: ${old_config.observability.metrics.collection_interval} -> ${new_config.observability.metrics.collection_interval}'
	}
	if old_config.observability.tracing.enabled != new_config.observability.tracing.enabled {
		changes.has_reloadable = true
		changes.reloadable_items << 'observability.tracing.enabled: ${old_config.observability.tracing.enabled} -> ${new_config.observability.tracing.enabled}'
	}
	if old_config.observability.tracing.sample_rate != new_config.observability.tracing.sample_rate {
		changes.has_reloadable = true
		changes.reloadable_items << 'observability.tracing.sample_rate: ${old_config.observability.tracing.sample_rate} -> ${new_config.observability.tracing.sample_rate}'
	}
	if old_config.rest.max_connections != new_config.rest.max_connections {
		changes.has_reloadable = true
		changes.reloadable_items << 'rest.max_connections: ${old_config.rest.max_connections} -> ${new_config.rest.max_connections}'
	}
	if old_config.rest.sse_heartbeat_interval_ms != new_config.rest.sse_heartbeat_interval_ms {
		changes.has_reloadable = true
		changes.reloadable_items << 'rest.sse_heartbeat_interval_ms: ${old_config.rest.sse_heartbeat_interval_ms} -> ${new_config.rest.sse_heartbeat_interval_ms}'
	}
	if old_config.rest.sse_connection_timeout_ms != new_config.rest.sse_connection_timeout_ms {
		changes.has_reloadable = true
		changes.reloadable_items << 'rest.sse_connection_timeout_ms: ${old_config.rest.sse_connection_timeout_ms} -> ${new_config.rest.sse_connection_timeout_ms}'
	}
	if old_config.rest.ws_max_message_size != new_config.rest.ws_max_message_size {
		changes.has_reloadable = true
		changes.reloadable_items << 'rest.ws_max_message_size: ${old_config.rest.ws_max_message_size} -> ${new_config.rest.ws_max_message_size}'
	}
	if old_config.rest.ws_ping_interval_ms != new_config.rest.ws_ping_interval_ms {
		changes.has_reloadable = true
		changes.reloadable_items << 'rest.ws_ping_interval_ms: ${old_config.rest.ws_ping_interval_ms} -> ${new_config.rest.ws_ping_interval_ms}'
	}

	return changes
}

/// ReloadableConfig는 핫 리로드를 지원하는 컴포넌트를 위한 인터페이스입니다.
pub interface ReloadableConfig {
mut:
	/// on_config_reload는 설정이 리로드될 때 호출됩니다.
	on_config_reload(new_config Config)
}

/// get_reloadable_settings는 런타임에 리로드 가능한 설정 목록을 반환합니다.
/// 반환값: 리로드 가능한 설정 키 목록
pub fn get_reloadable_settings() []string {
	return [
		'broker.max_connections',
		'broker.request_timeout_ms',
		'broker.idle_timeout_ms',
		'broker.max_request_size',
		'rest.max_connections',
		'rest.sse_heartbeat_interval_ms',
		'rest.sse_connection_timeout_ms',
		'rest.ws_max_message_size',
		'rest.ws_ping_interval_ms',
		'observability.logging.level',
		'observability.metrics.enabled',
		'observability.metrics.collection_interval',
		'observability.tracing.enabled',
		'observability.tracing.sample_rate',
	]
}

/// get_non_reloadable_settings는 재시작이 필요한 설정 목록을 반환합니다.
/// 반환값: 리로드 불가능한 설정 키 목록
pub fn get_non_reloadable_settings() []string {
	return [
		'broker.host',
		'broker.port',
		'broker.broker_id',
		'broker.cluster_id',
		'rest.host',
		'rest.port',
		'storage.engine',
		'storage.s3.*',
		'storage.postgres.*',
		'observability.metrics.prometheus_port',
	]
}
