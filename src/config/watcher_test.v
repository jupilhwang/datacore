/// 설정 감시자 테스트 모듈
module config

import os
import time

/// test_config_watcher_creation은 설정 감시자 생성을 테스트합니다.
fn test_config_watcher_creation() {
	// 임시 설정 파일 생성
	tmp_path := '/tmp/test_config_watcher.toml'
	os.write_file(tmp_path, '
[broker]
port = 9092
broker_id = 1
max_connections = 100

[storage]
engine = "memory"

[observability.logging]
level = "info"
') or {
		assert false, 'Failed to create test config file: ${err}'
		return
	}
	defer {
		os.rm(tmp_path) or {}
	}

	// 감시자 생성
	mut watcher := new_config_watcher(WatcherConfig{
		file_path:      tmp_path
		check_interval: 100 * time.millisecond
	}) or {
		assert false, 'Failed to create watcher: ${err}'
		return
	}

	// 초기 설정 검증
	cfg := watcher.get_config()
	assert cfg.broker.port == 9092
	assert cfg.broker.broker_id == 1
	assert cfg.broker.max_connections == 100
	assert cfg.observability.logging.level == 'info'
}

/// test_detect_reloadable_changes는 리로드 가능한 변경 사항 감지를 테스트합니다.
fn test_detect_reloadable_changes() {
	old_config := Config{
		broker:        BrokerConfig{
			port:               9092
			broker_id:          1
			max_connections:    100
			request_timeout_ms: 30000
		}
		observability: ObservabilityConfig{
			logging: LoggingConfig{
				level: 'info'
			}
		}
	}

	new_config := Config{
		broker:        BrokerConfig{
			port:               9092  // 동일 - 리로드 불가
			broker_id:          1     // 동일 - 리로드 불가
			max_connections:    200   // 변경됨 - 리로드 가능
			request_timeout_ms: 60000 // 변경됨 - 리로드 가능
		}
		observability: ObservabilityConfig{
			logging: LoggingConfig{
				level: 'debug' // 변경됨 - 리로드 가능
			}
		}
	}

	changes := detect_config_changes(old_config, new_config)

	assert changes.has_reloadable == true
	assert changes.has_non_reloadable == false
	assert changes.reloadable_items.len == 3
}

/// test_detect_non_reloadable_changes는 리로드 불가능한 변경 사항 감지를 테스트합니다.
fn test_detect_non_reloadable_changes() {
	old_config := Config{
		broker:  BrokerConfig{
			port:      9092
			broker_id: 1
		}
		storage: StorageConfig{
			engine: 'memory'
		}
	}

	new_config := Config{
		broker:  BrokerConfig{
			port:      9093 // 변경됨 - 리로드 불가
			broker_id: 2    // 변경됨 - 리로드 불가
		}
		storage: StorageConfig{
			engine: 's3' // 변경됨 - 리로드 불가
		}
	}

	changes := detect_config_changes(old_config, new_config)

	assert changes.has_non_reloadable == true
	assert changes.non_reloadable_items.len == 3
	assert 'broker.port' in changes.non_reloadable_items
	assert 'broker.broker_id' in changes.non_reloadable_items
	assert 'storage.engine' in changes.non_reloadable_items
}

/// test_get_reloadable_settings는 리로드 가능한 설정 목록 조회를 테스트합니다.
fn test_get_reloadable_settings() {
	settings := get_reloadable_settings()

	assert settings.len > 0
	assert 'broker.max_connections' in settings
	assert 'broker.request_timeout_ms' in settings
	assert 'observability.logging.level' in settings
}

/// test_get_non_reloadable_settings는 리로드 불가능한 설정 목록 조회를 테스트합니다.
fn test_get_non_reloadable_settings() {
	settings := get_non_reloadable_settings()

	assert settings.len > 0
	assert 'broker.port' in settings
	assert 'broker.broker_id' in settings
	assert 'storage.engine' in settings
}

/// test_watcher_start_stop은 감시자 시작/중지를 테스트합니다.
fn test_watcher_start_stop() {
	// 임시 설정 파일 생성
	tmp_path := '/tmp/test_watcher_start_stop.toml'
	os.write_file(tmp_path, '
[broker]
port = 9092

[storage]
engine = "memory"
') or {
		assert false, 'Failed to create test config file'
		return
	}
	defer {
		os.rm(tmp_path) or {}
	}

	mut watcher := new_config_watcher(WatcherConfig{
		file_path:      tmp_path
		check_interval: 50 * time.millisecond
	}) or {
		assert false, 'Failed to create watcher: ${err}'
		return
	}

	// 감시자 시작
	watcher.start()

	// 시작 대기
	time.sleep(100 * time.millisecond)

	// 감시자 중지
	watcher.stop()

	// 감시자가 정상적으로 중지되어야 함
	time.sleep(100 * time.millisecond)
}

/// test_callback_registration은 콜백 등록을 테스트합니다.
fn test_callback_registration() {
	// 임시 설정 파일 생성
	tmp_path := '/tmp/test_callback.toml'
	os.write_file(tmp_path, '
[broker]
port = 9092

[storage]
engine = "memory"
') or {
		assert false, 'Failed to create test config file'
		return
	}
	defer {
		os.rm(tmp_path) or {}
	}

	mut watcher := new_config_watcher(WatcherConfig{
		file_path:      tmp_path
		check_interval: 50 * time.millisecond
	}) or {
		assert false, 'Failed to create watcher: ${err}'
		return
	}

	// 콜백 등록
	mut callback_called := false
	watcher.on_reload(fn [mut callback_called] (cfg Config) {
		unsafe {
			callback_called = true
		}
	})

	// 참고: 파일 수정을 통한 전체 통합 테스트는
	// 감시자를 스폰하고 파일을 수정해야 함
}
