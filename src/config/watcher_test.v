// Configuration Watcher Tests
module config

import os
import time

fn test_config_watcher_creation() {
	// Create a temporary config file
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

	// Create watcher
	mut watcher := new_config_watcher(WatcherConfig{
		file_path:      tmp_path
		check_interval: 100 * time.millisecond
	}) or {
		assert false, 'Failed to create watcher: ${err}'
		return
	}

	// Verify initial config
	cfg := watcher.get_config()
	assert cfg.broker.port == 9092
	assert cfg.broker.broker_id == 1
	assert cfg.broker.max_connections == 100
	assert cfg.observability.logging.level == 'info'
}

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
			port:               9092  // Same - non-reloadable
			broker_id:          1     // Same - non-reloadable
			max_connections:    200   // Changed - reloadable
			request_timeout_ms: 60000 // Changed - reloadable
		}
		observability: ObservabilityConfig{
			logging: LoggingConfig{
				level: 'debug' // Changed - reloadable
			}
		}
	}

	changes := detect_config_changes(old_config, new_config)

	assert changes.has_reloadable == true
	assert changes.has_non_reloadable == false
	assert changes.reloadable_items.len == 3
}

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
			port:      9093 // Changed - non-reloadable
			broker_id: 2    // Changed - non-reloadable
		}
		storage: StorageConfig{
			engine: 's3' // Changed - non-reloadable
		}
	}

	changes := detect_config_changes(old_config, new_config)

	assert changes.has_non_reloadable == true
	assert changes.non_reloadable_items.len == 3
	assert 'broker.port' in changes.non_reloadable_items
	assert 'broker.broker_id' in changes.non_reloadable_items
	assert 'storage.engine' in changes.non_reloadable_items
}

fn test_get_reloadable_settings() {
	settings := get_reloadable_settings()

	assert settings.len > 0
	assert 'broker.max_connections' in settings
	assert 'broker.request_timeout_ms' in settings
	assert 'observability.logging.level' in settings
}

fn test_get_non_reloadable_settings() {
	settings := get_non_reloadable_settings()

	assert settings.len > 0
	assert 'broker.port' in settings
	assert 'broker.broker_id' in settings
	assert 'storage.engine' in settings
}

fn test_watcher_start_stop() {
	// Create a temporary config file
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

	// Start watcher
	watcher.start()

	// Give it time to start
	time.sleep(100 * time.millisecond)

	// Stop watcher
	watcher.stop()

	// Watcher should stop gracefully
	time.sleep(100 * time.millisecond)
}

fn test_callback_registration() {
	// Create a temporary config file
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

	// Register callback
	mut callback_called := false
	watcher.on_reload(fn [mut callback_called] (cfg Config) {
		unsafe {
			callback_called = true
		}
	})

	// Note: Full integration test with file modification would require
	// spawning the watcher and modifying the file
}
