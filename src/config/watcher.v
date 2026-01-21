// Configuration Hot-Reload Watcher
// Monitors config file for changes and triggers reload callbacks
module config

import os
import sync
import time

// ConfigCallback is a function type for reload callbacks
pub type ConfigCallback = fn (Config)

// ConfigWatcher monitors a configuration file for changes
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

// WatcherConfig holds configuration watcher settings
pub struct WatcherConfig {
pub:
	file_path      string        = 'config.toml'
	check_interval time.Duration = 2 * time.second // Check every 2 seconds
}

// new_config_watcher creates a new configuration watcher
pub fn new_config_watcher(watcher_config WatcherConfig) !&ConfigWatcher {
	// Load initial config
	initial_config := load_config(watcher_config.file_path)!

	// Get initial file modification time
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

// on_reload registers a callback to be called when config is reloaded
pub fn (mut w ConfigWatcher) on_reload(callback ConfigCallback) {
	w.lock.@lock()
	defer { w.lock.unlock() }
	w.callbacks << callback
}

// get_config returns the current configuration (thread-safe)
pub fn (mut w ConfigWatcher) get_config() Config {
	w.lock.@lock()
	defer { w.lock.unlock() }
	return w.current_config
}

// start starts the configuration watcher in background (thread-safe)
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

// stop stops the configuration watcher (thread-safe)
pub fn (mut w ConfigWatcher) stop() {
	w.lock.@lock()
	w.running = false
	w.lock.unlock()
	println('[ConfigWatcher] Stopped')
}

// is_running returns whether the watcher is running (thread-safe)
fn (mut w ConfigWatcher) is_running() bool {
	w.lock.@lock()
	defer { w.lock.unlock() }
	return w.running
}

// watch_loop is the main loop that checks for file changes
fn (mut w ConfigWatcher) watch_loop() {
	for w.is_running() {
		time.sleep(w.check_interval)

		if !w.is_running() {
			break
		}

		// Check if file has been modified
		file_info := os.stat(w.file_path) or {
			// File might be temporarily unavailable during write
			continue
		}

		w.lock.@lock()
		last_mod := w.last_modified
		w.lock.unlock()

		if file_info.mtime != last_mod {
			// File has been modified
			w.lock.@lock()
			w.last_modified = file_info.mtime
			w.lock.unlock()

			// Try to reload config
			w.reload_config()
		}
	}
}

// reload_config reloads the configuration file and notifies callbacks
fn (mut w ConfigWatcher) reload_config() {
	// Small delay to ensure file write is complete
	time.sleep(100 * time.millisecond)

	new_config := load_config(w.file_path) or {
		println('[ConfigWatcher] Failed to reload config: ${err}')
		return
	}

	// Check what changed and if it's reloadable
	changes := detect_config_changes(w.current_config, new_config)

	if changes.has_non_reloadable {
		println('[ConfigWatcher] Warning: Non-reloadable settings changed. Server restart required for:')
		for item in changes.non_reloadable_items {
			println('[ConfigWatcher]   - ${item}')
		}
	}

	if changes.has_reloadable {
		// Update current config
		w.lock.@lock()
		w.current_config = new_config
		callbacks := w.callbacks.clone()
		w.lock.unlock()

		println('[ConfigWatcher] Configuration reloaded successfully')
		for item in changes.reloadable_items {
			println('[ConfigWatcher]   - ${item}')
		}

		// Notify all callbacks
		for callback in callbacks {
			callback(new_config)
		}
	}
}

// ConfigChanges represents detected configuration changes
struct ConfigChanges {
mut:
	has_reloadable       bool
	has_non_reloadable   bool
	reloadable_items     []string
	non_reloadable_items []string
}

// detect_config_changes compares two configurations and detects changes
fn detect_config_changes(old_config Config, new_config Config) ConfigChanges {
	mut changes := ConfigChanges{}

	// Non-reloadable settings (require restart)
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

	// Reloadable settings (can be changed at runtime)
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

// ReloadableConfig is an interface for components that support hot-reload
pub interface ReloadableConfig {
mut:
	// on_config_reload is called when configuration is reloaded
	on_config_reload(new_config Config)
}

// get_reloadable_settings returns a list of settings that can be reloaded at runtime
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

// get_non_reloadable_settings returns a list of settings that require restart
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
