// Manages controller election using distributed locks
// v0.28.0: Leader election implementation for multi-broker clusters
module cluster

import service.port
import sync
import time

// Controller Election Constants

// Lock name for controller election
const controller_lock_name = 'controller-election'

// Controller election lock TTL (milliseconds)
const controller_lock_ttl_ms = i64(30000)

// Controller refresh interval (must be shorter than TTL)
const controller_refresh_interval_ms = 10000

// Controller Elector

/// ControllerElector manages controller election in multi-broker mode
pub struct ControllerElector {
	broker_id i32
mut:
	// Cluster metadata port for distributed locks
	metadata_port ?port.ClusterMetadataPort
	// Current controller state
	is_controller bool
	controller_id i32
	// Lock state
	lock_acquired bool
	last_refresh  i64
	// Thread safety
	lock sync.RwMutex
	// Background worker control
	running bool
	// Logger
	logger port.LoggerPort
	// Callback functions
	on_become_controller  ?fn ()
	on_lose_controller    ?fn ()
	on_controller_changed ?fn (i32)
}

/// ControllerElectorConfig holds configuration for controller election
pub struct ControllerElectorConfig {
pub:
	broker_id           i32
	lock_ttl_ms         i64 = controller_lock_ttl_ms
	refresh_interval_ms int = controller_refresh_interval_ms
}

/// new_controller_elector creates a new controller elector
pub fn new_controller_elector(config ControllerElectorConfig, metadata_port ?port.ClusterMetadataPort) &ControllerElector {
	return &ControllerElector{
		broker_id:     config.broker_id
		metadata_port: metadata_port
		is_controller: false
		controller_id: -1
		lock_acquired: false
		running:       false
		logger:        port.new_noop_logger()
	}
}

// Election Operations

/// try_become_controller attempts to become the controller
/// Returns true if this broker becomes the controller
pub fn (mut e ControllerElector) try_become_controller() !bool {
	e.lock.@lock()
	defer { e.lock.unlock() }

	// Return true if already controller
	if e.is_controller && e.lock_acquired {
		return true
	}

	// Distributed storage required for multi-broker election
	if mut mp := e.metadata_port {
		holder_id := 'broker-${e.broker_id}'
		acquired := mp.try_acquire_lock(controller_lock_name, holder_id, controller_lock_ttl_ms)!

		if acquired {
			e.is_controller = true
			e.lock_acquired = true
			e.controller_id = e.broker_id
			e.last_refresh = time.now().unix_milli()

			// Trigger callback
			if callback := e.on_become_controller {
				spawn callback()
			}

			e.logger.info('ControllerElection: Broker became controller broker_id=${e.broker_id}')
			return true
		} else {
			// Another broker is the controller - try to identify who
			e.is_controller = false
			e.lock_acquired = false
			return false
		}
	}

	// Single-broker mode - always the controller
	e.is_controller = true
	e.controller_id = e.broker_id
	return true
}

/// refresh_controller_lock refreshes the controller lock if held
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
			// Lock lost
			e.lose_controller_internal()
			return false
		}
	}

	// Single-broker mode - always succeeds
	return true
}

/// resign_controller voluntarily gives up the controller role
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
	e.logger.info('ControllerElection: Broker resigned as controller broker_id=${e.broker_id}')
}

/// lose_controller_internal handles loss of controller state (must be called while holding lock)
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

// Query Operations

/// is_controller returns whether this broker is the controller
pub fn (e &ControllerElector) is_controller() bool {
	return e.is_controller
}

/// get_controller_id returns the current controller ID (-1 if unknown)
pub fn (e &ControllerElector) get_controller_id() i32 {
	return e.controller_id
}

/// discover_controller discovers the current controller
pub fn (mut e ControllerElector) discover_controller() !i32 {
	e.lock.rlock()
	defer { e.lock.runlock() }

	// Return our ID if we are the controller
	if e.is_controller {
		return e.broker_id
	}

	// Try to get from cluster metadata
	if mut mp := e.metadata_port {
		metadata := mp.get_cluster_metadata()!
		return metadata.controller_id
	}

	// Single-broker mode
	return e.broker_id
}

/// start starts the controller election background worker
pub fn (mut e ControllerElector) start() {
	e.running = true
	spawn e.election_loop()
}

/// stop stops the controller election worker and releases the lock
pub fn (mut e ControllerElector) stop() {
	e.running = false
	e.resign_controller() or {}
}

fn (mut e ControllerElector) election_loop() {
	// Initial election attempt
	e.try_become_controller() or {
		e.logger.error('ControllerElection: Initial election failed error=${err.str()}')
	}

	interval := time.Duration(controller_refresh_interval_ms * time.millisecond)

	for e.running {
		time.sleep(interval)

		if !e.running {
			break
		}

		if e.lock_acquired {
			// Refresh lock
			e.refresh_controller_lock() or {
				e.logger.error('ControllerElection: Lock refresh failed error=${err.str()}')
				// Attempt to reacquire
				e.try_become_controller() or {}
			}
		} else {
			// Attempt to become controller
			e.try_become_controller() or {
				// Another broker is controller, normal situation
			}
		}
	}
}

// Callbacks

/// set_on_become_controller sets the callback for when this broker becomes controller
pub fn (mut e ControllerElector) set_on_become_controller(callback fn ()) {
	e.on_become_controller = callback
}

/// set_on_lose_controller sets the callback for when this broker loses the controller role
pub fn (mut e ControllerElector) set_on_lose_controller(callback fn ()) {
	e.on_lose_controller = callback
}

/// set_on_controller_changed sets the callback for when the controller changes
pub fn (mut e ControllerElector) set_on_controller_changed(callback fn (i32)) {
	e.on_controller_changed = callback
}

// Controller Tasks (run only on controller)

/// ControllerTask represents a task that should only run on the controller
pub struct ControllerTask {
	name     string
	interval time.Duration
	task     ?fn () !
}

/// ControllerTaskRunner executes tasks only when this broker is the controller
pub struct ControllerTaskRunner {
mut:
	elector &ControllerElector
	tasks   []ControllerTask
	running bool
	logger  port.LoggerPort
}

/// new_controller_task_runner creates a new task runner
pub fn new_controller_task_runner(elector &ControllerElector) &ControllerTaskRunner {
	return &ControllerTaskRunner{
		elector: elector
		tasks:   []ControllerTask{}
		running: false
		logger:  port.new_noop_logger()
	}
}

/// add_task adds a task to run when acting as controller
pub fn (mut r ControllerTaskRunner) add_task(name string, interval time.Duration, task fn () !) {
	r.tasks << ControllerTask{
		name:     name
		interval: interval
		task:     task
	}
}

/// start starts all task runners
pub fn (mut r ControllerTaskRunner) start() {
	r.running = true
	for task in r.tasks {
		spawn r.run_task(task)
	}
}

/// stop stops all task runners
pub fn (mut r ControllerTaskRunner) stop() {
	r.running = false
}

fn (mut r ControllerTaskRunner) run_task(task ControllerTask) {
	for r.running {
		if r.elector.is_controller() {
			if task_fn := task.task {
				task_fn() or {
					r.logger.error('ControllerTask: Task execution failed task_name=${task.name} error=${err.str()}')
				}
			}
		}
		time.sleep(task.interval)
	}
}
