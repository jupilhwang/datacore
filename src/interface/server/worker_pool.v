/// Interface Layer - Worker Pool
///
/// This module limits the number of concurrent connection handlers
/// to prevent goroutine explosions under high load.
///
/// It manages worker slots using a semaphore pattern,
/// and supports slot acquisition with a timeout.
module server

import sync
import sync.stdatomic
import time
import os
import infra.performance.engines

/// WorkerPoolConfig holds the configuration for a worker pool.
pub struct WorkerPoolConfig {
pub:
	max_workers      int = 1000
	queue_size       int = 5000
	acquire_timeout  int = 5000
	metrics_interval int = 60
	// NUMA configuration (v0.33.0)
	numa_aware        bool
	numa_bind_workers bool = true
}

/// WorkerPoolMetrics tracks statistics for the worker pool.
pub struct WorkerPoolMetrics {
pub mut:
	active_workers     int
	peak_workers       int
	queued_connections int
	total_acquired     u64
	total_released     u64
	total_timeouts     u64
	total_rejected     u64
	// NUMA statistics (v0.33.0)
	numa_bindings      i64
	numa_binding_fails i64
}

/// WorkerPool manages a fixed-size pool of worker slots for connection handling.
/// It uses the semaphore pattern to limit concurrent execution.
pub struct WorkerPool {
mut:
	config       WorkerPoolConfig
	semaphore    chan bool
	metrics      WorkerPoolMetrics
	metrics_lock sync.Mutex
	running      bool
	// NUMA-related fields (v0.33.0)
	numa_node_count int
	next_numa_node  i64
}

/// new_worker_pool creates a new worker pool.
/// It creates a semaphore channel of size max_workers and fills it with tokens.
pub fn new_worker_pool(config WorkerPoolConfig) &WorkerPool {
	// Create semaphore channel with capacity max_workers
	mut sem := chan bool{cap: config.max_workers}
	// Pre-fill the channel with tokens (indicating available slots)
	for _ in 0 .. config.max_workers {
		sem <- true
	}

	// Detect NUMA node count (v0.33.0)
	numa_nodes := get_numa_node_count()

	return &WorkerPool{
		config:          config
		semaphore:       sem
		running:         true
		numa_node_count: numa_nodes
		next_numa_node:  0
	}
}

/// acquire attempts to acquire a worker slot.
/// Returns true on success, false on timeout or if the pool is shutting down.
pub fn (mut wp WorkerPool) acquire() bool {
	if !wp.running {
		return false
	}

	// Attempt acquisition with timeout
	select {
		_ := <-wp.semaphore {
			// Slot acquired successfully
			wp.update_metrics_on_acquire()
			return true
		}
		wp.config.acquire_timeout * time.millisecond {
			// Timeout
			wp.metrics_lock.@lock()
			wp.metrics.total_timeouts += 1
			wp.metrics_lock.unlock()
			return false
		}
	}

	return false
}

/// try_acquire attempts to acquire a worker slot without blocking.
/// Returns true on success, false if no slots are available.
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

/// release returns a worker slot back to the pool.
/// Must be called in a pair with acquire().
pub fn (mut wp WorkerPool) release() {
	// Update metrics (lock-protected)
	wp.metrics_lock.@lock()
	wp.metrics.active_workers -= 1
	wp.metrics.total_released += 1
	wp.metrics_lock.unlock()

	// Return token to semaphore (non-blocking)
	select {
		wp.semaphore <- true {}
		else {
			// This should not occur if acquire/release are called in pairs
			// Happens if the channel is full (abnormal situation)
		}
	}
}

/// update_metrics_on_acquire updates metrics when a slot is acquired.
/// It increments active worker count, total acquired count, and updates peak worker count.
fn (mut wp WorkerPool) update_metrics_on_acquire() {
	wp.metrics_lock.@lock()
	wp.metrics.active_workers += 1
	wp.metrics.total_acquired += 1
	if wp.metrics.active_workers > wp.metrics.peak_workers {
		wp.metrics.peak_workers = wp.metrics.active_workers
	}
	wp.metrics_lock.unlock()
}

/// get_metrics returns the current worker pool metrics.
pub fn (mut wp WorkerPool) get_metrics() WorkerPoolMetrics {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.metrics
}

/// active_count returns the number of active workers.
pub fn (mut wp WorkerPool) active_count() int {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.metrics.active_workers
}

/// available_slots returns the number of available worker slots.
pub fn (mut wp WorkerPool) available_slots() int {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.config.max_workers - wp.metrics.active_workers
}

/// shutdown gracefully shuts down the worker pool.
/// Sets the running flag to false and drains the semaphore.
pub fn (mut wp WorkerPool) shutdown() {
	wp.running = false
	// Drain the semaphore (unblocks any pending acquire calls)
	for {
		select {
			_ := <-wp.semaphore {}
			else {
				break
			}
		}
	}
}

/// is_running checks whether the pool is still running.
pub fn (wp &WorkerPool) is_running() bool {
	return wp.running
}

/// WorkerGuard provides RAII-style worker slot management.
/// Use with defer to ensure the slot is always released.
/// Example: defer { guard.release() }
pub struct WorkerGuard {
mut:
	pool     &WorkerPool
	released bool
}

/// new_worker_guard creates a new worker guard.
/// Create this guard after acquiring a worker slot to ensure automatic release.
pub fn new_worker_guard(mut pool WorkerPool) WorkerGuard {
	return WorkerGuard{
		pool:     pool
		released: false
	}
}

/// release releases the worker slot.
/// Idempotent — safe to call multiple times.
pub fn (mut g WorkerGuard) release() {
	if !g.released {
		g.pool.release()
		g.released = true
	}
}

/// bind_worker_to_numa binds the current worker to a NUMA node.
/// Distributes workers across nodes in a round-robin fashion.
/// Does nothing if NUMA is disabled or unsupported.
pub fn (mut wp WorkerPool) bind_worker_to_numa() {
	if !wp.config.numa_aware || !wp.config.numa_bind_workers {
		return
	}

	if wp.numa_node_count <= 1 {
		return
	}

	// Select next node using atomic fetch-and-add for lock-free round-robin
	raw := stdatomic.add_i64(&wp.next_numa_node, 1) - 1
	node := int(raw % i64(wp.numa_node_count))

	// Attempt to bind to the NUMA node
	if bind_thread_to_numa_node(node) {
		stdatomic.add_i64(&wp.metrics.numa_bindings, 1)
	} else {
		stdatomic.add_i64(&wp.metrics.numa_binding_fails, 1)
	}
}

/// get_numa_node_count returns the number of NUMA nodes in the system.
/// Returns the actual value only on Linux; returns 1 on other platforms.
fn get_numa_node_count() int {
	$if linux {
		// Determine count by the number of /sys/devices/system/node/node* directories
		nodes := os.ls('/sys/devices/system/node') or { return 1 }
		mut count := 0
		for node in nodes {
			if node.starts_with('node') {
				count++
			}
		}
		return if count > 0 { count } else { 1 }
	} $else {
		return 1
	}
}

/// bind_thread_to_numa_node binds the current thread to the specified NUMA node.
/// Only works on Linux; always returns false on other platforms.
fn bind_thread_to_numa_node(node int) bool {
	$if linux {
		// Use libnuma if available, otherwise use sched_setaffinity
		// Here we simply call the function from the numa module
		return engines.bind_to_node(node)
	} $else {
		return false
	}
}
