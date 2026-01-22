// Interface Layer - Worker Pool
// Limits concurrent connection handlers to prevent goroutine explosion under high load
module server

import sync
import time

// WorkerPoolConfig configures the worker pool
pub struct WorkerPoolConfig {
pub:
	max_workers      int = 1000  // Maximum concurrent connection handlers
	queue_size       int = 5000  // Maximum pending connections in queue
	acquire_timeout  int = 5000  // Timeout (ms) to acquire a worker slot
	metrics_interval int = 60    // Metrics logging interval (seconds)
}

// WorkerPoolMetrics tracks worker pool statistics
pub struct WorkerPoolMetrics {
pub mut:
	active_workers     int // Current active workers
	peak_workers       int // Peak concurrent workers
	queued_connections int // Current queued connections
	total_acquired     u64 // Total successful slot acquisitions
	total_released     u64 // Total slot releases
	total_timeouts     u64 // Total acquisition timeouts
	total_rejected     u64 // Total rejected (queue full)
}

// WorkerPool manages a fixed pool of worker slots for connection handling
pub struct WorkerPool {
mut:
	config       WorkerPoolConfig
	semaphore    chan bool          // Counting semaphore for worker slots
	metrics      WorkerPoolMetrics
	metrics_lock sync.Mutex
	running      bool
}

// new_worker_pool creates a new worker pool
pub fn new_worker_pool(config WorkerPoolConfig) &WorkerPool {
	// Create semaphore channel with capacity = max_workers
	// Pre-fill with tokens
	mut sem := chan bool{cap: config.max_workers}
	for _ in 0 .. config.max_workers {
		sem <- true
	}

	return &WorkerPool{
		config:    config
		semaphore: sem
		running:   true
	}
}

// acquire attempts to acquire a worker slot
// Returns true if successful, false if timeout or pool is shutting down
pub fn (mut wp WorkerPool) acquire() bool {
	if !wp.running {
		return false
	}

	// Try to acquire with timeout
	select {
		_ := <-wp.semaphore {
			// Successfully acquired slot
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

// try_acquire attempts to acquire a worker slot without blocking
// Returns true if successful, false if no slots available
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

// release releases a worker slot back to the pool
pub fn (mut wp WorkerPool) release() {
	wp.metrics_lock.@lock()
	wp.metrics.active_workers -= 1
	wp.metrics.total_released += 1
	wp.metrics_lock.unlock()

	// Return token to semaphore (non-blocking)
	select {
		wp.semaphore <- true {}
		else {
			// Should never happen if acquire/release are paired
		}
	}
}

// update_metrics_on_acquire updates metrics when a slot is acquired
fn (mut wp WorkerPool) update_metrics_on_acquire() {
	wp.metrics_lock.@lock()
	wp.metrics.active_workers += 1
	wp.metrics.total_acquired += 1
	if wp.metrics.active_workers > wp.metrics.peak_workers {
		wp.metrics.peak_workers = wp.metrics.active_workers
	}
	wp.metrics_lock.unlock()
}

// get_metrics returns current worker pool metrics
pub fn (mut wp WorkerPool) get_metrics() WorkerPoolMetrics {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.metrics
}

// active_count returns the number of active workers
pub fn (mut wp WorkerPool) active_count() int {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.metrics.active_workers
}

// available_slots returns the number of available worker slots
pub fn (mut wp WorkerPool) available_slots() int {
	wp.metrics_lock.@lock()
	defer { wp.metrics_lock.unlock() }
	return wp.config.max_workers - wp.metrics.active_workers
}

// shutdown gracefully shuts down the worker pool
pub fn (mut wp WorkerPool) shutdown() {
	wp.running = false
	// Drain semaphore (unblock any waiting acquires)
	for {
		select {
			_ := <-wp.semaphore {}
			else { break }
		}
	}
}

// is_running checks if the pool is still running
pub fn (wp &WorkerPool) is_running() bool {
	return wp.running
}

// WorkerGuard provides RAII-style worker slot management
// Use with defer to ensure slot is always released
pub struct WorkerGuard {
mut:
	pool     &WorkerPool
	released bool
}

// new_worker_guard creates a new worker guard
pub fn new_worker_guard(mut pool WorkerPool) WorkerGuard {
	return WorkerGuard{
		pool:     pool
		released: false
	}
}

// release releases the worker slot (idempotent)
pub fn (mut g WorkerGuard) release() {
	if !g.released {
		g.pool.release()
		g.released = true
	}
}
