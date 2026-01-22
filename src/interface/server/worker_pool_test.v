// Interface Layer - Worker Pool Tests
module server

import time

fn test_worker_pool_basic_acquire_release() {
	config := WorkerPoolConfig{
		max_workers:     10
		acquire_timeout: 1000
	}
	mut pool := new_worker_pool(config)

	// Acquire a slot
	assert pool.acquire() == true
	assert pool.active_count() == 1

	// Release
	pool.release()
	assert pool.active_count() == 0

	pool.shutdown()
}

fn test_worker_pool_max_workers() {
	config := WorkerPoolConfig{
		max_workers:     3
		acquire_timeout: 100
	}
	mut pool := new_worker_pool(config)

	// Acquire all slots
	assert pool.acquire() == true
	assert pool.acquire() == true
	assert pool.acquire() == true
	assert pool.active_count() == 3

	// Next acquire should timeout
	assert pool.acquire() == false

	// Release one
	pool.release()
	assert pool.active_count() == 2

	// Now we can acquire again
	assert pool.acquire() == true
	assert pool.active_count() == 3

	pool.shutdown()
}

fn test_worker_pool_try_acquire() {
	config := WorkerPoolConfig{
		max_workers:     2
		acquire_timeout: 1000
	}
	mut pool := new_worker_pool(config)

	// Try acquire (non-blocking)
	assert pool.try_acquire() == true
	assert pool.try_acquire() == true
	assert pool.active_count() == 2

	// Try acquire should fail immediately (no blocking)
	assert pool.try_acquire() == false

	pool.shutdown()
}

fn test_worker_pool_metrics() {
	config := WorkerPoolConfig{
		max_workers:     5
		acquire_timeout: 100
	}
	mut pool := new_worker_pool(config)

	// Initial metrics
	mut metrics := pool.get_metrics()
	assert metrics.active_workers == 0
	assert metrics.peak_workers == 0
	assert metrics.total_acquired == 0

	// Acquire some
	pool.acquire()
	pool.acquire()
	pool.acquire()

	metrics = pool.get_metrics()
	assert metrics.active_workers == 3
	assert metrics.peak_workers == 3
	assert metrics.total_acquired == 3

	// Release some
	pool.release()

	metrics = pool.get_metrics()
	assert metrics.active_workers == 2
	assert metrics.total_released == 1

	pool.shutdown()
}

fn test_worker_pool_shutdown() {
	config := WorkerPoolConfig{
		max_workers:     5
		acquire_timeout: 1000
	}
	mut pool := new_worker_pool(config)

	assert pool.is_running() == true

	pool.acquire()
	pool.acquire()

	pool.shutdown()

	assert pool.is_running() == false
	// Acquire should fail after shutdown
	assert pool.acquire() == false
}

fn test_worker_pool_available_slots() {
	config := WorkerPoolConfig{
		max_workers:     10
		acquire_timeout: 1000
	}
	mut pool := new_worker_pool(config)

	assert pool.available_slots() == 10

	pool.acquire()
	pool.acquire()
	pool.acquire()

	assert pool.available_slots() == 7

	pool.release()

	assert pool.available_slots() == 8

	pool.shutdown()
}

fn test_worker_guard() {
	config := WorkerPoolConfig{
		max_workers:     5
		acquire_timeout: 1000
	}
	mut pool := new_worker_pool(config)

	// Acquire and create guard
	pool.acquire()
	mut guard := new_worker_guard(mut pool)

	assert pool.active_count() == 1

	// Release via guard
	guard.release()
	assert pool.active_count() == 0

	// Double release should be idempotent
	guard.release()
	assert pool.active_count() == 0

	pool.shutdown()
}
