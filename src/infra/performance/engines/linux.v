/// Linux-optimized performance engine
/// Uses io_uring for async I/O when available, with graceful fallback to generic engine
module engines

import sync
import sync.stdatomic
import time
import infra.performance.core
import infra.observability

$if linux {
	#include <fcntl.h>

	fn C.open(path &char, flags int, mode int) int
}

/// FdCacheEntry represents a cached file descriptor with LRU tracking.
struct FdCacheEntry {
	fd        int
	last_used i64 // timestamp in milliseconds
}

/// FdCache provides thread-safe LRU caching of file descriptors
/// to avoid repeated open/close syscalls in io_uring I/O paths.
struct FdCache {
mut:
	entries  map[string]FdCacheEntry // "path:flags" -> fd
	mu       sync.Mutex
	max_size int = 64 // max cached FDs
}

/// LinuxPerformanceEngine provides Linux-specific performance optimizations.
/// Delegates pool management to GenericPerformanceEngine and uses io_uring for file I/O.
pub struct LinuxPerformanceEngine {
mut:
	generic           GenericPerformanceEngine
	io_ring_available bool
	io_ring           IoUring
	ops_count         i64
	fd_cache          FdCache
}

/// name returns the engine name.
pub fn (e LinuxPerformanceEngine) name() string {
	return 'Linux (io_uring)'
}

/// get_or_open returns a cached fd or opens a new one.
/// Uses LRU eviction when the cache reaches max_size.
fn (mut c FdCache) get_or_open(path string, flags int, mode int) !int {
	cache_key := '${path}:${flags}'
	c.mu.lock()
	defer {
		c.mu.unlock()
	}

	if entry := c.entries[cache_key] {
		c.entries[cache_key] = FdCacheEntry{
			fd:        entry.fd
			last_used: time.now().unix_milli()
		}
		return entry.fd
	}

	$if linux {
		fd := C.open(path.str, flags, mode)
		if fd < 0 {
			return error('Failed to open ${path}')
		}

		if c.entries.len >= c.max_size {
			c.evict_oldest()
		}

		c.entries[cache_key] = FdCacheEntry{
			fd:        fd
			last_used: time.now().unix_milli()
		}
		return fd
	}
	return error('FdCache not available on this platform')
}

/// evict_oldest removes the least recently used entry from the cache.
fn (mut c FdCache) evict_oldest() {
	mut oldest_key := ''
	mut oldest_time := i64(9223372036854775807)
	for key, entry in c.entries {
		if entry.last_used < oldest_time {
			oldest_time = entry.last_used
			oldest_key = key
		}
	}
	if oldest_key.len > 0 {
		$if linux {
			if entry := c.entries[oldest_key] {
				C.close(entry.fd)
			}
		}
		c.entries.delete(oldest_key)
	}
}

/// close_all closes all cached file descriptors and clears the cache.
fn (mut c FdCache) close_all() {
	c.mu.lock()
	defer {
		c.mu.unlock()
	}
	$if linux {
		for _, entry in c.entries {
			C.close(entry.fd)
		}
	}
	c.entries = map[string]FdCacheEntry{}
}

/// init initializes the engine with configuration.
/// Initializes generic pools first, then attempts io_uring setup.
/// io_uring failure is non-fatal; the engine falls back to generic I/O.
pub fn (mut e LinuxPerformanceEngine) init(config core.PerformanceConfig) ! {
	e.generic.init(config)!
	e.fd_cache = FdCache{
		max_size: 64
	}

	$if linux {
		e.io_ring = new_io_uring(IoUringConfig{ queue_depth: 64 }) or {
			observability.log_with_context('performance', .warn, 'LinuxPerformanceEngine',
				'io_uring init failed, using generic I/O fallback', {
				'error': err.str()
			})
			e.io_ring_available = false
			return
		}
		e.io_ring_available = true
		observability.log_with_context('performance', .info, 'LinuxPerformanceEngine',
			'io_uring initialized successfully', {
			'queue_depth': '64'
		})
	}
}

/// get_buffer delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) get_buffer(size int) &core.Buffer {
	return e.generic.get_buffer(size)
}

/// put_buffer delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) put_buffer(buf &core.Buffer) {
	e.generic.put_buffer(buf)
}

/// get_record delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) get_record() &core.PooledRecord {
	return e.generic.get_record()
}

/// put_record delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) put_record(r &core.PooledRecord) {
	e.generic.put_record(r)
}

/// get_batch delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) get_batch() &core.PooledRecordBatch {
	return e.generic.get_batch()
}

/// put_batch delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) put_batch(b &core.PooledRecordBatch) {
	e.generic.put_batch(b)
}

/// get_request delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) get_request() &core.PooledRequest {
	return e.generic.get_request()
}

/// put_request delegates to generic engine.
pub fn (mut e LinuxPerformanceEngine) put_request(r &core.PooledRequest) {
	e.generic.put_request(r)
}

/// read_file_at reads data using io_uring when available, falling back to generic I/O.
pub fn (mut e LinuxPerformanceEngine) read_file_at(path string, offset i64, size int) ![]u8 {
	stdatomic.add_i64(&e.ops_count, 1)
	if e.io_ring_available {
		data := e.io_uring_read(path, offset, size) or {
			return e.generic.read_file_at(path, offset, size)
		}
		return data
	}
	return e.generic.read_file_at(path, offset, size)
}

/// write_file_at writes data using io_uring when available, falling back to generic I/O.
pub fn (mut e LinuxPerformanceEngine) write_file_at(path string, offset i64, data []u8) ! {
	stdatomic.add_i64(&e.ops_count, 1)
	if e.io_ring_available {
		e.io_uring_write(path, offset, data) or {
			return e.generic.write_file_at(path, offset, data)
		}
		return
	}
	e.generic.write_file_at(path, offset, data)!
}

/// get_stats returns performance statistics with atomic ops_count tracking.
pub fn (mut e LinuxPerformanceEngine) get_stats() core.PerformanceStats {
	buf_stats := e.generic.buffer_pool.get_stats()
	return core.PerformanceStats{
		engine_name:   e.name()
		buffer_hits:   buf_stats.total_hits()
		buffer_misses: buf_stats.total_misses()
		ops_count:     u64(stdatomic.load_i64(&e.ops_count))
	}
}

// -- io_uring file I/O helpers (Linux only) --

/// io_uring_read performs a file read via io_uring's submission/completion queue.
fn (mut e LinuxPerformanceEngine) io_uring_read(path string, offset i64, size int) ![]u8 {
	$if linux {
		fd := e.fd_cache.get_or_open(path, 0, 0)!

		mut buf := []u8{len: size}
		if !e.io_ring.prep_read(fd, buf, offset, 0) {
			return error('io_uring_read: submission queue full')
		}
		result := e.io_ring.wait_cqe()!
		if !result.success {
			return error('io_uring_read: I/O error code ${result.result}')
		}
		return buf
	}
	return error('io_uring not available on this platform')
}

/// io_uring_write performs a file write via io_uring's submission/completion queue.
fn (mut e LinuxPerformanceEngine) io_uring_write(path string, offset i64, data []u8) ! {
	$if linux {
		fd := e.fd_cache.get_or_open(path, 2, 0o644)!

		if !e.io_ring.prep_write(fd, data, offset, 0) {
			return error('io_uring_write: submission queue full')
		}
		result := e.io_ring.wait_cqe()!
		if !result.success {
			return error('io_uring_write: I/O error code ${result.result}')
		}
	} $else {
		return error('io_uring not available on this platform')
	}
}

/// close releases io_uring resources and cached file descriptors.
pub fn (mut e LinuxPerformanceEngine) close() {
	if e.io_ring_available {
		e.io_ring.close()
		e.io_ring_available = false
	}
	e.fd_cache.close_all()
}
