/// Linux-optimized performance engine
/// Uses io_uring for async I/O when available, with graceful fallback to generic engine
module engines

import sync.stdatomic
import infra.performance.core
import infra.observability

$if linux {
	#include <fcntl.h>

	fn C.open(path &char, flags int, mode int) int
}

/// LinuxPerformanceEngine provides Linux-specific performance optimizations.
/// Delegates pool management to GenericPerformanceEngine and uses io_uring for file I/O.
pub struct LinuxPerformanceEngine {
mut:
	generic           GenericPerformanceEngine
	io_ring_available bool
	io_ring           IoUring
	ops_count         i64
}

/// name returns the engine name.
pub fn (e LinuxPerformanceEngine) name() string {
	return 'Linux (io_uring)'
}

/// init initializes the engine with configuration.
/// Initializes generic pools first, then attempts io_uring setup.
/// io_uring failure is non-fatal; the engine falls back to generic I/O.
pub fn (mut e LinuxPerformanceEngine) init(config core.PerformanceConfig) ! {
	e.generic.init(config)!

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
		fd := C.open(path.str, 0, 0)
		if fd < 0 {
			return error('io_uring_read: failed to open file: ${path}')
		}
		defer {
			C.close(fd)
		}

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
		fd := C.open(path.str, 2, 0o644)
		if fd < 0 {
			return error('io_uring_write: failed to open file: ${path}')
		}
		defer {
			C.close(fd)
		}

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

// TODO: IoUring has a close() method for resource cleanup.
// PerformanceEngine interface currently lacks a shutdown/cleanup method.
// Add cleanup support when the interface is extended.
