module performance

import infra.performance.core
import infra.performance.engines

// ============================================================================
// Performance Manager - Strategy Facade
// ============================================================================

@[heap]
pub struct PerformanceManager {
pub mut:
	engine  PerformanceEngine
	enabled bool
}

// new_performance_manager creates a performance manager with the best available engine
pub fn new_performance_manager(config PerformanceConfig) &PerformanceManager {
	mut engine := PerformanceEngine(engines.GenericPerformanceEngine{})

	$if linux {
		if config.enable_linux_optimizations {
			// TODO: Initialize LinuxPerformanceEngine when fully implemented
			// For now, use Generic but log as Linux if available
			// mut linux_engine := engines.LinuxPerformanceEngine{}
			// engine = linux_engine
		}
	}

	engine.init(config) or {
		// Fallback to minimal/generic on error
		println('Failed to init optimized engine, falling back to Generic')
	}

	return &PerformanceManager{
		engine:  engine
		enabled: true
	}
}

// ============================================================================
// Convenience Methods Proxying to Engine
// ============================================================================

pub fn (mut m PerformanceManager) get_buffer(size int) &core.Buffer {
	return m.engine.get_buffer(size)
}

pub fn (mut m PerformanceManager) put_buffer(buf &core.Buffer) {
	m.engine.put_buffer(buf)
}

pub fn (mut m PerformanceManager) get_record() &core.PooledRecord {
	return m.engine.get_record()
}

pub fn (mut m PerformanceManager) put_record(r &core.PooledRecord) {
	m.engine.put_record(r)
}

pub fn (mut m PerformanceManager) get_batch() &core.PooledRecordBatch {
	return m.engine.get_batch()
}

pub fn (mut m PerformanceManager) put_batch(b &core.PooledRecordBatch) {
	m.engine.put_batch(b)
}

pub fn (mut m PerformanceManager) get_request() &core.PooledRequest {
	return m.engine.get_request()
}

pub fn (mut m PerformanceManager) put_request(r &core.PooledRequest) {
	m.engine.put_request(r)
}

// ============================================================================
// Helper functions (Moved from manager.v original)
// ============================================================================

pub fn (mut m PerformanceManager) with_buffer(min_size int, f fn (mut core.Buffer)) {
	mut buf := m.get_buffer(min_size)
	defer { m.put_buffer(buf) }
	f(mut buf)
}

// compute_partition computes Kafka partition for a key
pub fn (m &PerformanceManager) compute_partition(key []u8, num_partitions int) int {
	return core.kafka_partition(key, num_partitions)
}

// compute_checksum computes CRC32 checksum
pub fn (m &PerformanceManager) compute_checksum(data []u8) u32 {
	return core.crc32_ieee(data)
}

// ============================================================================
// Global Performance Manager
// Note: Requires -enable-globals flag for tests in infra/performance/
// The main test target in Makefile excludes these tests intentionally.
// ============================================================================

__global (
	g_performance &PerformanceManager
)

// init_global_performance initializes the global performance manager
pub fn init_global_performance(config PerformanceConfig) {
	unsafe {
		g_performance = new_performance_manager(config)
	}
}

// get_global_performance returns the global performance manager
pub fn get_global_performance() &PerformanceManager {
	if unsafe { g_performance == nil } {
		// Safe default if not initialized
		init_global_performance(PerformanceConfig{})
	}
	return unsafe { g_performance }
}

pub fn (mut m PerformanceManager) get_stats() PerformanceStats {
	return m.engine.get_stats()
}
