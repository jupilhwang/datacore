module performance

import infra.observability
import infra.performance.core
import infra.performance.engines

// Performance manager - strategy facade

/// PerformanceManager is the central manager for performance optimization.
@[heap]
pub struct PerformanceManager {
pub mut:
	engine  PerformanceEngine
	enabled bool
}

/// new_performance_manager creates a performance manager with the best available engine.
pub fn new_performance_manager(config core.PerformanceConfig) &PerformanceManager {
	mut engine := PerformanceEngine(engines.GenericPerformanceEngine{})

	$if linux {
		if config.enable_linux_optimizations {
			// TODO(jira#XXX): initialize when LinuxPerformanceEngine is fully implemented
			// For now, use Generic but log that Linux is available
			// mut linux_engine := engines.LinuxPerformanceEngine{}
			// engine = linux_engine
		}
	}

	engine.init(config) or {
		// On error, fall back to minimal/generic engine
		observability.log_with_context('performance', .warn, 'PerformanceManager', 'Failed to init optimized engine, falling back to Generic',
			{
			'error': err.str()
		})
	}

	return &PerformanceManager{
		engine:  engine
		enabled: true
	}
}

// Convenience methods that proxy to the engine

/// get_buffer acquires a buffer of the specified size.
pub fn (mut m PerformanceManager) get_buffer(size int) &core.Buffer {
	return m.engine.get_buffer(size)
}

/// put_buffer returns a buffer to the pool.
pub fn (mut m PerformanceManager) put_buffer(buf &core.Buffer) {
	m.engine.put_buffer(buf)
}

/// get_record acquires a pooled record.
pub fn (mut m PerformanceManager) get_record() &core.PooledRecord {
	return m.engine.get_record()
}

/// put_record returns a record to the pool.
pub fn (mut m PerformanceManager) put_record(r &core.PooledRecord) {
	m.engine.put_record(r)
}

/// get_batch acquires a pooled batch.
pub fn (mut m PerformanceManager) get_batch() &core.PooledRecordBatch {
	return m.engine.get_batch()
}

/// put_batch returns a batch to the pool.
pub fn (mut m PerformanceManager) put_batch(b &core.PooledRecordBatch) {
	m.engine.put_batch(b)
}

/// get_request acquires a pooled request.
pub fn (mut m PerformanceManager) get_request() &core.PooledRequest {
	return m.engine.get_request()
}

/// put_request returns a request to the pool.
pub fn (mut m PerformanceManager) put_request(r &core.PooledRequest) {
	m.engine.put_request(r)
}

// Helper functions (originally from manager.v)

/// with_buffer executes a function with a pooled buffer.
pub fn (mut m PerformanceManager) with_buffer(min_size int, f fn (mut core.Buffer)) {
	mut buf := m.get_buffer(min_size)
	defer { m.put_buffer(buf) }
	f(mut buf)
}

/// compute_partition computes the Kafka partition for a key.
pub fn (m &PerformanceManager) compute_partition(key []u8, num_partitions int) int {
	return core.kafka_partition(key, num_partitions)
}

/// compute_checksum computes a CRC32 checksum.
pub fn (m &PerformanceManager) compute_checksum(data []u8) u32 {
	return core.crc32_ieee(data)
}

// Global performance manager (singleton pattern)

// Struct holding the singleton instance
struct GlobalPerformanceHolder {
mut:
	instance &PerformanceManager = unsafe { nil }
	is_init  bool
}

// Module-level singleton holder
const global_holder = &GlobalPerformanceHolder{}

/// init_global_performance initializes the global performance manager.
pub fn init_global_performance(config core.PerformanceConfig) {
	mut holder := unsafe { global_holder }
	if !holder.is_init {
		unsafe {
			holder.instance = new_performance_manager(config)
			holder.is_init = true
		}
	}
}

/// get_global_performance returns the global performance manager.
pub fn get_global_performance() &PerformanceManager {
	holder := unsafe { global_holder }
	if !holder.is_init {
		// Not initialized — use safe defaults
		init_global_performance(core.PerformanceConfig{})
	}
	return unsafe { holder.instance }
}

/// get_stats returns performance statistics.
pub fn (mut m PerformanceManager) get_stats() core.PerformanceStats {
	return m.engine.get_stats()
}
