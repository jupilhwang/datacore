module performance

import infra.performance.core

// Re-export config types from core for backward compatibility
pub type PerformanceConfig = core.PerformanceConfig
pub type PerformanceStats = core.PerformanceStats

// PerformanceEngine defines the standard interface for all performance optimization plugins
pub interface PerformanceEngine {
	name() string
mut:
	init(config core.PerformanceConfig) !

	// Buffer management
	get_buffer(size int) &core.Buffer
	put_buffer(buf &core.Buffer)

	// Object pooling
	get_record() &core.PooledRecord
	put_record(r &core.PooledRecord)
	get_batch() &core.PooledRecordBatch
	put_batch(b &core.PooledRecordBatch)
	get_request() &core.PooledRequest
	put_request(r &core.PooledRequest)

	// I/O Operations
	read_file_at(path string, offset i64, size int) ![]u8
	write_file_at(path string, offset i64, data []u8) !

	// Stats
	get_stats() core.PerformanceStats
}
