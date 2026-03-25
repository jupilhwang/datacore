module performance

import infra.performance.core

/// Re-exports configuration types from core for backward compatibility.
pub type PerformanceConfig = core.PerformanceConfig

/// PerformanceStats type.
pub type PerformanceStats = core.PerformanceStats

/// PerformanceEngine defines the standard interface for all performance optimization plugins.
pub interface PerformanceEngine {
	/// name returns the engine name.
	name() string
mut:
	/// init initializes the engine with configuration.
	init(config core.PerformanceConfig) !

	// Buffer management
	/// get_buffer acquires a buffer of the specified size.
	get_buffer(size int) &core.Buffer
	/// put_buffer returns a buffer to the pool.
	put_buffer(buf &core.Buffer)

	// Object pooling
	/// get_record acquires a pooled record.
	get_record() &core.PooledRecord
	/// put_record returns a record to the pool.
	put_record(r &core.PooledRecord)
	/// get_batch acquires a pooled batch.
	get_batch() &core.PooledRecordBatch
	/// put_batch returns a batch to the pool.
	put_batch(b &core.PooledRecordBatch)
	/// get_request acquires a pooled request.
	get_request() &core.PooledRequest
	/// put_request returns a request to the pool.
	put_request(r &core.PooledRequest)

	// I/O operations
	/// read_file_at reads data from the specified offset in a file.
	read_file_at(path string, offset i64, size int) ![]u8
	/// write_file_at writes data to the specified offset in a file.
	write_file_at(path string, offset i64, data []u8) !

	/// get_stats returns performance statistics.
	get_stats() core.PerformanceStats

	/// close releases engine resources (io_uring mappings, cached fds, etc.).
	close()
}
