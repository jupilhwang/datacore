module performance

import infra.performance.core

// PerformanceConfig holds all performance-related settings
pub struct PerformanceConfig {
pub:
	buffer_pool_max_tiny    int = 1000
	buffer_pool_max_small   int = 500
	buffer_pool_max_medium  int = 100
	buffer_pool_max_large   int = 20
	buffer_pool_max_huge    int = 5
	buffer_pool_prewarm     bool = true
	record_pool_max_size    int = 10000
	batch_pool_max_size     int = 1000
	request_pool_max_size   int = 5000
	enable_buffer_pooling   bool = true
	enable_object_pooling   bool = true
	enable_zero_copy        bool = true
	enable_linux_optimizations bool = true
}

// PerformanceEngine defines the standard interface for all performance optimization plugins
pub interface PerformanceEngine {
	name() string
	init(config PerformanceConfig) !
	
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
	get_stats() PerformanceStats
}

// PerformanceStats - Unified stats from the engine
pub struct PerformanceStats {
pub:
	engine_name string
	buffer_hits u64
	buffer_misses u64
	ops_count   u64
}
