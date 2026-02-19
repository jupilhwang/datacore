/// Infrastructure layer - Performance configuration
/// Module containing all performance-related configuration
module core

/// PerformanceConfig holds all performance-related configuration.
pub struct PerformanceConfig {
pub:
	buffer_pool_max_tiny       int  = 1000
	buffer_pool_max_small      int  = 500
	buffer_pool_max_medium     int  = 100
	buffer_pool_max_large      int  = 20
	buffer_pool_max_huge       int  = 5
	buffer_pool_prewarm        bool = true
	record_pool_max_size       int  = 10000
	batch_pool_max_size        int  = 1000
	request_pool_max_size      int  = 5000
	enable_buffer_pooling      bool = true
	enable_object_pooling      bool = true
	enable_zero_copy           bool = true
	enable_linux_optimizations bool = true
}

/// PerformanceStats holds aggregated statistics for an engine.
pub struct PerformanceStats {
pub:
	engine_name   string
	buffer_hits   u64
	buffer_misses u64
	ops_count     u64
}
