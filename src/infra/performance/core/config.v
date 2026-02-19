/// 인프라 레이어 - 성능 설정
/// 성능 관련 모든 설정을 담고 있는 모듈
module core

/// PerformanceConfig는 모든 성능 관련 설정을 담고 있습니다.
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

/// PerformanceStats는 엔진의 통합 통계를 담고 있습니다.
pub struct PerformanceStats {
pub:
	engine_name   string
	buffer_hits   u64
	buffer_misses u64
	ops_count     u64
}
