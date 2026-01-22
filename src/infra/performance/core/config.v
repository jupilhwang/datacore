/// 인프라 레이어 - 성능 설정
/// 성능 관련 모든 설정을 담고 있는 모듈
module core

/// PerformanceConfig는 모든 성능 관련 설정을 담고 있습니다.
pub struct PerformanceConfig {
pub:
	buffer_pool_max_tiny       int  = 1000  // tiny 버퍼 풀 최대 크기
	buffer_pool_max_small      int  = 500   // small 버퍼 풀 최대 크기
	buffer_pool_max_medium     int  = 100   // medium 버퍼 풀 최대 크기
	buffer_pool_max_large      int  = 20    // large 버퍼 풀 최대 크기
	buffer_pool_max_huge       int  = 5     // huge 버퍼 풀 최대 크기
	buffer_pool_prewarm        bool = true  // 버퍼 풀 사전 워밍 활성화
	record_pool_max_size       int  = 10000 // 레코드 풀 최대 크기
	batch_pool_max_size        int  = 1000  // 배치 풀 최대 크기
	request_pool_max_size      int  = 5000  // 요청 풀 최대 크기
	enable_buffer_pooling      bool = true  // 버퍼 풀링 활성화
	enable_object_pooling      bool = true  // 객체 풀링 활성화
	enable_zero_copy           bool = true  // 제로카피 활성화
	enable_linux_optimizations bool = true  // 리눅스 최적화 활성화
}

/// PerformanceStats는 엔진의 통합 통계를 담고 있습니다.
pub struct PerformanceStats {
pub:
	engine_name   string // 엔진 이름
	buffer_hits   u64    // 버퍼 히트 수
	buffer_misses u64    // 버퍼 미스 수
	ops_count     u64    // 작업 수
}
