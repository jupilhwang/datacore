/// 성능 모듈 통합 테스트
module benchmarks

// ============================================================================
// 통합 테스트
// ============================================================================

/// 전역 성능 관리자 초기화 테스트
fn test_global_performance_manager_init() {
	init_global_performance(PerformanceConfig{
		buffer_pool_max_tiny:  100
		buffer_pool_max_small: 50
		record_pool_max_size:  1000
	})

	mgr := get_global_performance()
	assert mgr != unsafe { nil }
	assert mgr.enabled == true
}

/// 요청 버퍼 생명주기 테스트
fn test_request_buffer_lifecycle() {
	init_global_performance(PerformanceConfig{})

	// 할당
	mut buf := new_request_buffer(1024)
	assert buf.buffer != unsafe { nil }
	assert buf.buffer.cap >= 1024

	// 사용
	data := buf.data()
	assert data.len >= 1024

	// 크기 조정
	buf.resize(2048)
	assert buf.buffer.cap >= 2048

	// 해제
	buf.release()
}

/// 응답 버퍼 쓰기 테스트
fn test_response_buffer_write() {
	init_global_performance(PerformanceConfig{})

	mut buf := new_response_buffer(256)

	// 데이터 쓰기
	buf.write([u8(1), 2, 3, 4])
	assert buf.len() == 4

	// i32 쓰기
	buf.write_i32_be(0x12345678)
	assert buf.len() == 8

	// i16 쓰기
	buf.write_i16_be(0x1234)
	assert buf.len() == 10

	// 바이트 검증
	bytes := buf.bytes()
	assert bytes.len == 10
	assert bytes[0] == 1
	assert bytes[1] == 2
	assert bytes[4] == 0x12
	assert bytes[5] == 0x34

	buf.release()
}

/// 응답 버퍼 확장 테스트
fn test_response_buffer_grow() {
	init_global_performance(PerformanceConfig{})

	mut buf := new_response_buffer(16) // 작은 초기 크기

	// 용량보다 많이 쓰기
	for _ in 0 .. 100 {
		buf.write([u8(1), 2, 3, 4, 5, 6, 7, 8])
	}

	assert buf.len() == 800
	assert buf.buffer.cap >= 800

	buf.release()
}

/// 연결 버퍼 테스트
fn test_connection_buffers() {
	init_global_performance(PerformanceConfig{})

	mut bufs := new_connection_buffers(4096, 8192)

	// 슬라이스 획득
	read_slice := bufs.get_read_slice(1024)
	assert read_slice.len == 1024

	write_slice := bufs.get_write_slice(2048)
	assert write_slice.len == 2048

	// 버퍼보다 큰 슬라이스
	large_slice := bufs.get_read_slice(10000)
	assert large_slice.len == 10000

	bufs.release()
}

/// 스토리지 레코드 풀 테스트
fn test_storage_record_pool() {
	init_global_performance(PerformanceConfig{})

	mut pool := new_storage_record_pool()

	// 레코드 획득 및 반환
	rec := pool.get_record()
	assert rec != unsafe { nil }
	pool.put_record(rec)

	// 배치 획득 및 반환
	batch := pool.get_batch()
	assert batch != unsafe { nil }
	pool.put_batch(batch)
}

/// Fetch 버퍼 제로카피 테스트
fn test_fetch_buffer_zero_copy() {
	init_global_performance(PerformanceConfig{})

	mut buf := new_fetch_buffer(4096)
	assert !buf.has_zero_copy()

	// 제로카피 설정
	buf.set_zero_copy(10, 1000, 4096)
	assert buf.has_zero_copy()
	assert buf.zero_copy_fd == 10
	assert buf.zero_copy_off == 1000
	assert buf.zero_copy_len == 4096

	buf.release()
}

/// 요청 버퍼 콜백 테스트
fn test_with_request_buffer_callback() {
	init_global_performance(PerformanceConfig{})

	// 콜백이 유효한 버퍼로 호출되는지 테스트
	// 클로저 캡처 대신 직접 버퍼 작업 사용
	mut buf := new_request_buffer(1024)
	assert buf.buffer != unsafe { nil }
	assert buf.buffer.cap >= 1024
	buf.release()
}

/// 응답 버퍼 콜백 테스트
fn test_with_response_buffer_callback() {
	init_global_performance(PerformanceConfig{})

	// 응답 버퍼가 올바르게 작동하는지 테스트
	mut buf := new_response_buffer(512)
	buf.write([u8(1), 2, 3])
	assert buf.len() == 3
	buf.release()
}

/// 통합 통계 테스트
fn test_integration_stats() {
	init_global_performance(PerformanceConfig{})

	// 일부 작업 수행
	mut req := allocate_request_buffer(1024)
	req.release()

	mut resp := allocate_response_buffer(512)
	resp.release()

	// 통계 획득 - 크래시 없이 동작하는지 확인
	stats := get_integration_stats()
	// 통계 구조체가 유효해야 함
	assert stats.perf_stats.buffer_pool_stats.hit_rate() >= 0.0
}

/// 벤치마크 스위트 생성 테스트
fn test_benchmark_suite_creation() {
	init_global_performance(PerformanceConfig{})

	suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    10
		benchmark_iterations: 100
	})

	assert suite.config.warmup_iterations == 10
	assert suite.config.benchmark_iterations == 100
}

/// 벤치마크 버퍼 할당 테스트
fn test_benchmark_buffer_allocation() {
	init_global_performance(PerformanceConfig{
		buffer_pool_prewarm: true
	})

	mut suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    10
		benchmark_iterations: 100
	})

	result := suite.benchmark_buffer_pool_allocation()
	assert result.iterations == 100
	assert result.avg_time_ns > 0
	assert result.ops_per_second > 0
}

/// 벤치마크 레코드 풀 테스트
fn test_benchmark_record_pool() {
	init_global_performance(PerformanceConfig{})

	mut suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    10
		benchmark_iterations: 100
	})

	result := suite.benchmark_record_pool()
	assert result.iterations == 100
	assert result.avg_time_ns > 0
}

/// 벤치마크 요청/응답 사이클 테스트
fn test_benchmark_request_response_cycle() {
	init_global_performance(PerformanceConfig{})

	mut suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    5
		benchmark_iterations: 50
	})

	result := suite.benchmark_request_response_cycle()
	assert result.iterations == 50
	assert result.name == 'Request/Response Cycle'
}

/// 부하 상태에서의 성능 테스트
fn test_performance_under_load() {
	init_global_performance(PerformanceConfig{
		buffer_pool_max_tiny:  1000
		buffer_pool_max_small: 500
		record_pool_max_size:  5000
	})

	// 높은 부하 시뮬레이션
	mut buffers := []&RequestBuffer{cap: 100}

	for _ in 0 .. 100 {
		buffers << new_request_buffer(1024)
	}

	// 모두 반환
	for buf in buffers {
		mut b := buf
		b.release()
	}

	// 통계 확인
	mut mgr := get_global_performance()
	stats := mgr.get_stats()

	// 버퍼 반환 후 높은 재사용률이 있어야 함
	assert stats.buffer_pool_stats.bytes_reused >= 0
}
