/// 인프라 레이어 - 성능 벤치마크 스위트
/// 버퍼 풀, 객체 풀, 제로카피에 대한 종합 벤치마크
module benchmarks

import time
import infra.performance.core
import infra.performance

// 벤치마크 설정

/// BenchmarkConfig는 벤치마크 실행 설정을 정의합니다.
pub struct BenchmarkConfig {
pub:
	warmup_iterations    int   = 1000
	benchmark_iterations int   = 10000
	buffer_sizes         []int = [64, 256, 1024, 4096, 16384, 65536]
	concurrent_workers   int   = 4
}

/// BenchmarkResult는 벤치마크 결과를 저장합니다.
pub struct BenchmarkResult {
pub:
	name           string
	iterations     int
	total_time_ns  i64
	avg_time_ns    i64
	min_time_ns    i64
	max_time_ns    i64
	ops_per_second f64
	memory_saved   i64
}

/// BenchmarkSuite는 모든 벤치마크를 실행하는 스위트입니다.
@[heap]
pub struct BenchmarkSuite {
mut:
	config  BenchmarkConfig
	results []BenchmarkResult
	manager &performance.PerformanceManager
}

/// new_benchmark_suite는 새로운 벤치마크 스위트를 생성합니다.
pub fn new_benchmark_suite(config BenchmarkConfig) &BenchmarkSuite {
	return &BenchmarkSuite{
		config:  config
		results: []BenchmarkResult{}
		manager: performance.get_global_performance()
	}
}

// 버퍼 풀 벤치마크

/// benchmark_buffer_pool_allocation은 버퍼 할당 성능을 벤치마크합니다.
pub fn (mut s BenchmarkSuite) benchmark_buffer_pool_allocation() BenchmarkResult {
	// 워밍업
	for _ in 0 .. s.config.warmup_iterations {
		buf := s.manager.get_buffer(1024)
		s.manager.put_buffer(buf)
	}

	mut times := []i64{cap: s.config.benchmark_iterations}

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()
		buf := s.manager.get_buffer(1024)
		s.manager.put_buffer(buf)
		times << time.sys_mono_now() - start
	}

	return s.calculate_result('BufferPool Allocation (1KB)', times)
}

/// benchmark_buffer_pool_vs_heap는 풀 할당과 힙 할당을 비교합니다.
pub fn (mut s BenchmarkSuite) benchmark_buffer_pool_vs_heap() []BenchmarkResult {
	mut results := []BenchmarkResult{}

	for size in s.config.buffer_sizes {
		// 풀 할당
		mut pooled_times := []i64{cap: s.config.benchmark_iterations}
		for _ in 0 .. s.config.benchmark_iterations {
			start := time.sys_mono_now()
			buf := s.manager.get_buffer(size)
			s.manager.put_buffer(buf)
			pooled_times << time.sys_mono_now() - start
		}
		results << s.calculate_result('Pooled ${size}B', pooled_times)

		// 힙 할당 (기준선)
		mut heap_times := []i64{cap: s.config.benchmark_iterations}
		for _ in 0 .. s.config.benchmark_iterations {
			start := time.sys_mono_now()
			_ := []u8{len: size}
			heap_times << time.sys_mono_now() - start
		}
		results << s.calculate_result('Heap ${size}B', heap_times)
	}

	return results
}

/// benchmark_buffer_pool_hit_rate는 캐시 적중률을 벤치마크합니다.
pub fn (mut s BenchmarkSuite) benchmark_buffer_pool_hit_rate() BenchmarkResult {
	// 풀 사전 워밍업
	mut buffers := []&core.Buffer{cap: 100}
	for _ in 0 .. 100 {
		buffers << s.manager.get_buffer(1024)
	}
	for buf in buffers {
		s.manager.put_buffer(buf)
	}

	// 이제 벤치마크 - 높은 적중률이 예상됨
	mut times := []i64{cap: s.config.benchmark_iterations}
	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()
		buf := s.manager.get_buffer(1024)
		s.manager.put_buffer(buf)
		times << time.sys_mono_now() - start
	}

	stats := s.manager.get_stats()
	mut result := s.calculate_result('BufferPool Hit Rate Test', times)
	result = BenchmarkResult{
		...result
		memory_saved: i64(stats.buffer_hits)
	}

	return result
}

// 객체 풀 벤치마크

/// benchmark_record_pool은 레코드 풀 성능을 벤치마크합니다.
pub fn (mut s BenchmarkSuite) benchmark_record_pool() BenchmarkResult {
	// 워밍업
	for _ in 0 .. s.config.warmup_iterations {
		rec := s.manager.get_record()
		s.manager.put_record(rec)
	}

	mut times := []i64{cap: s.config.benchmark_iterations}
	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()
		rec := s.manager.get_record()
		s.manager.put_record(rec)
		times << time.sys_mono_now() - start
	}

	return s.calculate_result('RecordPool Allocation', times)
}

/// benchmark_batch_pool은 배치 풀 성능을 벤치마크합니다.
pub fn (mut s BenchmarkSuite) benchmark_batch_pool() BenchmarkResult {
	// 워밍업
	for _ in 0 .. s.config.warmup_iterations {
		batch := s.manager.get_batch()
		s.manager.put_batch(batch)
	}

	mut times := []i64{cap: s.config.benchmark_iterations}
	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()
		batch := s.manager.get_batch()
		s.manager.put_batch(batch)
		times << time.sys_mono_now() - start
	}

	return s.calculate_result('BatchPool Allocation', times)
}

/// benchmark_request_pool은 요청 풀 성능을 벤치마크합니다.
pub fn (mut s BenchmarkSuite) benchmark_request_pool() BenchmarkResult {
	// 워밍업
	for _ in 0 .. s.config.warmup_iterations {
		req := s.manager.get_request()
		s.manager.put_request(req)
	}

	mut times := []i64{cap: s.config.benchmark_iterations}
	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()
		req := s.manager.get_request()
		s.manager.put_request(req)
		times << time.sys_mono_now() - start
	}

	return s.calculate_result('RequestPool Allocation', times)
}

// 통합 벤치마크

/// benchmark_request_response_cycle은 전체 요청/응답 사이클을 시뮬레이션합니다.
pub fn (mut s BenchmarkSuite) benchmark_request_response_cycle() BenchmarkResult {
	// 시뮬레이션: 요청 읽기 -> 처리 -> 응답 쓰기
	mut times := []i64{cap: s.config.benchmark_iterations}

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()

		// 요청 버퍼 획득
		mut req_buf := new_request_buffer(4096)

		// 처리 시뮬레이션
		_ := req_buf.data()

		// 응답 버퍼 획득
		mut resp_buf := new_response_buffer(8192)
		resp_buf.write_i32_be(100)
		resp_buf.write([u8(1), 2, 3, 4])

		// 버퍼 해제
		req_buf.release()
		resp_buf.release()

		times << time.sys_mono_now() - start
	}

	return s.calculate_result('Request/Response Cycle', times)
}

/// benchmark_connection_lifecycle은 연결 생명주기를 시뮬레이션합니다.
pub fn (mut s BenchmarkSuite) benchmark_connection_lifecycle() BenchmarkResult {
	mut times := []i64{cap: s.config.benchmark_iterations}

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()

		// 연결 설정 시뮬레이션
		mut conn_bufs := new_connection_buffers(8192, 16384)

		// I/O 작업 시뮬레이션
		_ := conn_bufs.get_read_slice(1024)
		_ := conn_bufs.get_write_slice(2048)

		// 연결 종료
		conn_bufs.release()

		times << time.sys_mono_now() - start
	}

	return s.calculate_result('Connection Lifecycle', times)
}

/// benchmark_storage_operations은 풀링을 사용한 스토리지 작업을 시뮬레이션합니다.
pub fn (mut s BenchmarkSuite) benchmark_storage_operations() BenchmarkResult {
	mut times := []i64{cap: s.config.benchmark_iterations}
	mut pool := new_storage_record_pool()

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()

		// 스토리지용 레코드 생성 시뮬레이션
		mut batch := pool.get_batch()
		for _ in 0 .. 10 {
			rec := pool.get_record()
			pool.put_record(rec)
		}
		pool.put_batch(batch)

		times << time.sys_mono_now() - start
	}

	return s.calculate_result('Storage Operations (10 records)', times)
}

// 결과 계산

/// calculate_result는 시간 측정값으로부터 벤치마크 결과를 계산합니다.
fn (s &BenchmarkSuite) calculate_result(name string, times []i64) BenchmarkResult {
	if times.len == 0 {
		return BenchmarkResult{
			name: name
		}
	}

	mut total := i64(0)
	mut min_t := times[0]
	mut max_t := times[0]

	for t in times {
		total += t
		if t < min_t {
			min_t = t
		}
		if t > max_t {
			max_t = t
		}
	}

	avg := total / times.len
	ops_per_sec := if avg > 0 { f64(1_000_000_000) / f64(avg) } else { 0.0 }

	return BenchmarkResult{
		name:           name
		iterations:     times.len
		total_time_ns:  total
		avg_time_ns:    avg
		min_time_ns:    min_t
		max_time_ns:    max_t
		ops_per_second: ops_per_sec
	}
}

// 모든 벤치마크 실행

/// run_all은 모든 벤치마크를 실행하고 결과를 반환합니다.
pub fn (mut s BenchmarkSuite) run_all() []BenchmarkResult {
	mut results := []BenchmarkResult{}

	println('╔══════════════════════════════════════════════════════════════╗')
	println('║           DataCore Performance Benchmark Suite               ║')
	println('╚══════════════════════════════════════════════════════════════╝')
	println('')

	// 버퍼 풀 벤치마크
	println('▶ Running Buffer Pool Benchmarks...')
	results << s.benchmark_buffer_pool_allocation()
	results << s.benchmark_buffer_pool_hit_rate()
	pool_vs_heap := s.benchmark_buffer_pool_vs_heap()
	results << pool_vs_heap

	// 객체 풀 벤치마크
	println('▶ Running Object Pool Benchmarks...')
	results << s.benchmark_record_pool()
	results << s.benchmark_batch_pool()
	results << s.benchmark_request_pool()

	// 통합 벤치마크
	println('▶ Running Integration Benchmarks...')
	results << s.benchmark_request_response_cycle()
	results << s.benchmark_connection_lifecycle()
	results << s.benchmark_storage_operations()

	s.results = results
	return results
}

/// print_results는 벤치마크 결과를 포맷된 테이블로 출력합니다.
pub fn (mut s BenchmarkSuite) print_results() {
	println('')
	println('┌────────────────────────────────┬────────────┬────────────┬────────────┬──────────────┐')
	println('│ Benchmark                      │ Iterations │ Avg (ns)   │ Min (ns)   │ Ops/sec      │')
	println('├────────────────────────────────┼────────────┼────────────┼────────────┼──────────────┤')

	for result in s.results {
		name := result.name.limit(30)
		println('│ ${name:-30} │ ${result.iterations:10} │ ${result.avg_time_ns:10} │ ${result.min_time_ns:10} │ ${result.ops_per_second:12.0} │')
	}

	println('└────────────────────────────────┴────────────┴────────────┴────────────┴──────────────┘')

	// 풀 통계 출력
	stats := s.manager.get_stats()
	println('')
	println('Pool Statistics:')
	println('  Engine: ${stats.engine_name}')
	println('  Buffer Pool:')
	println('    - Hits: ${stats.buffer_hits}, Misses: ${stats.buffer_misses}')
	hit_rate := if stats.buffer_hits + stats.buffer_misses > 0 {
		f64(stats.buffer_hits) / f64(stats.buffer_hits + stats.buffer_misses) * 100.0
	} else {
		0.0
	}
	println('    - Hit Rate: ${hit_rate:.2f}%')
	println('  Operations: ${stats.ops_count}')
}

// 빠른 벤치마크 진입점

/// run_quick_benchmark는 기본 설정으로 빠른 벤치마크를 실행합니다.
pub fn run_quick_benchmark() {
	performance.init_global_performance(performance.PerformanceConfig{
		buffer_pool_prewarm: true
	})

	mut suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    100
		benchmark_iterations: 1000
		buffer_sizes:         [256, 1024, 4096]
	})

	suite.run_all()
	suite.print_results()
}
