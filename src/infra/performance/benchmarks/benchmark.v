/// Infrastructure layer - Performance benchmark suite
/// Comprehensive benchmarks for buffer pool, object pool, and zero-copy
module benchmarks

import time
import infra.performance.core
import infra.performance

// Benchmark configuration

/// BenchmarkConfig defines the benchmark execution settings.
pub struct BenchmarkConfig {
pub:
	warmup_iterations    int   = 1000
	benchmark_iterations int   = 10000
	buffer_sizes         []int = [64, 256, 1024, 4096, 16384, 65536]
	concurrent_workers   int   = 4
}

/// BenchmarkResult stores benchmark results.
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

/// BenchmarkSuite is the suite that runs all benchmarks.
@[heap]
pub struct BenchmarkSuite {
mut:
	config  BenchmarkConfig
	results []BenchmarkResult
	manager &performance.PerformanceManager
}

/// new_benchmark_suite creates a new benchmark suite.
pub fn new_benchmark_suite(config BenchmarkConfig) &BenchmarkSuite {
	return &BenchmarkSuite{
		config:  config
		results: []BenchmarkResult{}
		manager: performance.get_global_performance()
	}
}

// Buffer pool benchmarks

/// benchmark_buffer_pool_allocation benchmarks buffer allocation performance.
pub fn (mut s BenchmarkSuite) benchmark_buffer_pool_allocation() BenchmarkResult {
	// Warmup
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

/// benchmark_buffer_pool_vs_heap compares pool allocation vs heap allocation.
pub fn (mut s BenchmarkSuite) benchmark_buffer_pool_vs_heap() []BenchmarkResult {
	mut results := []BenchmarkResult{}

	for size in s.config.buffer_sizes {
		// Pool allocation
		mut pooled_times := []i64{cap: s.config.benchmark_iterations}
		for _ in 0 .. s.config.benchmark_iterations {
			start := time.sys_mono_now()
			buf := s.manager.get_buffer(size)
			s.manager.put_buffer(buf)
			pooled_times << time.sys_mono_now() - start
		}
		results << s.calculate_result('Pooled ${size}B', pooled_times)

		// Heap allocation (baseline)
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

/// benchmark_buffer_pool_hit_rate benchmarks cache hit rate.
pub fn (mut s BenchmarkSuite) benchmark_buffer_pool_hit_rate() BenchmarkResult {
	// Pre-warm the pool
	mut buffers := []&core.Buffer{cap: 100}
	for _ in 0 .. 100 {
		buffers << s.manager.get_buffer(1024)
	}
	for buf in buffers {
		s.manager.put_buffer(buf)
	}

	// Now benchmark - high hit rate expected
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

// Object pool benchmarks

/// benchmark_record_pool benchmarks record pool performance.
pub fn (mut s BenchmarkSuite) benchmark_record_pool() BenchmarkResult {
	// Warmup
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

/// benchmark_batch_pool benchmarks batch pool performance.
pub fn (mut s BenchmarkSuite) benchmark_batch_pool() BenchmarkResult {
	// Warmup
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

/// benchmark_request_pool benchmarks request pool performance.
pub fn (mut s BenchmarkSuite) benchmark_request_pool() BenchmarkResult {
	// Warmup
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

// Integration benchmarks

/// benchmark_request_response_cycle simulates a full request/response cycle.
pub fn (mut s BenchmarkSuite) benchmark_request_response_cycle() BenchmarkResult {
	// Simulation: read request -> process -> write response
	mut times := []i64{cap: s.config.benchmark_iterations}

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()

		// Acquire request buffer
		mut req_buf := new_request_buffer(4096)

		// Simulate processing
		_ := req_buf.data()

		// Acquire response buffer
		mut resp_buf := new_response_buffer(8192)
		resp_buf.write_i32_be(100)
		resp_buf.write([u8(1), 2, 3, 4])

		// Release buffers
		req_buf.release()
		resp_buf.release()

		times << time.sys_mono_now() - start
	}

	return s.calculate_result('Request/Response Cycle', times)
}

/// benchmark_connection_lifecycle simulates a connection lifecycle.
pub fn (mut s BenchmarkSuite) benchmark_connection_lifecycle() BenchmarkResult {
	mut times := []i64{cap: s.config.benchmark_iterations}

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()

		// Simulate connection setup
		mut conn_bufs := new_connection_buffers(8192, 16384)

		// Simulate I/O operations
		_ := conn_bufs.get_read_slice(1024)
		_ := conn_bufs.get_write_slice(2048)

		// Close connection
		conn_bufs.release()

		times << time.sys_mono_now() - start
	}

	return s.calculate_result('Connection Lifecycle', times)
}

/// benchmark_storage_operations simulates storage operations using pooling.
pub fn (mut s BenchmarkSuite) benchmark_storage_operations() BenchmarkResult {
	mut times := []i64{cap: s.config.benchmark_iterations}
	mut pool := new_storage_record_pool()

	for _ in 0 .. s.config.benchmark_iterations {
		start := time.sys_mono_now()

		// Simulate record creation for storage
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

// Result calculation

/// calculate_result calculates benchmark results from timing measurements.
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

// Run all benchmarks

/// run_all runs all benchmarks and returns results.
pub fn (mut s BenchmarkSuite) run_all() []BenchmarkResult {
	mut results := []BenchmarkResult{}

	println('╔══════════════════════════════════════════════════════════════╗')
	println('║           DataCore Performance Benchmark Suite               ║')
	println('╚══════════════════════════════════════════════════════════════╝')
	println('')

	// Buffer pool benchmarks
	println('▶ Running Buffer Pool Benchmarks...')
	results << s.benchmark_buffer_pool_allocation()
	results << s.benchmark_buffer_pool_hit_rate()
	pool_vs_heap := s.benchmark_buffer_pool_vs_heap()
	results << pool_vs_heap

	// Object pool benchmarks
	println('▶ Running Object Pool Benchmarks...')
	results << s.benchmark_record_pool()
	results << s.benchmark_batch_pool()
	results << s.benchmark_request_pool()

	// Integration benchmarks
	println('▶ Running Integration Benchmarks...')
	results << s.benchmark_request_response_cycle()
	results << s.benchmark_connection_lifecycle()
	results << s.benchmark_storage_operations()

	s.results = results
	return results
}

/// print_results prints benchmark results as a formatted table.
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

	// Print pool statistics
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

// Quick benchmark entry point

/// run_quick_benchmark runs a quick benchmark with default configuration.
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
