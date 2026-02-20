// Benchmark infrastructure for replication system performance measurement.
// Provides BenchmarkResult struct, run_benchmark function, and helpers for test data generation.
module main

import time
import rand
import log

// BenchmarkResult holds the measured performance metrics of a single benchmark run.
pub struct BenchmarkResult {
pub mut:
	name           string
	iterations     int
	total_time_ns  i64
	avg_time_ns    i64
	min_time_ns    i64
	max_time_ns    i64
	ops_per_second f64
}

// run_benchmark executes fn for the given number of iterations and measures performance.
// Includes warmup_count warmup iterations before the measurement period begins.
// Returns a BenchmarkResult with aggregated timing statistics.
pub fn run_benchmark(name string, iterations int, warmup_count int, fn_ fn ()) BenchmarkResult {
	// Warmup phase
	for _ in 0 .. warmup_count {
		fn_()
	}

	mut min_ns := i64(9_223_372_036_854_775_807) // i64 max
	mut max_ns := i64(0)
	mut total_ns := i64(0)

	for _ in 0 .. iterations {
		start := time.now()
		fn_()
		elapsed := time.since(start).nanoseconds()

		total_ns += elapsed
		if elapsed < min_ns {
			min_ns = elapsed
		}
		if elapsed > max_ns {
			max_ns = elapsed
		}
	}

	avg_ns := if iterations > 0 { total_ns / i64(iterations) } else { i64(0) }
	total_sec := f64(total_ns) / 1_000_000_000.0
	ops_sec := if total_sec > 0.0 { f64(iterations) / total_sec } else { 0.0 }

	return BenchmarkResult{
		name:           name
		iterations:     iterations
		total_time_ns:  total_ns
		avg_time_ns:    avg_ns
		min_time_ns:    if min_ns == i64(9_223_372_036_854_775_807) { i64(0) } else { min_ns }
		max_time_ns:    max_ns
		ops_per_second: ops_sec
	}
}

// print_result prints a BenchmarkResult in a human-readable table row format.
pub fn print_result(r BenchmarkResult) {
	avg_us := f64(r.avg_time_ns) / 1000.0
	min_us := f64(r.min_time_ns) / 1000.0
	max_us := f64(r.max_time_ns) / 1000.0
	println('  ${r.name:-45} | iters=${r.iterations:-6} | avg=${avg_us:8.1f}us | min=${min_us:8.1f}us | max=${max_us:8.1f}us | ${r.ops_per_second:12.0f} ops/sec')
}

// print_header prints the benchmark table header.
pub fn print_header() {
	sep := '-'.repeat(130)
	println('  ${sep}')
	println('  Benchmark                                      | Iters   | Avg (us)     | Min (us)     | Max (us)     |   Throughput')
	println('  ${sep}')
}

// print_footer prints the benchmark table footer.
pub fn print_footer() {
	sep := '-'.repeat(130)
	println('  ${sep}')
}

// make_bench_logger returns a log.Log set to warn level to suppress debug/info noise during benchmarks.
pub fn make_bench_logger() log.Log {
	mut l := log.Log{}
	l.set_level(.warn)
	return l
}

// make_record_data generates a byte slice of the requested size filled with pseudo-random content.
// Uses a simple pattern based on the record index to avoid allocator overhead.
pub fn make_record_data(size int, index int) []u8 {
	mut data := []u8{len: size}
	base := u8(index & 0xFF)
	for i in 0 .. size {
		data[i] = base ^ u8(i & 0xFF)
	}
	return data
}

// make_records_batch returns a slice of num_records byte slices, each of record_size bytes.
pub fn make_records_batch(num_records int, record_size int) [][]u8 {
	mut batch := [][]u8{cap: num_records}
	for i in 0 .. num_records {
		batch << make_record_data(record_size, i)
	}
	return batch
}

// random_topic returns a random topic name to avoid cache effects across benchmark runs.
pub fn random_topic() string {
	return 'bench-topic-${rand.u32()}'
}

// format_throughput formats a throughput number with K/M suffix for readability.
pub fn format_throughput(ops_per_sec f64) string {
	if ops_per_sec >= 1_000_000 {
		return '${ops_per_sec / 1_000_000.0:.2f}M ops/sec'
	} else if ops_per_sec >= 1_000 {
		return '${ops_per_sec / 1_000.0:.2f}K ops/sec'
	}
	return '${ops_per_sec:.0f} ops/sec'
}

// check_target asserts whether a benchmark result meets the expected minimum throughput.
// Prints PASS or FAIL with the target and actual value.
pub fn check_throughput_target(r BenchmarkResult, target_ops_per_sec f64) bool {
	passed := r.ops_per_second >= target_ops_per_sec
	status := if passed { 'PASS' } else { 'FAIL' }
	println('  [${status}] ${r.name}: ${format_throughput(r.ops_per_second)} (target: ${format_throughput(target_ops_per_sec)})')
	return passed
}

// check_latency_target asserts whether a benchmark result meets the maximum average latency.
// Prints PASS or FAIL with the target and actual value.
pub fn check_latency_target(r BenchmarkResult, target_avg_ns i64) bool {
	passed := r.avg_time_ns <= target_avg_ns
	status := if passed { 'PASS' } else { 'FAIL' }
	actual_us := f64(r.avg_time_ns) / 1000.0
	target_us := f64(target_avg_ns) / 1000.0
	println('  [${status}] ${r.name}: avg=${actual_us:.1f}us (target: <${target_us:.1f}us)')
	return passed
}
