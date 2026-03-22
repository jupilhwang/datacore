module benchmarks

/// Comprehensive I/O benchmark suite
/// Compares performance of various I/O strategies:
/// - Regular file I/O
/// - Memory-mapped I/O (mmap)
/// - DMA / Scatter-Gather I/O
/// - io_uring (Linux)
/// - NUMA-aware allocation
import os
import time
import infra.performance.engines
import infra.performance.sysio
import infra.performance.core

// I/O benchmark configuration

/// IoBenchmarkConfig defines I/O benchmark settings.
pub struct IoBenchmarkConfig {
pub:
	// Test parameters
	iterations  int   = 100
	warmup_runs int   = 10
	data_size   usize = 4096
	file_size   i64   = 1024 * 1024

	// Test selection
	test_regular_io   bool = true
	test_mmap         bool = true
	test_dma          bool = true
	test_io_uring     bool = true
	test_numa         bool = true
	test_buffer_pools bool = true

	// Output
	verbose       bool
	output_format OutputFormat = .text
}

/// OutputFormat defines benchmark result output format.
pub enum OutputFormat {
	text
	json
	markdown
}

// Benchmark results

/// IoBenchmarkResults stores I/O benchmark results.
pub struct IoBenchmarkResults {
pub mut:
	test_name       string
	iterations      int
	total_time_ns   i64
	avg_time_ns     i64
	min_time_ns     i64
	max_time_ns     i64
	throughput_mbps f64
	ops_per_sec     f64
	data_size       usize
}

/// IoBenchmarkSuite is the I/O benchmark suite.
pub struct IoBenchmarkSuite {
pub mut:
	config  IoBenchmarkConfig
	results []IoBenchmarkResults
	system  SystemInfo
}

/// SystemInfo stores system information.
pub struct SystemInfo {
pub:
	os_name            string
	numa_nodes         int
	numa_available     bool
	io_uring_available bool
	mmap_available     bool
	dma_available      bool
	total_memory       i64
}

// Benchmark runner

/// new_io_benchmark_suite creates a new benchmark suite.
pub fn new_io_benchmark_suite(config IoBenchmarkConfig) IoBenchmarkSuite {
	return IoBenchmarkSuite{
		config:  config
		results: []IoBenchmarkResults{}
		system:  detect_system_capabilities()
	}
}

/// detect_system_capabilities detects system capabilities.
fn detect_system_capabilities() SystemInfo {
	topology := engines.get_numa_topology()
	io_caps := engines.get_async_io_capabilities()
	dma_caps := sysio.get_platform_capabilities()

	mut detected_os := 'Unknown'
	$if linux {
		detected_os = 'Linux'
	} $else $if macos {
		detected_os = 'macOS'
	} $else $if windows {
		detected_os = 'Windows'
	}

	total_mem := if topology.nodes.len > 0 { topology.nodes[0].total_mem } else { i64(0) }

	return SystemInfo{
		os_name:            detected_os
		numa_nodes:         topology.node_count
		numa_available:     topology.available
		io_uring_available: io_caps.has_io_uring
		mmap_available:     true
		dma_available:      dma_caps.has_scatter_gather
		total_memory:       total_mem
	}
}

/// run_all runs all configured benchmarks.
pub fn (mut s IoBenchmarkSuite) run_all() {
	if s.config.verbose {
		println('Starting benchmark suite...')
		println('System: ${s.system.os_name}, NUMA nodes: ${s.system.numa_nodes}')
	}

	// Create test file
	test_file := create_test_file(s.config.file_size)
	defer {
		os.rm(test_file) or {}
	}

	if s.config.test_regular_io {
		s.bench_regular_read(test_file)
		s.bench_regular_write(test_file)
	}

	if s.config.test_mmap {
		s.bench_mmap_read(test_file)
		s.bench_mmap_write(test_file)
	}

	if s.config.test_dma {
		s.bench_dma_read(test_file)
		s.bench_dma_write(test_file)
	}

	if s.config.test_io_uring {
		s.bench_io_uring(test_file)
	}

	if s.config.test_numa {
		s.bench_numa_allocation()
	}

	if s.config.test_buffer_pools {
		s.bench_buffer_pools()
	}
}

// Individual benchmarks

/// bench_regular_read benchmarks regular file reading.
fn (mut s IoBenchmarkSuite) bench_regular_read(path string) {
	mut times := []i64{cap: s.config.iterations}

	// Warmup
	for _ in 0 .. s.config.warmup_runs {
		os.read_file(path) or { continue }
	}

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		_ = os.read_file(path) or { '' }
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Regular Read', times, s.config.data_size)
}

/// bench_regular_write benchmarks regular file writing.
fn (mut s IoBenchmarkSuite) bench_regular_write(path string) {
	mut times := []i64{cap: s.config.iterations}
	data := []u8{len: int(s.config.data_size), init: u8(index % 256)}
	test_path := '${path}.write_test'

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		os.write_file(test_path, data.bytestr()) or { continue }
		times << time.since(start).nanoseconds()
	}

	os.rm(test_path) or {}
	s.results << calculate_io_results('Regular Write', times, s.config.data_size)
}

/// bench_mmap_read benchmarks memory-mapped reading.
fn (mut s IoBenchmarkSuite) bench_mmap_read(path string) {
	mut times := []i64{cap: s.config.iterations}

	// Warmup
	for _ in 0 .. s.config.warmup_runs {
		if mut mm := sysio.MmapFile.open(path, true) {
			if region := mm.map_region(0, int(s.config.data_size)) {
				mm.unmap_region(region) or {}
			}
			mm.close() or {}
		}
	}

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		if mut mm := sysio.MmapFile.open(path, true) {
			if region := mm.map_region(0, int(s.config.data_size)) {
				mm.unmap_region(region) or {}
			}
			mm.close() or {}
		}
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Mmap Read', times, s.config.data_size)
}

/// bench_mmap_write benchmarks memory-mapped writing.
fn (mut s IoBenchmarkSuite) bench_mmap_write(path string) {
	mut times := []i64{cap: s.config.iterations}
	test_path := '${path}.mmap_write'

	// Create file first
	os.write_file(test_path, []u8{len: int(s.config.file_size)}.bytestr()) or { return }

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		if mut mm := sysio.MmapFile.open(test_path, false) {
			if region := mm.map_region(0, int(s.config.data_size)) {
				mm.sync_region(region) or {}
				mm.unmap_region(region) or {}
			}
			mm.close() or {}
		}
		times << time.since(start).nanoseconds()
	}

	os.rm(test_path) or {}
	s.results << calculate_io_results('Mmap Write', times, s.config.data_size)
}

/// bench_dma_read benchmarks DMA scatter reading.
fn (mut s IoBenchmarkSuite) bench_dma_read(path string) {
	mut times := []i64{cap: s.config.iterations}
	caps := sysio.get_platform_capabilities()

	if !caps.has_scatter_gather {
		s.results << IoBenchmarkResults{
			test_name: 'DMA Read (skipped - not available)'
		}
		return
	}

	// Prepare scatter buffers
	mut bufs := [
		sysio.new_sg_buffer(1024),
		sysio.new_sg_buffer(1024),
		sysio.new_sg_buffer(1024),
		sysio.new_sg_buffer(1024),
	]

	mut file := os.open_file(path, 'r') or { return }
	defer {
		file.close()
	}

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		_ = sysio.scatter_read_native(file.fd, mut bufs)
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('DMA Scatter Read', times, s.config.data_size)
}

/// bench_dma_write benchmarks DMA gather writing.
fn (mut s IoBenchmarkSuite) bench_dma_write(path string) {
	mut times := []i64{cap: s.config.iterations}
	caps := sysio.get_platform_capabilities()
	test_path := '${path}.dma_write'

	if !caps.has_scatter_gather {
		s.results << IoBenchmarkResults{
			test_name: 'DMA Write (skipped - not available)'
		}
		return
	}

	// Prepare gather buffers
	bufs := [
		sysio.new_sg_buffer_from([]u8{len: 1024, init: 0xAA}),
		sysio.new_sg_buffer_from([]u8{len: 1024, init: 0xBB}),
		sysio.new_sg_buffer_from([]u8{len: 1024, init: 0xCC}),
		sysio.new_sg_buffer_from([]u8{len: 1024, init: 0xDD}),
	]

	// Benchmark
	for _ in 0 .. s.config.iterations {
		mut fd := os.open_file(test_path, 'w') or { continue }

		start := time.now()
		_ = sysio.gather_write_native(fd.fd, bufs)
		times << time.since(start).nanoseconds()

		fd.close()
	}

	os.rm(test_path) or {}
	s.results << calculate_io_results('DMA Gather Write', times, s.config.data_size)
}

/// bench_io_uring benchmarks io_uring async I/O.
fn (mut s IoBenchmarkSuite) bench_io_uring(path string) {
	mut times := []i64{cap: s.config.iterations}

	// Check io_uring capability first
	caps := engines.get_async_io_capabilities()
	if !caps.has_io_uring {
		s.results << IoBenchmarkResults{
			test_name: 'io_uring (skipped - not available)'
		}
		return
	}

	config := engines.IoUringConfig{
		queue_depth: 32
	}

	mut ring := engines.new_io_uring(config) or {
		s.results << IoBenchmarkResults{
			test_name: 'io_uring (skipped - init failed)'
		}
		return
	}
	defer {
		ring.close()
	}

	buf := []u8{len: int(s.config.data_size)}

	// Async read benchmark
	for i in 0 .. s.config.iterations {
		mut file := os.open_file(path, 'r') or { continue }

		start := time.now()
		ring.prep_read(file.fd, buf, 0, u64(i))
		ring.submit(1) or { continue }
		_ = ring.wait_cqe() or { continue }
		times << time.since(start).nanoseconds()

		file.close()
	}

	s.results << calculate_io_results('io_uring Read', times, s.config.data_size)
}

/// bench_numa_allocation benchmarks NUMA memory allocation.
fn (mut s IoBenchmarkSuite) bench_numa_allocation() {
	mut times := []i64{cap: s.config.iterations}
	size := s.config.data_size

	// NUMA local allocation benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		mem := engines.numa_alloc_local(size)
		// Touch memory to verify it was actually allocated
		unsafe {
			C.memset(mem.ptr, 0, size)
		}
		engines.numa_free(mem)
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('NUMA Local Alloc', times, size)

	// Interleaved allocation benchmark
	mut times2 := []i64{cap: s.config.iterations}
	for _ in 0 .. s.config.iterations {
		start := time.now()
		mem := engines.numa_alloc_interleaved(size)
		unsafe {
			C.memset(mem.ptr, 0, size)
		}
		engines.numa_free(mem)
		times2 << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('NUMA Interleaved Alloc', times2, size)
}

/// bench_buffer_pools benchmarks buffer pool performance.
fn (mut s IoBenchmarkSuite) bench_buffer_pools() {
	// Buffer pool benchmark
	mut times := []i64{cap: s.config.iterations}

	mut pool := core.new_buffer_pool(core.PoolConfig{})

	// Warmup
	for _ in 0 .. s.config.warmup_runs {
		buf := pool.get(int(s.config.data_size))
		pool.put(buf)
	}

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		buf := pool.get(int(s.config.data_size))
		// Simulate operation
		unsafe {
			if buf.data.len > 0 {
				C.memset(buf.data.data, 0, usize(buf.data.len))
			}
		}
		pool.put(buf)
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Buffer Pool Get/Put', times, s.config.data_size)

	// NUMA buffer pool benchmark
	mut numa_times := []i64{cap: s.config.iterations}

	mut numa_pool := engines.new_numa_buffer_pool(engines.NumaBufferConfig{
		buffer_size:      s.config.data_size
		buffers_per_node: 100
	})
	defer {
		numa_pool.close()
	}

	// Benchmark
	for _ in 0 .. s.config.iterations {
		start := time.now()
		if buf := numa_pool.get_buffer() {
			unsafe {
				C.memset(buf.ptr, 0, buf.size)
			}
			numa_pool.put_buffer(buf)
		}
		numa_times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('NUMA Buffer Pool', numa_times, s.config.data_size)

	// Record pool benchmark (object pool)
	mut obj_times := []i64{cap: s.config.iterations}

	mut rec_pool := core.new_record_pool(100)

	for _ in 0 .. s.config.iterations {
		start := time.now()
		rec := rec_pool.get()
		rec_pool.put(rec)
		obj_times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Record Pool', obj_times, s.config.data_size)
}

// Result calculation and formatting

/// calculate_io_results calculates I/O benchmark results from timing measurements.
fn calculate_io_results(name string, times []i64, data_size usize) IoBenchmarkResults {
	if times.len == 0 {
		return IoBenchmarkResults{
			test_name: name
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
	throughput := if avg > 0 {
		f64(data_size) / (f64(avg) / 1_000_000_000.0) / (1024 * 1024)
	} else {
		0.0
	}
	ops := if avg > 0 { 1_000_000_000.0 / f64(avg) } else { 0.0 }

	return IoBenchmarkResults{
		test_name:       name
		iterations:      times.len
		total_time_ns:   total
		avg_time_ns:     avg
		min_time_ns:     min_t
		max_time_ns:     max_t
		throughput_mbps: throughput
		ops_per_sec:     ops
		data_size:       data_size
	}
}

/// format_results formats results according to the output format.
pub fn (s &IoBenchmarkSuite) format_results() string {
	match s.config.output_format {
		.text { return s.format_text() }
		.json { return s.format_json() }
		.markdown { return s.format_markdown() }
	}
}

/// format_text formats results as text.
fn (s &IoBenchmarkSuite) format_text() string {
	mut sb := []string{}

	sb << '=================================='
	sb << 'DataCore I/O Benchmark Results'
	sb << '=================================='
	sb << ''
	sb << 'System Information:'
	sb << '  OS: ${s.system.os_name}'
	sb << '  NUMA Nodes: ${s.system.numa_nodes}'
	sb << '  NUMA Available: ${s.system.numa_available}'
	sb << '  io_uring Available: ${s.system.io_uring_available}'
	sb << '  DMA Available: ${s.system.dma_available}'
	sb << ''
	sb << 'Benchmark Configuration:'
	sb << '  Iterations: ${s.config.iterations}'
	sb << '  Data Size: ${s.config.data_size} bytes'
	sb << ''
	sb << 'Results:'
	sb << '------------------------------------------------------------'
	sb << '${pad_right('Test Name', 25)} | ${pad_right('Avg (ns)', 12)} | ${pad_right('Throughput',
		12)} | Ops/sec'
	sb << '------------------------------------------------------------'

	for r in s.results {
		if r.iterations > 0 {
			throughput_str := '${r.throughput_mbps:.2f} MB/s'
			ops_str := '${r.ops_per_sec:.0f}'
			sb << '${pad_right(r.test_name, 25)} | ${pad_right(r.avg_time_ns.str(), 12)} | ${pad_right(throughput_str,
				12)} | ${ops_str}'
		} else {
			sb << '${r.test_name}'
		}
	}

	sb << '------------------------------------------------------------'
	return sb.join('\n')
}

/// format_markdown formats results as markdown.
fn (s &IoBenchmarkSuite) format_markdown() string {
	mut sb := []string{}

	sb << '# DataCore I/O Benchmark Results'
	sb << ''
	sb << '## System Information'
	sb << ''
	sb << '| Property | Value |'
	sb << '|----------|-------|'
	sb << '| OS | ${s.system.os_name} |'
	sb << '| NUMA Nodes | ${s.system.numa_nodes} |'
	sb << '| NUMA Available | ${s.system.numa_available} |'
	sb << '| io_uring Available | ${s.system.io_uring_available} |'
	sb << '| DMA Available | ${s.system.dma_available} |'
	sb << ''
	sb << '## Configuration'
	sb << ''
	sb << '- Iterations: ${s.config.iterations}'
	sb << '- Data Size: ${s.config.data_size} bytes'
	sb << ''
	sb << '## Results'
	sb << ''
	sb << '| Test Name | Avg (ns) | Min (ns) | Max (ns) | Throughput | Ops/sec |'
	sb << '|-----------|----------|----------|----------|------------|---------|'

	for r in s.results {
		if r.iterations > 0 {
			sb << '| ${r.test_name} | ${r.avg_time_ns} | ${r.min_time_ns} | ${r.max_time_ns} | ${r.throughput_mbps:.2f} MB/s | ${r.ops_per_sec:.0f} |'
		} else {
			sb << '| ${r.test_name} | - | - | - | - | - |'
		}
	}

	return sb.join('\n')
}

/// format_json formats results as JSON.
fn (s &IoBenchmarkSuite) format_json() string {
	mut sb := []string{}

	sb << '{'
	sb << '  "system": {'
	sb << '    "os": "${s.system.os_name}",'
	sb << '    "numa_nodes": ${s.system.numa_nodes},'
	sb << '    "numa_available": ${s.system.numa_available},'
	sb << '    "io_uring_available": ${s.system.io_uring_available},'
	sb << '    "dma_available": ${s.system.dma_available}'
	sb << '  },'
	sb << '  "config": {'
	sb << '    "iterations": ${s.config.iterations},'
	sb << '    "data_size": ${s.config.data_size}'
	sb << '  },'
	sb << '  "results": ['

	for i, r in s.results {
		comma := if i < s.results.len - 1 { ',' } else { '' }
		sb << '    {'
		sb << '      "test_name": "${r.test_name}",'
		sb << '      "iterations": ${r.iterations},'
		sb << '      "avg_time_ns": ${r.avg_time_ns},'
		sb << '      "min_time_ns": ${r.min_time_ns},'
		sb << '      "max_time_ns": ${r.max_time_ns},'
		sb << '      "throughput_mbps": ${r.throughput_mbps},'
		sb << '      "ops_per_sec": ${r.ops_per_sec}'
		sb << '    }${comma}'
	}

	sb << '  ]'
	sb << '}'

	return sb.join('\n')
}

// Utility functions

/// create_test_file creates a test file of the specified size.
fn create_test_file(size i64) string {
	path := '/tmp/datacore_bench_${time.now().unix()}.dat'

	// Create file with random data
	mut data := []u8{len: int(size)}
	for i in 0 .. data.len {
		data[i] = u8(i % 256)
	}

	os.write_file(path, data.bytestr()) or { return '' }
	return path
}

/// pad_right pads a string to the specified width on the right.
fn pad_right(s string, width int) string {
	if s.len >= width {
		return s
	}
	return s + ' '.repeat(width - s.len)
}

// Quick benchmark functions

/// run_quick_io_benchmark runs a quick benchmark with default settings.
pub fn run_quick_io_benchmark() string {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		iterations:  50
		warmup_runs: 5
		data_size:   4096
	})

	suite.run_all()
	return suite.format_results()
}

/// run_comprehensive_io_benchmark runs a comprehensive benchmark.
pub fn run_comprehensive_io_benchmark() string {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		iterations:    200
		warmup_runs:   20
		data_size:     65536
		file_size:     10 * 1024 * 1024
		verbose:       true
		output_format: .markdown
	})

	suite.run_all()
	return suite.format_results()
}

/// compare_io_methods returns comparison results for various I/O methods.
pub fn compare_io_methods(data_size usize, iterations int) []IoBenchmarkResults {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		iterations:        iterations
		data_size:         data_size
		test_numa:         false
		test_buffer_pools: false
	})

	suite.run_all()
	return suite.results
}

/// compare_memory_strategies returns comparison results for memory allocation strategies.
pub fn compare_memory_strategies(size usize, iterations int) []IoBenchmarkResults {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		iterations:      iterations
		data_size:       size
		test_regular_io: false
		test_mmap:       false
		test_dma:        false
		test_io_uring:   false
	})

	suite.run_all()
	return suite.results
}
