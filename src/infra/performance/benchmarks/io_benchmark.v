module benchmarks

/// 종합 I/O 벤치마크 스위트
/// 다양한 I/O 전략의 성능을 비교합니다:
/// - 일반 파일 I/O
/// - 메모리 매핑 I/O (mmap)
/// - DMA / Scatter-Gather I/O
/// - io_uring (Linux)
/// - NUMA 인식 할당
import os
import time
import infra.performance.engines
import infra.performance.io
import infra.performance.core

// I/O 벤치마크 설정

/// IoBenchmarkConfig는 I/O 벤치마크 설정을 정의합니다.
pub struct IoBenchmarkConfig {
pub:
	// 테스트 매개변수
	iterations  int   = 100         // 반복 횟수
	warmup_runs int   = 10          // 워밍업 실행 횟수
	data_size   usize = 4096        // 데이터 크기 (바이트)
	file_size   i64   = 1024 * 1024 // 파일 테스트용 크기 (1MB)

	// 테스트 선택
	test_regular_io   bool = true // 일반 I/O 테스트
	test_mmap         bool = true // mmap 테스트
	test_dma          bool = true // DMA 테스트
	test_io_uring     bool = true // io_uring 테스트
	test_numa         bool = true // NUMA 테스트
	test_buffer_pools bool = true // 버퍼 풀 테스트

	// 출력
	verbose       bool // 상세 출력
	output_format OutputFormat = .text // 출력 형식
}

/// OutputFormat은 벤치마크 결과 출력 형식을 정의합니다.
pub enum OutputFormat {
	text     // 텍스트 형식
	json     // JSON 형식
	markdown // 마크다운 형식
}

// 벤치마크 결과

/// IoBenchmarkResults는 I/O 벤치마크 결과를 저장합니다.
pub struct IoBenchmarkResults {
pub mut:
	test_name       string // 테스트 이름
	iterations      int    // 반복 횟수
	total_time_ns   i64
	avg_time_ns     i64
	min_time_ns     i64
	max_time_ns     i64
	throughput_mbps f64   // 처리량 (MB/s)
	ops_per_sec     f64   // 초당 작업 수
	data_size       usize // 데이터 크기
}

/// IoBenchmarkSuite는 I/O 벤치마크 스위트입니다.
pub struct IoBenchmarkSuite {
pub mut:
	config  IoBenchmarkConfig    // 벤치마크 설정
	results []IoBenchmarkResults // 벤치마크 결과 목록
	system  SystemInfo           // 시스템 정보
}

/// SystemInfo는 시스템 정보를 저장합니다.
pub struct SystemInfo {
pub:
	os_name            string // 운영체제 이름
	numa_nodes         int    // NUMA 노드 수
	numa_available     bool   // NUMA 사용 가능 여부
	io_uring_available bool   // io_uring 사용 가능 여부
	mmap_available     bool   // mmap 사용 가능 여부
	dma_available      bool   // DMA 사용 가능 여부
	total_memory       i64    // 총 메모리
}

// 벤치마크 러너

/// new_io_benchmark_suite는 새로운 벤치마크 스위트를 생성합니다.
pub fn new_io_benchmark_suite(config IoBenchmarkConfig) IoBenchmarkSuite {
	return IoBenchmarkSuite{
		config:  config
		results: []IoBenchmarkResults{}
		system:  detect_system_capabilities()
	}
}

/// detect_system_capabilities는 시스템 기능을 감지합니다.
fn detect_system_capabilities() SystemInfo {
	topology := engines.get_numa_topology()
	io_caps := engines.get_async_io_capabilities()
	dma_caps := io.get_platform_capabilities()

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
		mmap_available:     true // 폴백을 통해 항상 사용 가능
		dma_available:      dma_caps.has_scatter_gather
		total_memory:       total_mem
	}
}

/// run_all은 설정된 모든 벤치마크를 실행합니다.
pub fn (mut s IoBenchmarkSuite) run_all() {
	if s.config.verbose {
		println('Starting benchmark suite...')
		println('System: ${s.system.os_name}, NUMA nodes: ${s.system.numa_nodes}')
	}

	// 테스트 파일 생성
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

// 개별 벤치마크

/// bench_regular_read는 일반 파일 읽기를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_regular_read(path string) {
	mut times := []i64{cap: s.config.iterations}

	// 워밍업
	for _ in 0 .. s.config.warmup_runs {
		os.read_file(path) or { continue }
	}

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		_ = os.read_file(path) or { '' }
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Regular Read', times, s.config.data_size)
}

/// bench_regular_write는 일반 파일 쓰기를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_regular_write(path string) {
	mut times := []i64{cap: s.config.iterations}
	data := []u8{len: int(s.config.data_size), init: u8(index % 256)}
	test_path := '${path}.write_test'

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		os.write_file(test_path, data.bytestr()) or { continue }
		times << time.since(start).nanoseconds()
	}

	os.rm(test_path) or {}
	s.results << calculate_io_results('Regular Write', times, s.config.data_size)
}

/// bench_mmap_read는 메모리 매핑 읽기를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_mmap_read(path string) {
	mut times := []i64{cap: s.config.iterations}

	// 워밍업
	for _ in 0 .. s.config.warmup_runs {
		if mut mm := io.MmapFile.open(path, true) {
			if region := mm.map_region(0, int(s.config.data_size)) {
				mm.unmap_region(region) or {}
			}
			mm.close() or {}
		}
	}

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		if mut mm := io.MmapFile.open(path, true) {
			if region := mm.map_region(0, int(s.config.data_size)) {
				mm.unmap_region(region) or {}
			}
			mm.close() or {}
		}
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Mmap Read', times, s.config.data_size)
}

/// bench_mmap_write는 메모리 매핑 쓰기를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_mmap_write(path string) {
	mut times := []i64{cap: s.config.iterations}
	test_path := '${path}.mmap_write'

	// 먼저 파일 생성
	os.write_file(test_path, []u8{len: int(s.config.file_size)}.bytestr()) or { return }

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		if mut mm := io.MmapFile.open(test_path, false) {
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

/// bench_dma_read는 DMA scatter 읽기를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_dma_read(path string) {
	mut times := []i64{cap: s.config.iterations}
	caps := io.get_platform_capabilities()

	if !caps.has_scatter_gather {
		s.results << IoBenchmarkResults{
			test_name: 'DMA Read (skipped - not available)'
		}
		return
	}

	// scatter 버퍼 준비
	mut bufs := [
		io.new_sg_buffer(1024),
		io.new_sg_buffer(1024),
		io.new_sg_buffer(1024),
		io.new_sg_buffer(1024),
	]

	mut file := os.open_file(path, 'r') or { return }
	defer {
		file.close()
	}

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		_ = io.scatter_read_native(file.fd, mut bufs)
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('DMA Scatter Read', times, s.config.data_size)
}

/// bench_dma_write는 DMA gather 쓰기를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_dma_write(path string) {
	mut times := []i64{cap: s.config.iterations}
	caps := io.get_platform_capabilities()
	test_path := '${path}.dma_write'

	if !caps.has_scatter_gather {
		s.results << IoBenchmarkResults{
			test_name: 'DMA Write (skipped - not available)'
		}
		return
	}

	// gather 버퍼 준비
	bufs := [
		io.new_sg_buffer_from([]u8{len: 1024, init: 0xAA}),
		io.new_sg_buffer_from([]u8{len: 1024, init: 0xBB}),
		io.new_sg_buffer_from([]u8{len: 1024, init: 0xCC}),
		io.new_sg_buffer_from([]u8{len: 1024, init: 0xDD}),
	]

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		mut fd := os.open_file(test_path, 'w') or { continue }

		start := time.now()
		_ = io.gather_write_native(fd.fd, bufs)
		times << time.since(start).nanoseconds()

		fd.close()
	}

	os.rm(test_path) or {}
	s.results << calculate_io_results('DMA Gather Write', times, s.config.data_size)
}

/// bench_io_uring은 io_uring 비동기 I/O를 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_io_uring(path string) {
	mut times := []i64{cap: s.config.iterations}

	// 먼저 io_uring 기능 확인
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

	// 비동기 읽기 벤치마크
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

/// bench_numa_allocation은 NUMA 메모리 할당을 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_numa_allocation() {
	mut times := []i64{cap: s.config.iterations}
	size := s.config.data_size

	// NUMA 로컬 할당 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		mem := engines.numa_alloc_local(size)
		// 메모리가 실제로 할당되었는지 확인하기 위해 터치
		unsafe {
			C.memset(mem.ptr, 0, size)
		}
		engines.numa_free(mem)
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('NUMA Local Alloc', times, size)

	// 인터리브 할당 벤치마크
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

/// bench_buffer_pools는 버퍼 풀 성능을 벤치마크합니다.
fn (mut s IoBenchmarkSuite) bench_buffer_pools() {
	// 버퍼 풀 벤치마크
	mut times := []i64{cap: s.config.iterations}

	mut pool := core.new_buffer_pool(core.PoolConfig{})

	// 워밍업
	for _ in 0 .. s.config.warmup_runs {
		buf := pool.get(int(s.config.data_size))
		pool.put(buf)
	}

	// 벤치마크
	for _ in 0 .. s.config.iterations {
		start := time.now()
		buf := pool.get(int(s.config.data_size))
		// 작업 시뮬레이션
		unsafe {
			if buf.data.len > 0 {
				C.memset(buf.data.data, 0, usize(buf.data.len))
			}
		}
		pool.put(buf)
		times << time.since(start).nanoseconds()
	}

	s.results << calculate_io_results('Buffer Pool Get/Put', times, s.config.data_size)

	// NUMA 버퍼 풀 벤치마크
	mut numa_times := []i64{cap: s.config.iterations}

	mut numa_pool := engines.new_numa_buffer_pool(engines.NumaBufferConfig{
		buffer_size:      s.config.data_size
		buffers_per_node: 100
	})
	defer {
		numa_pool.close()
	}

	// 벤치마크
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

	// 레코드 풀 벤치마크 (객체 풀)
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

// 결과 계산 및 포맷팅

/// calculate_io_results는 시간 측정값으로부터 I/O 벤치마크 결과를 계산합니다.
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

/// format_results는 출력 형식에 따라 결과를 포맷합니다.
pub fn (s &IoBenchmarkSuite) format_results() string {
	match s.config.output_format {
		.text { return s.format_text() }
		.json { return s.format_json() }
		.markdown { return s.format_markdown() }
	}
}

/// format_text는 결과를 텍스트 형식으로 포맷합니다.
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

/// format_markdown은 결과를 마크다운 형식으로 포맷합니다.
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

/// format_json은 결과를 JSON 형식으로 포맷합니다.
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

// 유틸리티 함수

/// create_test_file은 지정된 크기의 테스트 파일을 생성합니다.
fn create_test_file(size i64) string {
	path := '/tmp/datacore_bench_${time.now().unix()}.dat'

	// 랜덤 데이터로 파일 생성
	mut data := []u8{len: int(size)}
	for i in 0 .. data.len {
		data[i] = u8(i % 256)
	}

	os.write_file(path, data.bytestr()) or { return '' }
	return path
}

/// pad_right는 문자열을 지정된 너비로 오른쪽 패딩합니다.
fn pad_right(s string, width int) string {
	if s.len >= width {
		return s
	}
	return s + ' '.repeat(width - s.len)
}

// 빠른 벤치마크 함수

/// run_quick_io_benchmark는 기본 설정으로 빠른 벤치마크를 실행합니다.
pub fn run_quick_io_benchmark() string {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		iterations:  50
		warmup_runs: 5
		data_size:   4096
	})

	suite.run_all()
	return suite.format_results()
}

/// run_comprehensive_io_benchmark는 종합 벤치마크를 실행합니다.
pub fn run_comprehensive_io_benchmark() string {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		iterations:    200
		warmup_runs:   20
		data_size:     65536            // 64KB
		file_size:     10 * 1024 * 1024 // 10MB
		verbose:       true
		output_format: .markdown
	})

	suite.run_all()
	return suite.format_results()
}

/// compare_io_methods는 다양한 I/O 방법의 비교 결과를 반환합니다.
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

/// compare_memory_strategies는 메모리 할당 전략 비교 결과를 반환합니다.
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
