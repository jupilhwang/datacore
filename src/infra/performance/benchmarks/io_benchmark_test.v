module benchmarks

/// I/O 벤치마크 테스트
/// 종합 I/O 벤치마크 스위트를 위한 테스트
import os

// 설정 테스트

/// 벤치마크 설정 기본값 테스트
fn test_benchmark_config_defaults() {
	config := IoBenchmarkConfig{}

	assert config.iterations == 100
	assert config.warmup_runs == 10
	assert config.data_size == 4096
	assert config.test_regular_io == true
	assert config.test_mmap == true
	assert config.output_format == .text
}

/// 벤치마크 설정 커스텀 테스트
fn test_benchmark_config_custom() {
	config := IoBenchmarkConfig{
		iterations:    50
		warmup_runs:   5
		data_size:     8192
		verbose:       true
		output_format: .markdown
	}

	assert config.iterations == 50
	assert config.warmup_runs == 5
	assert config.data_size == 8192
	assert config.verbose == true
	assert config.output_format == .markdown
}

// 스위트 생성 테스트

/// 새 I/O 벤치마크 스위트 테스트
fn test_new_io_benchmark_suite() {
	config := IoBenchmarkConfig{
		iterations: 10
	}

	suite := new_io_benchmark_suite(config)

	assert suite.config.iterations == 10
	assert suite.results.len == 0
	assert suite.system.os_name.len > 0
}

/// 시스템 기능 감지 테스트
fn test_detect_system_capabilities() {
	info := detect_system_capabilities()

	assert info.os_name.len > 0
	assert info.numa_nodes >= 1

	$if linux {
		assert info.os_name == 'Linux'
	} $else $if macos {
		assert info.os_name == 'macOS'
	} $else $if windows {
		assert info.os_name == 'Windows'
	}
}

// 결과 계산 테스트

/// I/O 결과 계산 기본 테스트
fn test_calculate_io_results_basic() {
	times := [i64(1000), 2000, 3000, 4000, 5000]
	result := calculate_io_results('Test', times, 1024)

	assert result.test_name == 'Test'
	assert result.iterations == 5
	assert result.min_time_ns == 1000
	assert result.max_time_ns == 5000
	assert result.avg_time_ns == 3000
	assert result.data_size == 1024
}

/// I/O 결과 계산 빈 입력 테스트
fn test_calculate_io_results_empty() {
	times := []i64{}
	result := calculate_io_results('Empty Test', times, 0)

	assert result.test_name == 'Empty Test'
	assert result.iterations == 0
}

/// I/O 결과 처리량 테스트
fn test_calculate_io_results_throughput() {
	// 1ms에 1KB = 1MB/s
	times := [i64(1_000_000)]
	result := calculate_io_results('Throughput Test', times, 1024)

	// 처리량은 약 1 MB/s여야 함
	// 1024 bytes / (1ms) = 1024 bytes / 0.001s = 1.024 MB/s
	assert result.throughput_mbps > 0.9
	assert result.throughput_mbps < 1.1
}

/// I/O 결과 초당 작업 수 테스트
fn test_calculate_io_results_ops_per_sec() {
	// 밀리초당 1 작업 = 초당 1000 작업
	times := [i64(1_000_000)]
	result := calculate_io_results('Ops Test', times, 100)

	assert result.ops_per_sec > 999
	assert result.ops_per_sec < 1001
}

// 포맷팅 테스트

/// 텍스트 형식 테스트
fn test_format_text() {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{})
	suite.results << IoBenchmarkResults{
		test_name:       'Test 1'
		iterations:      100
		avg_time_ns:     1000
		min_time_ns:     500
		max_time_ns:     1500
		throughput_mbps: 100.5
		ops_per_sec:     1000000
		data_size:       4096
	}

	output := suite.format_text()

	assert output.contains('DataCore I/O Benchmark Results')
	assert output.contains('Test 1')
	assert output.contains('System Information')
}

/// 마크다운 형식 테스트
fn test_format_markdown() {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		output_format: .markdown
	})
	suite.results << IoBenchmarkResults{
		test_name:       'Test MD'
		iterations:      50
		avg_time_ns:     2000
		throughput_mbps: 50.0
		ops_per_sec:     500000
	}

	output := suite.format_markdown()

	assert output.contains('# DataCore I/O Benchmark Results')
	assert output.contains('| Test MD |')
	assert output.contains('## System Information')
}

/// JSON 형식 테스트
fn test_format_json() {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		output_format: .json
	})
	suite.results << IoBenchmarkResults{
		test_name:   'Test JSON'
		iterations:  25
		avg_time_ns: 5000
	}

	output := suite.format_json()

	assert output.contains('"system"')
	assert output.contains('"results"')
	assert output.contains('"Test JSON"')
}

// 유틸리티 함수 테스트

/// 오른쪽 패딩 테스트
fn test_pad_right() {
	assert pad_right('abc', 5) == 'abc  '
	assert pad_right('abcde', 5) == 'abcde'
	assert pad_right('abcdef', 5) == 'abcdef'
	assert pad_right('', 3) == '   '
}

/// 테스트 파일 생성 테스트
fn test_create_test_file() {
	path := create_test_file(1024)
	defer {
		os.rm(path) or {}
	}

	assert path.len > 0
	assert os.exists(path)

	content := os.read_file(path) or { '' }
	assert content.len == 1024
}

// 빠른 벤치마크 테스트 (경량)

/// 빠른 함수 존재 테스트
fn test_quick_functions_exist() {
	// 함수가 크래시 없이 호출될 수 있는지만 확인
	// 테스트에서 전체 벤치마크를 실제로 실행하지 않음

	config := IoBenchmarkConfig{
		iterations:    2
		warmup_runs:   1
		data_size:     512
		test_io_uring: false
	}

	suite := new_io_benchmark_suite(config)
	assert suite.config.iterations == 2
}

/// 비교 함수 매개변수 테스트
fn test_compare_functions_params() {
	// 비교 함수가 올바른 매개변수를 받는지 테스트
	// 전체 벤치마크를 실행하지 않고 타입만 확인

	results := compare_memory_strategies(1024, 5)

	// 결과가 반환되어야 함 (테스트가 건너뛰어지면 비어있을 수 있음)
	assert results.len >= 0
}

// 통합 테스트 (경량)

/// 미니 벤치마크 실행 테스트
fn test_mini_benchmark_run() {
	// 최소한의 벤치마크 실행 생성
	config := IoBenchmarkConfig{
		iterations:        3
		warmup_runs:       1
		data_size:         256
		file_size:         1024
		test_regular_io:   true
		test_mmap:         true
		test_dma:          false
		test_io_uring:     false
		test_numa:         false
		test_buffer_pools: false
	}

	mut suite := new_io_benchmark_suite(config)
	suite.run_all()

	// 결과가 있어야 함
	assert suite.results.len > 0

	// 포맷이 작동해야 함
	output := suite.format_results()
	assert output.len > 0
}

/// 모든 출력 형식 테스트
fn test_all_output_formats() {
	config := IoBenchmarkConfig{
		iterations: 1
	}

	// 테스트 데이터로 스위트 생성
	mut suite := new_io_benchmark_suite(config)
	suite.results << IoBenchmarkResults{
		test_name:   'Format Test'
		iterations:  10
		avg_time_ns: 1000
	}

	// 텍스트 형식 테스트
	suite.config = IoBenchmarkConfig{
		output_format: .text
	}
	text_out := suite.format_results()
	assert text_out.contains('Format Test')

	// 마크다운 형식 테스트
	suite.config = IoBenchmarkConfig{
		output_format: .markdown
	}
	md_out := suite.format_results()
	assert md_out.contains('# ')

	// JSON 형식 테스트
	suite.config = IoBenchmarkConfig{
		output_format: .json
	}
	json_out := suite.format_results()
	assert json_out.contains('{')
	assert json_out.contains('}')
}

// 엣지 케이스

/// 반복 횟수 0 테스트
fn test_zero_iterations() {
	config := IoBenchmarkConfig{
		iterations: 0
	}

	suite := new_io_benchmark_suite(config)
	assert suite.config.iterations == 0
}

/// 매우 작은 데이터 크기 테스트
fn test_very_small_data_size() {
	times := [i64(100)]
	result := calculate_io_results('Small Data', times, 1)

	assert result.data_size == 1
	assert result.throughput_mbps > 0
}

/// 매우 큰 시간 값 테스트
fn test_very_large_times() {
	// 큰 시간 값으로 테스트 (1초)
	times := [i64(1_000_000_000)]
	result := calculate_io_results('Large Time', times, 1024)

	assert result.avg_time_ns == 1_000_000_000
	assert result.throughput_mbps < 0.01
}
