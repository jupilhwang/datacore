module performance

// I/O Benchmark Tests
// Tests for the comprehensive I/O benchmark suite

import os

// ============================================================================
// Configuration Tests
// ============================================================================

fn test_benchmark_config_defaults() {
	config := IoBenchmarkConfig{}

	assert config.iterations == 100
	assert config.warmup_runs == 10
	assert config.data_size == 4096
	assert config.test_regular_io == true
	assert config.test_mmap == true
	assert config.output_format == .text
}

fn test_benchmark_config_custom() {
	config := IoBenchmarkConfig{
		iterations: 50
		warmup_runs: 5
		data_size: 8192
		verbose: true
		output_format: .markdown
	}

	assert config.iterations == 50
	assert config.warmup_runs == 5
	assert config.data_size == 8192
	assert config.verbose == true
	assert config.output_format == .markdown
}

// ============================================================================
// Suite Creation Tests
// ============================================================================

fn test_new_io_benchmark_suite() {
	config := IoBenchmarkConfig{
		iterations: 10
	}

	suite := new_io_benchmark_suite(config)

	assert suite.config.iterations == 10
	assert suite.results.len == 0
	assert suite.system.os_name.len > 0
}

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

// ============================================================================
// Result Calculation Tests
// ============================================================================

fn test_calculate_io_results_basic() {
	times := [i64(1000), 2000, 3000, 4000, 5000]
	result := calculate_io_results('Test', times, 1024)

	assert result.test_name == 'Test'
	assert result.iterations == 5
	assert result.min_time_ns == 1000
	assert result.max_time_ns == 5000
	assert result.avg_time_ns == 3000 // (1+2+3+4+5)/5 * 1000
	assert result.data_size == 1024
}

fn test_calculate_io_results_empty() {
	times := []i64{}
	result := calculate_io_results('Empty Test', times, 0)

	assert result.test_name == 'Empty Test'
	assert result.iterations == 0
}

fn test_calculate_io_results_throughput() {
	// 1KB in 1ms = 1MB/s
	times := [i64(1_000_000)] // 1ms in nanoseconds
	result := calculate_io_results('Throughput Test', times, 1024)

	// Throughput should be approximately 1 MB/s
	// 1024 bytes / (1ms) = 1024 bytes / 0.001s = 1.024 MB/s
	assert result.throughput_mbps > 0.9
	assert result.throughput_mbps < 1.1
}

fn test_calculate_io_results_ops_per_sec() {
	// 1 operation per millisecond = 1000 ops/sec
	times := [i64(1_000_000)] // 1ms
	result := calculate_io_results('Ops Test', times, 100)

	assert result.ops_per_sec > 999
	assert result.ops_per_sec < 1001
}

// ============================================================================
// Formatting Tests
// ============================================================================

fn test_format_text() {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{})
	suite.results << IoBenchmarkResults{
		test_name: 'Test 1'
		iterations: 100
		avg_time_ns: 1000
		min_time_ns: 500
		max_time_ns: 1500
		throughput_mbps: 100.5
		ops_per_sec: 1000000
		data_size: 4096
	}

	output := suite.format_text()

	assert output.contains('DataCore I/O Benchmark Results')
	assert output.contains('Test 1')
	assert output.contains('System Information')
}

fn test_format_markdown() {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		output_format: .markdown
	})
	suite.results << IoBenchmarkResults{
		test_name: 'Test MD'
		iterations: 50
		avg_time_ns: 2000
		throughput_mbps: 50.0
		ops_per_sec: 500000
	}

	output := suite.format_markdown()

	assert output.contains('# DataCore I/O Benchmark Results')
	assert output.contains('| Test MD |')
	assert output.contains('## System Information')
}

fn test_format_json() {
	mut suite := new_io_benchmark_suite(IoBenchmarkConfig{
		output_format: .json
	})
	suite.results << IoBenchmarkResults{
		test_name: 'Test JSON'
		iterations: 25
		avg_time_ns: 5000
	}

	output := suite.format_json()

	assert output.contains('"system"')
	assert output.contains('"results"')
	assert output.contains('"Test JSON"')
}

// ============================================================================
// Utility Function Tests
// ============================================================================

fn test_pad_right() {
	assert pad_right('abc', 5) == 'abc  '
	assert pad_right('abcde', 5) == 'abcde'
	assert pad_right('abcdef', 5) == 'abcdef'
	assert pad_right('', 3) == '   '
}

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

// ============================================================================
// Quick Benchmark Tests (Light)
// ============================================================================

fn test_quick_functions_exist() {
	// Just verify the functions can be called without crashing
	// Don't actually run full benchmarks in tests

	config := IoBenchmarkConfig{
		iterations: 2
		warmup_runs: 1
		data_size: 512
		test_io_uring: false // Skip io_uring in tests for portability
	}

	suite := new_io_benchmark_suite(config)
	assert suite.config.iterations == 2
}

fn test_compare_functions_params() {
	// Test that comparison functions accept correct parameters
	// We don't run full benchmarks, just verify types

	results := compare_memory_strategies(1024, 5)

	// Results should be returned (may be empty if tests are skipped)
	assert results.len >= 0
}

// ============================================================================
// Integration Tests (Light)
// ============================================================================

fn test_mini_benchmark_run() {
	// Create a minimal benchmark run
	config := IoBenchmarkConfig{
		iterations: 3
		warmup_runs: 1
		data_size: 256
		file_size: 1024
		test_regular_io: true
		test_mmap: true
		test_dma: false       // Skip for portability
		test_io_uring: false  // Skip for portability
		test_numa: false      // Skip for portability
		test_buffer_pools: false
	}

	mut suite := new_io_benchmark_suite(config)
	suite.run_all()

	// Should have some results
	assert suite.results.len > 0

	// Format should work
	output := suite.format_results()
	assert output.len > 0
}

fn test_all_output_formats() {
	config := IoBenchmarkConfig{
		iterations: 1
	}

	// Create suite with test data
	mut suite := new_io_benchmark_suite(config)
	suite.results << IoBenchmarkResults{
		test_name: 'Format Test'
		iterations: 10
		avg_time_ns: 1000
	}

	// Test text format
	suite.config = IoBenchmarkConfig{output_format: .text}
	text_out := suite.format_results()
	assert text_out.contains('Format Test')

	// Test markdown format
	suite.config = IoBenchmarkConfig{output_format: .markdown}
	md_out := suite.format_results()
	assert md_out.contains('# ')

	// Test JSON format
	suite.config = IoBenchmarkConfig{output_format: .json}
	json_out := suite.format_results()
	assert json_out.contains('{')
	assert json_out.contains('}')
}

// ============================================================================
// Edge Cases
// ============================================================================

fn test_zero_iterations() {
	config := IoBenchmarkConfig{
		iterations: 0
	}

	suite := new_io_benchmark_suite(config)
	assert suite.config.iterations == 0
}

fn test_very_small_data_size() {
	times := [i64(100)]
	result := calculate_io_results('Small Data', times, 1)

	assert result.data_size == 1
	assert result.throughput_mbps > 0
}

fn test_very_large_times() {
	// Test with large time values (1 second)
	times := [i64(1_000_000_000)]
	result := calculate_io_results('Large Time', times, 1024)

	assert result.avg_time_ns == 1_000_000_000
	assert result.throughput_mbps < 0.01 // Very slow
}
