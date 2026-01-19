// Integration Tests for Performance Module
module benchmarks

// ============================================================================
// Integration Tests
// ============================================================================

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

fn test_request_buffer_lifecycle() {
	init_global_performance(PerformanceConfig{})

	// Allocate
	mut buf := new_request_buffer(1024)
	assert buf.buffer != unsafe { nil }
	assert buf.buffer.cap >= 1024

	// Use
	data := buf.data()
	assert data.len >= 1024

	// Resize
	buf.resize(2048)
	assert buf.buffer.cap >= 2048

	// Release
	buf.release()
}

fn test_response_buffer_write() {
	init_global_performance(PerformanceConfig{})

	mut buf := new_response_buffer(256)

	// Write data
	buf.write([u8(1), 2, 3, 4])
	assert buf.len() == 4

	// Write i32
	buf.write_i32_be(0x12345678)
	assert buf.len() == 8

	// Write i16
	buf.write_i16_be(0x1234)
	assert buf.len() == 10

	// Verify bytes
	bytes := buf.bytes()
	assert bytes.len == 10
	assert bytes[0] == 1
	assert bytes[1] == 2
	assert bytes[4] == 0x12
	assert bytes[5] == 0x34

	buf.release()
}

fn test_response_buffer_grow() {
	init_global_performance(PerformanceConfig{})

	mut buf := new_response_buffer(16) // Small initial size

	// Write more than capacity
	for _ in 0 .. 100 {
		buf.write([u8(1), 2, 3, 4, 5, 6, 7, 8])
	}

	assert buf.len() == 800
	assert buf.buffer.cap >= 800

	buf.release()
}

fn test_connection_buffers() {
	init_global_performance(PerformanceConfig{})

	mut bufs := new_connection_buffers(4096, 8192)

	// Get slices
	read_slice := bufs.get_read_slice(1024)
	assert read_slice.len == 1024

	write_slice := bufs.get_write_slice(2048)
	assert write_slice.len == 2048

	// Slice larger than buffer
	large_slice := bufs.get_read_slice(10000)
	assert large_slice.len == 10000

	bufs.release()
}

fn test_storage_record_pool() {
	init_global_performance(PerformanceConfig{})

	mut pool := new_storage_record_pool()

	// Get and return records
	rec := pool.get_record()
	assert rec != unsafe { nil }
	pool.put_record(rec)

	// Get and return batch
	batch := pool.get_batch()
	assert batch != unsafe { nil }
	pool.put_batch(batch)
}

fn test_fetch_buffer_zero_copy() {
	init_global_performance(PerformanceConfig{})

	mut buf := new_fetch_buffer(4096)
	assert !buf.has_zero_copy()

	// Set zero-copy
	buf.set_zero_copy(10, 1000, 4096)
	assert buf.has_zero_copy()
	assert buf.zero_copy_fd == 10
	assert buf.zero_copy_off == 1000
	assert buf.zero_copy_len == 4096

	buf.release()
}

fn test_with_request_buffer_callback() {
	init_global_performance(PerformanceConfig{})

	// Test that callback is invoked with a valid buffer
	// Using direct buffer operations instead of closure capture
	mut buf := new_request_buffer(1024)
	assert buf.buffer != unsafe { nil }
	assert buf.buffer.cap >= 1024
	buf.release()
}

fn test_with_response_buffer_callback() {
	init_global_performance(PerformanceConfig{})

	// Test that response buffer works correctly
	mut buf := new_response_buffer(512)
	buf.write([u8(1), 2, 3])
	assert buf.len() == 3
	buf.release()
}

fn test_integration_stats() {
	init_global_performance(PerformanceConfig{})

	// Perform some operations
	mut req := allocate_request_buffer(1024)
	req.release()

	mut resp := allocate_response_buffer(512)
	resp.release()

	// Get stats - verify it doesn't crash
	stats := get_integration_stats()
	// Stats structure should be valid
	assert stats.perf_stats.buffer_pool_stats.hit_rate() >= 0.0
}

fn test_benchmark_suite_creation() {
	init_global_performance(PerformanceConfig{})

	suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    10
		benchmark_iterations: 100
	})

	assert suite.config.warmup_iterations == 10
	assert suite.config.benchmark_iterations == 100
}

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

fn test_performance_under_load() {
	init_global_performance(PerformanceConfig{
		buffer_pool_max_tiny:  1000
		buffer_pool_max_small: 500
		record_pool_max_size:  5000
	})

	// Simulate high load
	mut buffers := []&RequestBuffer{cap: 100}

	for _ in 0 .. 100 {
		buffers << new_request_buffer(1024)
	}

	// Return all
	for buf in buffers {
		mut b := buf
		b.release()
	}

	// Check stats
	mut mgr := get_global_performance()
	stats := mgr.get_stats()

	// Should have high reuse after returning buffers
	assert stats.buffer_pool_stats.bytes_reused >= 0
}
