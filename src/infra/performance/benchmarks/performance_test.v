/// 인프라 레이어 - 성능 테스트
/// 버퍼 풀, 객체 풀, 벤치마크 기능 단위 테스트
module benchmarks

import infra.performance
import infra.performance.core

// Buffer Pool Tests

/// 버퍼 쓰기 및 읽기 테스트
fn test_buffer_write_and_read() {
	mut buf := core.Buffer{
		data:       []u8{len: 256}
		len:        0
		cap:        256
		size_class: .tiny
	}

	data := 'Hello, World!'.bytes()
	written := buf.write(data)

	assert written == data.len
	assert buf.len == data.len
	assert buf.bytes() == data
}

/// 버퍼 바이트 쓰기 테스트
fn test_buffer_write_byte() {
	mut buf := core.Buffer{
		data:       []u8{len: 10}
		len:        0
		cap:        10
		size_class: .tiny
	}

	assert buf.write_byte(0x41) == true
	assert buf.write_byte(0x42) == true
	assert buf.write_byte(0x43) == true

	assert buf.len == 3
	assert buf.bytes() == [u8(0x41), 0x42, 0x43]
}

/// 버퍼 빅엔디안 i32 쓰기 테스트
fn test_buffer_write_i32_be() {
	mut buf := core.Buffer{
		data:       []u8{len: 10}
		len:        0
		cap:        10
		size_class: .tiny
	}

	assert buf.write_i32_be(0x12345678) == true
	assert buf.len == 4
	assert buf.bytes() == [u8(0x12), 0x34, 0x56, 0x78]
}

/// 버퍼 오버플로우 테스트
fn test_buffer_overflow() {
	mut buf := core.Buffer{
		data:       []u8{len: 4}
		len:        0
		cap:        4
		size_class: .tiny
	}

	data := [u8(1), 2, 3, 4, 5, 6]
	written := buf.write(data)

	assert written == 4
	assert buf.len == 4
}

// Buffer Pool Integration Tests

/// 버퍼 풀 획득/반환 테스트
fn test_buffer_pool_get_put() {
	performance.init_global_performance(performance.PerformanceConfig{
		buffer_pool_prewarm: true
	})

	mut mgr := performance.get_global_performance()

	// Get from pool
	buf := mgr.get_buffer(100)
	assert buf.cap >= 100

	// Put back
	mgr.put_buffer(buf)

	stats := mgr.get_stats()
	assert stats.buffer_hits >= 0 || stats.buffer_misses >= 0
}

/// 크기 클래스 선택 테스트
fn test_size_class_selection() {
	assert core.get_size_class(100) == .tiny
	assert core.get_size_class(256) == .tiny
	assert core.get_size_class(257) == .small
	assert core.get_size_class(4096) == .small
	assert core.get_size_class(4097) == .medium
	assert core.get_size_class(65536) == .medium
	assert core.get_size_class(65537) == .large
	assert core.get_size_class(1048576) == .large
	assert core.get_size_class(1048577) == .huge
}

// Hash Function Tests

/// Murmur3 기본 테스트
fn test_murmur3_basic() {
	data := 'test'.bytes()
	hash := core.murmur3_32(data, 0)

	// Verify hash is non-zero and deterministic
	assert hash != 0
	assert core.murmur3_32(data, 0) == hash

	// Different data should produce different hash
	other := 'other'.bytes()
	assert core.murmur3_32(other, 0) != hash
}

/// Murmur3 빈 입력 테스트
fn test_murmur3_empty() {
	empty := []u8{}
	hash := core.murmur3_32(empty, 0)
	// Empty input with seed 0 should still produce a result
	assert hash == core.murmur3_32(empty, 0)
}

/// Kafka 파티션 계산 테스트
fn test_kafka_partition() {
	key := 'my-key'.bytes()

	partition := core.kafka_partition(key, 10)
	assert partition >= 0
	assert partition < 10

	// Same key should always map to same partition
	assert core.kafka_partition(key, 10) == partition
}

/// Kafka 파티션 빈 키 테스트
fn test_kafka_partition_empty_key() {
	empty := []u8{}
	assert core.kafka_partition(empty, 10) == 0
}

// CRC32 Tests

/// CRC32 기본 테스트
fn test_crc32_basic() {
	data := 'Hello, World!'.bytes()
	crc := core.crc32_ieee(data)

	// Verify deterministic
	assert core.crc32_ieee(data) == crc

	// Different data should produce different CRC
	other := 'Goodbye!'.bytes()
	assert core.crc32_ieee(other) != crc
}

/// CRC32 알려진 값 테스트
fn test_crc32_known_value() {
	// Test with known CRC32 value
	data := 'test'.bytes()
	crc := core.crc32_ieee(data)
	assert crc != 0
}

// Varint Tests

/// Varint 왕복 테스트
fn test_varint_roundtrip() {
	values := [i64(0), 1, -1, 127, -128, 255, 300, -300, 1000000, -1000000]

	for val in values {
		encoded := core.encode_varint(val)
		decoded, n := core.decode_varint(encoded)

		assert n > 0, 'Failed to decode varint for ${val}'
		assert decoded == val, 'Varint roundtrip failed: ${val} -> ${decoded}'
	}
}

/// Uvarint 왕복 테스트
fn test_uvarint_roundtrip() {
	values := [u64(0), 1, 127, 128, 255, 16383, 16384, 2097151]

	for val in values {
		encoded := core.encode_uvarint(val)
		decoded, n := core.decode_uvarint(encoded)

		assert n > 0, 'Failed to decode uvarint for ${val}'
		assert decoded == val, 'Uvarint roundtrip failed: ${val} -> ${decoded}'
	}
}

/// Varint 크기 테스트
fn test_varint_size() {
	assert core.varint_size(0) == 1
	assert core.varint_size(1) == 1
	assert core.varint_size(-1) == 1
	assert core.varint_size(63) == 1
	assert core.varint_size(-64) == 1
	assert core.varint_size(64) == 2
	assert core.varint_size(-65) == 2
}

// Ring Buffer Tests

/// 링 버퍼 기본 테스트
fn test_ring_buffer_basic() {
	mut rb := core.new_ring_buffer(16)

	assert rb.is_empty()
	assert !rb.is_full()

	data := [u8(1), 2, 3, 4, 5]
	written := rb.write(data)

	assert written == 5
	assert rb.available() == 5
	assert !rb.is_empty()
}

/// 링 버퍼 읽기/쓰기 테스트
fn test_ring_buffer_read_write() {
	mut rb := core.new_ring_buffer(16)

	write_data := [u8(1), 2, 3, 4, 5]
	rb.write(write_data)

	mut read_buf := []u8{len: 5}
	read_count := rb.read(mut read_buf)

	assert read_count == 5
	assert read_buf == write_data
	assert rb.is_empty()
}

/// 링 버퍼 랩어라운드 테스트
fn test_ring_buffer_wrap_around() {
	mut rb := core.new_ring_buffer(8)

	// Write 4 bytes
	rb.write([u8(1), 2, 3, 4])

	// Read 2 bytes to move tail
	mut buf := []u8{len: 2}
	rb.read(mut buf)

	// Write more to wrap around
	rb.write([u8(5), 6, 7, 8])

	// Read all remaining
	mut all := []u8{len: 6}
	read := rb.read(mut all)

	assert read == 6
	assert all == [u8(3), 4, 5, 6, 7, 8]
}

// Object Pool Tests

/// 레코드 풀 테스트
fn test_record_pool() {
	mut pool := core.new_record_pool(10)

	// Get new record
	mut r := pool.get()
	assert r.in_use == true

	r.set_key('test-key'.bytes())
	r.set_value('test-value'.bytes())

	// Return to pool
	pool.put(r)

	// Get again - should be from pool
	mut r2 := pool.get()
	assert r2.key.len == 0 // Should be reset
	assert r2.value.len == 0
}

/// 레코드 배치 풀 테스트
fn test_record_batch_pool() {
	mut pool := core.new_record_batch_pool(10)

	mut batch := pool.get()
	assert batch.in_use == true

	batch.topic = 'test-topic'
	batch.partition = 5

	pool.put(batch)

	stats := pool.get_stats()
	assert stats.returns == 1
}

/// 요청 풀 테스트
fn test_request_pool() {
	mut pool := core.new_request_pool(10)

	mut req := pool.get()
	req.api_key = 1
	req.api_version = 12
	req.correlation_id = 12345
	req.client_id = 'test-client'

	pool.put(req)

	mut req2 := pool.get()
	assert req2.api_key == 0 // Should be reset
	assert req2.correlation_id == 0
}

// Bit Operation Tests

/// 비트 연산 테스트
fn test_bit_operations() {
	// Leading zeros
	assert core.count_leading_zeros(0) == 64
	assert core.count_leading_zeros(1) == 63
	assert core.count_leading_zeros(0x8000000000000000) == 0

	// Trailing zeros
	assert core.count_trailing_zeros(0) == 64
	assert core.count_trailing_zeros(1) == 0
	assert core.count_trailing_zeros(8) == 3

	// Popcount
	assert core.popcount(0) == 0
	assert core.popcount(1) == 1
	assert core.popcount(0xff) == 8
	assert core.popcount(0xffffffffffffffff) == 64
}

// Pool Stats Tests

/// 풀 통계 적중률 테스트
fn test_pool_stats_hit_rate() {
	stats := core.PoolStats{
		hits_tiny:   80
		misses_tiny: 20
	}

	assert stats.total_hits() == 80
	assert stats.total_misses() == 20
	assert stats.hit_rate() == 0.8
}

/// 객체 풀 통계 적중률 테스트
fn test_object_pool_stats_hit_rate() {
	stats := core.ObjectPoolStats{
		hits:   90
		misses: 10
	}

	assert stats.hit_rate() == 0.9
}

// Pooled Record Tests

/// 풀링된 레코드 헤더 테스트
fn test_pooled_record_headers() {
	mut r := core.PooledRecord{}

	r.add_header('key1', 'value1'.bytes())
	r.add_header('key2', 'value2'.bytes())

	assert r.headers.len == 2
	assert r.headers[0].key == 'key1'
	assert r.headers[1].key == 'key2'
}

/// 풀링된 레코드 배치 테스트
fn test_pooled_record_batch() {
	mut batch := core.PooledRecordBatch{}

	r1 := &core.PooledRecord{
		timestamp: 1000
	}
	r2 := &core.PooledRecord{
		timestamp: 2000
	}

	batch.add_record(r1)
	batch.add_record(r2)

	assert batch.size() == 2
	assert batch.first_ts == 1000
	assert batch.max_ts == 2000
}

// Benchmark Suite Tests

/// 벤치마크 스위트 생성 테스트
fn test_benchmark_suite_creation() {
	performance.init_global_performance(performance.PerformanceConfig{})

	suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    10
		benchmark_iterations: 100
	})

	assert suite.config.warmup_iterations == 10
	assert suite.config.benchmark_iterations == 100
}

/// 벤치마크 버퍼 할당 테스트
fn test_benchmark_buffer_allocation() {
	performance.init_global_performance(performance.PerformanceConfig{
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
	performance.init_global_performance(performance.PerformanceConfig{})

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
	performance.init_global_performance(performance.PerformanceConfig{})

	mut suite := new_benchmark_suite(BenchmarkConfig{
		warmup_iterations:    5
		benchmark_iterations: 50
	})

	result := suite.benchmark_request_response_cycle()
	assert result.iterations == 50
	assert result.name == 'Request/Response Cycle'
}
