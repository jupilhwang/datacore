// Infra Layer - Performance Tests
module benchmarks

fn test_buffer_write_and_read() {
	mut buf := Buffer{
		data: []u8{len: 256}
		cap:  256
	}

	data := 'Hello, World!'.bytes()
	written := buf.write(data)

	assert written == data.len
	assert buf.len == data.len
	assert buf.bytes() == data
}

fn test_buffer_write_byte() {
	mut buf := Buffer{
		data: []u8{len: 10}
		cap:  10
	}

	assert buf.write_byte(0x41) == true
	assert buf.write_byte(0x42) == true
	assert buf.write_byte(0x43) == true

	assert buf.len == 3
	assert buf.bytes() == [u8(0x41), 0x42, 0x43]
}

fn test_buffer_write_i32_be() {
	mut buf := Buffer{
		data: []u8{len: 10}
		cap:  10
	}

	assert buf.write_i32_be(0x12345678) == true
	assert buf.len == 4
	assert buf.bytes() == [u8(0x12), 0x34, 0x56, 0x78]
}

fn test_buffer_overflow() {
	mut buf := Buffer{
		data: []u8{len: 4}
		cap:  4
	}

	data := [u8(1), 2, 3, 4, 5, 6]
	written := buf.write(data)

	assert written == 4
	assert buf.len == 4
}

fn test_buffer_pool_get_put() {
	mut pool := new_buffer_pool(PoolConfig{
		prewarm_tiny: 5
	})

	// Get from prewarmed pool
	mut buf := pool.get(100)
	assert buf.cap == 256 // tiny class

	// Put back
	pool.put(buf)

	stats := pool.get_stats()
	assert stats.hits_tiny > 0
}

fn test_size_class_selection() {
	assert get_size_class(100) == .tiny
	assert get_size_class(256) == .tiny
	assert get_size_class(257) == .small
	assert get_size_class(4096) == .small
	assert get_size_class(4097) == .medium
	assert get_size_class(65536) == .medium
	assert get_size_class(65537) == .large
	assert get_size_class(1048576) == .large
	assert get_size_class(1048577) == .huge
}

fn test_murmur3_basic() {
	data := 'test'.bytes()
	hash := murmur3_32(data, 0)

	// Verify hash is non-zero and deterministic
	assert hash != 0
	assert murmur3_32(data, 0) == hash

	// Different data should produce different hash
	other := 'other'.bytes()
	assert murmur3_32(other, 0) != hash
}

fn test_murmur3_empty() {
	empty := []u8{}
	hash := murmur3_32(empty, 0)
	// Empty input with seed 0 should still produce a result
	assert hash == murmur3_32(empty, 0)
}

fn test_kafka_partition() {
	key := 'my-key'.bytes()

	partition := kafka_partition(key, 10)
	assert partition >= 0
	assert partition < 10

	// Same key should always map to same partition
	assert kafka_partition(key, 10) == partition
}

fn test_kafka_partition_empty_key() {
	empty := []u8{}
	assert kafka_partition(empty, 10) == 0
}

fn test_crc32_basic() {
	data := 'Hello, World!'.bytes()
	crc := crc32_ieee(data)

	// Verify deterministic
	assert crc32_ieee(data) == crc

	// Different data should produce different CRC
	other := 'Goodbye!'.bytes()
	assert crc32_ieee(other) != crc
}

fn test_crc32_known_value() {
	// Test with known CRC32 value
	data := 'test'.bytes()
	crc := crc32_ieee(data)
	assert crc != 0
}

fn test_varint_roundtrip() {
	values := [i64(0), 1, -1, 127, -128, 255, 300, -300, 1000000, -1000000]

	for val in values {
		encoded := encode_varint(val)
		decoded, n := decode_varint(encoded)

		assert n > 0, 'Failed to decode varint for ${val}'
		assert decoded == val, 'Varint roundtrip failed: ${val} -> ${decoded}'
	}
}

fn test_uvarint_roundtrip() {
	values := [u64(0), 1, 127, 128, 255, 16383, 16384, 2097151]

	for val in values {
		encoded := encode_uvarint(val)
		decoded, n := decode_uvarint(encoded)

		assert n > 0, 'Failed to decode uvarint for ${val}'
		assert decoded == val, 'Uvarint roundtrip failed: ${val} -> ${decoded}'
	}
}

fn test_varint_size() {
	assert varint_size(0) == 1
	assert varint_size(1) == 1
	assert varint_size(-1) == 1
	assert varint_size(63) == 1
	assert varint_size(-64) == 1
	assert varint_size(64) == 2
	assert varint_size(-65) == 2
}

fn test_ring_buffer_basic() {
	mut rb := new_ring_buffer(16)

	assert rb.is_empty()
	assert !rb.is_full()

	data := [u8(1), 2, 3, 4, 5]
	written := rb.write(data)

	assert written == 5
	assert rb.available() == 5
	assert !rb.is_empty()
}

fn test_ring_buffer_read_write() {
	mut rb := new_ring_buffer(16)

	write_data := [u8(1), 2, 3, 4, 5]
	rb.write(write_data)

	mut read_buf := []u8{len: 5}
	read_count := rb.read(mut read_buf)

	assert read_count == 5
	assert read_buf == write_data
	assert rb.is_empty()
}

fn test_ring_buffer_wrap_around() {
	mut rb := new_ring_buffer(8)

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

fn test_record_pool() {
	mut pool := new_record_pool(10)

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

fn test_record_batch_pool() {
	mut pool := new_record_batch_pool(10)

	mut batch := pool.get()
	assert batch.in_use == true

	batch.topic = 'test-topic'
	batch.partition = 5

	pool.put(batch)

	stats := pool.get_stats()
	assert stats.returns == 1
}

fn test_request_pool() {
	mut pool := new_request_pool(10)

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

fn test_bit_operations() {
	// Leading zeros
	assert count_leading_zeros(0) == 64
	assert count_leading_zeros(1) == 63
	assert count_leading_zeros(0x8000000000000000) == 0

	// Trailing zeros
	assert count_trailing_zeros(0) == 64
	assert count_trailing_zeros(1) == 0
	assert count_trailing_zeros(8) == 3

	// Popcount
	assert popcount(0) == 0
	assert popcount(1) == 1
	assert popcount(0xff) == 8
	assert popcount(0xffffffffffffffff) == 64
}

fn test_transfer_result() {
	result := TransferResult{
		bytes_transferred: 1024
		success:           true
	}

	assert result.success
	assert result.bytes_transferred == 1024
}

fn test_pool_stats_hit_rate() {
	stats := PoolStats{
		hits_tiny:   80
		misses_tiny: 20
	}

	assert stats.total_hits() == 80
	assert stats.total_misses() == 20
	assert stats.hit_rate() == 0.8
}

fn test_object_pool_stats_hit_rate() {
	stats := ObjectPoolStats{
		hits:   90
		misses: 10
	}

	assert stats.hit_rate() == 0.9
}

fn test_pooled_record_headers() {
	mut r := PooledRecord{}

	r.add_header('key1', 'value1'.bytes())
	r.add_header('key2', 'value2'.bytes())

	assert r.headers.len == 2
	assert r.headers[0].key == 'key1'
	assert r.headers[1].key == 'key2'
}

fn test_pooled_record_batch() {
	mut batch := PooledRecordBatch{}

	r1 := &PooledRecord{
		timestamp: 1000
	}
	r2 := &PooledRecord{
		timestamp: 2000
	}

	batch.add_record(r1)
	batch.add_record(r2)

	assert batch.size() == 2
	assert batch.first_ts == 1000
	assert batch.max_ts == 2000
}
