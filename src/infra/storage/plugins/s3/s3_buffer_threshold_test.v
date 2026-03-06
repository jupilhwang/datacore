// Unit tests for min_flush_bytes threshold logic in buffer manager
module s3

import time

// test_skip_flush_when_buffer_below_threshold verifies that a partition buffer
// smaller than min_flush_bytes is skipped (not included in flush batches).
fn test_skip_flush_when_buffer_below_threshold() {
	config := S3Config{
		prefix:               'test/'
		bucket_name:          'test-bucket'
		region:               'us-east-1'
		min_flush_bytes:      4096
		max_flush_skip_count: 40
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	// Add a small record (well below 4096 bytes)
	partition_key := 'test-topic:0'
	small_record := StoredRecord{
		offset:    0
		timestamp: time.now()
		key:       'k'.bytes()
		value:     'v'.bytes()
		headers:   map[string][]u8{}
	}
	adapter.topic_partition_buffers[partition_key] = TopicPartitionBuffer{
		records:            [small_record]
		current_size_bytes: i64(small_record.key.len + small_record.value.len +
			record_overhead_bytes)
	}

	// Collect flush batches with threshold applied
	batches, _ := adapter.collect_flush_batches()

	// Buffer is below threshold so it should be skipped
	assert batches.len == 0, 'Expected 0 flush batches but got ${batches.len}'
}

// test_force_flush_after_max_skip_count verifies that a partition buffer
// is force-flushed after exceeding max_flush_skip_count consecutive skips.
fn test_force_flush_after_max_skip_count() {
	config := S3Config{
		prefix:               'test/'
		bucket_name:          'test-bucket'
		region:               'us-east-1'
		min_flush_bytes:      4096
		max_flush_skip_count: 3
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'
	small_record := StoredRecord{
		offset:    0
		timestamp: time.now()
		key:       'k'.bytes()
		value:     'v'.bytes()
		headers:   map[string][]u8{}
	}
	adapter.topic_partition_buffers[partition_key] = TopicPartitionBuffer{
		records:            [small_record]
		current_size_bytes: i64(small_record.key.len + small_record.value.len +
			record_overhead_bytes)
	}

	// Simulate max_flush_skip_count skips already happened
	adapter.flush_skip_counts[partition_key] = 3

	// Now collect should force flush despite being below threshold
	batches, _ := adapter.collect_flush_batches()

	assert batches.len == 1, 'Expected 1 flush batch (forced) but got ${batches.len}'
	assert batches[0].key == partition_key
	assert batches[0].records.len == 1

	// Skip counter should have been reset after flush
	assert adapter.flush_skip_counts[partition_key] == 0
}

// test_flush_normally_when_threshold_zero verifies that when min_flush_bytes=0,
// the threshold feature is disabled and all non-empty buffers are flushed normally.
fn test_flush_normally_when_threshold_zero() {
	config := S3Config{
		prefix:               'test/'
		bucket_name:          'test-bucket'
		region:               'us-east-1'
		min_flush_bytes:      0
		max_flush_skip_count: 40
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'
	small_record := StoredRecord{
		offset:    0
		timestamp: time.now()
		key:       'k'.bytes()
		value:     'v'.bytes()
		headers:   map[string][]u8{}
	}
	adapter.topic_partition_buffers[partition_key] = TopicPartitionBuffer{
		records:            [small_record]
		current_size_bytes: i64(small_record.key.len + small_record.value.len +
			record_overhead_bytes)
	}

	batches, _ := adapter.collect_flush_batches()

	// min_flush_bytes=0 disables threshold, so buffer should be flushed
	assert batches.len == 1, 'Expected 1 flush batch but got ${batches.len}'
	assert batches[0].key == partition_key
}

// test_flush_when_buffer_above_threshold verifies that a buffer meeting or
// exceeding min_flush_bytes is flushed normally.
fn test_flush_when_buffer_above_threshold() {
	config := S3Config{
		prefix:               'test/'
		bucket_name:          'test-bucket'
		region:               'us-east-1'
		min_flush_bytes:      100
		max_flush_skip_count: 40
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'
	// Create a record with value large enough to exceed threshold
	large_value := []u8{len: 200, init: u8(65)}
	large_record := StoredRecord{
		offset:    0
		timestamp: time.now()
		key:       'key'.bytes()
		value:     large_value
		headers:   map[string][]u8{}
	}
	adapter.topic_partition_buffers[partition_key] = TopicPartitionBuffer{
		records:            [large_record]
		current_size_bytes: i64(large_record.key.len + large_record.value.len +
			record_overhead_bytes)
	}

	batches, _ := adapter.collect_flush_batches()

	assert batches.len == 1, 'Expected 1 flush batch but got ${batches.len}'
	assert batches[0].records.len == 1
}

// test_skip_counter_increments_on_skip verifies that the skip counter
// increments each time a partition is skipped.
fn test_skip_counter_increments_on_skip() {
	config := S3Config{
		prefix:               'test/'
		bucket_name:          'test-bucket'
		region:               'us-east-1'
		min_flush_bytes:      4096
		max_flush_skip_count: 40
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'
	small_record := StoredRecord{
		offset:    0
		timestamp: time.now()
		key:       'k'.bytes()
		value:     'v'.bytes()
		headers:   map[string][]u8{}
	}
	adapter.topic_partition_buffers[partition_key] = TopicPartitionBuffer{
		records:            [small_record]
		current_size_bytes: i64(small_record.key.len + small_record.value.len +
			record_overhead_bytes)
	}

	// First call: skip counter should become 1
	batches_1, _ := adapter.collect_flush_batches()
	assert batches_1.len == 0
	assert adapter.flush_skip_counts[partition_key] == 1

	// Second call: skip counter should become 2
	batches_2, _ := adapter.collect_flush_batches()
	assert batches_2.len == 0
	assert adapter.flush_skip_counts[partition_key] == 2
}

// test_skip_counter_resets_on_normal_flush verifies that the skip counter
// resets to 0 when a partition buffer is flushed normally (above threshold).
fn test_skip_counter_resets_on_normal_flush() {
	config := S3Config{
		prefix:               'test/'
		bucket_name:          'test-bucket'
		region:               'us-east-1'
		min_flush_bytes:      100
		max_flush_skip_count: 40
	}
	mut adapter := S3StorageAdapter{
		config: config
	}

	partition_key := 'test-topic:0'

	// Pre-set a skip count
	adapter.flush_skip_counts[partition_key] = 5

	large_value := []u8{len: 200, init: u8(65)}
	large_record := StoredRecord{
		offset:    0
		timestamp: time.now()
		key:       'key'.bytes()
		value:     large_value
		headers:   map[string][]u8{}
	}
	adapter.topic_partition_buffers[partition_key] = TopicPartitionBuffer{
		records:            [large_record]
		current_size_bytes: i64(large_record.key.len + large_record.value.len +
			record_overhead_bytes)
	}

	batches, _ := adapter.collect_flush_batches()

	assert batches.len == 1
	// Skip counter should be reset after flush
	assert adapter.flush_skip_counts[partition_key] == 0
}
