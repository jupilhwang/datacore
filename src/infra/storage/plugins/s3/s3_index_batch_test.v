// Unit tests for index batch update logic in buffer manager
// Verifies that partition index updates are batched to reduce S3 PUT frequency
module s3

import time

// Helper: creates an S3StorageAdapter with index batch config for testing
fn create_test_adapter_with_batch_config(batch_size int) S3StorageAdapter {
	config := S3Config{
		prefix:                  'test/'
		bucket_name:             'test-bucket'
		region:                  'us-east-1'
		index_batch_size:        batch_size
		index_flush_interval_ms: 500
	}
	return S3StorageAdapter{
		config: config
	}
}

// Helper: creates a LogSegment for testing
fn create_test_log_segment(start_offset i64, end_offset i64, key string) LogSegment {
	return LogSegment{
		start_offset: start_offset
		end_offset:   end_offset
		key:          key
		size_bytes:   1024
		created_at:   time.now()
	}
}

// test_pending_index_accumulates_segments verifies that segments are accumulated
// in pending_index_updates instead of being written to S3 immediately
// when the batch size has not been reached.
fn test_pending_index_accumulates_segments() {
	mut adapter := create_test_adapter_with_batch_config(5)

	partition_key := 'test-topic:0'
	segment := create_test_log_segment(0, 9, 'test/topics/test-topic/partitions/0/log-0-9.bin')

	adapter.add_pending_index_segment(partition_key, segment)

	// Segment should be in pending buffer
	assert partition_key in adapter.pending_index_updates
	assert adapter.pending_index_updates[partition_key].len == 1

	// Counter should be incremented
	assert adapter.index_flush_counter[partition_key] == 1
}

// test_index_not_updated_until_batch_size_reached verifies that with N-1 segments
// accumulated, no flush is triggered (pending segments remain in buffer).
fn test_index_not_updated_until_batch_size_reached() {
	batch_size := 5
	mut adapter := create_test_adapter_with_batch_config(batch_size)

	partition_key := 'test-topic:0'

	// Add N-1 segments
	for i in 0 .. batch_size - 1 {
		offset_start := i64(i * 10)
		offset_end := offset_start + 9
		key := 'test/topics/test-topic/partitions/0/log-${offset_start}-${offset_end}.bin'
		segment := create_test_log_segment(offset_start, offset_end, key)
		adapter.add_pending_index_segment(partition_key, segment)
	}

	// All N-1 segments should still be pending
	assert adapter.pending_index_updates[partition_key].len == batch_size - 1
	assert adapter.index_flush_counter[partition_key] == batch_size - 1
}

// test_should_flush_index_returns_true_at_batch_size verifies that
// should_flush_index returns true when the batch size threshold is reached.
fn test_should_flush_index_returns_true_at_batch_size() {
	batch_size := 5
	mut adapter := create_test_adapter_with_batch_config(batch_size)

	partition_key := 'test-topic:0'

	// Add N segments
	for i in 0 .. batch_size {
		offset_start := i64(i * 10)
		offset_end := offset_start + 9
		key := 'test/topics/test-topic/partitions/0/log-${offset_start}-${offset_end}.bin'
		segment := create_test_log_segment(offset_start, offset_end, key)
		adapter.add_pending_index_segment(partition_key, segment)
	}

	// At exactly batch_size, should_flush_index must return true
	assert adapter.should_flush_index(partition_key) == true
}

// test_should_flush_index_returns_false_below_batch_size verifies that
// should_flush_index returns false when below the batch size threshold.
fn test_should_flush_index_returns_false_below_batch_size() {
	batch_size := 5
	mut adapter := create_test_adapter_with_batch_config(batch_size)

	partition_key := 'test-topic:0'
	segment := create_test_log_segment(0, 9, 'test/topics/test-topic/partitions/0/log-0-9.bin')
	adapter.add_pending_index_segment(partition_key, segment)

	// Below batch_size, should return false
	assert adapter.should_flush_index(partition_key) == false
}

// test_drain_pending_segments extracts and clears pending segments for a partition.
fn test_drain_pending_segments() {
	batch_size := 3
	mut adapter := create_test_adapter_with_batch_config(batch_size)

	partition_key := 'test-topic:0'

	// Add 3 segments
	for i in 0 .. batch_size {
		offset_start := i64(i * 10)
		offset_end := offset_start + 9
		key := 'test/topics/test-topic/partitions/0/log-${offset_start}-${offset_end}.bin'
		segment := create_test_log_segment(offset_start, offset_end, key)
		adapter.add_pending_index_segment(partition_key, segment)
	}

	// Drain should return all pending segments and clear the buffer
	drained := adapter.drain_pending_index_segments(partition_key)

	assert drained.len == batch_size
	assert adapter.pending_index_updates[partition_key].len == 0
	assert adapter.index_flush_counter[partition_key] == 0
}

// test_index_batch_size_one_behaves_like_immediate verifies that when
// index_batch_size=1, should_flush_index returns true after every single segment.
fn test_index_batch_size_one_behaves_like_immediate() {
	mut adapter := create_test_adapter_with_batch_config(1)

	partition_key := 'test-topic:0'
	segment := create_test_log_segment(0, 9, 'test/topics/test-topic/partitions/0/log-0-9.bin')

	adapter.add_pending_index_segment(partition_key, segment)

	// batch_size=1 means flush immediately after every segment
	assert adapter.should_flush_index(partition_key) == true
	assert adapter.pending_index_updates[partition_key].len == 1
}

// test_get_pending_partition_keys returns all partition keys that have pending segments.
fn test_get_pending_partition_keys() {
	mut adapter := create_test_adapter_with_batch_config(5)

	// Add segments for multiple partitions
	seg1 := create_test_log_segment(0, 9, 'test/log-0-9.bin')
	seg2 := create_test_log_segment(0, 9, 'test/log-0-9.bin')

	adapter.add_pending_index_segment('topic-a:0', seg1)
	adapter.add_pending_index_segment('topic-b:1', seg2)

	keys := adapter.get_pending_index_partition_keys()

	assert keys.len == 2
	assert 'topic-a:0' in keys
	assert 'topic-b:1' in keys
}

// test_get_pending_partition_keys_excludes_empty verifies that partitions
// with no pending segments are excluded from the result.
fn test_get_pending_partition_keys_excludes_empty() {
	mut adapter := create_test_adapter_with_batch_config(5)

	// Add and then drain segments
	seg := create_test_log_segment(0, 9, 'test/log-0-9.bin')
	adapter.add_pending_index_segment('topic-a:0', seg)
	_ := adapter.drain_pending_index_segments('topic-a:0')

	keys := adapter.get_pending_index_partition_keys()

	assert keys.len == 0
}

// test_multiple_partitions_independent verifies that pending index updates
// for different partitions are independent.
fn test_multiple_partitions_independent() {
	batch_size := 3
	mut adapter := create_test_adapter_with_batch_config(batch_size)

	// Add 3 segments to partition 0
	for i in 0 .. batch_size {
		seg := create_test_log_segment(i64(i * 10), i64(i * 10 + 9), 'test/p0-${i}.bin')
		adapter.add_pending_index_segment('topic:0', seg)
	}

	// Add 1 segment to partition 1
	seg := create_test_log_segment(0, 9, 'test/p1-0.bin')
	adapter.add_pending_index_segment('topic:1', seg)

	// Partition 0 should be ready for flush
	assert adapter.should_flush_index('topic:0') == true
	// Partition 1 should not
	assert adapter.should_flush_index('topic:1') == false

	// Draining partition 0 should not affect partition 1
	_ := adapter.drain_pending_index_segments('topic:0')
	assert adapter.pending_index_updates['topic:1'].len == 1
}
