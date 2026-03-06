// Unit tests for offset commit consolidation (Task 2).
// Verifies that commit_offsets() uses the batch snapshot path (buffer_offset)
// instead of per-partition JSON writes (commit_single_offset).
module s3

import domain

// test_commit_offsets_uses_batch_path verifies that after commit_offsets(),
// offsets are buffered in offset_buffers (the batch path) rather than
// being written as individual JSON files via commit_single_offset().
fn test_commit_offsets_uses_batch_path() {
	mut adapter := create_test_adapter()

	group_id := 'test-group'
	offsets := [
		domain.PartitionOffset{
			topic:        'topic-a'
			partition:    0
			offset:       42
			leader_epoch: 1
			metadata:     ''
		},
		domain.PartitionOffset{
			topic:        'topic-a'
			partition:    1
			offset:       100
			leader_epoch: 1
			metadata:     ''
		},
	]

	adapter.commit_offsets(group_id, offsets) or {
		assert false, 'commit_offsets should not fail: ${err}'
		return
	}

	// After commit_offsets, offsets must be present in offset_buffers (batch path)
	adapter.offset_buffer_lock.lock()
	buf_exists := group_id in adapter.offset_buffers
	mut found_p0 := false
	mut found_p1 := false
	if buf_exists {
		buf := adapter.offset_buffers[group_id]
		if _ := buf.offsets['topic-a:0'] {
			found_p0 = true
		}
		if _ := buf.offsets['topic-a:1'] {
			found_p1 = true
		}
	}
	adapter.offset_buffer_lock.unlock()

	assert buf_exists, 'offset_buffers should contain the group after commit_offsets'
	assert found_p0, 'offset_buffers should contain topic-a:0'
	assert found_p1, 'offset_buffers should contain topic-a:1'
}

// test_no_individual_json_files_on_commit verifies that commit_offsets()
// does NOT produce per-partition JSON S3 PUTs (the old commit_single_offset path).
// It checks that the S3 PUT count metric only reflects batch operations,
// not individual partition writes.
fn test_no_individual_json_files_on_commit() {
	mut adapter := create_test_adapter()

	group_id := 'test-group-2'
	offsets := [
		domain.PartitionOffset{
			topic:        'orders'
			partition:    0
			offset:       10
			leader_epoch: 0
			metadata:     ''
		},
		domain.PartitionOffset{
			topic:        'orders'
			partition:    1
			offset:       20
			leader_epoch: 0
			metadata:     ''
		},
		domain.PartitionOffset{
			topic:        'orders'
			partition:    2
			offset:       30
			leader_epoch: 0
			metadata:     ''
		},
	]

	// Reset metrics to get a clean baseline
	adapter.reset_metrics()

	adapter.commit_offsets(group_id, offsets) or {
		assert false, 'commit_offsets should not fail: ${err}'
		return
	}

	// The batch path (buffer_offset) does NOT issue S3 PUTs at commit time.
	// PUTs only happen during flush_pending_offsets (background).
	// So s3_put_count must be 0 after commit_offsets returns.
	metrics := adapter.get_metrics()
	assert metrics.s3_put_count == 0, 'No S3 PUTs should occur during commit_offsets (batch path buffers only). Got ${metrics.s3_put_count}'
}

// test_commit_offsets_updates_offset_cache verifies that the offset cache
// is updated after buffering offsets through the batch path.
fn test_commit_offsets_updates_offset_cache() {
	mut adapter := create_test_adapter()

	group_id := 'cache-test-group'
	offsets := [
		domain.PartitionOffset{
			topic:        'events'
			partition:    0
			offset:       55
			leader_epoch: 1
			metadata:     'meta1'
		},
	]

	adapter.commit_offsets(group_id, offsets) or {
		assert false, 'commit_offsets should not fail: ${err}'
		return
	}

	// Verify offset cache is updated
	adapter.offset_lock.rlock()
	cached_val := if group_id in adapter.offset_cache {
		adapter.offset_cache[group_id]['events:0'] or { i64(-1) }
	} else {
		i64(-1)
	}
	adapter.offset_lock.runlock()

	assert cached_val == 55, 'Offset cache should contain committed offset 55. Got ${cached_val}'
}

// test_commit_offsets_empty_is_noop verifies that empty offset list is a no-op.
fn test_commit_offsets_empty_is_noop() {
	mut adapter := create_test_adapter()

	adapter.reset_metrics()

	adapter.commit_offsets('empty-group', []) or {
		assert false, 'commit_offsets with empty list should not fail: ${err}'
		return
	}

	metrics := adapter.get_metrics()
	assert metrics.s3_put_count == 0, 'No PUTs for empty offset list'
	assert metrics.offset_commit_count == 0, 'No commit count for empty offset list'
}

// create_test_adapter creates an S3StorageAdapter configured for unit testing.
// Uses default config that does not connect to any real S3.
fn create_test_adapter() &S3StorageAdapter {
	config := S3Config{
		prefix:                       'test/'
		bucket_name:                  'test-bucket'
		region:                       'us-east-1'
		broker_id:                    1
		offset_batch_enabled:         true
		offset_flush_interval_ms:     100
		offset_flush_threshold_count: 50
	}
	return &S3StorageAdapter{
		config:                  config
		topic_cache:             map[string]CachedTopic{}
		group_cache:             map[string]CachedGroup{}
		offset_cache:            map[string]map[string]i64{}
		topic_index_cache:       map[string]CachedPartitionIndex{}
		topic_partition_buffers: map[string]TopicPartitionBuffer{}
		offset_buffers:          map[string]OffsetGroupBuffer{}
		iceberg_writers:         map[string]&IcebergWriter{}
	}
}
