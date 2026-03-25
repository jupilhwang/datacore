// Tests for concurrent append safety and atomic flag behavior.
module s3

import time
import sync.stdatomic

fn test_reserve_offsets_concurrent_no_overlap() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:      'test/'
			bucket_name: 'test-bucket'
			region:      'us-east-1'
		}
	}

	partition_key := 'test-topic:0'

	// Pre-populate cache with a partition index at hw=0
	adapter.cache.topic_lock.@lock()
	adapter.cache.topic_index_cache[partition_key] = CachedPartitionIndex{
		index:     PartitionIndex{
			topic:           'test-topic'
			partition:       0
			earliest_offset: 0
			high_watermark:  0
			log_segments:    []
		}
		etag:      ''
		cached_at: time.now()
	}
	adapter.cache.topic_lock.unlock()

	// Spawn goroutines, each reserving offsets
	goroutine_count := 4
	records_per := 10
	ch := chan i64{cap: goroutine_count}

	for _ in 0 .. goroutine_count {
		spawn fn [mut adapter, partition_key, records_per, ch] () {
			mut p_lock := adapter.get_partition_append_lock(partition_key)
			p_lock.lock()
			base := adapter.reserve_offsets(partition_key, records_per)
			p_lock.unlock()
			ch <- base
		}()
	}

	mut bases := []i64{}
	for _ in 0 .. goroutine_count {
		bases << <-ch
	}

	bases.sort()

	// Verify no overlapping offset ranges
	for i in 0 .. bases.len - 1 {
		assert bases[i] + i64(records_per) <= bases[i + 1], 'offset ranges overlap: base[${i}]=${bases[i]}, base[${
			i + 1}]=${bases[i + 1]}'
	}

	// Verify final watermark equals sum of all reservations
	adapter.cache.topic_lock.rlock()
	final_hw := adapter.cache.topic_index_cache[partition_key].index.high_watermark
	adapter.cache.topic_lock.runlock()

	expected_hw := i64(goroutine_count * records_per)
	assert final_hw == expected_hw, 'expected hw=${expected_hw}, got ${final_hw}'
}

fn test_start_workers_is_idempotent() {
	mut adapter := S3StorageAdapter{
		config: S3Config{
			prefix:                 'test/'
			bucket_name:            'test-bucket'
			region:                 'us-east-1'
			batch_timeout_ms:       100
			compaction_interval_ms: 100
		}
	}

	// First start should succeed
	adapter.start_workers()
	assert stdatomic.load_i64(&adapter.is_running_flag) == 1, 'workers should be running after start'

	// Second start should be no-op (not double-start)
	adapter.start_workers()
	assert stdatomic.load_i64(&adapter.is_running_flag) == 1, 'workers should still be running'

	// Cleanup
	adapter.stop_workers()
	assert stdatomic.load_i64(&adapter.is_running_flag) == 0, 'workers should be stopped'
}
