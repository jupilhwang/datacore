// Infra Layer - S3 Index Batch Manager
// Handles index batch accumulation and flush operations for S3 storage.
// Separated from buffer_manager.v to maintain single-responsibility principle.
module s3

import json
import time
import infra.observability

// --- Index Batch Update Functions ---

/// add_pending_index_segment accumulates a segment into the pending index buffer.
/// Does not write to S3; the caller must check should_flush_index and
/// call flush_pending_index when the batch threshold is reached.
fn (mut a S3StorageAdapter) add_pending_index_segment(partition_key string, segment LogSegment) {
	if partition_key !in a.pending_index_updates {
		a.pending_index_updates[partition_key] = []LogSegment{}
	}
	a.pending_index_updates[partition_key] << segment
	a.index_flush_counter[partition_key] = a.pending_index_updates[partition_key].len
}

/// should_flush_index returns true when the number of pending segments
/// for the given partition has reached or exceeded index_batch_size.
fn (a &S3StorageAdapter) should_flush_index(partition_key string) bool {
	count := a.index_flush_counter[partition_key] or { 0 }
	return count >= a.config.index_batch_size
}

/// drain_pending_index_segments extracts all pending segments for a partition
/// and resets the counter. The caller is responsible for writing them to S3.
fn (mut a S3StorageAdapter) drain_pending_index_segments(partition_key string) []LogSegment {
	segments := a.pending_index_updates[partition_key] or { return []LogSegment{} }
	result := segments.clone()
	a.pending_index_updates[partition_key] = []LogSegment{}
	a.index_flush_counter[partition_key] = 0
	return result
}

/// get_pending_index_partition_keys returns partition keys that have pending segments.
fn (a &S3StorageAdapter) get_pending_index_partition_keys() []string {
	mut keys := []string{}
	for key, segments in a.pending_index_updates {
		if segments.len > 0 {
			keys << key
		}
	}
	return keys
}

/// flush_all_pending_indexes force-flushes all pending index updates to S3.
/// Called by flush_worker when index_flush_interval_ms elapses.
fn (mut a S3StorageAdapter) flush_all_pending_indexes() {
	a.index_flush_lock.lock()
	partition_keys := a.get_pending_index_partition_keys()
	mut flush_map := map[string][]LogSegment{}
	for pk in partition_keys {
		segments := a.drain_pending_index_segments(pk)
		if segments.len > 0 {
			flush_map[pk] = segments
		}
	}
	a.index_flush_lock.unlock()

	for pk, segments in flush_map {
		parts := pk.split(':')
		if parts.len != 2 {
			continue
		}
		topic := parts[0]
		partition := parts[1].int()
		a.index_update_lock.lock()
		a.write_index_with_segments(topic, partition, segments) or {
			observability.log_with_context('s3', .error, 'IndexBatchFlush', 'Forced index flush failed',
				{
				'partition_key': pk
				'segment_count': segments.len.str()
				'error':         err.msg()
			})
		}
		a.index_update_lock.unlock()
	}
}

/// write_index_with_segments reads the current index from S3, appends segments,
/// and writes the updated index back. This is the actual S3 I/O operation.
fn (mut a S3StorageAdapter) write_index_with_segments(topic string, partition int, segments []LogSegment) ! {
	index_key := a.partition_index_key(topic, partition)
	mut index := PartitionIndex{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		high_watermark:  0
		log_segments:    []
	}

	if data, _ := a.get_object(index_key, -1, -1) {
		if decoded := json.decode(PartitionIndex, data.bytestr()) {
			index = decoded
		}
	}

	// Build set for duplicate detection: O(1) per lookup
	mut segment_key_set := map[string]bool{}
	for seg in index.log_segments {
		segment_key_set[seg.key] = true
	}

	mut modified := false
	for seg in segments {
		if seg.key !in segment_key_set {
			index.log_segments << seg
			segment_key_set[seg.key] = true
			modified = true

			new_candidate := seg.end_offset + 1
			if new_candidate > index.high_watermark {
				index.high_watermark = new_candidate
			}
		}
	}

	if modified {
		// Sort segments by start_offset to maintain order
		index.log_segments.sort(a.start_offset < b.start_offset)

		// Write updated index to S3
		a.put_object(index_key, json.encode(index).bytes())!

		// Update local cache
		a.update_index_cache(topic, partition, index)
	}
}

/// update_index_cache updates the local index cache.
fn (mut a S3StorageAdapter) update_index_cache(topic string, partition int, index PartitionIndex) {
	cache_key := '${topic}:${partition}'
	a.topic_lock.@lock()
	defer {
		a.topic_lock.unlock()
	}

	if cached := a.topic_index_cache[cache_key] {
		// Maintain higher high_watermark between cache and S3
		mut final_index := index
		if cached.index.high_watermark > index.high_watermark {
			final_index.high_watermark = cached.index.high_watermark
		}
		a.topic_index_cache[cache_key] = CachedPartitionIndex{
			index:     final_index
			cached_at: time.now()
		}
	} else {
		a.topic_index_cache[cache_key] = CachedPartitionIndex{
			index:     index
			cached_at: time.now()
		}
	}
}
