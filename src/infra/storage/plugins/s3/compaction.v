// S3 Compaction Logic
// Handles segment compaction and merging for S3 storage
module s3

import json
import time

// compaction_worker periodically checks for segments to merge and performs compaction.
fn (mut a S3StorageAdapter) compaction_worker() {
	for a.compactor_running {
		time.sleep(a.config.compaction_interval_ms)

		a.compact_all_partitions() or {
			// In production, use structured logging here
			eprintln('[S3] Compaction failed: ${err}')
			continue
		}
	}
}

// compact_all_partitions iterates over all topics and partitions and attempts to merge small segments.
fn (mut a S3StorageAdapter) compact_all_partitions() ! {
	topics := a.list_topics()!

	// Use map/set to track active partitions to compact
	// For simplicity, we iterate over all known topics/partitions.

	for t in topics {
		for p in 0 .. t.partition_count {
			a.compact_partition(t.name, p)!
		}
	}
}

// compact_partition compacts segments for a specific partition
fn (mut a S3StorageAdapter) compact_partition(topic string, partition int) ! {
	// 1. Get current index
	mut index := a.get_partition_index(topic, partition)!

	// 2. Identify segments for compaction
	mut segments_to_compact := []LogSegment{}
	mut total_size := i64(0)

	for seg in index.log_segments {
		if total_size >= a.config.target_segment_bytes {
			break
		}

		// Only consider segments smaller than the target size
		if seg.size_bytes < a.config.target_segment_bytes {
			segments_to_compact << seg
			total_size += seg.size_bytes
		}
	}

	// Check if enough small segments were found
	if segments_to_compact.len < a.min_segment_count_to_compact
		|| total_size < a.config.target_segment_bytes / 2 {
		return
	}

	// 3. Perform Compaction
	// Merge segments and upload new large segment to S3
	a.merge_segments(topic, partition, mut index, segments_to_compact)!
}

// merge_segments merges multiple segments into a single larger segment
fn (mut a S3StorageAdapter) merge_segments(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	if segments.len == 0 {
		return
	}

	// Download all segment data in parallel (simplified to sequential for now)
	mut merged_data := []u8{}

	for seg in segments {
		data, _ := a.get_object(seg.key, -1, -1) or {
			// Log error and continue to next segment set, or return error
			// We return error to be safe.
			return error('Failed to download segment ${seg.key}: ${err}')
		}
		merged_data << data
	}

	// New segment metadata
	new_start_offset := segments[0].start_offset
	new_end_offset := segments[segments.len - 1].end_offset
	new_key := a.log_segment_key(topic, partition, new_start_offset, new_end_offset)

	// Upload new merged segment to S3
	a.put_object(new_key, merged_data)!

	// 4. Update index and delete old segments (Atomic Index Update)

	// Find the range of offsets covered by the merged segments
	start_index := index.log_segments.index(segments[0])
	if start_index < 0 {
		return error('Compaction internal error: start segment not found in index')
	}
	end_index := index.log_segments.index(segments[segments.len - 1])
	if end_index < 0 {
		return error('Compaction internal error: end segment not found in index')
	}

	// New list of log segments (excluding merged ones)
	mut new_log_segments := []LogSegment{}

	// Segments before the merged block
	if start_index > 0 {
		new_log_segments << index.log_segments[0..start_index]
	}

	// Add the new merged segment
	new_log_segments << LogSegment{
		start_offset: new_start_offset
		end_offset:   new_end_offset
		key:          new_key
		size_bytes:   i64(merged_data.len)
		created_at:   time.now()
	}

	// Segments after the merged block
	if end_index < index.log_segments.len - 1 {
		new_log_segments << index.log_segments[end_index + 1..]
	}

	// Update index object
	index.log_segments = new_log_segments
	index_key := a.partition_index_key(topic, partition)

	// Atomically write the new index (overwrite old one)
	a.put_object(index_key, json.encode(index).bytes())!

	// 5. Delete old segments (Non-critical step after index update)
	for seg in segments {
		a.delete_object(seg.key) or {
			eprintln('[S3] Failed to delete old segment ${seg.key}: ${err}')
		}
	}
}
