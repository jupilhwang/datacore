// Infra Layer - S3 Compaction Logic
// Segment compaction and merge processing for S3 storage
module s3

import domain
import json
import time
import sync.stdatomic
import infra.observability

// Compaction worker configuration constants
const max_consecutive_failures = 5
const failure_backoff_duration = 5 * time.minute
const compaction_sequential_threshold = 3
const max_compaction_concurrent = 10
const compaction_size_threshold_divisor = 2

/// SegmentDownloadResult holds one segment's downloaded data along with its
/// original index so that parallel downloads can be reassembled in order.
struct SegmentDownloadResult {
	index     int
	data      []u8
	error_msg string
}

/// compaction_worker periodically checks for segments to merge and performs compaction.
fn (mut a S3StorageAdapter) compaction_worker() {
	defer {
		a.worker_wg.done()
	}
	mut consecutive_failures := 0

	for stdatomic.load_i64(&a.is_running_flag) == 1 {
		time.sleep(a.config.compaction_interval_ms * time.millisecond)

		observability.log_with_context('s3', .debug, 'Compaction', 'Starting compaction cycle',
			{})

		a.compact_all_partitions() or {
			consecutive_failures++
			observability.log_with_context('s3', .error, 'Compaction', 'Compaction cycle failed',
				{
				'consecutive_failures': consecutive_failures.str()
				'max_failures':         max_consecutive_failures.str()
				'error':                err.msg()
			})

			// Too many consecutive failures; increase backoff
			if consecutive_failures >= max_consecutive_failures {
				observability.log_with_context('s3', .warn, 'Compaction', 'Too many consecutive failures, backing off',
					{
					'backoff_duration': failure_backoff_duration.str()
				})
				time.sleep(failure_backoff_duration)
				consecutive_failures = 0
			}
			continue
		}

		// Reset counter on success
		if consecutive_failures > 0 {
			observability.log_with_context('s3', .info, 'Compaction', 'Compaction succeeded after failures',
				{
				'previous_failures': consecutive_failures.str()
			})
			consecutive_failures = 0
		}
	}
}

/// compact_all_partitions iterates over all topics and partitions and attempts to merge small segments.
/// Optimizes performance through parallel processing.
fn (mut a S3StorageAdapter) compact_all_partitions() ! {
	topics := a.list_topics()!
	partition_keys := a.collect_partition_keys(topics)

	if partition_keys.len <= compaction_sequential_threshold {
		a.compact_partitions_sequential(partition_keys)
	} else {
		a.compact_partitions_parallel(partition_keys)
	}
}

/// collect_partition_keys builds a list of "topic:partition" keys from topics.
fn (a &S3StorageAdapter) collect_partition_keys(topics []domain.TopicMetadata) []string {
	mut keys := []string{}
	for t in topics {
		for p in 0 .. t.partition_count {
			keys << '${t.name}:${p}'
		}
	}
	return keys
}

/// compact_partitions_sequential compacts partitions one at a time.
fn (mut a S3StorageAdapter) compact_partitions_sequential(partition_keys []string) {
	for key in partition_keys {
		topic, partition := parse_partition_key(key) or { continue }
		a.compact_partition(topic, partition) or {
			observability.log_with_context('s3', .error, 'Compaction', 'Partition compaction failed',
				{
				'partition_key': key
				'error':         err.msg()
			})
		}
	}
}

/// compact_partitions_parallel compacts partitions concurrently with bounded parallelism.
fn (mut a S3StorageAdapter) compact_partitions_parallel(partition_keys []string) {
	max_concurrent := max_compaction_concurrent
	ch := chan bool{cap: max_concurrent}
	mut active := 0

	for key in partition_keys {
		for active >= max_concurrent {
			_ = <-ch
			active--
		}
		active++
		spawn fn [mut a, key, ch] () {
			topic, partition := parse_partition_key(key) or {
				ch <- true
				return
			}
			a.compact_partition(topic, partition) or {
				observability.log_with_context('s3', .error, 'Compaction', 'Partition compaction failed',
					{
					'partition_key': key
					'error':         err.msg()
				})
			}
			ch <- true
		}()
	}

	for _ in 0 .. active {
		_ = <-ch
	}
}

/// identify_segments_to_compact finds segments eligible for merging.
/// Returns compactable segments and their total size, or empty list
/// if not enough segments qualify.
fn (a &S3StorageAdapter) identify_segments_to_compact(index PartitionIndex) ([]LogSegment, i64) {
	mut segments := []LogSegment{}
	mut total_size := i64(0)

	for seg in index.log_segments {
		if total_size >= a.config.target_segment_bytes {
			break
		}
		if seg.size_bytes < a.config.target_segment_bytes {
			segments << seg
			total_size += seg.size_bytes
		}
	}

	if segments.len < a.min_segment_count_to_compact
		|| total_size < a.config.target_segment_bytes / compaction_size_threshold_divisor {
		return []LogSegment{}, i64(0)
	}
	return segments, total_size
}

/// try_compact_with_server_side performs server-side compaction with fallback to traditional merge.
fn (mut a S3StorageAdapter) try_compact_with_server_side(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	a.merge_segments_server_side(topic, partition, mut index, segments) or {
		err_msg := err.msg()
		if err_msg.contains('server_side_copy_unsupported') {
			observability.log_with_context('s3', .debug, 'Compaction', 'Server-side copy unsupported, using traditional merge',
				{
				'topic':     topic
				'partition': partition.str()
			})
		} else {
			observability.log_with_context('s3', .warn, 'Compaction', 'Server-side copy failed, falling back to traditional merge',
				{
				'topic':     topic
				'partition': partition.str()
				'error':     err_msg
			})
		}
		a.merge_segments(topic, partition, mut index, segments)!
	}
}

/// compact_partition compacts segments of a specific partition.
fn (mut a S3StorageAdapter) compact_partition(topic string, partition int) ! {
	start_time := time.now()
	a.record_compaction_start()

	mut index := a.get_partition_index(topic, partition)!
	segments_to_compact, total_size := a.identify_segments_to_compact(index)
	if segments_to_compact.len == 0 {
		return
	}

	if a.config.use_server_side_copy {
		a.try_compact_with_server_side(topic, partition, mut index, segments_to_compact) or {
			a.record_compaction_error()
			return err
		}
	} else {
		a.merge_segments(topic, partition, mut index, segments_to_compact) or {
			a.record_compaction_error()
			return err
		}
	}

	a.record_compaction_success(start_time, total_size)
}

/// merge_segments merges multiple segments into a single large segment.
/// Optimizes performance by downloading segments in parallel.
fn (mut a S3StorageAdapter) merge_segments(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	if segments.len == 0 {
		return
	}

	// 1. Download segments in parallel
	merged_data := a.download_segments_parallel(segments)!

	// 2. Create and upload new segment
	new_segment := a.create_merged_segment(topic, partition, segments, merged_data)!

	// 3. Update index
	a.update_index_with_merged_segment(topic, partition, mut index, segments, new_segment)!

	// 4. Delete old segments in parallel
	a.delete_segments_parallel(segments)
}

/// download_segments_parallel downloads multiple segments in parallel.
/// Uses a pre-allocated indexed results array so that each goroutine writes
/// to its own slot, preserving the original segment order without sorting.
fn (mut a S3StorageAdapter) download_segments_parallel(segments []LogSegment) ![]u8 {
	ch := chan SegmentDownloadResult{cap: segments.len}

	for i, seg in segments {
		spawn fn [mut a, seg, ch, i] () {
			data, _ := a.get_object(seg.key, -1, -1) or {
				observability.log_with_context('s3', .error, 'Compaction', 'Failed to download segment',
					{
					'segment_key': seg.key
					'error':       err.msg()
				})
				ch <- SegmentDownloadResult{
					index:     i
					data:      []u8{}
					error_msg: err.msg()
				}
				return
			}
			ch <- SegmentDownloadResult{
				index: i
				data:  data
			}
		}()
	}

	// Collect results into pre-allocated indexed array for order preservation
	mut results := [][]u8{len: segments.len}
	mut download_errors := []string{}

	for _ in 0 .. segments.len {
		result := <-ch
		if result.error_msg.len > 0 {
			download_errors << result.error_msg
		} else {
			results[result.index] = result.data
		}
	}

	if download_errors.len > 0 {
		return error('Failed to download ${download_errors.len} segments')
	}

	// Merge in original segment order (index 0, 1, 2, ...)
	mut merged_data := []u8{}
	for segment_data in results {
		merged_data << segment_data
	}

	return merged_data
}

/// create_merged_segment creates a new segment with merged data and uploads it to S3.
fn (mut a S3StorageAdapter) create_merged_segment(topic string, partition int, segments []LogSegment, merged_data []u8) !LogSegment {
	new_start_offset := segments[0].start_offset
	new_end_offset := segments[segments.len - 1].end_offset
	new_key := a.log_segment_key(topic, partition, new_start_offset, new_end_offset)

	// Upload new merged segment to S3
	a.put_object(new_key, merged_data)!

	return LogSegment{
		start_offset: new_start_offset
		end_offset:   new_end_offset
		key:          new_key
		size_bytes:   i64(merged_data.len)
		created_at:   time.now()
	}
}

/// update_index_with_merged_segment updates the index to reflect the merged segment.
fn (mut a S3StorageAdapter) update_index_with_merged_segment(topic string, partition int, mut index PartitionIndex, old_segments []LogSegment, new_segment LogSegment) ! {
	// Find the offset range covered by the merged segment
	start_index := index.log_segments.index(old_segments[0])
	if start_index < 0 {
		return error('Compaction internal error: start segment not found in index')
	}
	end_index := index.log_segments.index(old_segments[old_segments.len - 1])
	if end_index < 0 {
		return error('Compaction internal error: end segment not found in index')
	}

	// New log segment list (excluding merged ones)
	mut new_log_segments := []LogSegment{}

	// Segments before the merge block
	if start_index > 0 {
		new_log_segments << index.log_segments[0..start_index]
	}

	// Add newly merged segment
	new_log_segments << new_segment

	// Segments after the merge block
	if end_index < index.log_segments.len - 1 {
		new_log_segments << index.log_segments[end_index + 1..]
	}

	// Update index object
	index.log_segments = new_log_segments
	index_key := a.partition_index_key(topic, partition)

	// Atomically write new index (overwriting old one)
	a.put_object(index_key, json.encode(index).bytes())!
}

/// delete_segments_parallel deletes multiple segments using S3 Multi-Object Delete API.
/// Falls back to individual delete_object calls on batch API failure.
fn (mut a S3StorageAdapter) delete_segments_parallel(segments []LogSegment) {
	if segments.len == 0 {
		return
	}

	mut keys := []string{cap: segments.len}
	for seg in segments {
		keys << seg.key
	}

	a.delete_objects_batch(keys) or {
		observability.log_with_context('s3', .warn, 'Compaction', 'Batch delete failed, falling back to individual deletes',
			{
			'error':         err.msg()
			'segment_count': segments.len.str()
		})
		// Fallback: individual deletes
		for seg in segments {
			a.delete_object(seg.key) or {
				observability.log_with_context('s3', .error, 'Compaction', 'Failed to delete old segment',
					{
					'segment_key': seg.key
					'error':       err.msg()
				})
			}
		}
	}
}
