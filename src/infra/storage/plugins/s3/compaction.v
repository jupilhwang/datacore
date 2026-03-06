// Infra Layer - S3 Compaction Logic
// Segment compaction and merge processing for S3 storage
module s3

import json
import time
import infra.observability

// Compaction worker configuration constants
const max_consecutive_failures = 5
const failure_backoff_duration = 5 * time.minute
const compaction_sequential_threshold = 3
const max_compaction_concurrent = 10
const compaction_size_threshold_divisor = 2

/// compaction_worker periodically checks for segments to merge and performs compaction.
fn (mut a S3StorageAdapter) compaction_worker() {
	mut consecutive_failures := 0

	for a.compactor_running {
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

	// Collect list of partitions to compact
	mut partition_keys := []string{}
	for t in topics {
		for p in 0 .. t.partition_count {
			partition_keys << '${t.name}:${p}'
		}
	}

	// Parallel compaction (max 10 concurrent)
	if partition_keys.len <= compaction_sequential_threshold {
		// Small batch: sequential processing
		for key in partition_keys {
			parts := key.split(':')
			if parts.len == 2 {
				topic := parts[0]
				partition := parts[1].int()
				a.compact_partition(topic, partition) or {
					observability.log_with_context('s3', .error, 'Compaction', 'Partition compaction failed',
						{
						'partition_key': key
						'error':         err.msg()
					})
				}
			}
		}
	} else {
		// Large batch: parallel processing (using channels)
		// Limit channel buffer size to max_concurrent to control memory usage
		max_concurrent := max_compaction_concurrent
		ch := chan bool{cap: max_concurrent}
		mut active := 0

		for key in partition_keys {
			// Limit concurrent execution
			for active >= max_concurrent {
				_ = <-ch
				active--
			}

			active++
			spawn fn [mut a, key, ch] () {
				parts := key.split(':')
				if parts.len == 2 {
					topic := parts[0]
					partition := parts[1].int()
					a.compact_partition(topic, partition) or {
						observability.log_with_context('s3', .error, 'Compaction', 'Partition compaction failed',
							{
							'partition_key': key
							'error':         err.msg()
						})
					}
				}
				ch <- true
			}()
		}

		// Wait for all tasks to complete
		for _ in 0 .. active {
			_ = <-ch
		}
	}
}

/// compact_partition compacts segments of a specific partition.
fn (mut a S3StorageAdapter) compact_partition(topic string, partition int) ! {
	start_time := time.now()

	// Metric: compaction start
	a.metrics_lock.@lock()
	a.metrics.compaction_count++
	a.metrics_lock.unlock()

	// 1. Fetch current index
	mut index := a.get_partition_index(topic, partition)!

	// 2. Identify segments to compact
	mut segments_to_compact := []LogSegment{}
	mut total_size := i64(0)

	for seg in index.log_segments {
		if total_size >= a.config.target_segment_bytes {
			break
		}

		// Only consider segments smaller than target size
		if seg.size_bytes < a.config.target_segment_bytes {
			segments_to_compact << seg
			total_size += seg.size_bytes
		}
	}

	// Check if there are enough small segments
	if segments_to_compact.len < a.min_segment_count_to_compact
		|| total_size < a.config.target_segment_bytes / compaction_size_threshold_divisor {
		return
	}

	// 3. Perform compaction
	// Try server-side copy first if enabled, fall back to download-reupload
	if a.config.use_server_side_copy {
		a.merge_segments_server_side(topic, partition, mut index, segments_to_compact) or {
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
			// Fallback to traditional download-reupload merge
			a.merge_segments(topic, partition, mut index, segments_to_compact) or {
				a.metrics_lock.@lock()
				a.metrics.compaction_error_count++
				a.metrics_lock.unlock()
				return err
			}
		}
	} else {
		a.merge_segments(topic, partition, mut index, segments_to_compact) or {
			a.metrics_lock.@lock()
			a.metrics.compaction_error_count++
			a.metrics_lock.unlock()
			return err
		}
	}

	// Metric: compaction success
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.compaction_success_count++
	a.metrics.compaction_total_ms += elapsed_ms
	a.metrics.compaction_bytes_merged += total_size
	a.metrics_lock.unlock()
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
fn (mut a S3StorageAdapter) download_segments_parallel(segments []LogSegment) ![]u8 {
	ch := chan []u8{cap: segments.len}
	mut download_errors := []string{}

	for seg in segments {
		spawn fn [mut a, seg, ch] () {
			data, _ := a.get_object(seg.key, -1, -1) or {
				observability.log_with_context('s3', .error, 'Compaction', 'Failed to download segment',
					{
					'segment_key': seg.key
					'error':       err.msg()
				})
				ch <- []u8{}
				return
			}
			ch <- data
		}()
	}

	// Collect and merge download results
	mut merged_data := []u8{}
	for _ in 0 .. segments.len {
		data := <-ch
		if data.len == 0 {
			download_errors << 'segment download failed'
		} else {
			merged_data << data
		}
	}

	// Return error on download failure
	if download_errors.len > 0 {
		return error('Failed to download ${download_errors.len} segments')
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
