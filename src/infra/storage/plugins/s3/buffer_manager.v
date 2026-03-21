// Infra Layer - S3 Buffer Manager
// Buffer management and flush operation handling for S3 storage
module s3

import strconv
import time
import sync.stdatomic
import infra.observability

/// TopicPartitionBuffer holds records for a specific partition before flushing to S3.
/// Reduces S3 API call count and optimizes performance through batch writes.
struct TopicPartitionBuffer {
mut:
	records            []StoredRecord
	current_size_bytes i64
}

/// FlushBatch represents a batch of records for a single partition to be flushed to S3.
/// Used as a struct array to eliminate map lookup overhead.
struct FlushBatch {
	key     string
	records []StoredRecord
}

/// collect_flush_batches extracts flush-ready batches from partition buffers.
/// Applies min_flush_bytes threshold: skips partitions below the threshold
/// unless max_flush_skip_count consecutive skips have occurred.
/// Returns (flush_batches, skipped_count).
/// Caller must hold buffer_lock.
fn (mut a S3StorageAdapter) collect_flush_batches() ([]FlushBatch, int) {
	mut flush_batches := []FlushBatch{}
	mut skipped := 0
	min_bytes := a.config.min_flush_bytes
	max_skips := a.config.max_flush_skip_count

	for key, _ in a.topic_partition_buffers {
		if mut tp_buffer := a.topic_partition_buffers[key] {
			if tp_buffer.records.len > 0 {
				current_skips := a.flush_skip_counts[key] or { 0 }

				// Skip when: threshold enabled, buffer below threshold, and not exceeded max skips
				if min_bytes > 0 && tp_buffer.current_size_bytes < i64(min_bytes)
					&& current_skips < max_skips {
					a.flush_skip_counts[key] = current_skips + 1
					skipped++
					continue
				}

				flush_batches << FlushBatch{
					key:     key
					records: tp_buffer.records.clone()
				}
				tp_buffer.records.clear()
				tp_buffer.current_size_bytes = 0
				a.topic_partition_buffers[key] = tp_buffer
				a.flush_skip_counts[key] = 0
			}
		}
	}
	return flush_batches, skipped
}

/// async_flush_partition performs S3 put and index update for a single partition batch.
/// This function is called asynchronously and saves buffered records as an S3 segment.
/// Note: Currently only called from flush_worker; direct calls are disabled.
fn (mut a S3StorageAdapter) async_flush_partition(partition_key string) ! {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// 1. Acquire lock, copy buffer, clear in-memory buffer
	a.buffer_lock.@lock()

	mut tp_buffer := a.topic_partition_buffers[partition_key] or {
		a.buffer_lock.unlock()
		return
	}

	if tp_buffer.records.len == 0 {
		a.buffer_lock.unlock()
		return
	}

	buffer_data := tp_buffer.records.clone()
	tp_buffer.records.clear()
	tp_buffer.current_size_bytes = 0
	a.topic_partition_buffers[partition_key] = tp_buffer
	a.buffer_lock.unlock()

	// 2. Calculate offsets for the batch
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// 3. Encode segment and write to S3
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// Write segment to S3
	a.put_object(segment_key, segment_data) or {
		// Without retry on failure, data will be lost. Currently log and return error.
		observability.log_with_context('s3', .error, 'AsyncFlush', 'Segment put failed',
			{
			'partition_key': partition_key
			'segment_key':   segment_key
			'error':         err.msg()
		})
		return error('Segment put failed during async flush: ${err}')
	}

	// 4. Update partition index with new segment
	a.update_partition_index_with_segment(topic, partition, segment_key, base_offset,
		end_offset, segment_data.len) or {
		observability.log_with_context('s3', .error, 'AsyncFlush', 'Index update failed',
			{
			'partition_key': partition_key
			'segment_key':   segment_key
			'error':         err.msg()
		})
		return error('Index update failed during async flush: ${err}')
	}
}

/// flush_worker periodically flushes message, offset, and pending index buffers.
/// Runs at batch_timeout_ms intervals and stops when is_running_flag becomes 0.
fn (mut a S3StorageAdapter) flush_worker() {
	defer {
		a.worker_wg.done()
	}
	mut index_flush_elapsed_ms := 0
	for stdatomic.load_i64(&a.is_running_flag) == 1 {
		time.sleep(a.config.batch_timeout_ms * time.millisecond)
		index_flush_elapsed_ms += a.config.batch_timeout_ms

		// Dispatch messages and offsets in parallel
		go a.flush_pending_messages()
		go a.flush_pending_offsets()

		// Check if pending index updates need time-based forced flush
		if a.config.index_flush_interval_ms > 0
			&& index_flush_elapsed_ms >= a.config.index_flush_interval_ms {
			index_flush_elapsed_ms = 0
			go a.flush_all_pending_indexes()
		}
	}
}

/// flush_pending_messages flushes the message buffer to S3.
/// Applies min_flush_bytes threshold to skip small buffers and prevent micro-segments.
fn (mut a S3StorageAdapter) flush_pending_messages() {
	// Collect flush-ready batches while holding the lock
	a.buffer_lock.@lock()
	flush_batches, _ := a.collect_flush_batches()
	a.buffer_lock.unlock()

	// Flush each batch to S3 (without holding lock)
	for batch in flush_batches {
		if batch.records.len == 0 {
			continue
		}
		a.flush_buffer_to_s3(batch.key, batch.records) or {
			observability.log_with_context('s3', .error, 'FlushWorker', 'Flush failed',
				{
				'partition_key': batch.key
				'record_count':  batch.records.len.str()
				'error':         err.msg()
			})
			a.restore_failed_message_buffer(batch.key, batch.records)
			observability.log_with_context('s3', .warn, 'FlushWorker', 'Buffer restored after flush failure',
				{
				'partition_key': batch.key
				'record_count':  batch.records.len.str()
			})
		}
	}
}

/// restore_failed_message_buffer restores the message buffer on flush failure.
/// Prepends failed records to existing buffer to preserve order.
fn (mut a S3StorageAdapter) restore_failed_message_buffer(key string, buffer_data []StoredRecord) {
	a.buffer_lock.@lock()
	defer { a.buffer_lock.unlock() }

	if mut tp_buffer := a.topic_partition_buffers[key] {
		old_records := tp_buffer.records
		tp_buffer.records = []StoredRecord{cap: buffer_data.len + old_records.len}
		tp_buffer.records << buffer_data
		tp_buffer.records << old_records
		mut size := i64(0)
		for rec in tp_buffer.records {
			size += i64(rec.value.len + rec.key.len + record_overhead_bytes)
		}
		tp_buffer.current_size_bytes = size
		a.topic_partition_buffers[key] = tp_buffer
	} else {
		mut size := i64(0)
		for rec in buffer_data {
			size += i64(rec.value.len + rec.key.len + record_overhead_bytes)
		}
		a.topic_partition_buffers[key] = TopicPartitionBuffer{
			records:            buffer_data
			current_size_bytes: size
		}
	}
}

/// flush_buffer_to_s3 flushes a specific buffer batch to S3.
/// Saves the segment to S3 and updates the partition index.
/// Uses index_update_lock to prevent index corruption from concurrent updates.
fn (mut a S3StorageAdapter) flush_buffer_to_s3(partition_key string, buffer_data []StoredRecord) ! {
	start_time := time.now()

	// Metric: flush start
	a.metrics_lock.@lock()
	a.metrics.flush_count++
	a.metrics_lock.unlock()

	parts := partition_key.split(':')
	if parts.len != 2 {
		// Metric: flush failure
		a.metrics_lock.@lock()
		a.metrics.flush_error_count++
		a.metrics_lock.unlock()
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		// Metric: flush failure
		a.metrics_lock.@lock()
		a.metrics.flush_error_count++
		a.metrics_lock.unlock()
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// Calculate offsets for the batch
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// Encode segment and write to S3
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// Write segment to S3
	a.put_object(segment_key, segment_data) or {
		observability.log_with_context('s3', .error, 'Flush', 'Segment put failed', {
			'partition_key': partition_key
			'segment_key':   segment_key
			'error':         err.msg()
		})
		// Metric: flush failure
		a.metrics_lock.@lock()
		a.metrics.flush_error_count++
		a.metrics.s3_error_count++
		a.metrics_lock.unlock()
		return error('Segment put failed during flush: ${err}')
	}

	// Metric: S3 PUT success
	a.metrics_lock.@lock()
	a.metrics.s3_put_count++
	a.metrics_lock.unlock()

	// Update partition index with new segment
	// Use lock to prevent index corruption from concurrent updates
	a.index_update_lock.lock()
	defer {
		a.index_update_lock.unlock()
	}

	a.update_partition_index_with_segment(topic, partition, segment_key, base_offset,
		end_offset, segment_data.len) or {
		observability.log_with_context('s3', .error, 'Flush', 'Index update failed', {
			'partition_key': partition_key
			'segment_key':   segment_key
			'error':         err.msg()
		})
		// Metric: flush failure
		a.metrics_lock.@lock()
		a.metrics.flush_error_count++
		a.metrics_lock.unlock()
		return error('Index update failed during flush: ${err}')
	}

	// Metric: flush success
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.flush_success_count++
	a.metrics.flush_total_ms += elapsed_ms
	a.metrics_lock.unlock()
}

/// update_partition_index_with_segment adds a new segment to the partition index.
/// When index_batch_size > 1, accumulates segments in-memory and flushes to S3
/// only when the batch threshold is reached. When index_batch_size <= 1,
/// writes immediately (preserving original behavior).
fn (mut a S3StorageAdapter) update_partition_index_with_segment(topic string, partition int, segment_key string, base_offset i64, end_offset i64, segment_size int) ! {
	partition_key := '${topic}:${partition}'
	new_segment := LogSegment{
		start_offset: base_offset
		end_offset:   end_offset
		key:          segment_key
		size_bytes:   i64(segment_size)
		created_at:   time.now()
	}

	if a.config.index_batch_size <= 1 {
		// Immediate mode: write to S3 directly (original behavior)
		a.write_index_with_segments(topic, partition, [new_segment])!
		return
	}

	// Batch mode: accumulate in pending buffer
	a.index_flush_lock.lock()
	a.add_pending_index_segment(partition_key, new_segment)
	should_flush := a.should_flush_index(partition_key)
	mut segments_to_flush := []LogSegment{}
	if should_flush {
		segments_to_flush = a.drain_pending_index_segments(partition_key)
	}
	a.index_flush_lock.unlock()

	if should_flush && segments_to_flush.len > 0 {
		a.write_index_with_segments(topic, partition, segments_to_flush)!
	}
}
