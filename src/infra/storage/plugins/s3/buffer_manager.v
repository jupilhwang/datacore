// S3 Buffer Manager
// Handles buffer management and flush operations for S3 storage
module s3

import json
import strconv
import time

// TopicPartitionBuffer holds records for a specific partition before flushing to S3
struct TopicPartitionBuffer {
mut:
	records            []StoredRecord
	current_size_bytes i64 // Current total size of all records in this buffer
}

// async_flush_partition performs the S3 put and index update for a single partition batch.
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

	// 1. Acquire lock, copy buffer, and clear buffer in memory
	a.buffer_lock.lock()

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

	// 2. Calculate offsets for batch
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// 3. Encode and Write segment to S3
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// Write segment to S3
	a.put_object(segment_key, segment_data) or {
		// Failure means data loss if not retried. For now, log and return error.
		eprintln('[S3] ASYNC FLUSH FAILED for ${partition_key}: Segment put failed: ${err}')
		return error('Segment put failed during async flush: ${err}')
	}

	// 4. Update partition index with new segment (MUST be atomic/safe from concurrent updates)

	// Get the current index from S3 directly (bypass cache) to ensure we have the latest persisted state
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

	// Update index with new segment
	// The segment is added if it extends beyond the current S3 high_watermark
	// This is based on S3 persisted state, not in-memory cache
	if base_offset >= index.high_watermark {
		// High watermark is calculated based on the data that has been successfully stored to S3.
		index.high_watermark = end_offset + 1 // New high watermark
		index.log_segments << LogSegment{
			start_offset: base_offset
			end_offset:   end_offset
			key:          segment_key
			size_bytes:   i64(segment_data.len)
			created_at:   time.now()
		}

		// Write updated index to S3
		a.put_object(index_key, json.encode(index).bytes()) or {
			eprintln('[S3] ASYNC FLUSH FAILED for ${partition_key}: Index put failed: ${err}')
			return error('Index put failed during async flush: ${err}')
		}

		// Update local cache with new index (preserving the higher high_watermark from in-memory)
		cache_key := '${topic}:${partition}'
		a.topic_lock.@lock()
		if cached := a.topic_index_cache[cache_key] {
			// Keep the higher high_watermark between cache and S3
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
		a.topic_lock.unlock()
	} else {
		// This segment overlaps with already stored data, skip updating index
		eprintln('[S3] ASYNC FLUSH WARNING: Segment base_offset ${base_offset} < high_watermark ${index.high_watermark}. Index not updated.')
	}
}

// flush_worker periodically flushes all buffers that have accumulated data.
// Uses sequential processing per partition to avoid index conflicts.
fn (mut a S3StorageAdapter) flush_worker() {
	for a.compactor_running { // Use compactor_running flag to stop both workers
		time.sleep(a.config.batch_timeout_ms)

		// Process each partition's buffer while holding the lock
		// This prevents race conditions where append modifies the buffer
		// between collecting keys and flushing
		a.buffer_lock.lock()

		// Collect all partition keys that have data
		mut keys_to_flush := []string{}
		for key, tp_buffer in a.topic_partition_buffers {
			if tp_buffer.records.len > 0 {
				keys_to_flush << key
			}
		}

		// For each key, extract buffer data while still holding the lock
		mut flush_batches := map[string][]StoredRecord{}
		for key in keys_to_flush {
			if mut tp_buffer := a.topic_partition_buffers[key] {
				if tp_buffer.records.len > 0 {
					// Clone and clear the buffer atomically
					flush_batches[key] = tp_buffer.records.clone()
					tp_buffer.records.clear()
					tp_buffer.current_size_bytes = 0
					a.topic_partition_buffers[key] = tp_buffer
				}
			}
		}

		a.buffer_lock.unlock()

		// Now flush each batch to S3 (without holding the lock)
		for key, buffer_data in flush_batches {
			if buffer_data.len == 0 {
				continue
			}
			a.flush_buffer_to_s3(key, buffer_data) or {
				eprintln('[S3] Flush failed for ${key}: ${err}')
				// On failure, restore the buffer data to prevent data loss
				a.buffer_lock.lock()
				if mut tp_buffer := a.topic_partition_buffers[key] {
					// Prepend the failed data to the existing buffer
					mut restored := buffer_data.clone()
					restored << tp_buffer.records
					tp_buffer.records = restored
					// Recalculate size
					mut size := i64(0)
					for rec in tp_buffer.records {
						size += i64(rec.value.len + rec.key.len + 30)
					}
					tp_buffer.current_size_bytes = size
					a.topic_partition_buffers[key] = tp_buffer
				} else {
					// Buffer was deleted, recreate it
					mut size := i64(0)
					for rec in buffer_data {
						size += i64(rec.value.len + rec.key.len + 30)
					}
					a.topic_partition_buffers[key] = TopicPartitionBuffer{
						records:            buffer_data.clone()
						current_size_bytes: size
					}
				}
				a.buffer_lock.unlock()
				eprintln('[S3] Buffer restored for ${key} with ${buffer_data.len} records')
			}
		}
	}
}

// flush_buffer_to_s3 flushes a specific buffer batch to S3
fn (mut a S3StorageAdapter) flush_buffer_to_s3(partition_key string, buffer_data []StoredRecord) ! {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// Calculate offsets for batch
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// Encode and Write segment to S3
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// Write segment to S3
	a.put_object(segment_key, segment_data) or {
		eprintln('[S3] FLUSH FAILED for ${partition_key}: Segment put failed: ${err}')
		return error('Segment put failed during flush: ${err}')
	}

	// Update partition index with new segment
	// Use lock to prevent concurrent index updates from corrupting the index
	a.index_update_lock.lock()
	defer {
		a.index_update_lock.unlock()
	}

	// Get the current index from S3 directly (bypass cache)
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

	// Always add the segment if it doesn't already exist
	// Check for duplicate by comparing segment key
	mut segment_exists := false
	for seg in index.log_segments {
		if seg.key == segment_key {
			segment_exists = true
			break
		}
	}

	if !segment_exists {
		index.log_segments << LogSegment{
			start_offset: base_offset
			end_offset:   end_offset
			key:          segment_key
			size_bytes:   i64(segment_data.len)
			created_at:   time.now()
		}

		// Sort segments by start_offset to maintain order
		index.log_segments.sort(a.start_offset < b.start_offset)

		// Update high_watermark to the maximum end_offset + 1
		for seg in index.log_segments {
			if seg.end_offset + 1 > index.high_watermark {
				index.high_watermark = seg.end_offset + 1
			}
		}

		// Write updated index to S3
		a.put_object(index_key, json.encode(index).bytes()) or {
			eprintln('[S3] FLUSH FAILED for ${partition_key}: Index put failed: ${err}')
			return error('Index put failed during flush: ${err}')
		}

		// Update local cache
		cache_key := '${topic}:${partition}'
		a.topic_lock.@lock()
		if cached := a.topic_index_cache[cache_key] {
			// Keep the higher high_watermark between cache and S3
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
		a.topic_lock.unlock()
	}
}
