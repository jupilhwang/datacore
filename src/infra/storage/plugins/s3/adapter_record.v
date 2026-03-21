// Record operations (append, fetch, delete) for S3StorageAdapter.
module s3

import domain
import time
import json
import sync
import infra.observability

/// get_partition_append_lock returns the per-partition mutex for offset reservation,
/// creating one if it does not yet exist. The map-level mutex is held only for
/// lookup/insertion; the returned mutex itself is used by the caller.
fn (mut a S3StorageAdapter) get_partition_append_lock(partition_key string) &sync.Mutex {
	a.partition_append_mu.lock()
	defer { a.partition_append_mu.unlock() }
	if mtx := a.partition_append_locks[partition_key] {
		return mtx
	}
	mtx := &sync.Mutex{}
	a.partition_append_locks[partition_key] = mtx
	return mtx
}

/// reserve_offsets atomically reads the current high_watermark for a partition
/// and advances it by `count`, returning the old value as the base offset.
/// This prevents concurrent appends from receiving overlapping offset ranges.
fn (mut a S3StorageAdapter) reserve_offsets(partition_key string, count int) i64 {
	a.topic_lock.@lock()
	defer { a.topic_lock.unlock() }
	mut base := i64(0)
	if cached := a.topic_index_cache[partition_key] {
		base = cached.index.high_watermark
		mut updated_index := cached.index
		updated_index.high_watermark = base + i64(count)
		a.topic_index_cache[partition_key] = CachedPartitionIndex{
			index:     updated_index
			etag:      cached.etag
			cached_at: cached.cached_at
		}
	}
	return base
}

/// append appends records to a partition.
/// Selects sync/async path based on required_acks:
///   acks=0: appends to in-memory buffer and returns immediately (best-effort)
///   acks=1/-1: returns after S3 PUT + index update completes (durability guaranteed)
pub fn (mut a S3StorageAdapter) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	if records.len == 0 {
		return domain.AppendResult{
			base_offset:      0
			log_append_time:  time.now().unix_milli()
			log_start_offset: 0
		}
	}

	// 1. Fetch current partition index (populates cache, provides earliest_offset)
	mut index := a.get_partition_index(topic, partition)!
	partition_key := '${topic}:${partition}'

	// 2. Reserve offsets atomically under per-partition lock.
	// This prevents concurrent appends from reading the same high_watermark
	// and producing overlapping offset ranges.
	mut p_lock := a.get_partition_append_lock(partition_key)
	p_lock.lock()
	base_offset := a.reserve_offsets(partition_key, records.len)
	p_lock.unlock()

	// 3. Create StoredRecords with reserved offsets
	mut bytes_to_add := i64(0)
	mut stored_records := []StoredRecord{}

	for i, rec in records {
		srec := StoredRecord{
			offset:    base_offset + i64(i)
			timestamp: if rec.timestamp.unix_milli() == 0 { time.now() } else { rec.timestamp }
			key:       rec.key
			value:     rec.value
			headers:   rec.headers
		}
		stored_records << srec
		bytes_to_add += i64(srec.value.len + srec.key.len + record_overhead_bytes)
	}

	if required_acks == 0 {
		// === acks=0: async path (existing behavior) ===
		// Append to in-memory buffer and return immediately.
		// flush_worker drains the buffer every batch_timeout_ms automatically.
		a.buffer_lock.lock()
		mut tp_buffer := a.topic_partition_buffers[partition_key] or {
			TopicPartitionBuffer{
				records:            []
				current_size_bytes: 0
			}
		}

		tp_buffer.records << stored_records
		tp_buffer.current_size_bytes += bytes_to_add

		a.topic_partition_buffers[partition_key] = tp_buffer
		a.buffer_lock.unlock()
	} else {
		// === acks=1/-1: sync path (durability guarantee) ===
		if a.config.sync_linger_ms > 0 {
			// Linger path: batch multiple sync requests within the linger window
			ch := chan LingerResult{cap: 1}
			a.sync_linger_lock.lock()
			should_flush := a.add_to_sync_linger_buffer(partition_key, stored_records,
				ch)
			mut flush_buf := SyncLingerBuffer{}
			if should_flush {
				flush_buf = a.drain_sync_linger_buffer(partition_key)
			}
			a.sync_linger_lock.unlock()

			if should_flush && flush_buf.records.len > 0 {
				a.flush_sync_linger_buffer(topic, partition, flush_buf)
			}

			// Wait for result from linger flush
			result := <-ch
			if err_val := result.err {
				return error('durable append failed (acks=${required_acks}): ${err_val}')
			}
		} else {
			// Immediate path: write directly to S3 (original behavior)
			a.sync_append_immediate(topic, partition, stored_records, required_acks)!
		}
	}

	// Append records to Iceberg table (when Iceberg is enabled)
	if a.is_iceberg_enabled() {
		a.append_to_iceberg(topic, partition, records, base_offset) or {
			observability.log_with_context('s3', .warn, 'IcebergAppend', 'Failed to append to Iceberg',
				{
				'topic':     topic
				'partition': partition.str()
				'error':     err.str()
			})
		}
	}

	// high_watermark is already advanced by reserve_offsets above;
	// no duplicate cache update needed here.

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  time.now().unix_milli()
		log_start_offset: index.earliest_offset
	}
}

/// fetch retrieves records from a partition.
pub fn (mut a S3StorageAdapter) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	partition_key := '${topic}:${partition}'

	// Fetch S3 index for segment information
	index := a.get_partition_index(topic, partition)!

	if offset < index.earliest_offset {
		return error('Offset out of range (too old): ${offset} < ${index.earliest_offset}')
	}

	// Find relevant segments
	mut all_records := []domain.Record{}
	mut bytes_read := 0
	mut highest_offset_read := offset - 1

	// 1. Try reading from S3 segments first
	for seg in index.log_segments {
		if seg.end_offset < offset {
			continue
		}
		if seg.start_offset > offset + i64(max_bytes / fetch_offset_estimate_divisor) {
			break
		}

		// Fetch segment from S3
		// Optimization: use Range Request when reading from start of segment
		mut data := []u8{}
		if offset == seg.start_offset && max_bytes > 0 {
			mut fetch_size := i64(max_bytes) * fetch_size_multiplier
			if fetch_size > seg.size_bytes {
				fetch_size = -1
			}
			range_end := if fetch_size > 0 { fetch_size } else { -1 }
			data, _ = a.get_object(seg.key, 0, range_end) or { continue }
		} else {
			// Random access without index: must download full segment
			data, _ = a.get_object(seg.key, -1, -1) or { continue }
		}

		stored_records := decode_stored_records(data)

		for rec in stored_records {
			if rec.offset >= offset && bytes_read < max_bytes {
				// Convert StoredRecord to domain.Record
				all_records << domain.Record{
					key:       rec.key
					value:     rec.value
					headers:   rec.headers
					timestamp: rec.timestamp
				}
				bytes_read += rec.value.len + rec.key.len
				if rec.offset > highest_offset_read {
					highest_offset_read = rec.offset
				}
			}
		}

		if bytes_read >= max_bytes {
			break
		}
	}

	// 2. Also read from in-memory buffer (data not yet flushed to S3)
	// Important for data not yet persisted
	if bytes_read < max_bytes {
		a.buffer_lock.lock()
		if tp_buffer := a.topic_partition_buffers[partition_key] {
			for rec in tp_buffer.records {
				// Read records at or above requested offset not already read from S3 segments
				if rec.offset >= offset && rec.offset > highest_offset_read
					&& bytes_read < max_bytes {
					all_records << domain.Record{
						key:       rec.key
						value:     rec.value
						headers:   rec.headers
						timestamp: rec.timestamp
					}
					bytes_read += rec.value.len + rec.key.len
					if rec.offset > highest_offset_read {
						highest_offset_read = rec.offset
					}
				}
			}
		}
		a.buffer_lock.unlock()
	}

	// Offset of the first record actually returned
	actual_first_offset := if all_records.len > 0 { offset } else { index.high_watermark }

	return domain.FetchResult{
		records:            all_records
		first_offset:       actual_first_offset
		high_watermark:     index.high_watermark
		last_stable_offset: index.high_watermark
		log_start_offset:   index.earliest_offset
	}
}

/// delete_records deletes records before the specified offset.
pub fn (mut a S3StorageAdapter) delete_records(topic string, partition int, before_offset i64) ! {
	mut index := a.get_partition_index(topic, partition)!

	// Find segments to delete
	mut segments_to_delete := []string{}
	mut remaining_segments := []LogSegment{}

	for seg in index.log_segments {
		if seg.end_offset < before_offset {
			segments_to_delete << seg.key
		} else {
			remaining_segments << seg
		}
	}

	// Delete segments from S3 using batch API
	if segments_to_delete.len > 0 {
		a.delete_objects_batch(segments_to_delete) or {
			observability.log_with_context('s3', .warn, 'S3Client', 'Batch delete failed in delete_records, falling back',
				{
				'error': err.msg()
				'count': segments_to_delete.len.str()
			})
			// Fallback: individual deletes
			for key in segments_to_delete {
				a.delete_object(key) or {
					observability.log_with_context('s3', .warn, 'S3Client', 'delete_records: failed to delete segment ${key}',
						{
						'key':   key
						'error': err.msg()
					})
				}
			}
		}
	}

	// Update index
	index.earliest_offset = before_offset
	index.log_segments = remaining_segments

	index_key := a.partition_index_key(topic, partition)
	a.put_object(index_key, json.encode(index).bytes())!
}
