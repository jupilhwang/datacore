// Infra Layer - S3 Sync Linger Buffer
// Batches acks=1/-1 produce requests within a configurable linger window
// to reduce S3 PUT count by combining multiple requests into one segment.
module s3

import strconv
import time

/// LingerResult holds the outcome of a linger flush, delivered to all waiters.
struct LingerResult {
	err         ?IError
	base_offset i64
}

/// SyncLingerBuffer accumulates records from multiple sync produce requests
/// within a single linger window for a specific partition.
struct SyncLingerBuffer {
mut:
	records    []StoredRecord
	channels   []chan LingerResult
	created_at i64
	size_bytes i64
}

/// add_to_sync_linger_buffer appends records to the linger buffer for a partition.
/// Returns true if the buffer should be flushed immediately (batch_max_bytes exceeded).
/// Caller must hold sync_linger_lock (or be in a single-threaded test context).
fn (mut a S3StorageAdapter) add_to_sync_linger_buffer(partition_key string, records []StoredRecord, ch chan LingerResult) bool {
	mut bytes_added := i64(0)
	for rec in records {
		bytes_added += i64(rec.value.len + rec.key.len + record_overhead_bytes)
	}

	if partition_key in a.sync_linger_buffers {
		mut buf := a.sync_linger_buffers[partition_key]
		buf.records << records
		buf.channels << ch
		buf.size_bytes += bytes_added
		a.sync_linger_buffers[partition_key] = buf
	} else {
		a.sync_linger_buffers[partition_key] = SyncLingerBuffer{
			records:    records.clone()
			channels:   [ch]
			created_at: time.now().unix_milli()
			size_bytes: bytes_added
		}
	}

	buf := a.sync_linger_buffers[partition_key]
	return buf.size_bytes >= a.config.batch_max_bytes
}

/// drain_sync_linger_buffer extracts and removes the linger buffer for a partition.
/// Returns the drained buffer contents. Caller must hold sync_linger_lock (or test).
fn (mut a S3StorageAdapter) drain_sync_linger_buffer(partition_key string) SyncLingerBuffer {
	if buf := a.sync_linger_buffers[partition_key] {
		result := SyncLingerBuffer{
			records:    buf.records.clone()
			channels:   buf.channels.clone()
			created_at: buf.created_at
			size_bytes: buf.size_bytes
		}
		a.sync_linger_buffers.delete(partition_key)
		return result
	}
	return SyncLingerBuffer{}
}

/// get_expired_sync_linger_keys returns partition keys whose linger window has expired.
/// Caller must hold sync_linger_lock (or be in a single-threaded test context).
fn (a &S3StorageAdapter) get_expired_sync_linger_keys() []string {
	mut expired := []string{}
	now_ms := time.now().unix_milli()
	linger_ms := i64(a.config.sync_linger_ms)
	for key, buf in a.sync_linger_buffers {
		if buf.records.len > 0 && (now_ms - buf.created_at) >= linger_ms {
			expired << key
		}
	}
	return expired
}

/// notify_sync_linger_waiters sends a LingerResult to all waiting channels.
fn notify_sync_linger_waiters(channels []chan LingerResult, result LingerResult) {
	for ch in channels {
		ch <- result
	}
}

/// sync_append_immediate performs a direct S3 write for a single sync append.
/// Used when sync_linger_ms=0 (linger disabled) to preserve original behavior.
fn (mut a S3StorageAdapter) sync_append_immediate(topic string, partition int, stored_records []StoredRecord, required_acks i16) ! {
	start_time := time.now()
	base_offset_val := stored_records[0].offset
	end_offset := stored_records[stored_records.len - 1].offset
	segment_data := encode_stored_records(stored_records)
	segment_key := a.log_segment_key(topic, partition, base_offset_val, end_offset)

	a.put_object(segment_key, segment_data) or {
		a.record_sync_append_error()
		return error('durable append failed (acks=${required_acks}): S3 PUT error: ${err}')
	}

	a.record_sync_append_put()

	a.index_update_lock.lock()
	a.update_partition_index_with_segment(topic, partition, segment_key, base_offset_val,
		end_offset, segment_data.len) or {
		a.index_update_lock.unlock()
		a.record_sync_append_index_error()
		return error('durable append failed (acks=${required_acks}): index update error: ${err}')
	}
	a.index_update_lock.unlock()

	elapsed_ms := time.since(start_time).milliseconds()
	a.record_sync_append_success(elapsed_ms)
}

/// flush_sync_linger_buffer writes a drained linger buffer to S3 and notifies all waiters.
fn (mut a S3StorageAdapter) flush_sync_linger_buffer(topic string, partition int, buf SyncLingerBuffer) {
	if buf.records.len == 0 {
		return
	}

	start_time := time.now()
	base_offset_val := buf.records[0].offset
	end_offset := buf.records[buf.records.len - 1].offset
	segment_data := encode_stored_records(buf.records)
	segment_key := a.log_segment_key(topic, partition, base_offset_val, end_offset)

	// S3 PUT
	a.put_object(segment_key, segment_data) or {
		a.record_sync_append_error()
		err_result := LingerResult{
			err:         error('S3 PUT error: ${err}')
			base_offset: base_offset_val
		}
		notify_sync_linger_waiters(buf.channels, err_result)
		return
	}

	a.record_sync_append_put()

	// Index update
	a.index_update_lock.lock()
	a.update_partition_index_with_segment(topic, partition, segment_key, base_offset_val,
		end_offset, segment_data.len) or {
		a.index_update_lock.unlock()
		a.record_sync_append_index_error()
		err_result := LingerResult{
			err:         error('index update error: ${err}')
			base_offset: base_offset_val
		}
		notify_sync_linger_waiters(buf.channels, err_result)
		return
	}
	a.index_update_lock.unlock()

	elapsed_ms := time.since(start_time).milliseconds()
	a.record_sync_append_success(elapsed_ms)

	ok_result := LingerResult{
		base_offset: base_offset_val
	}
	notify_sync_linger_waiters(buf.channels, ok_result)
}

/// sync_linger_worker periodically checks for expired linger buffers and flushes them.
/// Poll interval is clamped to max(1, min(5, sync_linger_ms / 2)) for CPU efficiency.
/// Stops when compactor_running becomes false.
/// On shutdown, drains all remaining linger buffers to prevent goroutine leaks.
fn (mut a S3StorageAdapter) sync_linger_worker() {
	mut poll_ms := a.config.sync_linger_ms / 2
	if poll_ms < 1 {
		poll_ms = 1
	}
	if poll_ms > 5 {
		poll_ms = 5
	}
	for a.compactor_running {
		time.sleep(poll_ms * time.millisecond)

		a.sync_linger_lock.lock()
		expired_keys := a.get_expired_sync_linger_keys()
		mut flush_list := []SyncLingerFlushItem{}
		for key in expired_keys {
			buf := a.drain_sync_linger_buffer(key)
			if buf.records.len > 0 {
				flush_list << SyncLingerFlushItem{
					partition_key: key
					buffer:        buf
				}
			}
		}
		a.sync_linger_lock.unlock()

		for item in flush_list {
			parts := item.partition_key.split(':')
			if parts.len != 2 {
				continue
			}
			topic := parts[0]
			partition := strconv.atoi(parts[1]) or { continue }
			a.flush_sync_linger_buffer(topic, partition, item.buffer)
		}
	}

	// Graceful shutdown: flush all remaining linger buffers
	a.drain_all_sync_linger_buffers()
}

/// SyncLingerFlushItem pairs a partition key with its drained buffer for flushing.
struct SyncLingerFlushItem {
	partition_key string
	buffer        SyncLingerBuffer
}

/// drain_all_sync_linger_buffers flushes all remaining linger buffers during shutdown.
/// Ensures no goroutines are left permanently blocked on their result channels.
fn (mut a S3StorageAdapter) drain_all_sync_linger_buffers() {
	a.sync_linger_lock.lock()
	mut remaining := []SyncLingerFlushItem{}
	for key, _ in a.sync_linger_buffers {
		buf := a.drain_sync_linger_buffer(key)
		if buf.records.len > 0 {
			remaining << SyncLingerFlushItem{
				partition_key: key
				buffer:        buf
			}
		}
	}
	a.sync_linger_lock.unlock()

	for item in remaining {
		parts := item.partition_key.split(':')
		if parts.len != 2 {
			continue
		}
		topic := parts[0]
		partition := strconv.atoi(parts[1]) or { continue }
		a.flush_sync_linger_buffer(topic, partition, item.buffer)
	}
}
