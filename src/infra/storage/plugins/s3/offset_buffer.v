// Infra Layer - S3 Offset Batch Buffer
// Buffers consumer group offsets in memory and periodically flushes them to S3.
// Follows the failure recovery pattern of flush_worker (see buffer_manager.v).
module s3

import domain
import time
import infra.observability

/// OffsetGroupBuffer buffers consumer group offsets in memory.
/// flush_worker periodically flushes dirty buffers to S3.
struct OffsetGroupBuffer {
pub mut:
	offsets       map[string]OffsetEntry
	version       i32
	dirty         bool
	dirty_count   int
	last_flush_at i64
}

/// buffer_offset accumulates an offset into the in-memory buffer.
/// Must be called with offset_buffer_lock held by the caller.
/// Returns true when the threshold is exceeded, indicating an immediate flush is needed.
fn (mut a S3StorageAdapter) buffer_offset(group_id string, offset domain.PartitionOffset) bool {
	cache_key := '${offset.topic}:${offset.partition}'

	if group_id !in a.offset_batcher.buffers {
		a.offset_batcher.buffers[group_id] = OffsetGroupBuffer{
			offsets:     map[string]OffsetEntry{}
			dirty:       true
			dirty_count: 1
		}
	}

	mut buf := a.offset_batcher.buffers[group_id]
	buf.offsets[cache_key] = OffsetEntry{
		offset:       offset.offset
		leader_epoch: offset.leader_epoch
		metadata:     offset.metadata
		committed_at: time.now().unix_milli()
	}
	buf.dirty = true
	buf.dirty_count++
	a.offset_batcher.buffers[group_id] = buf

	return a.config.offset_flush_threshold_count > 0
		&& buf.dirty_count >= a.config.offset_flush_threshold_count
}

/// get_buffered_offset looks up an offset from the buffer.
/// Returns none if not found in the buffer.
/// Must be called with offset_buffer_lock held by the caller.
fn (a &S3StorageAdapter) get_buffered_offset(group_id string, topic string, partition int) ?i64 {
	cache_key := '${topic}:${partition}'
	if group_id in a.offset_batcher.buffers {
		if entry := a.offset_batcher.buffers[group_id].offsets[cache_key] {
			return entry.offset
		}
	}
	return none
}

/// flush_pending_offsets flushes all dirty group offsets to S3.
/// Called via spawn from flush_worker.
fn (mut a S3StorageAdapter) flush_pending_offsets() {
	// 1. Acquire offset_buffer_lock and extract dirty groups
	a.offset_batcher.buffer_lock.lock()

	mut groups_to_flush := map[string]OffsetGroupBuffer{}
	for group_id, _ in a.offset_batcher.buffers {
		buf := a.offset_batcher.buffers[group_id]
		if buf.dirty {
			now := time.now().unix_milli()
			// Move existing map and replace with empty map (avoids clone)
			mut updated := a.offset_batcher.buffers[group_id]
			groups_to_flush[group_id] = OffsetGroupBuffer{
				offsets:       updated.offsets
				version:       updated.version
				dirty:         false
				dirty_count:   0
				last_flush_at: now
			}
			updated.offsets = map[string]OffsetEntry{}
			updated.dirty = false
			updated.dirty_count = 0
			updated.last_flush_at = now
			a.offset_batcher.buffers[group_id] = updated
		}
	}

	a.offset_batcher.buffer_lock.unlock()

	if groups_to_flush.len == 0 {
		return
	}

	// 2. Flush each group to S3
	for group_id, buf in groups_to_flush {
		a.flush_group_offsets(group_id, buf) or {
			// Restore buffer on failure (follows flush_worker pattern in buffer_manager.v)
			a.restore_offset_buffer(group_id, buf)
			// Metric: offset flush failure
			a.metrics_collector.mu.@lock()
			a.metrics_collector.data.offset_flush_error_count++
			a.metrics_collector.mu.unlock()
			observability.log_with_context('s3', .error, 'OffsetFlush', 'Failed to flush offsets',
				{
				'group_id': group_id
				'error':    err.msg()
			})
		}
	}
}

/// flush_group_offsets saves a single group's offset snapshot to S3.
/// Uses a merge-on-conflict strategy.
fn (mut a S3StorageAdapter) flush_group_offsets(group_id string, buf OffsetGroupBuffer) ! {
	snapshot_key := a.offset_snapshot_key(group_id)

	// 1. Fetch current snapshot from S3 (use empty snapshot if not found)
	mut remote := new_offset_snapshot(a.config.broker_id)
	data, _ := a.get_object(snapshot_key, -1, -1) or { []u8{}, '' }
	if data.len > 0 {
		remote = try_decode_offset_data(data) or {
			// Start with empty snapshot on decode failure
			new_offset_snapshot(a.config.broker_id)
		}
	}

	// 2. Convert local buffer to snapshot
	local := OffsetSnapshot{
		version:   buf.version
		broker_id: a.config.broker_id
		timestamp: time.now().unix_milli()
		offsets:   buf.offsets.clone()
	}

	// 3. Merge (select max offset per partition)
	merged := merge_offset_snapshots(local, remote)

	// 4. Encode and S3 PUT
	encoded := encode_offset_snapshot(merged)
	a.put_object(snapshot_key, encoded)!

	// 5. Update buffer version
	a.offset_batcher.buffer_lock.lock()
	if group_id in a.offset_batcher.buffers {
		mut updated := a.offset_batcher.buffers[group_id]
		updated.version = merged.version
		a.offset_batcher.buffers[group_id] = updated
	}
	a.offset_batcher.buffer_lock.unlock()

	// Metric: offset flush success
	a.metrics_collector.mu.@lock()
	a.metrics_collector.data.offset_flush_count++
	a.metrics_collector.data.offset_flush_success_count++
	a.metrics_collector.mu.unlock()
}

/// merge_offset_entries merges offset entries from source into target.
/// Selects the entry with the higher offset when the same key exists in both.
fn merge_offset_entries(mut target map[string]OffsetEntry, source map[string]OffsetEntry) {
	for key, entry in source {
		if key in target {
			if entry.offset > target[key].offset {
				target[key] = entry
			}
		} else {
			target[key] = entry
		}
	}
}

/// restore_offset_buffer restores the buffer to its previous state on flush failure.
/// Follows the flush_worker failure recovery pattern in buffer_manager.v.
fn (mut a S3StorageAdapter) restore_offset_buffer(group_id string, failed_buf OffsetGroupBuffer) {
	a.offset_batcher.buffer_lock.lock()
	defer { a.offset_batcher.buffer_lock.unlock() }

	if group_id in a.offset_batcher.buffers {
		mut current := a.offset_batcher.buffers[group_id]
		merge_offset_entries(mut current.offsets, failed_buf.offsets)
		current.dirty = true
		current.dirty_count += failed_buf.dirty_count
		a.offset_batcher.buffers[group_id] = current
	} else {
		// Recreate if group was deleted
		a.offset_batcher.buffers[group_id] = OffsetGroupBuffer{
			offsets:       failed_buf.offsets
			version:       failed_buf.version
			dirty:         true
			dirty_count:   failed_buf.dirty_count
			last_flush_at: 0
		}
	}
}

/// offset_snapshot_key generates the S3 key for an offset snapshot.
fn (a &S3StorageAdapter) offset_snapshot_key(group_id string) string {
	return '${a.config.prefix}offsets/${group_id}/snapshot.bin'
}
