// Manages share partitions, record acquisition, acknowledgement, and release.
// Share groups allow multiple consumers to share the same partition for message processing.
module group

import domain
import service.port
import sync
import time

// Share partition manager

/// SharePartitionManager manages share partitions and record states.
/// Handles operations such as record acquisition, acknowledgement, and release.
pub struct SharePartitionManager {
mut:
	// Share partitions keyed by "group_id:topic:partition"
	partitions map[string]&domain.SharePartition
	// Storage for persistence
	storage port.StoragePort
	// Thread safety
	lock sync.RwMutex
}

/// new_share_partition_manager creates a new share partition manager.
pub fn new_share_partition_manager(storage port.StoragePort) &SharePartitionManager {
	return &SharePartitionManager{
		partitions: map[string]&domain.SharePartition{}
		storage:    storage
	}
}

// Partition management

/// get_or_create_partition gets or creates a share partition.
pub fn (mut m SharePartitionManager) get_or_create_partition(group_id string, topic_name string, partition i32) &domain.SharePartition {
	m.lock.@lock()
	defer { m.lock.unlock() }

	return m.get_or_create_partition_internal(group_id, topic_name, partition)
}

/// get_partition returns a share partition.
pub fn (mut m SharePartitionManager) get_partition(group_id string, topic_name string, partition i32) ?&domain.SharePartition {
	m.lock.rlock()
	defer { m.lock.runlock() }

	key := '${group_id}:${topic_name}:${partition}'
	return m.partitions[key] or { return none }
}

/// delete_partitions_for_group deletes all partitions for a group.
pub fn (mut m SharePartitionManager) delete_partitions_for_group(group_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut to_delete := []string{}
	for key, _ in m.partitions {
		if key.starts_with('${group_id}:') {
			to_delete << key
		}
	}
	for key in to_delete {
		m.partitions.delete(key)
	}
}

/// get_partition_stats returns partition statistics for a group.
/// Returns: (partition count, total acquired, total acknowledged, total released, total rejected)
pub fn (mut m SharePartitionManager) get_partition_stats(group_id string) (int, i64, i64, i64, i64) {
	m.lock.rlock()
	defer { m.lock.runlock() }

	mut partition_count := 0
	mut total_acquired := i64(0)
	mut total_acknowledged := i64(0)
	mut total_released := i64(0)
	mut total_rejected := i64(0)

	for key, sp in m.partitions {
		if key.starts_with('${group_id}:') {
			partition_count += 1
			total_acquired += sp.total_acquired
			total_acknowledged += sp.total_acknowledged
			total_released += sp.total_released
			total_rejected += sp.total_rejected
		}
	}

	return partition_count, total_acquired, total_acknowledged, total_released, total_rejected
}

// Record acquisition

/// acquire_records acquires records for a consumer.
/// Acquired records are locked for the specified duration for that consumer.
pub fn (mut m SharePartitionManager) acquire_records(group_id string, member_id string, topic_name string, partition i32, max_records int, lock_duration_ms i64, max_partition_locks int) []domain.AcquiredRecordInfo {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut sp := m.get_or_create_partition_internal(group_id, topic_name, partition)
	now := time.now().unix_milli()

	mut acquired := []domain.AcquiredRecordInfo{}
	mut offset := sp.start_offset

	// Find acquirable records
	for acquired.len < max_records && offset <= sp.end_offset {
		state := sp.record_states[offset] or { domain.RecordState.available }

		if state == .available {
			// Check maximum partition lock count
			if sp.acquired_records.len >= max_partition_locks {
				break
			}

			// Get delivery count from persistent tracking map
			prev_count := sp.delivery_counts[offset] or { i32(0) }
			delivery_count := prev_count + 1
			sp.delivery_counts[offset] = delivery_count

			// Acquire record
			sp.record_states[offset] = .acquired
			sp.acquired_records[offset] = domain.AcquiredRecord{
				offset:          offset
				member_id:       member_id
				delivery_count:  delivery_count
				acquired_at:     now
				lock_expires_at: now + lock_duration_ms
			}

			acquired << domain.AcquiredRecordInfo{
				offset:         offset
				delivery_count: delivery_count
				timestamp:      now
			}

			sp.total_acquired += 1
		}

		offset += 1
	}

	// Update end_offset if needed
	if offset > sp.end_offset {
		sp.end_offset = offset
	}

	// Auto-save: persist state after acquisition
	if acquired.len > 0 {
		m.persist_state(sp)
	}

	return acquired
}

// Record acknowledgement

/// acknowledge_records handles record acknowledgements.
/// Supports three types: accept, release, and reject.
pub fn (mut m SharePartitionManager) acknowledge_records(group_id string, member_id string, batch domain.AcknowledgementBatch, delivery_attempt_limit i32) domain.ShareAcknowledgeResult {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut sp := m.get_or_create_partition_internal(group_id, batch.topic_name, batch.partition)

	// Process each offset in the batch
	for offset := batch.first_offset; offset <= batch.last_offset; offset++ {
		// Skip gap offsets
		if offset in batch.gap_offsets {
			continue
		}

		// Verify this member acquired the record
		acquired := sp.acquired_records[offset] or { continue }

		if acquired.member_id != member_id {
			// Record not owned by this member
			continue
		}

		match batch.acknowledge_type {
			.accept {
				// Successfully processed
				sp.record_states[offset] = .acknowledged
				sp.acquired_records.delete(offset)
				sp.total_acknowledged += 1
			}
			.release {
				// Release for redelivery
				if acquired.delivery_count < delivery_attempt_limit {
					sp.record_states[offset] = .available
				} else {
					// Maximum attempts reached - archive
					sp.record_states[offset] = .archived
				}
				sp.acquired_records.delete(offset)
				sp.total_released += 1
			}
			.reject {
				// Rejected as unprocessable
				sp.record_states[offset] = .archived
				sp.acquired_records.delete(offset)
				sp.total_rejected += 1
			}
		}
	}

	// Advance SPSO (Share Partition Start Offset) if possible
	m.advance_spso_internal(mut sp)

	// Auto-save: persist state after acknowledgement
	m.persist_state(sp)

	return domain.ShareAcknowledgeResult{
		topic_name: batch.topic_name
		partition:  batch.partition
		error_code: 0
	}
}

// Record release

/// release_expired_locks_with_limits releases records whose acquisition locks have expired.
/// Uses the delivery attempt limits from the provided map.
pub fn (mut m SharePartitionManager) release_expired_locks_with_limits(group_limits map[string]i32) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	now := time.now().unix_milli()

	for _, mut sp in m.partitions {
		delivery_limit := group_limits[sp.group_id] or { continue }

		mut expired := []i64{}
		for offset, acquired in sp.acquired_records {
			if now > acquired.lock_expires_at {
				expired << offset
			}
		}

		for offset in expired {
			acquired := sp.acquired_records[offset] or { continue }

			// Check delivery count
			if acquired.delivery_count >= delivery_limit {
				// Maximum attempts reached - archive
				sp.record_states[offset] = .archived
			} else {
				// Release for redelivery
				sp.record_states[offset] = .available
			}
			sp.acquired_records.delete(offset)
			sp.total_released += 1
		}

		// Advance SPSO if possible
		m.advance_spso_internal(mut sp)
	}
}

/// release_member_records releases all records acquired by a member.
pub fn (mut m SharePartitionManager) release_member_records(group_id string, member_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	m.release_member_records_internal(group_id, member_id)
}

/// release_member_records_internal releases member records internally without a lock.
pub fn (mut m SharePartitionManager) release_member_records_internal(group_id string, member_id string) {
	for _, mut sp in m.partitions {
		if sp.group_id != group_id {
			continue
		}

		mut to_release := []i64{}
		for offset, acquired in sp.acquired_records {
			if acquired.member_id == member_id {
				to_release << offset
			}
		}

		for offset in to_release {
			sp.record_states[offset] = .available
			sp.acquired_records.delete(offset)
		}
	}
}

// Internal helpers

/// get_or_create_partition_internal gets or creates a partition without acquiring a lock.
fn (mut m SharePartitionManager) get_or_create_partition_internal(group_id string, topic_name string, partition i32) &domain.SharePartition {
	key := '${group_id}:${topic_name}:${partition}'

	if sp := m.partitions[key] {
		return sp
	}

	mut start_offset := i64(0)
	if info := m.storage.get_partition_info(topic_name, partition) {
		start_offset = info.high_watermark
	}

	mut sp := &domain.SharePartition{
		...domain.new_share_partition(topic_name, partition, group_id, start_offset)
	}
	m.partitions[key] = sp
	return sp
}

/// advance_spso_internal advances the SPSO past all acknowledged/archived records.
fn (mut m SharePartitionManager) advance_spso_internal(mut sp domain.SharePartition) {
	// Advance SPSO past all acknowledged/archived records
	mut new_start := sp.start_offset
	for new_start <= sp.end_offset {
		state := sp.record_states[new_start] or { break }
		if state == .acknowledged || state == .archived {
			// Clean up state and delivery count for this offset
			sp.record_states.delete(new_start)
			sp.delivery_counts.delete(new_start)
			new_start += 1
		} else {
			break
		}
	}
	sp.start_offset = new_start
}

// Persistence operations

/// persist_state persists a partition state to storage.
fn (mut m SharePartitionManager) persist_state(sp &domain.SharePartition) {
	state := sp.to_state()
	m.storage.save_share_partition_state(state) or {}
}

/// load_state loads a partition state from storage.
fn (mut m SharePartitionManager) load_state(group_id string, topic_name string, partition i32) ?domain.SharePartition {
	state := m.storage.load_share_partition_state(group_id, topic_name, partition)?
	return state.to_partition()
}

/// persist_all_states persists all partition states to storage.
pub fn (mut m SharePartitionManager) persist_all_states() {
	m.lock.rlock()
	defer { m.lock.runlock() }

	for _, sp in m.partitions {
		state := sp.to_state()
		m.storage.save_share_partition_state(state) or {}
	}
}

/// load_all_states loads all partition states for a group from storage.
pub fn (mut m SharePartitionManager) load_all_states(group_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	states := m.storage.load_all_share_partition_states(group_id)
	for state in states {
		key := '${state.group_id}:${state.topic_name}:${state.partition}'
		sp := state.to_partition()
		m.partitions[key] = &domain.SharePartition{
			...sp
		}
	}
}
