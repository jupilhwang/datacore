// Infra Layer - in-memory storage adapter: share partition state operations
module memory

import domain

/// save_share_partition_state saves a SharePartition state.
pub fn (mut a MemoryStorageAdapter) save_share_partition_state(state domain.SharePartitionState) ! {
	a.share_lock.@lock()
	defer { a.share_lock.unlock() }

	key := '${state.group_id}:${state.topic_name}:${state.partition}'
	is_new := key !in a.share_partition_states
	a.share_partition_states[key] = state

	// Update group index only when this is a new key
	if is_new {
		group_id := state.group_id
		if group_id !in a.share_partition_by_group {
			a.share_partition_by_group[group_id] = []string{}
		}
		a.share_partition_by_group[group_id] << key
	}
}

/// load_share_partition_state loads a SharePartition state.
/// Returns none if not found.
pub fn (mut a MemoryStorageAdapter) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	a.share_lock.rlock()
	defer { a.share_lock.runlock() }

	key := '${group_id}:${topic_name}:${partition}'
	return a.share_partition_states[key] or { return none }
}

/// delete_share_partition_state deletes a SharePartition state.
pub fn (mut a MemoryStorageAdapter) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {
	a.share_lock.@lock()
	defer { a.share_lock.unlock() }

	key := '${group_id}:${topic_name}:${partition}'
	a.share_partition_states.delete(key)

	// Remove key from the group index using swap-remove for O(1) deletion (order is not significant)
	if mut keys := a.share_partition_by_group[group_id] {
		for i, k in keys {
			if k == key {
				keys[i] = keys[keys.len - 1]
				keys.delete_last()
				break
			}
		}
		if keys.len == 0 {
			a.share_partition_by_group.delete(group_id)
		} else {
			a.share_partition_by_group[group_id] = keys
		}
	}
}

/// load_all_share_partition_states loads all SharePartition states for a group.
/// Uses the group index for O(k) lookup instead of O(n) full scan.
pub fn (mut a MemoryStorageAdapter) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	a.share_lock.rlock()
	defer { a.share_lock.runlock() }

	keys := a.share_partition_by_group[group_id] or { return []domain.SharePartitionState{} }
	mut result := []domain.SharePartitionState{cap: keys.len}
	for key in keys {
		if state := a.share_partition_states[key] {
			result << state
		}
	}
	return result
}
