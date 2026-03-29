// Infra Layer - in-memory storage adapter: consumer group operations
module memory

import domain
import infra.observability
import sync.stdatomic

// --- Group metrics helpers ---

fn (mut a MemoryStorageAdapter) inc_group_save() {
	stdatomic.add_i64(&a.metrics.group_save_count, 1)
}

fn (mut a MemoryStorageAdapter) inc_group_load() {
	stdatomic.add_i64(&a.metrics.group_load_count, 1)
}

fn (mut a MemoryStorageAdapter) inc_group_delete() {
	stdatomic.add_i64(&a.metrics.group_delete_count, 1)
}

// --- Group methods ---

/// save_group saves a consumer group.
pub fn (mut a MemoryStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	a.inc_group_save()

	a.groups_lock.@lock()
	defer { a.groups_lock.unlock() }

	a.groups[group.group_id] = group
}

/// load_group loads a consumer group.
pub fn (mut a MemoryStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	a.inc_group_load()

	a.groups_lock.rlock()
	defer { a.groups_lock.runlock() }

	if group := a.groups[group_id] {
		return group
	}

	a.inc_error()
	return error('group not found: ${group_id}')
}

/// delete_group deletes a consumer group.
pub fn (mut a MemoryStorageAdapter) delete_group(group_id string) ! {
	a.inc_group_delete()

	a.groups_lock.@lock()
	defer { a.groups_lock.unlock() }

	if group_id !in a.groups {
		a.inc_error()
		return error('group not found: ${group_id}')
	}
	a.groups.delete(group_id)
	a.offsets.delete(group_id)

	observability.log_with_context('memory', .info, 'Group', 'Group deleted', {
		'group_id': group_id
	})
}

/// list_groups returns a list of all consumer groups.
pub fn (mut a MemoryStorageAdapter) list_groups() ![]domain.GroupInfo {
	a.groups_lock.rlock()
	defer { a.groups_lock.runlock() }

	mut result := []domain.GroupInfo{}
	for _, group in a.groups {
		result << domain.GroupInfo{
			group_id:      group.group_id
			protocol_type: group.protocol_type
			state:         group.state.str()
		}
	}
	return result
}
