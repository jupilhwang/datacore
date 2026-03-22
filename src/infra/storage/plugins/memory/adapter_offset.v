// Infra Layer - in-memory storage adapter: offset operations
module memory

import domain
import infra.observability
import sync.stdatomic

// --- Offset metrics helpers ---

fn (mut a MemoryStorageAdapter) inc_offset_commit(count i64) {
	stdatomic.add_i64(&a.metrics.offset_commit_count, int(count))
}

fn (mut a MemoryStorageAdapter) inc_offset_fetch(count i64) {
	stdatomic.add_i64(&a.metrics.offset_fetch_count, int(count))
}

// --- Offset methods ---

/// commit_offsets commits offsets.
pub fn (mut a MemoryStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	a.inc_offset_commit(i64(offsets.len))

	a.groups_lock.@lock()
	defer { a.groups_lock.unlock() }

	if group_id !in a.offsets {
		a.offsets[group_id] = map[string]i64{}
	}

	for offset in offsets {
		key := '${offset.topic}:${offset.partition}'
		a.offsets[group_id][key] = offset.offset
	}

	observability.log_with_context('memory', .debug, 'Offset', 'Offsets committed', {
		'group_id': group_id
		'count':    offsets.len.str()
	})
}

/// fetch_offsets retrieves committed offsets.
pub fn (mut a MemoryStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	a.inc_offset_fetch(i64(partitions.len))

	a.groups_lock.rlock()
	defer { a.groups_lock.runlock() }

	mut results := []domain.OffsetFetchResult{}

	if group_id !in a.offsets {
		for part in partitions {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
		return results
	}

	for part in partitions {
		key := '${part.topic}:${part.partition}'
		offset := a.offsets[group_id][key] or { -1 }
		results << domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset
			metadata:   ''
			error_code: 0
		}
	}

	return results
}
