module replication

import domain
import time

// store_replica_buffer stores replicated record data in the in-memory buffer.
// Thread-safe; uses replica_buffers_lock internally.
// Buffers are keyed by "topic:partition" and appended in arrival order.
//
// Returns an error if the buffer size limit (max_replica_buffer_size_bytes) would be exceeded.
// TTL-expired messages are pruned from the partition buffer before the size check.
//
// Parameters:
// - buffer: ReplicaBuffer containing the replicated data to store
/// store_replica_buffer stores replicated record data in the in-memory buffer.
pub fn (mut m Manager) store_replica_buffer(buffer domain.ReplicaBuffer) ! {
	key := '${buffer.topic}:${buffer.partition}'
	now := time.now().unix_milli()

	m.replica_buffers_lock.@lock()
	if key !in m.replica_buffers {
		m.replica_buffers[key] = []domain.ReplicaBuffer{}
	}

	// Prune TTL-expired entries from this partition before inserting.
	// Only applies when replica_buffer_ttl_ms > 0.
	mut ttl_dropped := i64(0)
	if m.config.replica_buffer_ttl_ms > 0 {
		cutoff := now - m.config.replica_buffer_ttl_ms
		existing := m.replica_buffers[key] or { []domain.ReplicaBuffer{} }
		mut kept := []domain.ReplicaBuffer{}
		for b in existing {
			if b.timestamp >= cutoff {
				kept << b
			} else {
				ttl_dropped++
				m.total_buffer_bytes -= i64(b.records_data.len)
			}
		}
		m.replica_buffers[key] = kept
	}

	// Enforce max_replica_buffer_size_bytes using incremental counter (O(1)).
	if m.config.max_replica_buffer_size_bytes > 0 {
		if m.total_buffer_bytes + i64(buffer.records_data.len) > m.config.max_replica_buffer_size_bytes {
			m.replica_buffers_lock.unlock()
			m.stats_lock.@lock()
			if ttl_dropped > 0 {
				m.stats.record_ttl_dropped(ttl_dropped)
			}
			m.stats.record_buffer_overflow()
			m.stats_lock.unlock()
			if ttl_dropped > 0 {
				m.logger.warn('TTL: dropped ${ttl_dropped} expired buffer(s) from ${key}')
			}
			return error('replica buffer size limit exceeded for ${key}: limit=${m.config.max_replica_buffer_size_bytes} bytes')
		}
	}

	m.replica_buffers[key] << buffer
	m.total_buffer_bytes += i64(buffer.records_data.len)
	m.replica_buffers_lock.unlock()

	m.stats_lock.@lock()
	if ttl_dropped > 0 {
		m.stats.record_ttl_dropped(ttl_dropped)
	}
	m.stats_lock.unlock()

	if ttl_dropped > 0 {
		m.logger.warn('TTL: dropped ${ttl_dropped} expired buffer(s) from ${key} before insert')
	}
	m.logger.debug('Stored replica buffer for ${key}, offset ${buffer.offset}')
}

// delete_replica_buffer removes all buffered replicas with offset <= the given offset.
// Called when a FLUSH_ACK is received, confirming S3 persistence.
// Thread-safe; uses replica_buffers_lock internally.
//
// Parameters:
// - topic: topic name
// - partition: partition index
// - offset: flush offset; buffers with offset <= this value are removed
/// delete_replica_buffer removes all buffered replicas with offset <= the given offset.
pub fn (mut m Manager) delete_replica_buffer(topic string, partition i32, offset i64) ! {
	key := '${topic}:${partition}'

	m.replica_buffers_lock.@lock()
	if key in m.replica_buffers {
		mut remaining := []domain.ReplicaBuffer{}
		for buf in m.replica_buffers[key] {
			if buf.offset > offset {
				remaining << buf
			} else {
				m.total_buffer_bytes -= i64(buf.records_data.len)
			}
		}
		m.replica_buffers[key] = remaining
	}
	m.replica_buffers_lock.unlock()

	m.logger.debug('Deleted replica buffers for ${key} up to offset ${offset}')
}

// get_all_replica_buffers returns a flat list of all in-memory replica buffers
// across all topic:partition keys. Intended for crash recovery scenarios
// where buffered data needs to be replayed.
// Thread-safe; uses replica_buffers_lock internally.
//
// Returns: list of all ReplicaBuffer entries currently held in memory
/// get_all_replica_buffers returns a flat list of all in-memory replica buffers.
pub fn (mut m Manager) get_all_replica_buffers() ![]domain.ReplicaBuffer {
	m.replica_buffers_lock.@lock()
	mut all_buffers := []domain.ReplicaBuffer{}
	for _, buffers in m.replica_buffers {
		all_buffers << buffers
	}
	m.replica_buffers_lock.unlock()

	return all_buffers
}
