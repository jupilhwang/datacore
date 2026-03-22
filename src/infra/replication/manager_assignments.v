module replication

import domain
import rand
import time

// Replica Assignment Management
//
// This file encapsulates all replica assignment operations.
// Protected data group: assignments map + assignments_lock
//
// Lock ordering position: 1st (assignments_lock -> broker_health_lock -> replica_buffers_lock -> stats_lock)
//
// Also reads cluster_broker_refs (via get_cluster_broker_refs_snapshot) for peer selection.

// assign_replicas creates a new replica assignment for the given topic:partition.
// Peer selection uses broker_id-based exclusion when a stable ID is available, so the
// assignment remains correct even if this broker's address changes.
// Falls back to address-based exclusion for BrokerRef entries that have an empty broker_id
// (backward compatibility with older configurations).
//
// Selects up to replica_count brokers randomly from the available peer list using
// Fisher-Yates shuffle.
fn (mut m Manager) assign_replicas(topic string, partition i32) ! {
	// Filter out self using broker_id when available; otherwise fall back to address matching.
	mut available_refs := []domain.BrokerRef{}
	refs_snapshot := m.get_cluster_broker_refs_snapshot()
	for ref in refs_snapshot {
		is_self := if ref.broker_id != '' {
			// ID-based: exact match on stable broker_id
			ref.broker_id == m.broker_id
		} else {
			// Address-based fallback: check if the addr includes our replication port
			ref.addr.contains(':${m.config.replication_port}')
		}
		if !is_self {
			available_refs << ref
		}
	}

	if available_refs.len == 0 {
		return error('no available replica brokers')
	}

	// Select replica_count brokers randomly via Fisher-Yates shuffle on indices
	mut indices := []int{}
	for i := 0; i < available_refs.len; i++ {
		indices << i
	}
	for i := indices.len - 1; i > 0; i-- {
		j := rand.intn(i + 1) or { i }
		indices[i], indices[j] = indices[j], indices[i]
	}

	count := if m.config.replica_count < available_refs.len {
		m.config.replica_count
	} else {
		available_refs.len
	}

	// Store selected broker addresses for actual message delivery
	mut selected_addrs := []string{}
	for i := 0; i < count; i++ {
		selected_addrs << available_refs[indices[i]].addr
	}

	key := '${topic}:${partition}'
	assignment := domain.ReplicaAssignment{
		topic:           topic
		partition:       partition
		main_broker:     m.broker_id
		replica_brokers: selected_addrs
		assigned_time:   time.now().unix_milli()
	}

	m.assignments_lock.@lock()
	m.assignments[key] = assignment
	m.assignments_lock.unlock()
	m.logger.info('Assigned replicas for ${key}: ${selected_addrs}')
}

// get_replica_brokers returns the replica broker addresses for a given partition key.
// Returns an error if no assignment exists.
// Thread-safe; acquires assignments_lock internally.
fn (mut m Manager) get_replica_brokers(key string) ![]string {
	m.assignments_lock.@lock()
	assignment := m.assignments[key] or {
		m.assignments_lock.unlock()
		return error('no assignment for ${key}')
	}
	brokers := assignment.replica_brokers.clone()
	m.assignments_lock.unlock()
	return brokers
}

// get_assignment_leader returns the main broker for a given partition key.
// Returns an error if no assignment exists.
// Thread-safe; acquires assignments_lock internally.
fn (mut m Manager) get_assignment_leader(key string) !string {
	m.assignments_lock.@lock()
	assignment := m.assignments[key] or {
		m.assignments_lock.unlock()
		return error('no assignment for ${key}, cannot recover')
	}
	leader := assignment.main_broker
	m.assignments_lock.unlock()
	return leader
}

// get_or_create_replica_brokers returns replica brokers for a key, creating an assignment if needed.
// Thread-safe; acquires and releases assignments_lock internally.
fn (mut m Manager) get_or_create_replica_brokers(key string, topic string, partition i32) ![]string {
	m.assignments_lock.@lock()
	assignment := m.assignments[key] or {
		m.assignments_lock.unlock()
		m.assign_replicas(topic, partition)!
		m.assignments_lock.@lock()
		a := m.assignments[key] or {
			m.assignments_lock.unlock()
			return error('failed to create assignment for ${key}')
		}
		brokers := a.replica_brokers.clone()
		m.assignments_lock.unlock()
		return brokers
	}
	brokers := assignment.replica_brokers.clone()
	m.assignments_lock.unlock()
	return brokers
}
