module replication

import domain
import time

// Broker Health Tracking
//
// This file encapsulates all broker health state management.
// Protected data group: broker_health map + broker_health_lock
//
// Lock ordering position: 2nd (assignments_lock -> broker_health_lock -> replica_buffers_lock -> stats_lock)
//
// Also manages cluster_broker_refs snapshot access (cluster_broker_refs_lock).

// update_broker_health updates the health status for a given broker.
// Thread-safe; acquires broker_health_lock internally.
/// update_broker_health updates the health status for a given broker.
pub fn (mut m Manager) update_broker_health(broker_id string, health domain.ReplicationHealth) {
	m.broker_health_lock.@lock()
	m.broker_health[broker_id] = health
	m.broker_health_lock.unlock()
}

// get_broker_health_snapshot returns a copy of the broker health map.
// Thread-safe; acquires broker_health_lock internally.
/// get_broker_health_snapshot returns a copy of the broker health map.
pub fn (mut m Manager) get_broker_health_snapshot() map[string]domain.ReplicationHealth {
	m.broker_health_lock.@lock()
	mut result := map[string]domain.ReplicationHealth{}
	for k, v in m.broker_health {
		result[k] = v
	}
	m.broker_health_lock.unlock()
	return result
}

// get_stale_broker_ids returns broker IDs whose last heartbeat exceeds the threshold.
// Thread-safe; acquires broker_health_lock internally.
fn (mut m Manager) get_stale_broker_ids(threshold_ms i64) []string {
	now := time.now().unix_milli()
	mut stale := []string{}
	m.broker_health_lock.@lock()
	for id, health in m.broker_health {
		if now - health.last_heartbeat > threshold_ms {
			stale << id
		}
	}
	m.broker_health_lock.unlock()
	return stale
}

// get_dead_broker_addrs collects addresses of brokers marked as not alive.
// Translates health keys (broker_id) to addresses using the provided lookup map.
// Thread-safe; acquires broker_health_lock internally.
fn (mut m Manager) get_dead_broker_addrs(id_to_addr map[string]string) []string {
	mut dead := []string{}
	m.broker_health_lock.@lock()
	for health_key, health in m.broker_health {
		if !health.is_alive {
			addr := id_to_addr[health_key] or { health_key }
			dead << addr
		}
	}
	m.broker_health_lock.unlock()
	return dead
}

// get_alive_broker_addrs returns addresses of brokers that are currently alive.
// Uses the provided refs snapshot to map health keys to addresses.
// Thread-safe; acquires broker_health_lock internally.
fn (mut m Manager) get_alive_broker_addrs(refs_snapshot []domain.BrokerRef) []string {
	mut alive := []string{}
	m.broker_health_lock.@lock()
	for ref in refs_snapshot {
		health_key := if ref.broker_id != '' { ref.broker_id } else { ref.addr }
		health := m.broker_health[health_key] or { continue }
		if health.is_alive {
			alive << ref.addr
		}
	}
	m.broker_health_lock.unlock()
	return alive
}

// get_cluster_broker_refs_snapshot returns a cloned copy of cluster_broker_refs.
// Thread-safe; acquires cluster_broker_refs_lock internally.
fn (mut m Manager) get_cluster_broker_refs_snapshot() []domain.BrokerRef {
	m.cluster_broker_refs_lock.@lock()
	snapshot := m.cluster_broker_refs.clone()
	m.cluster_broker_refs_lock.unlock()
	return snapshot
}
