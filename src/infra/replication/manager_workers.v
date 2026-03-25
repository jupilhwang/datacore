module replication

import domain
import sync.stdatomic
import time
import rand

// update_cluster_broker_refs replaces the peer broker list used for replica assignment
// and heartbeat. Safe to call while the manager is running; the change takes effect on
// the next heartbeat/assignment cycle.
/// update_cluster_broker_refs replaces the live list of peer brokers.
fn (mut m Manager) update_cluster_broker_refs(refs []domain.BrokerRef) {
	m.cluster_broker_refs_lock.@lock()
	m.cluster_broker_refs = refs
	m.cluster_broker_refs_lock.unlock()
	m.logger.info('Cluster broker refs updated (count=${refs.len})')
}

// start_workers spawns periodic background coroutines for replication maintenance:
// - heartbeat_worker: sends heartbeat to cluster brokers at heartbeat_interval_ms
// - orphan_cleanup_worker: removes stale buffers at orphan_cleanup_interval_ms
// - reassignment_worker: reassigns replicas for dead brokers at reassignment_interval_ms
fn (mut m Manager) start_workers() {
	if !m.config.enabled {
		return
	}

	spawn m.heartbeat_worker()
	spawn m.orphan_cleanup_worker()
	spawn m.reassignment_worker()

	m.logger.info('Background workers started (heartbeat, orphan_cleanup, reassignment)')
}

// stop_workers signals all background workers to stop by relying on Manager.running_flag
// being set to false (done in stop()). Workers exit their loops on the next iteration.
fn (mut m Manager) stop_workers() {
	m.logger.info('Background workers stop signal sent')
}

// heartbeat_worker periodically sends heartbeat messages to all cluster brokers.
// Uses helper methods from manager_health.v and manager_stats.v for thread-safe access.
fn (mut m Manager) heartbeat_worker() {
	m.logger.info('Heartbeat worker started (interval=${m.config.heartbeat_interval_ms}ms)')

	for stdatomic.load_i64(&m.running_flag) == 1 {
		time.sleep(time.Duration(m.config.heartbeat_interval_ms * time.millisecond))
		if stdatomic.load_i64(&m.running_flag) != 1 {
			break
		}

		refs := m.get_cluster_broker_refs_snapshot()
		for ref in refs {
			broker_addr := ref.addr
			// Use stable broker_id as the health map key when available;
			// otherwise fall back to address for backward compatibility.
			health_key := if ref.broker_id != '' { ref.broker_id } else { broker_addr }
			msg := domain.ReplicationMessage{
				msg_type:       domain.ReplicationType.heartbeat
				correlation_id: rand.uuid_v4()
				sender_id:      m.broker_id
				timestamp:      time.now().unix_milli()
				success:        true
			}

			response := m.client.send(broker_addr, msg) or {
				m.update_broker_health(health_key, domain.ReplicationHealth{
					broker_id:      health_key
					is_alive:       false
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				})
				m.logger.error('Heartbeat failed for ${broker_addr} (id=${health_key}): ${err}')
				continue
			}

			if response.success {
				m.update_broker_health(health_key, domain.ReplicationHealth{
					broker_id:      health_key
					is_alive:       true
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				})
			} else {
				m.update_broker_health(health_key, domain.ReplicationHealth{
					broker_id:      health_key
					is_alive:       false
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				})
			}
		}

		// Check for stale brokers
		stale_brokers := m.get_stale_broker_ids(i64(m.config.heartbeat_interval_ms) * 3)
		if stale_brokers.len > 0 {
			m.logger.warn('Stale replica brokers detected: ${stale_brokers.join(', ')}')
		}

		m.record_stats_heartbeat()
	}

	m.logger.info('Heartbeat worker stopped')
}

// orphan_cleanup_worker periodically removes stale replica buffers.
// Effective TTL: replica_buffer_ttl_ms when > 0, otherwise falls back to replica_timeout_ms.
fn (mut m Manager) orphan_cleanup_worker() {
	// Use replica_buffer_ttl_ms when configured; fall back to replica_timeout_ms.
	ttl_ms := if m.config.replica_buffer_ttl_ms > 0 {
		m.config.replica_buffer_ttl_ms
	} else {
		i64(m.config.replica_timeout_ms)
	}
	m.logger.info('Orphan cleanup worker started (interval=${m.config.orphan_cleanup_interval_ms}ms, ttl=${ttl_ms}ms)')

	for stdatomic.load_i64(&m.running_flag) == 1 {
		time.sleep(time.Duration(m.config.orphan_cleanup_interval_ms * time.millisecond))
		if stdatomic.load_i64(&m.running_flag) != 1 {
			break
		}

		now := time.now().unix_milli()
		mut total_cleaned := 0

		// Clone keys under lock to avoid holding lock during iteration
		m.replica_buffers_lock.@lock()
		keys := m.replica_buffers.keys()
		m.replica_buffers_lock.unlock()

		for key in keys {
			m.replica_buffers_lock.@lock()
			buffers := m.replica_buffers[key] or {
				m.replica_buffers_lock.unlock()
				continue
			}
			mut remaining := []domain.ReplicaBuffer{}
			mut cleaned_count := 0

			for buf in buffers {
				if now - buf.timestamp > ttl_ms {
					cleaned_count++
					m.total_buffer_bytes -= i64(buf.records_data.len)
				} else {
					remaining << buf
				}
			}

			if cleaned_count > 0 {
				m.replica_buffers[key] = remaining
				total_cleaned += cleaned_count
			}
			m.replica_buffers_lock.unlock()

			if cleaned_count > 0 {
				m.logger.info('Orphan cleanup: removed ${cleaned_count} stale buffers from ${key}')
			}
		}

		// Update stats outside buffer lock to avoid lock ordering issues
		if total_cleaned > 0 {
			m.record_stats_orphans_cleaned(total_cleaned)
			m.logger.info('Orphan cleanup: total ${total_cleaned} stale buffers removed this cycle')
		}
	}

	m.logger.info('Orphan cleanup worker stopped')
}

// reassignment_worker periodically checks for dead brokers and reassigns replicas.
// Uses helper methods from manager_health.v for thread-safe health queries.
fn (mut m Manager) reassignment_worker() {
	m.logger.info('Reassignment worker started (interval=${m.config.reassignment_interval_ms}ms)')

	for stdatomic.load_i64(&m.running_flag) == 1 {
		time.sleep(time.Duration(m.config.reassignment_interval_ms * time.millisecond))
		if stdatomic.load_i64(&m.running_flag) != 1 {
			break
		}

		// Build broker_id -> addr and addr -> broker_id lookup maps from cluster_broker_refs.
		refs_snapshot := m.get_cluster_broker_refs_snapshot()
		mut id_to_addr := map[string]string{}
		mut addr_to_id := map[string]string{}
		for ref in refs_snapshot {
			if ref.broker_id != '' {
				id_to_addr[ref.broker_id] = ref.addr
				addr_to_id[ref.addr] = ref.broker_id
			}
		}

		// Collect dead broker addresses
		dead_broker_addrs := m.get_dead_broker_addrs(id_to_addr)

		if dead_broker_addrs.len == 0 {
			continue
		}

		m.logger.info('Reassignment: detected ${dead_broker_addrs.len} dead broker addr(s): ${dead_broker_addrs}')

		// Collect alive broker addresses
		alive_brokers := m.get_alive_broker_addrs(refs_snapshot)

		// Reassign partitions under assignments lock
		m.assignments_lock.@lock()
		assignment_keys := m.assignments.keys()
		for key in assignment_keys {
			assignment := m.assignments[key] or { continue }
			mut new_replicas := []string{}
			mut changed := false

			for broker_addr in assignment.replica_brokers {
				if broker_addr in dead_broker_addrs {
					changed = true
					mut found_replacement := false
					for alive in alive_brokers {
						if alive !in new_replicas && alive !in assignment.replica_brokers {
							new_replicas << alive
							found_replacement = true
							break
						}
					}
					if !found_replacement {
						m.logger.error('Reassignment: no replacement available for dead broker ${broker_addr} in ${key}')
					}
				} else {
					new_replicas << broker_addr
				}
			}

			if changed {
				mut updated := assignment
				updated.replica_brokers = new_replicas
				updated.assigned_time = time.now().unix_milli()
				m.assignments[key] = updated
				m.logger.info('Reassignment: updated replicas for ${key}: ${new_replicas}')
			}
		}
		m.assignments_lock.unlock()
	}

	m.logger.info('Reassignment worker stopped')
}
