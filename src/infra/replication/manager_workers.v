module replication

import domain
import sync.stdatomic
import time
import rand

// update_cluster_broker_refs replaces the peer broker list used for replica assignment
// and heartbeat. Safe to call while the manager is running; the change takes effect on
// the next heartbeat/assignment cycle.
//
// Parameters:
// - refs: updated slice of BrokerRef containing each peer's stable ID and address.
/// update_cluster_broker_refs replaces the live list of peer brokers.
pub fn (mut m Manager) update_cluster_broker_refs(refs []domain.BrokerRef) {
	m.cluster_broker_refs_lock.@lock()
	m.cluster_broker_refs = refs
	m.cluster_broker_refs_lock.unlock()
	m.logger.info('Cluster broker refs updated (count=${refs.len})')
}

// start_workers spawns periodic background coroutines for replication maintenance:
// - heartbeat_worker: sends heartbeat to cluster brokers at heartbeat_interval_ms
// - orphan_cleanup_worker: removes stale buffers at orphan_cleanup_interval_ms
// - reassignment_worker: reassigns replicas for dead brokers at reassignment_interval_ms
//
// No-op if replication is disabled. Workers run until Manager.running is set to false.
fn (mut m Manager) start_workers() {
	if !m.config.enabled {
		return
	}

	spawn m.heartbeat_worker()
	spawn m.orphan_cleanup_worker()
	spawn m.reassignment_worker()

	m.logger.info('Background workers started (heartbeat, orphan_cleanup, reassignment)')
}

// stop_workers signals all background workers to stop by relying on Manager.running
// being set to false (done in stop()). Workers exit their loops on the next iteration.
fn (mut m Manager) stop_workers() {
	// running is already set to false in stop()
	// Workers will exit their loops on next iteration
	m.logger.info('Background workers stop signal sent')
}

// heartbeat_worker periodically sends heartbeat messages to all cluster brokers.
// Thread-safe; uses broker_health_lock for health map, stats_lock for stats.
fn (mut m Manager) heartbeat_worker() {
	m.logger.info('Heartbeat worker started (interval=${m.config.heartbeat_interval_ms}ms)')

	for stdatomic.load_i64(&m.running_flag) == 1 {
		time.sleep(time.Duration(m.config.heartbeat_interval_ms * time.millisecond))
		if stdatomic.load_i64(&m.running_flag) != 1 {
			break
		}

		m.cluster_broker_refs_lock.@lock()
		refs := m.cluster_broker_refs.clone()
		m.cluster_broker_refs_lock.unlock()
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
				m.broker_health_lock.@lock()
				m.broker_health[health_key] = domain.ReplicationHealth{
					broker_id:      health_key
					is_alive:       false
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
				m.broker_health_lock.unlock()
				m.logger.error('Heartbeat failed for ${broker_addr} (id=${health_key}): ${err}')
				continue
			}

			m.broker_health_lock.@lock()
			if response.success {
				m.broker_health[health_key] = domain.ReplicationHealth{
					broker_id:      health_key
					is_alive:       true
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
			} else {
				m.broker_health[health_key] = domain.ReplicationHealth{
					broker_id:      health_key
					is_alive:       false
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
			}
			m.broker_health_lock.unlock()
		}
		// Check for stale brokers
		now := time.now().unix_milli()
		mut stale_brokers := []string{}

		m.broker_health_lock.@lock()
		for id, health in m.broker_health {
			if now - health.last_heartbeat > m.config.heartbeat_interval_ms * 3 {
				stale_brokers << id
			}
		}
		m.broker_health_lock.unlock()

		if stale_brokers.len > 0 {
			m.logger.warn('Stale replica brokers detected: ${stale_brokers.join(', ')}')
			// Trigger reassignment logic could be called here or handled by reassignment_worker
		}

		m.stats_lock.@lock()
		m.stats.update_heartbeat()
		m.stats_lock.unlock()
	}

	m.logger.info('Heartbeat worker stopped')
}

// orphan_cleanup_worker periodically removes stale replica buffers.
// Effective TTL: replica_buffer_ttl_ms when > 0, otherwise falls back to replica_timeout_ms.
// Cleaned entries are counted in total_orphans_cleaned and total_ttl_dropped stats.
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
			m.stats_lock.@lock()
			m.stats.total_orphans_cleaned += total_cleaned
			m.stats.record_ttl_dropped(i64(total_cleaned))
			m.stats_lock.unlock()
			m.logger.info('Orphan cleanup: total ${total_cleaned} stale buffers removed this cycle')
		}
	}

	m.logger.info('Orphan cleanup worker stopped')
}

// reassignment_worker periodically checks for dead brokers and reassigns replicas.
// Thread-safe; uses broker_health_lock for health reads, assignments_lock for assignment updates.
fn (mut m Manager) reassignment_worker() {
	m.logger.info('Reassignment worker started (interval=${m.config.reassignment_interval_ms}ms)')

	for stdatomic.load_i64(&m.running_flag) == 1 {
		time.sleep(time.Duration(m.config.reassignment_interval_ms * time.millisecond))
		if stdatomic.load_i64(&m.running_flag) != 1 {
			break
		}

		// Build broker_id -> addr and addr -> broker_id lookup maps from cluster_broker_refs.
		// These maps allow reconciling broker_health keys (broker_id) with replica_brokers
		// values (addr) without holding any extra lock.
		m.cluster_broker_refs_lock.@lock()
		refs_snapshot := m.cluster_broker_refs.clone()
		m.cluster_broker_refs_lock.unlock()
		mut id_to_addr := map[string]string{}
		mut addr_to_id := map[string]string{}
		for ref in refs_snapshot {
			if ref.broker_id != '' {
				id_to_addr[ref.broker_id] = ref.addr
				addr_to_id[ref.addr] = ref.broker_id
			}
		}

		// Collect dead broker addresses under health lock.
		// broker_health is keyed by broker_id (or addr in legacy mode); translate to addr
		// using id_to_addr so comparisons against replica_brokers (which stores addrs) work.
		mut dead_broker_addrs := []string{}
		m.broker_health_lock.@lock()
		for health_key, health in m.broker_health {
			if !health.is_alive {
				// health_key may be broker_id or addr depending on legacy mode
				addr := id_to_addr[health_key] or { health_key }
				dead_broker_addrs << addr
			}
		}
		m.broker_health_lock.unlock()

		if dead_broker_addrs.len == 0 {
			continue
		}

		m.logger.info('Reassignment: detected ${dead_broker_addrs.len} dead broker addr(s): ${dead_broker_addrs}')

		// Collect alive broker addresses under health lock.
		// Health entries are keyed by broker_id (or addr when broker_id is absent).
		mut alive_brokers := []string{}
		m.broker_health_lock.@lock()
		for ref in refs_snapshot {
			health_key := if ref.broker_id != '' { ref.broker_id } else { ref.addr }
			health := m.broker_health[health_key] or { continue }
			if health.is_alive {
				alive_brokers << ref.addr
			}
		}
		m.broker_health_lock.unlock()

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
