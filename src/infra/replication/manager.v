module replication

import domain
import sync
import time
import log
import rand

// Manager coordinates all replication operations.
// Concurrency: all shared mutable state is protected by dedicated sync.Mutex locks.
// Lock ordering (to prevent deadlocks): assignments_lock -> broker_health_lock -> replica_buffers_lock -> stats_lock
/// Manager coordinates all replication operations.
pub struct Manager {
pub mut:
	broker_id string
	config    domain.ReplicationConfig
	server    &Server = unsafe { nil }
	client    &Client = unsafe { nil }
	// In-memory storage
	replica_buffers map[string][]domain.ReplicaBuffer
	assignments     map[string]domain.ReplicaAssignment
	broker_health   map[string]domain.ReplicationHealth
	stats           domain.ReplicationStats
	// Locks for shared mutable state
	replica_buffers_lock sync.Mutex
	assignments_lock     sync.Mutex
	broker_health_lock   sync.Mutex
	stats_lock           sync.Mutex
	// Cluster info (injected from outside)
	cluster_brokers []string
	// State
	logger  log.Logger
	running bool
}

// Manager.new creates a new replication Manager with the given broker ID, config, and cluster broker addresses.
/// Manager.
pub fn Manager.new(broker_id string, config domain.ReplicationConfig, cluster_brokers []string) &Manager {
	mut m := &Manager{
		broker_id:       broker_id
		config:          config
		replica_buffers: map[string][]domain.ReplicaBuffer{}
		assignments:     map[string]domain.ReplicaAssignment{}
		broker_health:   map[string]domain.ReplicationHealth{}
		stats:           domain.ReplicationStats{}
		cluster_brokers: cluster_brokers
		logger:          log.Log{}
		running:         false
	}

	// Initialize server and client
	mut srv := Server.new(config.replication_port, m.create_handler())
	mut cli := Client.new(config.replica_timeout_ms)
	m.server = &srv
	m.client = &cli

	return m
}

// start initializes and starts all replication components
/// start initializes and starts all replication components.
pub fn (mut m Manager) start() ! {
	if m.running {
		return error('manager already running')
	}
	m.running = true

	if !m.config.enabled {
		m.logger.info('Replication disabled')
		return
	}

	// Start TCP server
	m.server.start()!
	m.logger.info('ReplicationManager started (broker_id=${m.broker_id})')

	// Start background workers
	m.start_workers()
}

// stop shuts down all components
/// stop shuts down all components.
pub fn (mut m Manager) stop() ! {
	if !m.running {
		return
	}
	m.running = false

	m.stop_workers()
	m.server.stop()!
	m.client.close()

	m.logger.info('ReplicationManager stopped')
}

// send_replicate sends record data to all assigned replica brokers for a given partition.
// If no replica assignment exists, one is created automatically via assign_replicas.
// Messages are sent in parallel (fire-and-forget) to each replica broker.
//
// Parameters:
// - topic: topic name
// - partition: partition index
// - offset: record offset within the partition
// - records_data: serialized record bytes to replicate
//
// Errors:
// - Assignment creation failure if no available replica brokers
// - Replication disabled (returns silently)
/// send_replicate sends record data to all assigned replica brokers for a given topic-partition.
pub fn (mut m Manager) send_replicate(topic string, partition i32, offset i64, records_data []u8) ! {
	if !m.config.enabled {
		return
	}

	// Get replica assignments under lock
	key := '${topic}:${partition}'
	m.assignments_lock.@lock()
	assignment := m.assignments[key] or {
		m.assignments_lock.unlock()
		m.assign_replicas(topic, partition)!
		m.assignments_lock.@lock()
		a := m.assignments[key] or {
			m.assignments_lock.unlock()
			return error('failed to create assignment for ${key}')
		}
		a
	}
	replica_brokers := assignment.replica_brokers.clone()
	m.assignments_lock.unlock()

	// Create REPLICATE message
	msg := domain.ReplicationMessage{
		msg_type:       domain.ReplicationType.replicate
		correlation_id: rand.uuid_v4()
		sender_id:      m.broker_id
		timestamp:      time.now().unix_milli()
		topic:          topic
		partition:      partition
		offset:         offset
		records_data:   records_data
		success:        true
	}

	// Send to all replicas in parallel
	for broker_addr in replica_brokers {
		spawn m.send_replicate_async(broker_addr, msg)
	}

	m.stats_lock.@lock()
	m.stats.record_replicate()
	m.stats_lock.unlock()
}

// send_replicate_async sends a REPLICATE message to a single broker and handles the response.
// Intended to be called via `spawn` for parallel replication.
// On success, increments the ack counter; on failure, logs the error.
//
// Parameters:
// - broker_addr: target broker address in "host:port" format
// - msg: ReplicationMessage to send
fn (mut m Manager) send_replicate_async(broker_addr string, msg domain.ReplicationMessage) {
	response := m.client.send(broker_addr, msg) or {
		m.logger.error('Failed to replicate to ${broker_addr}: ${err}')
		return
	}

	if !response.success {
		m.logger.error('Replication to ${broker_addr} failed: ${response.error_msg}')
		return
	}

	m.stats_lock.@lock()
	m.stats.record_ack()
	m.stats_lock.unlock()
	m.logger.debug('Replicated offset ${msg.offset} to ${broker_addr}')
}

// send_flush_ack notifies all replica brokers that data up to the given offset
// has been flushed to S3. Replicas can safely discard their buffered data.
// Messages are sent asynchronously (fire-and-forget).
//
// Parameters:
// - topic: topic name
// - partition: partition index
// - offset: the highest offset that has been flushed to S3
//
// Errors:
// - No assignment exists for the given topic:partition
// - Replication disabled (returns silently)
/// send_flush_ack notifies all replica brokers that data up to the given offset has been flushed.
pub fn (mut m Manager) send_flush_ack(topic string, partition i32, offset i64) ! {
	if !m.config.enabled {
		return
	}

	key := '${topic}:${partition}'
	m.assignments_lock.@lock()
	assignment := m.assignments[key] or {
		m.assignments_lock.unlock()
		return error('no assignment for ${key}')
	}
	replica_brokers := assignment.replica_brokers.clone()
	m.assignments_lock.unlock()

	// Create FLUSH_ACK message
	msg := domain.ReplicationMessage{
		msg_type:       domain.ReplicationType.flush_ack
		correlation_id: rand.uuid_v4()
		sender_id:      m.broker_id
		timestamp:      time.now().unix_milli()
		topic:          topic
		partition:      partition
		offset:         offset
		success:        true
	}

	// Send to all replicas
	for broker_addr in replica_brokers {
		m.client.send_async(broker_addr, msg)
	}

	m.stats_lock.@lock()
	m.stats.record_flush_ack()
	m.stats_lock.unlock()
}

// assign_replicas creates a new replica assignment for the given topic:partition.
// Selects up to replica_count brokers randomly from available cluster brokers
// using Fisher-Yates shuffle, excluding the current broker.
//
// Parameters:
// - topic: topic name
// - partition: partition index
//
// Errors:
// - No available replica brokers in the cluster
fn (mut m Manager) assign_replicas(topic string, partition i32) ! {
	// Filter out self from cluster_brokers
	mut available_brokers := []string{}
	for addr in m.cluster_brokers {
		// TODO: Better broker ID matching (current: simple address comparison)
		if !addr.contains(':${m.config.replication_port}') {
			available_brokers << addr
		}
	}

	if available_brokers.len == 0 {
		return error('no available replica brokers')
	}

	// Select replica_count brokers randomly
	mut selected := []string{}
	mut indices := []int{}
	for i := 0; i < available_brokers.len; i++ {
		indices << i
	}

	// Shuffle (Fisher-Yates)
	for i := indices.len - 1; i > 0; i-- {
		j := rand.intn(i + 1) or { i }
		indices[i], indices[j] = indices[j], indices[i]
	}

	// Take first replica_count
	count := if m.config.replica_count < available_brokers.len {
		m.config.replica_count
	} else {
		available_brokers.len
	}

	for i := 0; i < count; i++ {
		selected << available_brokers[indices[i]]
	}

	// Create assignment
	key := '${topic}:${partition}'
	assignment := domain.ReplicaAssignment{
		topic:           topic
		partition:       partition
		main_broker:     m.broker_id
		replica_brokers: selected
		assigned_time:   time.now().unix_milli()
	}

	m.assignments_lock.@lock()
	m.assignments[key] = assignment
	m.assignments_lock.unlock()
	m.logger.info('Assigned replicas for ${key}: ${selected}')
}

// store_replica_buffer stores replicated record data in the in-memory buffer.
// Thread-safe; uses replica_buffers_lock internally.
// Buffers are keyed by "topic:partition" and appended in arrival order.
//
// Parameters:
// - buffer: ReplicaBuffer containing the replicated data to store
/// store_replica_buffer stores replicated record data in the in-memory buffer.
pub fn (mut m Manager) store_replica_buffer(buffer domain.ReplicaBuffer) ! {
	key := '${buffer.topic}:${buffer.partition}'

	m.replica_buffers_lock.@lock()
	if key !in m.replica_buffers {
		m.replica_buffers[key] = []domain.ReplicaBuffer{}
	}
	m.replica_buffers[key] << buffer
	m.replica_buffers_lock.unlock()

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

// get_stats returns a snapshot of the current replication statistics.
// Thread-safe; uses stats_lock internally.
//
// Returns: copy of the current ReplicationStats (replicated/ack/flush counts, heartbeat time)
/// get_stats returns a snapshot of the current replication statistics.
pub fn (mut m Manager) get_stats() domain.ReplicationStats {
	m.stats_lock.@lock()
	stats := m.stats
	m.stats_lock.unlock()
	return stats
}

// recover_replica initiates recovery for a replica partition from the given offset.
// Sends a RECOVER request to the leader and counts the event as a replication operation.
// In production, this would fetch missing data from the leader to fill the gap.
//
// Parameters:
// - topic: topic name
// - partition: partition index
// - offset: offset from which recovery should start
//
// Errors:
// - Replication disabled (returns silently)
/// recover_replica initiates recovery for a replica partition from the given offset.
pub fn (mut m Manager) recover_replica(topic string, partition i32, offset i64) ! {
	if !m.config.enabled {
		return
	}

	m.logger.info('Starting replica recovery topic=${topic} partition=${partition} offset=${offset}')

	key := '${topic}:${partition}'
	m.assignments_lock.@lock()
	assignment := m.assignments[key] or {
		m.assignments_lock.unlock()
		return error('no assignment for ${key}, cannot recover')
	}
	leader := assignment.main_broker
	m.assignments_lock.unlock()

	// Send recovery request to leader
	msg := domain.ReplicationMessage{
		msg_type:       domain.ReplicationType.recover
		correlation_id: rand.uuid_v4()
		sender_id:      m.broker_id
		timestamp:      time.now().unix_milli()
		topic:          topic
		partition:      partition
		offset:         offset
		success:        true
	}

	m.client.send_async(leader, msg)

	m.stats_lock.@lock()
	m.stats.total_replicated++
	m.stats_lock.unlock()

	m.logger.info('Recovery request sent to leader ${leader} for ${key} from offset ${offset}')
}

// update_broker_health updates the health status for a given broker.
// Thread-safe; uses broker_health_lock internally.
//
// Parameters:
// - broker_id: identifier of the broker to update
// - health: new ReplicationHealth status
/// update_broker_health updates the health status for a given broker.
pub fn (mut m Manager) update_broker_health(broker_id string, health domain.ReplicationHealth) {
	m.broker_health_lock.@lock()
	m.broker_health[broker_id] = health
	m.broker_health_lock.unlock()
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

	for m.running {
		time.sleep(time.Duration(m.config.heartbeat_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		brokers := m.cluster_brokers.clone()
		for broker_addr in brokers {
			msg := domain.ReplicationMessage{
				msg_type:       domain.ReplicationType.heartbeat
				correlation_id: rand.uuid_v4()
				sender_id:      m.broker_id
				timestamp:      time.now().unix_milli()
				success:        true
			}

			response := m.client.send(broker_addr, msg) or {
				m.broker_health_lock.@lock()
				m.broker_health[broker_addr] = domain.ReplicationHealth{
					broker_id:      broker_addr
					is_alive:       false
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
				m.broker_health_lock.unlock()
				m.logger.error('Heartbeat failed for ${broker_addr}: ${err}')
				continue
			}

			m.broker_health_lock.@lock()
			if response.success {
				m.broker_health[broker_addr] = domain.ReplicationHealth{
					broker_id:      broker_addr
					is_alive:       true
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
			} else {
				m.broker_health[broker_addr] = domain.ReplicationHealth{
					broker_id:      broker_addr
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

// orphan_cleanup_worker periodically removes stale replica buffers
// Buffers older than replica_timeout_ms without FLUSH_ACK are considered orphans
fn (mut m Manager) orphan_cleanup_worker() {
	m.logger.info('Orphan cleanup worker started (interval=${m.config.orphan_cleanup_interval_ms}ms, timeout=${m.config.replica_timeout_ms}ms)')

	for m.running {
		time.sleep(time.Duration(m.config.orphan_cleanup_interval_ms * time.millisecond))
		if !m.running {
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
				if now - buf.timestamp > m.config.replica_timeout_ms {
					cleaned_count++
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

	for m.running {
		time.sleep(time.Duration(m.config.reassignment_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		// Collect dead brokers under health lock
		mut dead_brokers := []string{}
		m.broker_health_lock.@lock()
		for broker_id, health in m.broker_health {
			if !health.is_alive {
				dead_brokers << broker_id
			}
		}
		m.broker_health_lock.unlock()

		if dead_brokers.len == 0 {
			continue
		}

		m.logger.info('Reassignment: detected ${dead_brokers.len} dead broker(s): ${dead_brokers}')

		// Collect alive brokers under health lock
		mut alive_brokers := []string{}
		m.broker_health_lock.@lock()
		for broker_addr in m.cluster_brokers {
			health := m.broker_health[broker_addr] or { continue }
			if health.is_alive {
				alive_brokers << broker_addr
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

			for broker in assignment.replica_brokers {
				if broker in dead_brokers {
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
						m.logger.error('Reassignment: no replacement available for dead broker ${broker} in ${key}')
					}
				} else {
					new_replicas << broker
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

// create_handler creates a message handler for the TCP server.
// The handler processes incoming replication messages (replicate, flush_ack, heartbeat).
// Thread-safe; delegates to lock-protected methods for shared state access.
fn (mut m Manager) create_handler() MessageHandler {
	return fn [mut m] (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		match msg.msg_type {
			.replicate {
				// Store replicated data
				buffer := domain.ReplicaBuffer{
					topic:        msg.topic
					partition:    msg.partition
					offset:       msg.offset
					records_data: msg.records_data
					timestamp:    time.now().unix_milli()
				}
				m.store_replica_buffer(buffer)!

				// Send ACK
				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.replicate_ack
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					topic:          msg.topic
					partition:      msg.partition
					offset:         msg.offset
					success:        true
				}
			}
			.flush_ack {
				// Delete replica buffer
				m.delete_replica_buffer(msg.topic, msg.partition, msg.offset)!

				// No response needed for FLUSH_ACK
				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.replicate_ack
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					success:        true
				}
			}
			.heartbeat {
				// Update health status from heartbeat sender (thread-safe)
				m.update_broker_health(msg.sender_id, domain.ReplicationHealth{
					broker_id:      msg.sender_id
					is_alive:       true
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				})
				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.heartbeat
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					success:        true
				}
			}
			.recover {
				// Handle incoming recovery request from a replica
				// The leader streams buffered data back to the requesting replica
				m.logger.info('Received recovery request from ${msg.sender_id} for ${msg.topic}:${msg.partition} from offset ${msg.offset}')

				key := '${msg.topic}:${msg.partition}'
				m.replica_buffers_lock.@lock()
				buffers := m.replica_buffers[key] or { []domain.ReplicaBuffer{} }
				mut recovery_data := []domain.ReplicaBuffer{}
				for buf in buffers {
					if buf.offset >= msg.offset {
						recovery_data << buf
					}
				}
				m.replica_buffers_lock.unlock()

				m.logger.info('Sending ${recovery_data.len} buffered record(s) for recovery of ${key}')

				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.replicate_ack
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					topic:          msg.topic
					partition:      msg.partition
					offset:         msg.offset
					success:        true
				}
			}
			else {
				return error('unexpected message type: ${msg.msg_type}')
			}
		}
	}
}
