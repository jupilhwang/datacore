module replication

import domain
import time
import log
import rand

/// Manager coordinates all replication operations
pub struct Manager {
pub mut:
	broker_id string
	config    domain.ReplicationConfig
	server    &Server = unsafe { nil }
	client    &Client = unsafe { nil }
	// In-memory storage
	replica_buffers map[string][]domain.ReplicaBuffer   // Key: "topic:partition"
	assignments     map[string]domain.ReplicaAssignment // Key: "topic:partition"
	broker_health   map[string]domain.ReplicationHealth // Key: broker_id
	stats           domain.ReplicationStats
	// Cluster info (injected from outside)
	cluster_brokers []string // List of all broker addresses (e.g., "localhost:9093")
	// State
	logger  log.Logger
	running bool
}

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

/// start initializes and starts all replication components
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

/// stop shuts down all components
pub fn (mut m Manager) stop() ! {
	if !m.running {
		return
	}
	m.running = false

	m.stop_workers()
	m.server.stop()!
	m.client.close()!

	m.logger.info('ReplicationManager stopped')
}

/// send_replicate sends data to replica brokers
pub fn (mut m Manager) send_replicate(topic string, partition i32, offset i64, records_data []u8) ! {
	if !m.config.enabled {
		return
	}

	// Get replica assignments
	key := '${topic}:${partition}'
	assignment := m.assignments[key] or {
		// No assignment yet, create one
		m.assign_replicas(topic, partition)!
		m.assignments[key] or { return error('failed to create assignment for ${key}') }
	}
	replica_brokers := assignment.replica_brokers.clone()

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

	m.stats.record_replicate()
}

/// send_replicate_async sends REPLICATE message asynchronously
fn (mut m Manager) send_replicate_async(broker_addr string, msg domain.ReplicationMessage) {
	response := m.client.send(broker_addr, msg) or {
		m.logger.error('Failed to replicate to ${broker_addr}: ${err}')
		return
	}

	if !response.success {
		m.logger.error('Replication to ${broker_addr} failed: ${response.error_msg}')
		return
	}

	m.stats.record_ack()
	m.logger.debug('Replicated offset ${msg.offset} to ${broker_addr}')
}

/// send_flush_ack sends FLUSH_ACK to all replica brokers
pub fn (mut m Manager) send_flush_ack(topic string, partition i32, offset i64) ! {
	if !m.config.enabled {
		return
	}

	key := '${topic}:${partition}'
	assignment := m.assignments[key] or { return error('no assignment for ${key}') }
	replica_brokers := assignment.replica_brokers.clone()

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
		m.client.send_async(broker_addr, msg)!
	}

	m.stats.record_flush_ack()
}

/// assign_replicas creates replica assignments for a partition
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

	m.assignments[key] = assignment
	m.logger.info('Assigned replicas for ${key}: ${selected}')
}

/// store_replica_buffer stores replicated data in memory
pub fn (mut m Manager) store_replica_buffer(buffer domain.ReplicaBuffer) ! {
	key := '${buffer.topic}:${buffer.partition}'

	if key !in m.replica_buffers {
		m.replica_buffers[key] = []domain.ReplicaBuffer{}
	}
	m.replica_buffers[key] << buffer

	m.logger.debug('Stored replica buffer for ${key}, offset ${buffer.offset}')
}

/// delete_replica_buffer deletes replica buffer after S3 flush confirmation
pub fn (mut m Manager) delete_replica_buffer(topic string, partition i32, offset i64) ! {
	key := '${topic}:${partition}'

	if key in m.replica_buffers {
		// Filter out buffers with offset <= flush offset
		mut remaining := []domain.ReplicaBuffer{}
		for buf in m.replica_buffers[key] {
			if buf.offset > offset {
				remaining << buf
			}
		}
		m.replica_buffers[key] = remaining
	}

	m.logger.debug('Deleted replica buffers for ${key} up to offset ${offset}')
}

/// get_all_replica_buffers returns all replica buffers (for crash recovery)
pub fn (m Manager) get_all_replica_buffers() ![]domain.ReplicaBuffer {
	mut all_buffers := []domain.ReplicaBuffer{}

	for _, buffers in m.replica_buffers {
		all_buffers << buffers
	}

	return all_buffers
}

/// get_stats returns replication statistics
pub fn (m Manager) get_stats() domain.ReplicationStats {
	return m.stats
}

/// start_workers starts periodic background workers
fn (mut m Manager) start_workers() {
	if !m.config.enabled {
		return
	}

	spawn m.heartbeat_worker()
	spawn m.orphan_cleanup_worker()
	spawn m.reassignment_worker()

	m.logger.info('Background workers started (heartbeat, orphan_cleanup, reassignment)')
}

/// stop_workers stops all background workers
fn (mut m Manager) stop_workers() {
	// running is already set to false in stop()
	// Workers will exit their loops on next iteration
	m.logger.info('Background workers stop signal sent')
}

/// heartbeat_worker periodically sends heartbeat messages to all cluster brokers
fn (mut m Manager) heartbeat_worker() {
	m.logger.info('Heartbeat worker started (interval=${m.config.heartbeat_interval_ms}ms)')

	for m.running {
		time.sleep(time.Duration(m.config.heartbeat_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		brokers := m.cluster_brokers.clone()
		for broker_addr in brokers {
			mut msg := domain.ReplicationMessage{
				msg_type:       domain.ReplicationType.heartbeat
				correlation_id: rand.uuid_v4()
				sender_id:      m.broker_id
				timestamp:      time.now().unix_milli()
				success:        true
			}

			response := m.client.send(broker_addr, msg) or {
				m.broker_health[broker_addr] = domain.ReplicationHealth{
					broker_id:      broker_addr
					is_alive:       false
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
				m.logger.error('Heartbeat failed for ${broker_addr}: ${err}')
				continue
			}

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
		}
		m.stats.update_heartbeat()
	}

	m.logger.info('Heartbeat worker stopped')
}

/// orphan_cleanup_worker periodically removes stale replica buffers
/// Buffers older than 60 seconds without FLUSH_ACK are considered orphans
fn (mut m Manager) orphan_cleanup_worker() {
	m.logger.info('Orphan cleanup worker started (interval=${m.config.orphan_cleanup_interval_ms}ms)')
	orphan_max_age_ms := i64(60000) // 60 seconds

	for m.running {
		time.sleep(time.Duration(m.config.orphan_cleanup_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		now := time.now().unix_milli()
		cutoff := now - orphan_max_age_ms

		keys := m.replica_buffers.keys()
		for key in keys {
			buffers := m.replica_buffers[key] or { continue }
			mut remaining := []domain.ReplicaBuffer{}
			mut cleaned_count := 0

			for buf in buffers {
				if buf.timestamp > cutoff {
					remaining << buf
				} else {
					cleaned_count++
				}
			}

			if cleaned_count > 0 {
				m.replica_buffers[key] = remaining
				for _ in 0 .. cleaned_count {
					m.stats.record_orphan_cleanup()
				}
				m.logger.info('Orphan cleanup: removed ${cleaned_count} stale buffers from ${key}')
			}
		}
	}

	m.logger.info('Orphan cleanup worker stopped')
}

/// reassignment_worker periodically checks for dead brokers and reassigns replicas
fn (mut m Manager) reassignment_worker() {
	m.logger.info('Reassignment worker started (interval=${m.config.reassignment_interval_ms}ms)')

	for m.running {
		time.sleep(time.Duration(m.config.reassignment_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		// Collect dead brokers
		mut dead_brokers := []string{}
		for broker_id, health in m.broker_health {
			if !health.is_alive {
				dead_brokers << broker_id
			}
		}

		if dead_brokers.len == 0 {
			continue
		}

		m.logger.info('Reassignment: detected ${dead_brokers.len} dead broker(s): ${dead_brokers}')

		// Collect alive brokers
		mut alive_brokers := []string{}
		for broker_addr in m.cluster_brokers {
			health := m.broker_health[broker_addr] or { continue }
			if health.is_alive {
				alive_brokers << broker_addr
			}
		}

		// Reassign partitions
		assignment_keys := m.assignments.keys()
		for key in assignment_keys {
			assignment := m.assignments[key] or { continue }
			mut new_replicas := []string{}
			mut changed := false

			for broker in assignment.replica_brokers {
				if broker in dead_brokers {
					changed = true
					// Find a replacement from alive brokers
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
	}

	m.logger.info('Reassignment worker stopped')
}

/// create_handler creates a message handler for the TCP server
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
				// Update health status from heartbeat sender
				m.broker_health[msg.sender_id] = domain.ReplicationHealth{
					broker_id:      msg.sender_id
					is_alive:       true
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				}
				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.heartbeat
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					success:        true
				}
			}
			else {
				return error('unexpected message type: ${msg.msg_type}')
			}
		}
	}
}
