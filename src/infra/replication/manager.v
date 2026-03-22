module replication

import domain
import sync
import sync.stdatomic
import time
import log
import rand

// Manager coordinates all replication operations.
// Concurrency: all shared mutable state is protected by dedicated sync.Mutex locks.
// Lock ordering (to prevent deadlocks): assignments_lock -> broker_health_lock -> replica_buffers_lock -> stats_lock
/// Manager coordinates all replication operations.
pub struct Manager {
pub mut:
	broker_id       string
	config          domain.ReplicationConfig
	binary_protocol BinaryProtocol
	server          &Server = unsafe { nil }
	client          &Client = unsafe { nil }
	// In-memory storage
	replica_buffers map[string][]domain.ReplicaBuffer
	assignments     map[string]domain.ReplicaAssignment
	broker_health   map[string]domain.ReplicationHealth
	stats           domain.ReplicationStats
	metrics         domain.ReplicationMetrics
	// Incremental byte counter for replica buffers (protected by replica_buffers_lock)
	total_buffer_bytes i64
	// Locks for shared mutable state
	replica_buffers_lock sync.Mutex
	assignments_lock     sync.Mutex
	broker_health_lock   sync.Mutex
	stats_lock           sync.Mutex
	// Per-partition metrics (keyed by "topic:partition")
	partition_metrics      map[string]domain.PartitionMetrics
	partition_metrics_lock sync.Mutex
	// Cluster info (injected from outside)
	// cluster_broker_refs stores the stable ID and current address of every peer broker.
	// When broker_id is set, matching uses ID-based logic. When empty, falls back to addr comparison.
	cluster_broker_refs      []domain.BrokerRef
	cluster_broker_refs_lock sync.Mutex
	// State
	logger       log.Logger
	running_flag i64
}

// Manager.new creates a new replication Manager with the given broker ID, config, and cluster broker refs.
// cluster_broker_refs holds each peer's stable broker_id and current address.
// For backward compatibility, you may pass BrokerRef with an empty broker_id; in that case
// the manager falls back to address-based self-exclusion.
/// Manager.new creates a new replication Manager with the given broker ID, config, and cluster broker refs.
pub fn Manager.new(broker_id string, config domain.ReplicationConfig, cluster_broker_refs []domain.BrokerRef) &Manager {
	mut m := &Manager{
		broker_id:           broker_id
		config:              config
		binary_protocol:     BinaryProtocol.new()
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		metrics:             domain.new_replication_metrics()
		partition_metrics:   map[string]domain.PartitionMetrics{}
		cluster_broker_refs: cluster_broker_refs
		logger:              log.Log{}
		running_flag:        0
	}

	// Initialize server and client (heap-allocated to prevent dangling pointers)
	m.server = Server.new(config.replication_port, m.create_handler())
	m.client = Client.new(config.replica_timeout_ms)

	return m
}

// start initializes and starts all replication components
/// start initializes and starts all replication components.
pub fn (mut m Manager) start() ! {
	if stdatomic.load_i64(&m.running_flag) == 1 {
		return error('manager already running')
	}
	stdatomic.store_i64(&m.running_flag, 1)

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
	if stdatomic.load_i64(&m.running_flag) != 1 {
		return
	}
	stdatomic.store_i64(&m.running_flag, 0)

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

	// Record incoming bytes and per-partition metrics
	m.metrics.record_bytes_in(i64(records_data.len))
	m.partition_metrics_lock.@lock()
	if key !in m.partition_metrics {
		m.partition_metrics[key] = domain.new_partition_metrics(topic, partition)
	}
	m.partition_metrics[key].record_sent(i64(records_data.len))
	m.partition_metrics_lock.unlock()

	m.stats_lock.@lock()
	m.stats.record_replicate()
	m.stats_lock.unlock()
}

// send_replicate_async sends a REPLICATE message to a single broker with exponential backoff retry.
// Intended to be called via `spawn` for parallel replication.
// Retries up to config.retry_count times before giving up. On success increments ack counter;
// on final failure logs the error and records a retry_exhausted metric.
//
// Parameters:
// - broker_addr: target broker address in "host:port" format
// - msg: ReplicationMessage to send
fn (mut m Manager) send_replicate_async(broker_addr string, msg domain.ReplicationMessage) {
	max_attempts := if m.config.retry_count > 0 { m.config.retry_count } else { 1 }

	m.metrics.increment_pending()
	defer {
		m.metrics.decrement_pending()
	}

	send_start := time.now()

	for attempt in 0 .. max_attempts {
		response := m.client.send(broker_addr, msg) or {
			if attempt < max_attempts - 1 {
				// Exponential backoff: 100ms * 2^attempt, capped at 5000ms
				backoff_ms := i64(100) * i64(u64(1) << u64(attempt))
				sleep_ms := if backoff_ms > 5000 { i64(5000) } else { backoff_ms }
				m.logger.warn('Replication to ${broker_addr} failed (attempt ${attempt + 1}/${max_attempts}): ${err}; retrying in ${sleep_ms}ms')
				m.metrics.record_retried_request()
				time.sleep(time.Duration(sleep_ms * time.millisecond))
				continue
			}
			m.logger.error('Replication to ${broker_addr} failed after ${max_attempts} attempt(s): ${err}')
			m.metrics.record_failed_request()
			m.stats_lock.@lock()
			m.stats.record_retry_exhausted()
			m.stats_lock.unlock()
			return
		}

		if !response.success {
			if attempt < max_attempts - 1 {
				backoff_ms := i64(100) * i64(u64(1) << u64(attempt))
				sleep_ms := if backoff_ms > 5000 { i64(5000) } else { backoff_ms }
				m.logger.warn('Replication to ${broker_addr} returned failure (attempt ${attempt + 1}/${max_attempts}): ${response.error_msg}; retrying in ${sleep_ms}ms')
				m.metrics.record_retried_request()
				time.sleep(time.Duration(sleep_ms * time.millisecond))
				continue
			}
			m.logger.error('Replication to ${broker_addr} failed after ${max_attempts} attempt(s): ${response.error_msg}')
			m.metrics.record_failed_request()
			m.stats_lock.@lock()
			m.stats.record_retry_exhausted()
			m.stats_lock.unlock()
			return
		}

		lag_ms := f64(time.since(send_start).milliseconds())
		m.metrics.record_lag_sample(lag_ms)
		m.metrics.record_bytes_out(i64(msg.records_data.len))

		// Update per-partition metrics
		key := '${msg.topic}:${msg.partition}'
		m.partition_metrics_lock.@lock()
		if key !in m.partition_metrics {
			m.partition_metrics[key] = domain.new_partition_metrics(msg.topic, msg.partition)
		}
		m.partition_metrics[key].record_acked()
		m.partition_metrics[key].record_lag(lag_ms)
		m.partition_metrics_lock.unlock()

		m.stats_lock.@lock()
		m.stats.record_ack()
		m.stats_lock.unlock()
		m.logger.debug('Replicated offset ${msg.offset} to ${broker_addr} (attempt ${attempt + 1})')
		return
	}
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
	m.cluster_broker_refs_lock.@lock()
	refs_snapshot := m.cluster_broker_refs.clone()
	m.cluster_broker_refs_lock.unlock()
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

// get_metrics returns a snapshot of the comprehensive replication metrics.
// Thread-safe.
//
// Returns: ReplicationMetricsSnapshot with throughput, latency, queue, and connection metrics
/// get_metrics returns a snapshot of the comprehensive replication metrics.
pub fn (mut m Manager) get_metrics() domain.ReplicationMetricsSnapshot {
	return m.metrics.snapshot()
}

// get_partition_metrics returns a copy of the per-partition metrics map.
// Thread-safe; uses partition_metrics_lock internally.
//
// Returns: map of "topic:partition" -> PartitionMetrics
/// get_partition_metrics returns per-partition replication metrics.
pub fn (mut m Manager) get_partition_metrics() map[string]domain.PartitionMetrics {
	m.partition_metrics_lock.@lock()
	mut result := map[string]domain.PartitionMetrics{}
	for k, v in m.partition_metrics {
		result[k] = v
	}
	m.partition_metrics_lock.unlock()
	return result
}

// flush_metrics_rates recomputes per-second throughput rates.
// Should be called periodically (e.g., every second) from a background task.
/// flush_metrics_rates recomputes per-second throughput rates.
pub fn (mut m Manager) flush_metrics_rates() {
	m.metrics.flush_rates()
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

				// Concatenate all buffered records_data into a single byte slice
				mut combined := []u8{}
				for rd in recovery_data {
					combined << rd.records_data
				}

				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.replicate_ack
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					topic:          msg.topic
					partition:      msg.partition
					offset:         msg.offset
					records_data:   combined
					success:        true
				}
			}
			else {
				return error('unexpected message type: ${msg.msg_type}')
			}
		}
	}
}
