module replication

import domain
import sync
import sync.stdatomic
import time
import infra.observability
import rand

// Manager coordinates all replication operations.
// Concurrency: all shared mutable state is protected by dedicated sync.Mutex locks.
// Lock ordering (to prevent deadlocks): assignments_lock -> broker_health_lock -> replica_buffers_lock -> stats_lock
// metrics_lock is a leaf lock (never held while another lock is held).
//
// Organized into focused files:
//   manager.v            - struct definition, constructor, lifecycle, core replication orchestration
//   manager_assignments.v - replica assignment operations (assignments_lock)
//   manager_health.v     - broker health tracking (broker_health_lock, cluster_broker_refs_lock)
//   manager_stats.v      - statistics and metrics queries (stats_lock, partition_metrics_lock)
//   manager_buffers.v    - replica buffer storage (replica_buffers_lock)
//   manager_workers.v    - background worker coroutines
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
	metrics_lock         sync.Mutex
	// Per-partition metrics (keyed by "topic:partition")
	partition_metrics      map[string]domain.PartitionMetrics
	partition_metrics_lock sync.Mutex
	// Cluster info (injected from outside)
	// cluster_broker_refs stores the stable ID and current address of every peer broker.
	// When broker_id is set, matching uses ID-based logic. When empty, falls back to addr comparison.
	cluster_broker_refs      []domain.BrokerRef
	cluster_broker_refs_lock sync.Mutex
	// State
	logger       &observability.Logger = unsafe { nil }
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
		logger:              observability.get_named_logger('replication.manager')
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
/// send_replicate sends record data to all assigned replica brokers for a given topic-partition.
pub fn (mut m Manager) send_replicate(topic string, partition i32, offset i64, records_data []u8) ! {
	if !m.config.enabled {
		return
	}

	key := '${topic}:${partition}'
	replica_brokers := m.get_or_create_replica_brokers(key, topic, partition)!

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
	m.metrics_lock.@lock()
	m.metrics.record_bytes_in(i64(records_data.len))
	m.metrics_lock.unlock()
	m.record_partition_sent(key, topic, partition, i64(records_data.len))
	m.record_stats_replicate()
}

// send_replicate_async sends a REPLICATE message to a single broker with exponential backoff retry.
// Intended to be called via `spawn` for parallel replication.
// Retries up to config.retry_count times before giving up.
fn (mut m Manager) send_replicate_async(broker_addr string, msg domain.ReplicationMessage) {
	max_attempts := if m.config.retry_count > 0 { m.config.retry_count } else { 1 }

	m.metrics_lock.@lock()
	m.metrics.increment_pending()
	m.metrics_lock.unlock()
	defer {
		m.metrics_lock.@lock()
		m.metrics.decrement_pending()
		m.metrics_lock.unlock()
	}

	send_start := time.now()

	for attempt in 0 .. max_attempts {
		response := m.client.send(broker_addr, msg) or {
			if attempt < max_attempts - 1 {
				backoff_ms := i64(100) * i64(u64(1) << u64(attempt))
				sleep_ms := if backoff_ms > 5000 { i64(5000) } else { backoff_ms }
				m.logger.warn('Replication to ${broker_addr} failed (attempt ${attempt + 1}/${max_attempts}): ${err}; retrying in ${sleep_ms}ms')
				m.metrics_lock.@lock()
				m.metrics.record_retried_request()
				m.metrics_lock.unlock()
				time.sleep(time.Duration(sleep_ms * time.millisecond))
				continue
			}
			m.logger.error('Replication to ${broker_addr} failed after ${max_attempts} attempt(s): ${err}')
			m.metrics_lock.@lock()
			m.metrics.record_failed_request()
			m.metrics_lock.unlock()
			m.record_stats_retry_exhausted()
			return
		}

		if !response.success {
			if attempt < max_attempts - 1 {
				backoff_ms := i64(100) * i64(u64(1) << u64(attempt))
				sleep_ms := if backoff_ms > 5000 { i64(5000) } else { backoff_ms }
				m.logger.warn('Replication to ${broker_addr} returned failure (attempt ${attempt + 1}/${max_attempts}): ${response.error_msg}; retrying in ${sleep_ms}ms')
				m.metrics_lock.@lock()
				m.metrics.record_retried_request()
				m.metrics_lock.unlock()
				time.sleep(time.Duration(sleep_ms * time.millisecond))
				continue
			}
			m.logger.error('Replication to ${broker_addr} failed after ${max_attempts} attempt(s): ${response.error_msg}')
			m.metrics_lock.@lock()
			m.metrics.record_failed_request()
			m.metrics_lock.unlock()
			m.record_stats_retry_exhausted()
			return
		}

		lag_ms := f64(time.since(send_start).milliseconds())
		m.metrics_lock.@lock()
		m.metrics.record_lag_sample(lag_ms)
		m.metrics.record_bytes_out(i64(msg.records_data.len))
		m.metrics_lock.unlock()

		key := '${msg.topic}:${msg.partition}'
		m.record_partition_acked(key, msg.topic, msg.partition, lag_ms)
		m.record_stats_ack()
		m.logger.debug('Replicated offset ${msg.offset} to ${broker_addr} (attempt ${attempt + 1})')
		return
	}
}

// send_flush_ack notifies all replica brokers that data up to the given offset
// has been flushed to S3. Replicas can safely discard their buffered data.
/// send_flush_ack notifies all replica brokers that data up to the given offset has been flushed.
pub fn (mut m Manager) send_flush_ack(topic string, partition i32, offset i64) ! {
	if !m.config.enabled {
		return
	}

	key := '${topic}:${partition}'
	replica_brokers := m.get_replica_brokers(key)!

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

	m.record_stats_flush_ack()
}

// recover_replica initiates recovery for a replica partition from the given offset.
// Sends a RECOVER request to the leader and counts the event as a replication operation.
/// recover_replica initiates recovery for a replica partition from the given offset.
pub fn (mut m Manager) recover_replica(topic string, partition i32, offset i64) ! {
	if !m.config.enabled {
		return
	}

	m.logger.info('Starting replica recovery topic=${topic} partition=${partition} offset=${offset}')

	key := '${topic}:${partition}'
	leader := m.get_assignment_leader(key)!

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

// create_handler creates a message handler for the TCP server.
// The handler processes incoming replication messages (replicate, flush_ack, heartbeat, recover).
fn (mut m Manager) create_handler() MessageHandler {
	return fn [mut m] (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		match msg.msg_type {
			.replicate {
				buffer := domain.ReplicaBuffer{
					topic:        msg.topic
					partition:    msg.partition
					offset:       msg.offset
					records_data: msg.records_data
					timestamp:    time.now().unix_milli()
				}
				m.store_replica_buffer(buffer)!

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
				m.delete_replica_buffer(msg.topic, msg.partition, msg.offset)!

				return domain.ReplicationMessage{
					msg_type:       domain.ReplicationType.replicate_ack
					correlation_id: msg.correlation_id
					sender_id:      m.broker_id
					timestamp:      time.now().unix_milli()
					success:        true
				}
			}
			.heartbeat {
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
