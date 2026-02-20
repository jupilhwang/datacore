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
	metrics         domain.ReplicationMetrics
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
	cluster_broker_refs []domain.BrokerRef
	// State
	logger  log.Logger
	running bool
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
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		metrics:             domain.new_replication_metrics()
		partition_metrics:   map[string]domain.PartitionMetrics{}
		cluster_broker_refs: cluster_broker_refs
		logger:              log.Log{}
		running:             false
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

	// Record incoming bytes and per-partition metrics
	m.metrics.record_bytes_in(i64(records_data.len))
	key2 := '${topic}:${partition}'
	m.partition_metrics_lock.@lock()
	if key2 !in m.partition_metrics {
		m.partition_metrics[key2] = domain.new_partition_metrics(topic, partition)
	}
	m.partition_metrics[key2].record_sent(i64(records_data.len))
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
	for ref in m.cluster_broker_refs.clone() {
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
			}
		}
		m.replica_buffers[key] = kept
	}

	// Enforce max_replica_buffer_size_bytes when limit is set (> 0).
	if m.config.max_replica_buffer_size_bytes > 0 {
		mut total_bytes := i64(0)
		for _, bufs in m.replica_buffers {
			for b in bufs {
				total_bytes += i64(b.records_data.len)
			}
		}
		if total_bytes + i64(buffer.records_data.len) > m.config.max_replica_buffer_size_bytes {
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

// update_cluster_broker_refs replaces the peer broker list used for replica assignment
// and heartbeat. Safe to call while the manager is running; the change takes effect on
// the next heartbeat/assignment cycle.
//
// Parameters:
// - refs: updated slice of BrokerRef containing each peer's stable ID and address.
/// update_cluster_broker_refs replaces the live list of peer brokers.
pub fn (mut m Manager) update_cluster_broker_refs(refs []domain.BrokerRef) {
	// No separate lock needed: cluster_broker_refs is only mutated here and read by
	// workers that clone the slice at the start of each iteration, so a direct assignment
	// is safe for the read side (workers tolerate a transient stale view for one cycle).
	m.cluster_broker_refs = refs
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

	for m.running {
		time.sleep(time.Duration(m.config.heartbeat_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		refs := m.cluster_broker_refs.clone()
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
				if now - buf.timestamp > ttl_ms {
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

	for m.running {
		time.sleep(time.Duration(m.config.reassignment_interval_ms * time.millisecond))
		if !m.running {
			break
		}

		// Build broker_id -> addr and addr -> broker_id lookup maps from cluster_broker_refs.
		// These maps allow reconciling broker_health keys (broker_id) with replica_brokers
		// values (addr) without holding any extra lock.
		refs_snapshot := m.cluster_broker_refs.clone()
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
