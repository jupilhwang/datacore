module domain

import time

/// BrokerRef holds a broker's stable ID and its current network address.
/// Used by the ReplicationManager to identify brokers independently of address changes.
/// If broker_id is empty, the implementation falls back to address-based matching
/// for backward compatibility with older configurations.
pub struct BrokerRef {
pub mut:
	broker_id string // Stable unique identifier (e.g., "broker-1" or UUID). Empty means legacy mode.
	addr      string // Current network address in "host:port" format.
}

/// ReplicationType defines an enumeration of related values.
pub enum ReplicationType {
	replicate
	replicate_ack
	flush_ack
	heartbeat
	recover
}

/// ReplicationMessage is a struct holding related data.
pub struct ReplicationMessage {
pub mut:
	msg_type       ReplicationType
	correlation_id string // UUID for tracking request/response pairs
	sender_id      string
	timestamp      i64
	topic          string
	partition      i32
	offset         i64
	records_data   []u8   // Serialized record data (only for REPLICATE)
	success        bool   // For ACKs
	error_msg      string // For error cases
}

/// ReplicaBuffer is a struct holding related data.
pub struct ReplicaBuffer {
pub mut:
	topic        string
	partition    i32
	offset       i64
	records_data []u8
	timestamp    i64 // When replicated
}

/// ReplicationConfig is a struct holding related data.
pub struct ReplicationConfig {
pub mut:
	enabled                    bool
	replication_port           int // TCP port for replication (default: 9094)
	replica_count              int
	replica_timeout_ms         int // Timeout for replica response (default: 5000)
	heartbeat_interval_ms      int // Heartbeat interval (default: 3000)
	reassignment_interval_ms   int // Replica reassignment interval (default: 30000)
	orphan_cleanup_interval_ms int // Orphan buffer cleanup interval (default: 60000)
	// Replication limits
	retry_count                   int = 3     // Maximum number of failed replication attempts before giving up (default: 3)
	replica_buffer_ttl_ms         i64 = 60000 // Maximum age (ms) of a message in the replica buffer; older messages are dropped (default: 60000)
	max_replica_buffer_size_bytes i64 // Maximum total bytes allowed in the replica buffer; 0 = unlimited (default: 0)
}

/// ReplicationStats is a struct holding related data.
pub struct ReplicationStats {
pub mut:
	total_replicated      i64
	total_ack_received    i64
	total_flush_ack_sent  i64
	total_orphans_cleaned i64
	last_heartbeat_time   i64
	replica_lag_ms        i64
	// Limit-related counters
	total_retry_exhausted i64 // How many times all retries were exhausted for a single replication attempt
	total_ttl_dropped     i64 // Messages dropped from replica buffer due to TTL expiry
	total_buffer_overflow i64 // How many times the buffer size limit was exceeded (resulting in message rejection)
}

// rolling_window_size is the number of samples retained for percentile calculations.
const rolling_window_size = 1024

/// ReplicationMetrics provides comprehensive observability for the replication subsystem.
/// Thread-safety is NOT provided at this layer; callers (infra) must synchronize access.
pub struct ReplicationMetrics {
pub mut:
	// --- Throughput ---
	bytes_in_per_sec     f64 // Incoming bytes per second (rolling rate)
	bytes_out_per_sec    f64 // Outgoing bytes per second (rolling rate)
	messages_in_per_sec  f64 // Messages ingested per second (rolling rate)
	messages_out_per_sec f64 // Messages replicated per second (rolling rate)
	// --- Latency (milliseconds) ---
	avg_replication_lag_ms f64 // Average replication lag
	max_replication_lag_ms f64 // Maximum replication lag observed
	p95_replication_lag_ms f64 // 95th-percentile replication lag
	p99_replication_lag_ms f64 // 99th-percentile replication lag
	// --- Queue / request counters ---
	pending_requests i64 // Number of replication requests currently in-flight
	failed_requests  i64 // Total failed replication requests (cumulative)
	retried_requests i64 // Total retried requests (cumulative)
	// --- Connection counters ---
	active_connections i64 // Current number of active TCP connections
	connection_errors  i64 // Total connection errors (cumulative)
	reconnection_count i64 // Total successful reconnections (cumulative)
	// --- Internal accumulators for rate calculation ---
	window_start_time   i64 // Unix-milli timestamp of current measurement window start
	window_bytes_in     i64 // Bytes received in current window
	window_bytes_out    i64 // Bytes sent in current window
	window_messages_in  i64 // Messages received in current window
	window_messages_out i64 // Messages sent in current window
	// --- Rolling-window lag samples for percentile calculation ---
	lag_samples     []f64 // Circular buffer of lag samples (ms)
	lag_sample_head int   // Write index into lag_samples (wraps at rolling_window_size)
	lag_sample_full bool  // True once the buffer has been filled at least once
}

/// new_replication_metrics creates a ReplicationMetrics instance ready for use.
pub fn new_replication_metrics() ReplicationMetrics {
	return ReplicationMetrics{
		window_start_time: time.now().unix_milli()
		lag_samples:       []f64{len: rolling_window_size, init: 0.0}
	}
}

/// record_bytes_in records incoming bytes and increments the message-in counter.
/// Call once per received message batch.
pub fn (mut m ReplicationMetrics) record_bytes_in(bytes i64) {
	m.window_bytes_in += bytes
	m.window_messages_in++
}

/// record_bytes_out records outgoing bytes and increments the message-out counter.
/// Call once per successfully replicated message batch.
pub fn (mut m ReplicationMetrics) record_bytes_out(bytes i64) {
	m.window_bytes_out += bytes
	m.window_messages_out++
}

/// record_lag_sample adds a single replication lag observation (in milliseconds)
/// to the rolling window. Percentile statistics are recalculated lazily on snapshot().
pub fn (mut m ReplicationMetrics) record_lag_sample(lag_ms f64) {
	// Write into circular buffer
	m.lag_samples[m.lag_sample_head] = lag_ms
	m.lag_sample_head = (m.lag_sample_head + 1) % rolling_window_size
	if !m.lag_sample_full && m.lag_sample_head == 0 {
		m.lag_sample_full = true
	}
}

/// recalculate_lag_stats recomputes avg/max/p95/p99 from the current sample buffer.
fn (mut m ReplicationMetrics) recalculate_lag_stats() {
	n := if m.lag_sample_full { rolling_window_size } else { m.lag_sample_head }
	if n == 0 {
		m.avg_replication_lag_ms = 0.0
		m.max_replication_lag_ms = 0.0
		m.p95_replication_lag_ms = 0.0
		m.p99_replication_lag_ms = 0.0
		return
	}

	// Copy active portion into a local slice for sorting
	mut sorted := []f64{len: n}
	for i in 0 .. n {
		sorted[i] = m.lag_samples[i]
	}

	// Insertion sort - acceptable for rolling_window_size <= 1024
	for i := 1; i < n; i++ {
		key := sorted[i]
		mut j := i - 1
		for j >= 0 && sorted[j] > key {
			sorted[j + 1] = sorted[j]
			j--
		}
		sorted[j + 1] = key
	}

	mut sum := f64(0)
	mut max_val := f64(0)
	for v in sorted {
		sum += v
		if v > max_val {
			max_val = v
		}
	}

	m.avg_replication_lag_ms = sum / f64(n)
	m.max_replication_lag_ms = max_val
	m.p95_replication_lag_ms = percentile_from_sorted(sorted, 0.95)
	m.p99_replication_lag_ms = percentile_from_sorted(sorted, 0.99)
}

/// percentile_from_sorted returns the p-th percentile (0.0-1.0) from a pre-sorted slice.
fn percentile_from_sorted(sorted []f64, p f64) f64 {
	n := sorted.len
	if n == 0 {
		return 0.0
	}
	idx := int(f64(n - 1) * p)
	return sorted[idx]
}

/// increment_pending increments the pending request counter.
pub fn (mut m ReplicationMetrics) increment_pending() {
	m.pending_requests++
}

/// decrement_pending decrements the pending request counter (floor: 0).
pub fn (mut m ReplicationMetrics) decrement_pending() {
	if m.pending_requests > 0 {
		m.pending_requests--
	}
}

/// record_failed_request increments the failed request counter.
pub fn (mut m ReplicationMetrics) record_failed_request() {
	m.failed_requests++
}

/// record_retried_request increments the retried request counter.
pub fn (mut m ReplicationMetrics) record_retried_request() {
	m.retried_requests++
}

/// set_active_connections updates the active connection gauge.
pub fn (mut m ReplicationMetrics) set_active_connections(count i64) {
	m.active_connections = count
}

/// record_connection_error increments the connection error counter.
pub fn (mut m ReplicationMetrics) record_connection_error() {
	m.connection_errors++
}

/// record_reconnection increments the reconnection counter.
pub fn (mut m ReplicationMetrics) record_reconnection() {
	m.reconnection_count++
}

/// flush_rates computes throughput rates for the elapsed window, updates the
/// per-second fields, and resets the window accumulators.
/// Call this periodically (e.g., every second) from a background goroutine.
pub fn (mut m ReplicationMetrics) flush_rates() {
	now := time.now().unix_milli()
	elapsed_ms := now - m.window_start_time
	if elapsed_ms > 0 {
		elapsed_sec := f64(elapsed_ms) / 1000.0
		m.bytes_in_per_sec = f64(m.window_bytes_in) / elapsed_sec
		m.bytes_out_per_sec = f64(m.window_bytes_out) / elapsed_sec
		m.messages_in_per_sec = f64(m.window_messages_in) / elapsed_sec
		m.messages_out_per_sec = f64(m.window_messages_out) / elapsed_sec
	}
	// Reset window
	m.window_start_time = now
	m.window_bytes_in = 0
	m.window_bytes_out = 0
	m.window_messages_in = 0
	m.window_messages_out = 0
}

/// snapshot returns a read-only copy of the current metrics state.
/// Lag percentile statistics are computed lazily inside this call.
pub fn (mut m ReplicationMetrics) snapshot() ReplicationMetricsSnapshot {
	m.recalculate_lag_stats()
	s := ReplicationMetricsSnapshot{
		bytes_in_per_sec:       m.bytes_in_per_sec
		bytes_out_per_sec:      m.bytes_out_per_sec
		messages_in_per_sec:    m.messages_in_per_sec
		messages_out_per_sec:   m.messages_out_per_sec
		avg_replication_lag_ms: m.avg_replication_lag_ms
		max_replication_lag_ms: m.max_replication_lag_ms
		p95_replication_lag_ms: m.p95_replication_lag_ms
		p99_replication_lag_ms: m.p99_replication_lag_ms
		pending_requests:       m.pending_requests
		failed_requests:        m.failed_requests
		retried_requests:       m.retried_requests
		active_connections:     m.active_connections
		connection_errors:      m.connection_errors
		reconnection_count:     m.reconnection_count
	}
	return s
}

/// reset_counters resets all cumulative counters and rate accumulators to zero
/// while preserving the current lag percentile state.
pub fn (mut m ReplicationMetrics) reset_counters() {
	now := time.now().unix_milli()
	m.bytes_in_per_sec = 0.0
	m.bytes_out_per_sec = 0.0
	m.messages_in_per_sec = 0.0
	m.messages_out_per_sec = 0.0
	m.failed_requests = 0
	m.retried_requests = 0
	m.connection_errors = 0
	m.reconnection_count = 0
	m.window_start_time = now
	m.window_bytes_in = 0
	m.window_bytes_out = 0
	m.window_messages_in = 0
	m.window_messages_out = 0
}

/// ReplicationMetricsSnapshot is a point-in-time, immutable view of ReplicationMetrics.
pub struct ReplicationMetricsSnapshot {
pub:
	// Throughput
	bytes_in_per_sec     f64
	bytes_out_per_sec    f64
	messages_in_per_sec  f64
	messages_out_per_sec f64
	// Latency
	avg_replication_lag_ms f64
	max_replication_lag_ms f64
	p95_replication_lag_ms f64
	p99_replication_lag_ms f64
	// Queue
	pending_requests i64
	failed_requests  i64
	retried_requests i64
	// Connection
	active_connections i64
	connection_errors  i64
	reconnection_count i64
}

/// PartitionMetrics tracks per-partition replication statistics.
pub struct PartitionMetrics {
pub mut:
	topic            string
	partition        i32
	bytes_replicated i64 // Total bytes sent to replicas
	messages_sent    i64 // Total messages sent to replicas
	messages_acked   i64 // Total messages acknowledged by replicas
	lag_samples      []f64
	lag_sample_head  int
	lag_sample_full  bool
}

/// new_partition_metrics creates a PartitionMetrics instance for the given topic/partition.
pub fn new_partition_metrics(topic string, partition i32) PartitionMetrics {
	return PartitionMetrics{
		topic:       topic
		partition:   partition
		lag_samples: []f64{len: rolling_window_size, init: 0.0}
	}
}

/// record_sent records a sent message batch with its byte size.
pub fn (mut pm PartitionMetrics) record_sent(bytes i64) {
	pm.bytes_replicated += bytes
	pm.messages_sent++
}

/// record_acked records an acknowledgement for a sent message.
pub fn (mut pm PartitionMetrics) record_acked() {
	pm.messages_acked++
}

/// record_lag records a lag observation (ms) for this partition.
pub fn (mut pm PartitionMetrics) record_lag(lag_ms f64) {
	pm.lag_samples[pm.lag_sample_head] = lag_ms
	pm.lag_sample_head = (pm.lag_sample_head + 1) % rolling_window_size
	if !pm.lag_sample_full && pm.lag_sample_head == 0 {
		pm.lag_sample_full = true
	}
}

/// avg_lag_ms returns the mean lag over all retained samples.
pub fn (mut pm PartitionMetrics) avg_lag_ms() f64 {
	n := if pm.lag_sample_full { rolling_window_size } else { pm.lag_sample_head }
	if n == 0 {
		return 0.0
	}
	mut sum := f64(0)
	for i in 0 .. n {
		sum += pm.lag_samples[i]
	}
	return sum / f64(n)
}

/// ReplicaAssignment is a struct holding related data.
pub struct ReplicaAssignment {
pub mut:
	topic           string
	partition       i32
	main_broker     string
	replica_brokers []string
	assigned_time   i64 // When this assignment was created
}

/// ReplicationHealth is a struct holding related data.
pub struct ReplicationHealth {
pub mut:
	broker_id      string
	is_alive       bool
	last_heartbeat i64
	lag_ms         i64
	pending_count  int
}

// default returns a ReplicationConfig with sensible default values.
// - Replication disabled by default
// - Port: 9094
// - Replica count: 2
// - Timeouts: 5000ms (replica), 3000ms (heartbeat)
// - Reassignment interval: 30000ms
// - Orphan cleanup interval: 60000ms
//
// Returns: ReplicationConfig with default settings
/// default returns a ReplicationConfig with sensible default values.
pub fn (cfg ReplicationConfig) default() ReplicationConfig {
	return ReplicationConfig{
		enabled:                       false
		replication_port:              9094
		replica_count:                 2
		replica_timeout_ms:            5000
		heartbeat_interval_ms:         3000
		reassignment_interval_ms:      30000
		orphan_cleanup_interval_ms:    60000
		retry_count:                   3
		replica_buffer_ttl_ms:         60000
		max_replica_buffer_size_bytes: 0
	}
}

// record_replicate increments the total_replicated counter by one.
// Called when a REPLICATE message is successfully sent to replica brokers.
/// record_replicate increments the total_replicated counter by one.
pub fn (mut stats ReplicationStats) record_replicate() {
	stats.total_replicated++
}

// record_ack increments the total_ack_received counter by one.
// Called when a REPLICATE_ACK response is received from a replica broker.
/// record_ack increments the total_ack_received counter by one.
pub fn (mut stats ReplicationStats) record_ack() {
	stats.total_ack_received++
}

// record_flush_ack increments the total_flush_ack_sent counter by one.
// Called when a FLUSH_ACK message is sent to replica brokers after S3 flush.
/// record_flush_ack increments the total_flush_ack_sent counter by one.
pub fn (mut stats ReplicationStats) record_flush_ack() {
	stats.total_flush_ack_sent++
}

// record_orphan_cleanup increments the total_orphans_cleaned counter by one.
// Called when an orphaned replica buffer is removed during periodic cleanup.
/// record_orphan_cleanup increments the total_orphans_cleaned counter by one.
pub fn (mut stats ReplicationStats) record_orphan_cleanup() {
	stats.total_orphans_cleaned++
}

// update_heartbeat updates the last_heartbeat_time to the current time.
// Called after each heartbeat cycle completes successfully.
/// update_heartbeat updates the last_heartbeat_time to the current time.
pub fn (mut stats ReplicationStats) update_heartbeat() {
	stats.last_heartbeat_time = time.now().unix_milli()
}

// record_retry_exhausted increments the total_retry_exhausted counter by one.
// Called when a replication attempt fails and all retries are exhausted.
/// record_retry_exhausted increments the total_retry_exhausted counter by one.
pub fn (mut stats ReplicationStats) record_retry_exhausted() {
	stats.total_retry_exhausted++
}

// record_ttl_dropped increments total_ttl_dropped by the given count.
// Called when messages are dropped from the replica buffer due to TTL expiry.
/// record_ttl_dropped increments total_ttl_dropped by count.
pub fn (mut stats ReplicationStats) record_ttl_dropped(count i64) {
	stats.total_ttl_dropped += count
}

// record_buffer_overflow increments the total_buffer_overflow counter by one.
// Called when a new replica buffer entry is rejected because the buffer is full.
/// record_buffer_overflow increments the total_buffer_overflow counter by one.
pub fn (mut stats ReplicationStats) record_buffer_overflow() {
	stats.total_buffer_overflow++
}
