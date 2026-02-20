module domain

import time
import json

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

// to_json converts a ReplicationMessage to a JSON string for logging and debugging.
//
// Returns: JSON string representation of the message (excludes records_data for brevity)
/// to_json returns the JSON representation.
pub fn (msg ReplicationMessage) to_json() string {
	msg_type_str := match msg.msg_type {
		.replicate { 'replicate' }
		.replicate_ack { 'replicate_ack' }
		.flush_ack { 'flush_ack' }
		.heartbeat { 'heartbeat' }
		.recover { 'recover' }
	}

	m := {
		'msg_type':       msg_type_str
		'correlation_id': msg.correlation_id
		'sender_id':      msg.sender_id
		'timestamp':      msg.timestamp.str()
		'topic':          msg.topic
		'partition':      msg.partition.str()
		'offset':         msg.offset.str()
		'success':        msg.success.str()
		'error_msg':      msg.error_msg
	}
	return json.encode(m)
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
