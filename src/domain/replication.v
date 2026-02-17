module domain

import time

/// ReplicationType defines the type of replication message
pub enum ReplicationType {
	replicate     // Main broker sends data to replica
	replicate_ack // Replica confirms receipt
	flush_ack     // Main broker confirms S3 flush
	heartbeat     // Periodic health check
}

/// ReplicationMessage represents a replication protocol message
pub struct ReplicationMessage {
pub mut:
	msg_type       ReplicationType
	correlation_id string // UUID for tracking request/response pairs
	sender_id      string // Broker ID of sender
	timestamp      i64    // Unix timestamp (ms)
	// Payload fields
	topic        string
	partition    i32
	offset       i64
	records_data []u8   // Serialized record data (only for REPLICATE)
	success      bool   // For ACKs
	error_msg    string // For error cases
}

/// ReplicaBuffer stores replicated data in memory
pub struct ReplicaBuffer {
pub mut:
	topic        string
	partition    i32
	offset       i64
	records_data []u8 // Serialized record data
	timestamp    i64  // When replicated
}

/// ReplicationConfig holds configuration for replication
pub struct ReplicationConfig {
pub mut:
	enabled                    bool // Enable replication (true for multi-broker)
	replication_port           int  // TCP port for replication (default: 9093)
	replica_count              int  // Number of replicas per partition
	replica_timeout_ms         int  // Timeout for replica response (default: 5000)
	heartbeat_interval_ms      int  // Heartbeat interval (default: 3000)
	reassignment_interval_ms   int  // Replica reassignment interval (default: 30000)
	orphan_cleanup_interval_ms int  // Orphan buffer cleanup interval (default: 60000)
}

/// ReplicationStats tracks replication metrics
pub struct ReplicationStats {
pub mut:
	total_replicated      i64
	total_ack_received    i64
	total_flush_ack_sent  i64
	total_orphans_cleaned i64
	last_heartbeat_time   i64
	replica_lag_ms        i64
}

/// ReplicaAssignment maps partitions to replica brokers
pub struct ReplicaAssignment {
pub mut:
	topic           string
	partition       i32
	main_broker     string   // Main broker ID
	replica_brokers []string // List of replica broker IDs
	assigned_time   i64      // When this assignment was created
}

/// ReplicationHealth represents health status of a replica
pub struct ReplicationHealth {
pub mut:
	broker_id      string
	is_alive       bool
	last_heartbeat i64
	lag_ms         i64
	pending_count  int
}

pub fn (mut msg ReplicationMessage) to_json() string {
	msg_type_str := match msg.msg_type {
		.replicate { 'replicate' }
		.replicate_ack { 'replicate_ack' }
		.flush_ack { 'flush_ack' }
		.heartbeat { 'heartbeat' }
	}

	return '{
		"msg_type": "${msg_type_str}",
		"correlation_id": "${msg.correlation_id}",
		"sender_id": "${msg.sender_id}",
		"timestamp": ${msg.timestamp},
		"topic": "${msg.topic}",
		"partition": ${msg.partition},
		"offset": ${msg.offset},
		"success": ${msg.success},
		"error_msg": "${msg.error_msg}"
	}'
}

pub fn (cfg ReplicationConfig) default() ReplicationConfig {
	return ReplicationConfig{
		enabled:                    false
		replication_port:           9093
		replica_count:              2
		replica_timeout_ms:         5000
		heartbeat_interval_ms:      3000
		reassignment_interval_ms:   30000
		orphan_cleanup_interval_ms: 60000
	}
}

pub fn (mut stats ReplicationStats) record_replicate() {
	stats.total_replicated++
}

pub fn (mut stats ReplicationStats) record_ack() {
	stats.total_ack_received++
}

pub fn (mut stats ReplicationStats) record_flush_ack() {
	stats.total_flush_ack_sent++
}

pub fn (mut stats ReplicationStats) record_orphan_cleanup() {
	stats.total_orphans_cleaned++
}

pub fn (mut stats ReplicationStats) update_heartbeat() {
	stats.last_heartbeat_time = time.now().unix_milli()
}
