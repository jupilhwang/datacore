// Domain Layer - Cluster and Multi-Broker Models
// Defines core types for multi-broker cluster coordination
module domain

import time

// ============================================================================
// Broker Registration
// ============================================================================

// BrokerInfo represents a broker in the cluster
pub struct BrokerInfo {
pub mut:
	broker_id i32
	host      string
	port      i32
	rack      string
	// Security
	security_protocol string // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	// Endpoints for different listeners
	endpoints []BrokerEndpoint
	// Status
	status BrokerStatus
	// Timestamps
	registered_at  i64
	last_heartbeat i64
	// Metadata
	version  string
	features []string
}

// BrokerEndpoint represents a listener endpoint
pub struct BrokerEndpoint {
pub:
	name              string // e.g., "PLAINTEXT", "SSL", "SASL_SSL"
	host              string
	port              i32
	security_protocol string
}

// BrokerStatus represents the status of a broker
pub enum BrokerStatus {
	starting // Broker is starting up
	active   // Broker is active and accepting requests
	draining // Broker is draining (not accepting new connections)
	shutdown // Broker is shutting down
	dead     // Broker is considered dead (missed heartbeats)
}

// BrokerHeartbeat represents a heartbeat from a broker
pub struct BrokerHeartbeat {
pub:
	broker_id      i32
	timestamp      i64
	current_load   BrokerLoad
	wants_shutdown bool
}

// BrokerLoad represents the current load of a broker
pub struct BrokerLoad {
pub:
	connections       int
	requests_per_sec  f64
	bytes_in_per_sec  f64
	bytes_out_per_sec f64
	cpu_percent       f64
	memory_percent    f64
}

// ============================================================================
// Cluster Metadata
// ============================================================================

// ClusterMetadata represents the cluster state
pub struct ClusterMetadata {
pub mut:
	cluster_id    string
	controller_id i32 // -1 if no controller (stateless mode)
	brokers       []BrokerInfo
	// Epoch for optimistic concurrency
	metadata_version i64
	updated_at       i64
}

// new_cluster_metadata creates a new cluster metadata instance
pub fn new_cluster_metadata(cluster_id string) ClusterMetadata {
	return ClusterMetadata{
		cluster_id:       cluster_id
		controller_id:    -1 // No controller in stateless mode
		brokers:          []BrokerInfo{}
		metadata_version: 0
		updated_at:       time.now().unix_milli()
	}
}

// ============================================================================
// Partition Assignment (for Stateless Multi-Broker)
// ============================================================================

// In stateless mode, partitions don't have a fixed leader.
// Any broker can serve any partition since data is in shared storage.
// However, we track "preferred" brokers for load balancing.

// PartitionAssignment represents partition metadata in multi-broker mode
pub struct PartitionAssignment {
pub mut:
	topic_name string
	topic_id   []u8
	partition  i32
	// In stateless mode, all active brokers can serve this partition
	// preferred_broker is used for load balancing hints
	preferred_broker i32
	// Replicas field for Kafka protocol compatibility
	// In stateless mode, all brokers are effectively replicas
	replica_brokers []i32
	isr_brokers     []i32 // In-sync replicas (all active brokers in stateless mode)
	// Epoch for partition state changes
	partition_epoch i32
}

// ============================================================================
// Storage Capability
// ============================================================================

// StorageCapability indicates what a storage engine supports
pub struct StorageCapability {
pub:
	name                  string
	supports_multi_broker bool
	supports_transactions bool
	supports_compaction   bool
	is_persistent         bool
	is_distributed        bool
}

// Predefined storage capabilities
pub const memory_storage_capability = StorageCapability{
	name:                  'memory'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   false
	is_persistent:         false
	is_distributed:        false
}

pub const sqlite_storage_capability = StorageCapability{
	name:                  'sqlite'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        false
}

pub const s3_storage_capability = StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

pub const postgresql_storage_capability = StorageCapability{
	name:                  'postgresql'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

// ============================================================================
// Cluster Configuration
// ============================================================================

// ClusterConfig holds cluster-wide configuration
pub struct ClusterConfig {
pub:
	cluster_id string
	// Broker registration
	broker_heartbeat_interval_ms i32 = 3000  // How often brokers send heartbeats
	broker_session_timeout_ms    i32 = 10000 // When to consider a broker dead
	// Metadata
	metadata_refresh_interval_ms i32 = 30000 // How often to refresh cluster metadata
	// Multi-broker mode
	multi_broker_enabled bool
}

// ============================================================================
// Cluster Events
// ============================================================================

// ClusterEvent represents events in the cluster
pub struct ClusterEvent {
pub:
	event_type ClusterEventType
	broker_id  i32
	timestamp  i64
	details    string
}

// ClusterEventType represents types of cluster events
pub enum ClusterEventType {
	broker_joined
	broker_left
	broker_failed
	metadata_updated
	partition_reassigned
}
