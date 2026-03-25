module domain

import time

/// PartitionAssignment represents broker assignment information for a partition.
/// Tracks which broker each partition of a topic is assigned to.
/// In Stateless mode, partitions have no fixed leader.
/// Since data resides on shared storage, any broker can serve any partition.
/// However, a "preferred" broker is tracked for load balancing purposes.
pub struct PartitionAssignment {
pub mut:
	topic_name       string
	topic_id         []u8
	partition        i32
	preferred_broker i32
	replica_brokers  []i32
	isr_brokers      []i32
	partition_epoch  i32
	assigned_at      i64
	reassigned_at    i64
}

/// PartitionAssignmentSnapshot represents all partition assignments at a specific point in time.
pub struct PartitionAssignmentSnapshot {
pub mut:
	cluster_id  string
	version     i64
	assignments []PartitionAssignment
	created_at  i64
	created_by  i32
}

/// PartitionAssignerConfig represents partition assignment configuration.
pub struct PartitionAssignerConfig {
pub mut:
	strategy      AssignmentStrategy
	rack_aware    bool
	sticky_assign bool
}

/// AssignmentStrategy represents the partition assignment strategy.
pub enum AssignmentStrategy {
	round_robin
	range
	sticky
}

/// BrokerInfo represents a broker within the cluster.
/// broker_id: unique broker ID
/// host: host address
/// port: port number
/// rack: rack information (for data locality)
/// security_protocol: security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
/// endpoints: list of listener endpoints
/// status: broker status
pub struct BrokerInfo {
pub mut:
	broker_id         i32
	host              string
	port              i32
	rack              string
	security_protocol string
	endpoints         []BrokerEndpoint
	status            BrokerStatus
	registered_at     i64
	last_heartbeat    i64
	version           string
	features          []string
}

/// BrokerEndpoint represents a listener endpoint.
/// name: endpoint name (e.g. "PLAINTEXT", "SSL", "SASL_SSL")
/// host: host address
/// port: port number
/// security_protocol: security protocol
pub struct BrokerEndpoint {
pub:
	name              string
	host              string
	port              i32
	security_protocol string
}

/// BrokerStatus represents the status of a broker.
/// starting: starting up
/// active: active (accepting requests)
/// draining: draining (rejecting new connections)
/// shutdown: shutting down
/// dead: dead (missed heartbeats)
pub enum BrokerStatus {
	starting
	active
	draining
	shutdown
	dead
}

/// BrokerHeartbeat represents a heartbeat from a broker.
/// broker_id: broker ID
/// timestamp: timestamp
/// current_load: current load information
/// wants_shutdown: whether the broker requests shutdown
pub struct BrokerHeartbeat {
pub:
	broker_id      i32
	timestamp      i64
	current_load   BrokerLoad
	wants_shutdown bool
}

/// BrokerLoad represents the current load of a broker.
/// connections: current number of connections
/// requests_per_sec: requests per second
/// bytes_in_per_sec: bytes received per second
/// bytes_out_per_sec: bytes sent per second
/// cpu_percent: CPU usage percentage
/// memory_percent: memory usage percentage
pub struct BrokerLoad {
pub:
	connections       int
	requests_per_sec  f64
	bytes_in_per_sec  f64
	bytes_out_per_sec f64
	cpu_percent       f64
	memory_percent    f64
}

/// ClusterMetadata represents the cluster state.
/// cluster_id: cluster ID
/// controller_id: controller broker ID (-1 in stateless mode)
/// brokers: list of brokers
/// metadata_version: metadata version (for optimistic concurrency)
/// updated_at: last update time
pub struct ClusterMetadata {
pub mut:
	cluster_id       string
	controller_id    i32
	brokers          []BrokerInfo
	metadata_version i64
	updated_at       i64
}

/// new_cluster_metadata creates a new cluster metadata instance.
pub fn new_cluster_metadata(cluster_id string) ClusterMetadata {
	return ClusterMetadata{
		cluster_id:       cluster_id
		controller_id:    -1 // No controller in Stateless mode
		brokers:          []BrokerInfo{}
		metadata_version: 0
		updated_at:       time.now().unix_milli()
	}
}

/// StorageCapability represents the features supported by a storage engine.
/// name: storage name
/// supports_multi_broker: whether multi-broker is supported
/// supports_transactions: whether transactions are supported
/// supports_compaction: whether compaction is supported
/// is_persistent: whether storage is persistent
/// is_distributed: whether storage is distributed
pub struct StorageCapability {
pub:
	name                  string
	supports_multi_broker bool
	supports_transactions bool
	supports_compaction   bool
	is_persistent         bool
	is_distributed        bool
}

/// memory_storage_capability constant.
pub const memory_storage_capability = StorageCapability{
	name:                  'memory'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   false
	is_persistent:         false
	is_distributed:        false
}

/// s3_storage_capability constant.
pub const s3_storage_capability = StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// postgresql_storage_capability constant.
pub const postgresql_storage_capability = StorageCapability{
	name:                  'postgresql'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// ClusterConfig holds cluster-wide configuration.
/// cluster_id: cluster ID
/// broker_heartbeat_interval_ms: heartbeat send interval (milliseconds)
/// broker_session_timeout_ms: broker session timeout (milliseconds)
/// metadata_refresh_interval_ms: metadata refresh interval (milliseconds)
/// multi_broker_enabled: whether multi-broker mode is enabled
pub struct ClusterConfig {
pub:
	cluster_id                   string
	broker_heartbeat_interval_ms i32 = 3000
	broker_session_timeout_ms    i32 = 10000
	metadata_refresh_interval_ms i32 = 30000
	multi_broker_enabled         bool
}

/// ClusterEvent represents an event occurring in the cluster.
/// event_type: event type
/// broker_id: related broker ID
/// timestamp: time of occurrence
/// details: detailed information
pub struct ClusterEvent {
pub:
	event_type ClusterEventType
	broker_id  i32
	timestamp  i64
	details    string
}

/// ClusterEventType represents the type of a cluster event.
/// broker_joined: broker joined
/// broker_left: broker left
/// broker_failed: broker failed
/// metadata_updated: metadata updated
/// partition_reassigned: partition reassigned
pub enum ClusterEventType {
	broker_joined
	broker_left
	broker_failed
	metadata_updated
	partition_reassigned
}
