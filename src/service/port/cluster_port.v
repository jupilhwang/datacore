// Cluster coordination interfaces defined in the use-case layer and implemented in the adapter layer.
// These interfaces follow the Dependency Inversion Principle of Clean Architecture.
// Sub-interfaces follow the Interface Segregation Principle (ISP) of SOLID.
module port

import domain

/// BrokerRegistryPort defines broker registration and discovery operations.
/// Used by services that manage broker lifecycle within the cluster.
pub interface BrokerRegistryPort {
mut:
	/// Registers a broker with the cluster.
	/// Returns the assigned broker_id (may differ from the request on conflict).
	register_broker(info domain.BrokerInfo) !domain.BrokerInfo

	/// Removes a broker from the cluster.
	deregister_broker(broker_id i32) !

	/// Updates the last heartbeat timestamp for a broker.
	update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) !

	/// Returns information for a specific broker.
	get_broker(broker_id i32) !domain.BrokerInfo

	/// Returns a list of all registered brokers.
	list_brokers() ![]domain.BrokerInfo

	/// Returns only active brokers (excluding dead/shutdown).
	list_active_brokers() ![]domain.BrokerInfo
}

/// ClusterStatePort defines cluster-level metadata operations.
/// Used by services that read or update the global cluster state.
pub interface ClusterStatePort {
mut:
	/// Returns the current cluster metadata.
	get_cluster_metadata() !domain.ClusterMetadata

	/// Updates cluster metadata using optimistic locking.
	/// Returns an error on version mismatch (concurrent modification).
	update_cluster_metadata(metadata domain.ClusterMetadata) !
}

/// PartitionAssignmentPort defines partition-broker assignment operations.
/// Used by services that manage how partitions are mapped to brokers.
pub interface PartitionAssignmentPort {
mut:
	/// Returns the assignment information for a specific partition.
	get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment

	/// Returns all partition assignment information for a topic.
	list_partition_assignments(topic_name string) ![]domain.PartitionAssignment

	/// Updates partition assignment information.
	update_partition_assignment(assignment domain.PartitionAssignment) !
}

/// DistributedLockPort defines distributed locking operations.
/// Used by services that require mutual exclusion across brokers.
pub interface DistributedLockPort {
mut:
	/// Attempts to acquire a distributed lock.
	/// Returns true on success, false if already held elsewhere.
	try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool

	/// Releases a distributed lock.
	release_lock(lock_name string, holder_id string) !

	/// Extends the TTL of a held lock.
	refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool
}

/// BrokerHealthPort defines broker health monitoring and capability operations.
/// Used by services that track broker availability and query storage features.
pub interface BrokerHealthPort {
mut:
	/// Marks a broker as dead (missed heartbeat).
	mark_broker_dead(broker_id i32) !

	/// Returns storage capability information.
	get_capability() domain.StorageCapability
}

/// ClusterMetadataPort is the composite interface combining all cluster sub-ports.
/// Implemented in distributed storage adapters (PostgreSQL, S3, etc.).
pub interface ClusterMetadataPort {
	BrokerRegistryPort
	ClusterStatePort
	PartitionAssignmentPort
	DistributedLockPort
	BrokerHealthPort
}
