// Service Layer - Cluster Metadata Port Interface
// Defines the interface for cluster metadata operations in multi-broker mode
module port

import domain

// ClusterMetadataPort defines operations for cluster coordination
// This interface is implemented by storage adapters that support multi-broker mode
pub interface ClusterMetadataPort {
mut:

	// ============================================================
	// Broker Registration
	// ============================================================
	// register_broker registers a broker in the cluster
	// Returns the assigned broker_id (may differ from requested if conflict)
	register_broker(info domain.BrokerInfo) !domain.BrokerInfo

	// deregister_broker removes a broker from the cluster
	deregister_broker(broker_id i32) !

	// update_broker_heartbeat updates the broker's last heartbeat time
	update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) !

	// get_broker returns information about a specific broker
	get_broker(broker_id i32) !domain.BrokerInfo

	// list_brokers returns all registered brokers
	list_brokers() ![]domain.BrokerInfo

	// list_active_brokers returns only active brokers (not dead/shutdown)
	list_active_brokers() ![]domain.BrokerInfo

	// ============================================================
	// Cluster Metadata
	// ============================================================
	// get_cluster_metadata returns the current cluster metadata
	get_cluster_metadata() !domain.ClusterMetadata

	// update_cluster_metadata updates cluster metadata with optimistic locking
	// Returns error if version mismatch (concurrent modification)
	update_cluster_metadata(metadata domain.ClusterMetadata) !

	// ============================================================
	// Partition Assignment
	// ============================================================
	// get_partition_assignment returns assignment for a partition
	get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment

	// list_partition_assignments returns all assignments for a topic
	list_partition_assignments(topic_name string) ![]domain.PartitionAssignment

	// update_partition_assignment updates partition assignment
	update_partition_assignment(assignment domain.PartitionAssignment) !

	// ============================================================
	// Distributed Locking (for coordination)
	// ============================================================
	// try_acquire_lock attempts to acquire a distributed lock
	// Returns true if lock acquired, false if already held by another
	try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool

	// release_lock releases a distributed lock
	release_lock(lock_name string, holder_id string) !

	// refresh_lock extends the TTL of a held lock
	refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool

	// ============================================================
	// Health Monitoring
	// ============================================================
	// mark_broker_dead marks a broker as dead (missed heartbeats)
	mark_broker_dead(broker_id i32) !

	// ============================================================
	// Capability (immutable)
	// ============================================================
	// get_capability returns the storage capability
	get_capability() domain.StorageCapability
}
