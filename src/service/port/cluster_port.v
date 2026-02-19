// Implemented in distributed storage adapters (PostgreSQL, etcd, etc.).
module port

import domain

/// ClusterMetadataPort defines operations for cluster coordination.
/// Implemented in storage adapters that support multi-broker mode.
pub interface ClusterMetadataPort {
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
	/// Returns the current cluster metadata.
	get_cluster_metadata() !domain.ClusterMetadata

	/// Updates cluster metadata using optimistic locking.
	/// Returns an error on version mismatch (concurrent modification).
	update_cluster_metadata(metadata domain.ClusterMetadata) !
	/// Returns the assignment information for a specific partition.
	get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment

	/// Returns all partition assignment information for a topic.
	list_partition_assignments(topic_name string) ![]domain.PartitionAssignment

	/// Updates partition assignment information.
	update_partition_assignment(assignment domain.PartitionAssignment) !
	/// Attempts to acquire a distributed lock.
	/// Returns true on success, false if already held elsewhere.
	try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool

	/// Releases a distributed lock.
	release_lock(lock_name string, holder_id string) !

	/// Extends the TTL of a held lock.
	refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool
	/// Marks a broker as dead (missed heartbeat).
	mark_broker_dead(broker_id i32) !
	/// Returns storage capability information.
	get_capability() domain.StorageCapability
}
