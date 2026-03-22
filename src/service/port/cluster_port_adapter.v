// Concrete adapter that wraps ClusterMetadataPort and satisfies each sub-port interface.
// V does not support interface-to-sub-interface conversion, so this adapter enables
// ISP-narrowed dependency injection by delegating all calls to the composite port.
module port

import domain

/// ClusterPortAdapter wraps a ClusterMetadataPort composite and implements
/// every sub-port interface via delegation. Pass a single adapter instance
/// wherever a narrow sub-port is required.
pub struct ClusterPortAdapter {
mut:
	inner ClusterMetadataPort
}

/// new_cluster_port_adapter creates a ClusterPortAdapter from a composite port.
pub fn new_cluster_port_adapter(cp ClusterMetadataPort) &ClusterPortAdapter {
	return &ClusterPortAdapter{
		inner: cp
	}
}

// --- BrokerRegistryPort delegation ---

pub fn (mut a ClusterPortAdapter) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	return a.inner.register_broker(info)
}

pub fn (mut a ClusterPortAdapter) deregister_broker(broker_id i32) ! {
	a.inner.deregister_broker(broker_id)!
}

pub fn (mut a ClusterPortAdapter) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
	a.inner.update_broker_heartbeat(heartbeat)!
}

pub fn (mut a ClusterPortAdapter) get_broker(broker_id i32) !domain.BrokerInfo {
	return a.inner.get_broker(broker_id)
}

pub fn (mut a ClusterPortAdapter) list_brokers() ![]domain.BrokerInfo {
	return a.inner.list_brokers()
}

pub fn (mut a ClusterPortAdapter) list_active_brokers() ![]domain.BrokerInfo {
	return a.inner.list_active_brokers()
}

// --- ClusterStatePort delegation ---

pub fn (mut a ClusterPortAdapter) get_cluster_metadata() !domain.ClusterMetadata {
	return a.inner.get_cluster_metadata()
}

pub fn (mut a ClusterPortAdapter) update_cluster_metadata(metadata domain.ClusterMetadata) ! {
	a.inner.update_cluster_metadata(metadata)!
}

// --- PartitionAssignmentPort delegation ---

pub fn (mut a ClusterPortAdapter) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	return a.inner.get_partition_assignment(topic_name, partition)
}

pub fn (mut a ClusterPortAdapter) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	return a.inner.list_partition_assignments(topic_name)
}

pub fn (mut a ClusterPortAdapter) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	a.inner.update_partition_assignment(assignment)!
}

// --- DistributedLockPort delegation ---

pub fn (mut a ClusterPortAdapter) try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	return a.inner.try_acquire_lock(lock_name, holder_id, ttl_ms)
}

pub fn (mut a ClusterPortAdapter) release_lock(lock_name string, holder_id string) ! {
	a.inner.release_lock(lock_name, holder_id)!
}

pub fn (mut a ClusterPortAdapter) refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	return a.inner.refresh_lock(lock_name, holder_id, ttl_ms)
}

// --- BrokerHealthPort delegation ---

pub fn (mut a ClusterPortAdapter) mark_broker_dead(broker_id i32) ! {
	a.inner.mark_broker_dead(broker_id)!
}

pub fn (mut a ClusterPortAdapter) get_capability() domain.StorageCapability {
	return a.inner.get_capability()
}
