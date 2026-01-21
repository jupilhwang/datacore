// S3 Cluster Metadata Adapter
// Implements ClusterMetadataPort for multi-broker coordination using S3
module s3

import domain
import time
import json

// S3ClusterMetadataAdapter implements ClusterMetadataPort using S3 as backend
pub struct S3ClusterMetadataAdapter {
pub mut:
	adapter &S3StorageAdapter
}

// new_s3_cluster_metadata_adapter creates a new S3 cluster metadata adapter
pub fn new_s3_cluster_metadata_adapter(adapter &S3StorageAdapter) &S3ClusterMetadataAdapter {
	return &S3ClusterMetadataAdapter{
		adapter: adapter
	}
}

// S3 key structure for cluster metadata:
// {prefix}/__cluster/brokers/{broker_id}.json
// {prefix}/__cluster/metadata.json
// {prefix}/__cluster/partitions/{topic}/{partition}.json
// {prefix}/__cluster/locks/{lock_name}.json

// ============================================================
// Broker Registration
// ============================================================

pub fn (mut a S3ClusterMetadataAdapter) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	key := a.broker_key(info.broker_id)

	// Check if broker already exists
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		// Update existing broker
		mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
			return error('failed to decode existing broker: ${err}')
		}
		broker.status = .active
		broker.last_heartbeat = time.now().unix_milli()

		data := json.encode(broker).bytes()
		a.adapter.put_object(key, data)!
		return broker
	}

	// Register new broker
	mut new_info := info
	new_info.registered_at = time.now().unix_milli()
	new_info.last_heartbeat = time.now().unix_milli()
	new_info.status = .active

	data := json.encode(new_info).bytes()
	a.adapter.put_object(key, data)!

	return new_info
}

pub fn (mut a S3ClusterMetadataAdapter) deregister_broker(broker_id i32) ! {
	key := a.broker_key(broker_id)

	// Get existing broker
	existing, _ := a.adapter.get_object(key, 0, -1) or { return }
	if existing.len == 0 {
		return
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or { return }
	broker.status = .shutdown

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

pub fn (mut a S3ClusterMetadataAdapter) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
	key := a.broker_key(heartbeat.broker_id)

	existing, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${heartbeat.broker_id}')
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}

	broker.last_heartbeat = heartbeat.timestamp
	if heartbeat.wants_shutdown {
		broker.status = .draining
	}

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

pub fn (mut a S3ClusterMetadataAdapter) get_broker(broker_id i32) !domain.BrokerInfo {
	key := a.broker_key(broker_id)

	data, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${broker_id}')
	}

	return json.decode(domain.BrokerInfo, data.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}
}

pub fn (mut a S3ClusterMetadataAdapter) list_brokers() ![]domain.BrokerInfo {
	prefix := a.brokers_prefix()
	objects := a.adapter.list_objects(prefix)!

	mut brokers := []domain.BrokerInfo{}
	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}
		data, _ := a.adapter.get_object(obj.key, 0, -1) or { continue }
		broker := json.decode(domain.BrokerInfo, data.bytestr()) or { continue }
		brokers << broker
	}

	return brokers
}

pub fn (mut a S3ClusterMetadataAdapter) list_active_brokers() ![]domain.BrokerInfo {
	all_brokers := a.list_brokers()!

	mut active := []domain.BrokerInfo{}
	for broker in all_brokers {
		if broker.status == .active {
			active << broker
		}
	}

	return active
}

// ============================================================
// Cluster Metadata
// ============================================================

pub fn (mut a S3ClusterMetadataAdapter) get_cluster_metadata() !domain.ClusterMetadata {
	key := a.cluster_metadata_key()

	data, _ := a.adapter.get_object(key, 0, -1) or {
		// Return default metadata if not exists
		return domain.ClusterMetadata{
			cluster_id:       'datacore-cluster'
			controller_id:    -1
			brokers:          []domain.BrokerInfo{}
			metadata_version: 0
			updated_at:       time.now().unix_milli()
		}
	}

	return json.decode(domain.ClusterMetadata, data.bytestr()) or {
		return error('failed to decode cluster metadata: ${err}')
	}
}

pub fn (mut a S3ClusterMetadataAdapter) update_cluster_metadata(metadata domain.ClusterMetadata) ! {
	key := a.cluster_metadata_key()

	// Check version for optimistic locking
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		current := json.decode(domain.ClusterMetadata, existing.bytestr()) or {
			return error('failed to decode existing metadata')
		}
		if current.metadata_version >= metadata.metadata_version {
			return error('metadata version conflict')
		}
	}

	mut updated := metadata
	updated.updated_at = time.now().unix_milli()

	data := json.encode(updated).bytes()
	a.adapter.put_object(key, data)!
}

// ============================================================
// Partition Assignment
// ============================================================

pub fn (mut a S3ClusterMetadataAdapter) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	key := a.partition_assignment_key(topic_name, partition)

	data, _ := a.adapter.get_object(key, 0, -1) or {
		return error('partition assignment not found')
	}

	return json.decode(domain.PartitionAssignment, data.bytestr()) or {
		return error('failed to decode partition assignment: ${err}')
	}
}

pub fn (mut a S3ClusterMetadataAdapter) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	prefix := a.partition_assignments_prefix(topic_name)
	objects := a.adapter.list_objects(prefix)!

	mut assignments := []domain.PartitionAssignment{}
	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}
		data, _ := a.adapter.get_object(obj.key, 0, -1) or { continue }
		assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or { continue }
		assignments << assignment
	}

	return assignments
}

pub fn (mut a S3ClusterMetadataAdapter) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	key := a.partition_assignment_key(assignment.topic_name, assignment.partition)

	data := json.encode(assignment).bytes()
	a.adapter.put_object(key, data)!
}

// ============================================================
// Distributed Locking
// ============================================================

// LockInfo represents a distributed lock
struct LockInfo {
	lock_name   string
	holder_id   string
	expires_at  i64
	acquired_at i64
}

pub fn (mut a S3ClusterMetadataAdapter) try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	key := a.lock_key(lock_name)
	now := time.now().unix_milli()

	// Check if lock exists and is still valid
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		lock_info := json.decode(LockInfo, existing.bytestr()) or {
			// Corrupted lock, try to acquire
			return a.create_lock(key, lock_name, holder_id, ttl_ms)
		}

		// Check if lock is expired
		if lock_info.expires_at > now {
			// Lock is still held by someone
			if lock_info.holder_id == holder_id {
				// We already hold the lock, refresh it
				return a.refresh_lock(lock_name, holder_id, ttl_ms)
			}
			return false
		}
		// Lock is expired, we can take it
	}

	return a.create_lock(key, lock_name, holder_id, ttl_ms)
}

fn (mut a S3ClusterMetadataAdapter) create_lock(key string, lock_name string, holder_id string, ttl_ms i64) !bool {
	now := time.now().unix_milli()
	lock_info := LockInfo{
		lock_name:   lock_name
		holder_id:   holder_id
		expires_at:  now + ttl_ms
		acquired_at: now
	}

	data := json.encode(lock_info).bytes()
	a.adapter.put_object(key, data)!
	return true
}

pub fn (mut a S3ClusterMetadataAdapter) release_lock(lock_name string, holder_id string) ! {
	key := a.lock_key(lock_name)

	// Verify we hold the lock
	existing, _ := a.adapter.get_object(key, 0, -1) or { return }
	if existing.len == 0 {
		return
	}

	lock_info := json.decode(LockInfo, existing.bytestr()) or { return }
	if lock_info.holder_id != holder_id {
		return error('lock not held by this holder')
	}

	a.adapter.delete_object(key)!
}

pub fn (mut a S3ClusterMetadataAdapter) refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	key := a.lock_key(lock_name)
	now := time.now().unix_milli()

	// Verify we hold the lock
	existing, _ := a.adapter.get_object(key, 0, -1) or { return false }
	if existing.len == 0 {
		return false
	}

	mut lock_info := json.decode(LockInfo, existing.bytestr()) or { return false }
	if lock_info.holder_id != holder_id {
		return false
	}

	// Refresh the lock
	lock_info = LockInfo{
		...lock_info
		expires_at: now + ttl_ms
	}

	data := json.encode(lock_info).bytes()
	a.adapter.put_object(key, data)!
	return true
}

// ============================================================
// Health Monitoring (for ClusterCoordinatorPort)
// ============================================================

pub fn (mut a S3ClusterMetadataAdapter) mark_broker_dead(broker_id i32) ! {
	key := a.broker_key(broker_id)

	existing, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${broker_id}')
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}

	broker.status = .dead

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

// ============================================================
// Capability
// ============================================================

pub fn (a &S3ClusterMetadataAdapter) get_capability() domain.StorageCapability {
	return s3_capability
}

// ============================================================
// Key Helpers
// ============================================================

fn (a &S3ClusterMetadataAdapter) broker_key(broker_id i32) string {
	return '${a.adapter.config.prefix}__cluster/brokers/${broker_id}.json'
}

fn (a &S3ClusterMetadataAdapter) brokers_prefix() string {
	return '${a.adapter.config.prefix}__cluster/brokers/'
}

fn (a &S3ClusterMetadataAdapter) cluster_metadata_key() string {
	return '${a.adapter.config.prefix}__cluster/metadata.json'
}

fn (a &S3ClusterMetadataAdapter) partition_assignment_key(topic_name string, partition i32) string {
	return '${a.adapter.config.prefix}__cluster/partitions/${topic_name}/${partition}.json'
}

fn (a &S3ClusterMetadataAdapter) partition_assignments_prefix(topic_name string) string {
	return '${a.adapter.config.prefix}__cluster/partitions/${topic_name}/'
}

fn (a &S3ClusterMetadataAdapter) lock_key(lock_name string) string {
	return '${a.adapter.config.prefix}__cluster/locks/${lock_name}.json'
}
