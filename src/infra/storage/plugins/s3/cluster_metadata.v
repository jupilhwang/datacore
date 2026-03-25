// Infra Layer - S3 Cluster Metadata Adapter
// ClusterMetadataPort implementation for multi-broker coordination using S3
module s3

import domain
import time
import json
import infra.observability

// V interface value copy 버그 우회: 모듈 수준 const holder로 config 백업 보존
struct ClusterMetadataConfigHolder {
mut:
	config  S3Config
	is_init bool
}

const g_cluster_metadata_config_holder = &ClusterMetadataConfigHolder{}

// get_cluster_metadata_config_backup은 백업된 config를 반환한다.
fn get_cluster_metadata_config_backup() S3Config {
	holder := unsafe { g_cluster_metadata_config_holder }
	return holder.config
}

// set_cluster_metadata_config_backup은 config를 백업한다.
fn set_cluster_metadata_config_backup(config S3Config) {
	mut holder := unsafe { g_cluster_metadata_config_holder }
	unsafe {
		holder.config = config
		holder.is_init = true
	}
}

/// S3ClusterMetadataAdapter implements ClusterMetadataPort using S3 as the backend.
/// Holds its own S3StorageAdapter instance to resolve config loss issue
/// when the adapter pointer is accessed through a V interface.
pub struct S3ClusterMetadataAdapter {
pub mut:
	adapter &S3StorageAdapter
}

/// new_s3_cluster_metadata_adapter creates a new S3 cluster metadata adapter.
/// Saves the adapter's config to a global backup and uses the original adapter directly.
pub fn new_s3_cluster_metadata_adapter(adapter &S3StorageAdapter) &S3ClusterMetadataAdapter {
	// V interface value copy 버그 우회: config를 모듈 홀더에 백업
	set_cluster_metadata_config_backup(adapter.config)
	observability.log_with_context('s3', .info, 'ClusterMetadata', 'Creating cluster metadata adapter',
		{
		'endpoint':    adapter.config.endpoint
		'region':      adapter.config.region
		'bucket_name': adapter.config.bucket_name
		'prefix':      adapter.config.prefix
	})
	return &S3ClusterMetadataAdapter{
		adapter: adapter
	}
}

/// new_s3_cluster_metadata_with_config creates an adapter with an explicit config.
/// To prevent config loss during V interface value copy,
/// holds config internally and forcibly sets it on the adapter too.
pub fn new_s3_cluster_metadata_with_config(config S3Config) !&S3ClusterMetadataAdapter {
	// V interface value copy 버그 우회: config를 모듈 홀더에 백업
	set_cluster_metadata_config_backup(config)
	mut own_adapter := new_s3_adapter(config)!
	// Forcibly overwrite adapter config with original
	own_adapter.config = config
	return &S3ClusterMetadataAdapter{
		adapter: own_adapter
	}
}

// S3 key structure for cluster metadata:
// {prefix}/__cluster/brokers/{broker_id}.json
// {prefix}/__cluster/metadata.json
// {prefix}/__cluster/assignments/{topic}/{partition}.json
// {prefix}/__cluster/locks/{lock_name}.json

// Broker Registration

/// register_broker registers a broker in the cluster.
pub fn (mut a S3ClusterMetadataAdapter) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	a.ensure_adapter_config()
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

/// deregister_broker deregisters a broker from the cluster.
pub fn (mut a S3ClusterMetadataAdapter) deregister_broker(broker_id i32) ! {
	a.ensure_adapter_config()
	key := a.broker_key(broker_id)

	// Fetch existing broker
	existing, _ := a.adapter.get_object(key, 0, -1) or {
		observability.log_with_context('s3', .warn, 'ClusterMetadata', 'failed to fetch broker for deregister',
			{
			'broker_id': broker_id.str()
			'error':     err.str()
		})
		return
	}
	if existing.len == 0 {
		return
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
		observability.log_with_context('s3', .warn, 'ClusterMetadata', 'failed to decode broker info for deregister',
			{
			'broker_id': broker_id.str()
			'error':     err.str()
		})
		return
	}
	broker.status = .shutdown

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

/// update_broker_heartbeat updates a broker's heartbeat.
pub fn (mut a S3ClusterMetadataAdapter) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
	a.ensure_adapter_config()
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

/// get_broker retrieves information for a specific broker.
pub fn (mut a S3ClusterMetadataAdapter) get_broker(broker_id i32) !domain.BrokerInfo {
	a.ensure_adapter_config()
	key := a.broker_key(broker_id)

	data, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${broker_id}')
	}

	return json.decode(domain.BrokerInfo, data.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}
}

/// list_brokers retrieves a list of all registered brokers.
pub fn (mut a S3ClusterMetadataAdapter) list_brokers() ![]domain.BrokerInfo {
	a.ensure_adapter_config()
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

/// list_active_brokers retrieves only the list of active brokers.
pub fn (mut a S3ClusterMetadataAdapter) list_active_brokers() ![]domain.BrokerInfo {
	a.ensure_adapter_config()
	all_brokers := a.list_brokers()!

	mut active := []domain.BrokerInfo{}
	for broker in all_brokers {
		if broker.status == .active {
			active << broker
		}
	}

	return active
}

// Cluster Metadata

/// get_cluster_metadata retrieves the current cluster metadata.
pub fn (mut a S3ClusterMetadataAdapter) get_cluster_metadata() !domain.ClusterMetadata {
	a.ensure_adapter_config()
	key := a.cluster_metadata_key()

	data, _ := a.adapter.get_object(key, 0, -1) or {
		// Return default metadata if not found
		return domain.ClusterMetadata{
			cluster_id:       a.adapter.config.cluster_id
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

/// update_cluster_metadata updates the cluster metadata.
/// Checks version for optimistic locking.
pub fn (mut a S3ClusterMetadataAdapter) update_cluster_metadata(metadata domain.ClusterMetadata) ! {
	a.ensure_adapter_config()
	key := a.cluster_metadata_key()

	// Version check for optimistic locking
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

/// get_partition_assignment retrieves assignment information for a specific topic-partition.
/// S3 key: {prefix}/__cluster/assignments/{topic}/{partition}.json
pub fn (mut a S3ClusterMetadataAdapter) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	a.ensure_adapter_config()
	key := a.partition_assignment_key(topic_name, partition)

	observability.log_with_context('s3', .debug, 'PartitionAssignment', 'Reading partition assignment from S3',
		{
		'topic':     topic_name
		'partition': partition.str()
		'key':       key
	})

	// Collect metrics
	a.adapter.metrics_collector.mu.@lock()
	a.adapter.metrics_collector.data.s3_get_count++
	a.adapter.metrics_collector.mu.unlock()

	data, _ := a.adapter.get_object(key, 0, -1) or {
		observability.log_with_context('s3', .warn, 'PartitionAssignment', 'Partition assignment not found',
			{
			'topic':     topic_name
			'partition': partition.str()
			'key':       key
			'error':     err.str()
		})
		a.adapter.metrics_collector.mu.@lock()
		a.adapter.metrics_collector.data.s3_error_count++
		a.adapter.metrics_collector.mu.unlock()
		return error('partition assignment not found: ${topic_name}/${partition}')
	}

	assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or {
		observability.log_with_context('s3', .error, 'PartitionAssignment', 'Failed to decode partition assignment',
			{
			'topic':     topic_name
			'partition': partition.str()
			'key':       key
			'error':     err.str()
		})
		return error('failed to decode partition assignment: ${err}')
	}

	observability.log_with_context('s3', .debug, 'PartitionAssignment', 'Successfully read partition assignment',
		{
		'topic':            topic_name
		'partition':        partition.str()
		'preferred_broker': assignment.preferred_broker.str()
	})

	return assignment
}

/// list_partition_assignments retrieves all partition assignments for a specific topic.
/// S3 prefix: {prefix}/__cluster/assignments/{topic}/
pub fn (mut a S3ClusterMetadataAdapter) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	a.ensure_adapter_config()
	prefix := a.partition_assignments_prefix(topic_name)

	observability.log_with_context('s3', .debug, 'PartitionAssignment', 'Listing partition assignments for topic',
		{
		'topic':  topic_name
		'prefix': prefix
	})

	// Collect metrics
	a.adapter.metrics_collector.mu.@lock()
	a.adapter.metrics_collector.data.s3_list_count++
	a.adapter.metrics_collector.mu.unlock()

	objects := a.adapter.list_objects(prefix)!

	mut assignments := []domain.PartitionAssignment{}
	mut failed_count := 0

	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}

		// Collect metrics
		a.adapter.metrics_collector.mu.@lock()
		a.adapter.metrics_collector.data.s3_get_count++
		a.adapter.metrics_collector.mu.unlock()

		data, _ := a.adapter.get_object(obj.key, 0, -1) or {
			failed_count++
			a.adapter.metrics_collector.mu.@lock()
			a.adapter.metrics_collector.data.s3_error_count++
			a.adapter.metrics_collector.mu.unlock()
			continue
		}

		assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or {
			failed_count++
			a.adapter.metrics_collector.mu.@lock()
			a.adapter.metrics_collector.data.s3_error_count++
			a.adapter.metrics_collector.mu.unlock()
			continue
		}

		assignments << assignment
	}

	observability.log_with_context('s3', .info, 'PartitionAssignment', 'Listed partition assignments for topic',
		{
		'topic':  topic_name
		'count':  assignments.len.str()
		'failed': failed_count.str()
	})

	return assignments
}

/// list_all_partition_assignments retrieves all partition assignments for all topics.
/// S3 prefix: {prefix}/__cluster/assignments/
pub fn (mut a S3ClusterMetadataAdapter) list_all_partition_assignments() ![]domain.PartitionAssignment {
	a.ensure_adapter_config()
	prefix := a.all_assignments_prefix()

	observability.log_with_context('s3', .debug, 'PartitionAssignment', 'Listing all partition assignments',
		{
		'prefix': prefix
	})

	// Collect metrics
	a.adapter.metrics_collector.mu.@lock()
	a.adapter.metrics_collector.data.s3_list_count++
	a.adapter.metrics_collector.mu.unlock()

	objects := a.adapter.list_objects(prefix)!

	mut assignments := []domain.PartitionAssignment{}
	mut failed_count := 0

	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}

		// Collect metrics
		a.adapter.metrics_collector.mu.@lock()
		a.adapter.metrics_collector.data.s3_get_count++
		a.adapter.metrics_collector.mu.unlock()

		data, _ := a.adapter.get_object(obj.key, 0, -1) or {
			failed_count++
			a.adapter.metrics_collector.mu.@lock()
			a.adapter.metrics_collector.data.s3_error_count++
			a.adapter.metrics_collector.mu.unlock()
			continue
		}

		assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or {
			failed_count++
			a.adapter.metrics_collector.mu.@lock()
			a.adapter.metrics_collector.data.s3_error_count++
			a.adapter.metrics_collector.mu.unlock()
			continue
		}

		assignments << assignment
	}

	observability.log_with_context('s3', .info, 'PartitionAssignment', 'Listed all partition assignments',
		{
		'total_count': assignments.len.str()
		'failed':      failed_count.str()
	})

	return assignments
}

/// update_partition_assignment updates a partition assignment.
/// S3 key: {prefix}/__cluster/assignments/{topic}/{partition}.json
/// Includes retry logic for reliable persistence.
pub fn (mut a S3ClusterMetadataAdapter) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	a.ensure_adapter_config()
	key := a.partition_assignment_key(assignment.topic_name, assignment.partition)

	observability.log_with_context('s3', .debug, 'PartitionAssignment', 'Updating partition assignment',
		{
		'topic':            assignment.topic_name
		'partition':        assignment.partition.str()
		'preferred_broker': assignment.preferred_broker.str()
		'key':              key
	})

	data := json.encode(assignment).bytes()

	// Retry logic
	mut last_error := ''
	for retry in 0 .. a.adapter.config.max_retries {
		if retry > 0 {
			observability.log_with_context('s3', .debug, 'PartitionAssignment', 'Retrying partition assignment update',
				{
				'topic':     assignment.topic_name
				'partition': assignment.partition.str()
				'retry':     retry.str()
			})
			time.sleep(a.adapter.config.retry_delay_ms * time.millisecond)
		}

		a.adapter.put_object(key, data) or {
			last_error = err.str()
			continue
		}

		// Success metric
		a.adapter.metrics_collector.mu.@lock()
		a.adapter.metrics_collector.data.s3_put_count++
		a.adapter.metrics_collector.mu.unlock()

		observability.log_with_context('s3', .info, 'PartitionAssignment', 'Successfully updated partition assignment',
			{
			'topic':     assignment.topic_name
			'partition': assignment.partition.str()
			'retry':     retry.str()
		})
		return
	}

	// All retries failed
	a.adapter.metrics_collector.mu.@lock()
	a.adapter.metrics_collector.data.s3_error_count++
	a.adapter.metrics_collector.mu.unlock()

	observability.log_with_context('s3', .error, 'PartitionAssignment', 'Failed to update partition assignment after retries',
		{
		'topic':       assignment.topic_name
		'partition':   assignment.partition.str()
		'key':         key
		'error':       last_error
		'max_retries': a.adapter.config.max_retries.str()
	})

	return error('failed to update partition assignment after ${a.adapter.config.max_retries} retries: ${last_error}')
}

// Distributed Locking

/// LockInfo represents a distributed lock.
struct LockInfo {
	lock_name   string
	holder_id   string
	expires_at  i64
	acquired_at i64
}

/// try_acquire_lock attempts to acquire a distributed lock.
/// The lock is automatically released when the TTL expires.
pub fn (mut a S3ClusterMetadataAdapter) try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	a.ensure_adapter_config()
	key := a.lock_key(lock_name)
	now := time.now().unix_milli()

	// Check if lock exists and is valid
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		lock_info := json.decode(LockInfo, existing.bytestr()) or {
			// Corrupted lock; attempt acquisition
			return a.create_lock(key, lock_name, holder_id, ttl_ms)
		}

		// Check if lock has expired
		if lock_info.expires_at > now {
			// Lock is still held by someone
			if lock_info.holder_id == holder_id {
				// Already holding the lock; renew
				return a.refresh_lock(lock_name, holder_id, ttl_ms)
			}
			return false
		}
		// Lock expired; can acquire
	}

	return a.create_lock(key, lock_name, holder_id, ttl_ms)
}

/// create_lock creates a new lock.
fn (mut a S3ClusterMetadataAdapter) create_lock(key string, lock_name string, holder_id string, ttl_ms i64) !bool {
	a.ensure_adapter_config()
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

/// release_lock releases a distributed lock.
pub fn (mut a S3ClusterMetadataAdapter) release_lock(lock_name string, holder_id string) ! {
	a.ensure_adapter_config()
	key := a.lock_key(lock_name)

	// Check if lock is held
	existing, _ := a.adapter.get_object(key, 0, -1) or {
		observability.log_with_context('s3', .warn, 'ClusterMetadata', 'failed to fetch lock info for release',
			{
			'lock_name': lock_name
			'holder_id': holder_id
			'error':     err.str()
		})
		return
	}
	if existing.len == 0 {
		return
	}

	lock_info := json.decode(LockInfo, existing.bytestr()) or {
		observability.log_with_context('s3', .warn, 'ClusterMetadata', 'failed to decode lock info for release',
			{
			'lock_name': lock_name
			'holder_id': holder_id
			'error':     err.str()
		})
		return
	}
	if lock_info.holder_id != holder_id {
		return error('lock not held by this holder')
	}

	a.adapter.delete_object(key)!
}

/// refresh_lock refreshes the TTL of a distributed lock.
pub fn (mut a S3ClusterMetadataAdapter) refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	a.ensure_adapter_config()
	key := a.lock_key(lock_name)
	now := time.now().unix_milli()

	// Check if lock is held
	existing, _ := a.adapter.get_object(key, 0, -1) or { return false }
	if existing.len == 0 {
		return false
	}

	mut lock_info := json.decode(LockInfo, existing.bytestr()) or { return false }
	if lock_info.holder_id != holder_id {
		return false
	}

	// Refresh lock
	lock_info = LockInfo{
		...lock_info
		expires_at: now + ttl_ms
	}

	data := json.encode(lock_info).bytes()
	a.adapter.put_object(key, data)!
	return true
}

// Health Monitoring - For ClusterCoordinatorPort

/// mark_broker_dead marks a broker as dead.
pub fn (mut a S3ClusterMetadataAdapter) mark_broker_dead(broker_id i32) ! {
	a.ensure_adapter_config()
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

// Storage Capability

/// get_capability returns S3 storage capability information.
pub fn (a &S3ClusterMetadataAdapter) get_capability() domain.StorageCapability {
	return s3_capability
}

// Config Recovery (V interface value copy bug workaround)

// V interface value copy 버그 우회: adapter의 config가 손실된 경우 모듈 홀더에서 복구
fn (mut a S3ClusterMetadataAdapter) ensure_adapter_config() {
	backup := get_cluster_metadata_config_backup()
	if a.adapter.config.endpoint.len == 0 && backup.endpoint.len > 0 {
		a.adapter.config = backup
	}
}

// Key Helpers

/// broker_key returns the S3 key for broker information.
fn (a &S3ClusterMetadataAdapter) broker_key(broker_id i32) string {
	return '${a.adapter.config.prefix}__cluster/brokers/${broker_id}.json'
}

/// brokers_prefix returns the S3 prefix for listing brokers.
fn (a &S3ClusterMetadataAdapter) brokers_prefix() string {
	return '${a.adapter.config.prefix}__cluster/brokers/'
}

/// cluster_metadata_key returns the S3 key for cluster metadata.
fn (a &S3ClusterMetadataAdapter) cluster_metadata_key() string {
	return '${a.adapter.config.prefix}__cluster/metadata.json'
}

/// partition_assignment_key returns the S3 key for a partition assignment.
/// Format: {prefix}/__cluster/assignments/{topic_name}/{partition_id}.json
fn (a &S3ClusterMetadataAdapter) partition_assignment_key(topic_name string, partition i32) string {
	return '${a.adapter.config.prefix}__cluster/assignments/${topic_name}/${partition}.json'
}

/// partition_assignments_prefix returns the S3 prefix for listing partition assignments for a specific topic.
/// Format: {prefix}/__cluster/assignments/{topic_name}/
fn (a &S3ClusterMetadataAdapter) partition_assignments_prefix(topic_name string) string {
	return '${a.adapter.config.prefix}__cluster/assignments/${topic_name}/'
}

/// all_assignments_prefix returns the S3 prefix for listing all partition assignments.
/// Format: {prefix}/__cluster/assignments/
fn (a &S3ClusterMetadataAdapter) all_assignments_prefix() string {
	return '${a.adapter.config.prefix}__cluster/assignments/'
}

/// lock_key returns the S3 key for a distributed lock.
fn (a &S3ClusterMetadataAdapter) lock_key(lock_name string) string {
	return '${a.adapter.config.prefix}__cluster/locks/${lock_name}.json'
}
