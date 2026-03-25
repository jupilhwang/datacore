// Infra Layer - in-memory storage adapter
// In-memory storage implementation using partition-level locking
// Suitable for testing and single-broker environments
module memory

import domain
import service.port
import sync
import sync.stdatomic

/// MemoryMetrics tracks metrics for memory storage operations.
/// All fields are accessed atomically - no external lock required.
struct MemoryMetrics {
mut:
	// Topic operation metrics
	topic_create_count i64
	topic_delete_count i64
	topic_lookup_count i64
	// Record operation metrics
	append_count         i64
	append_record_count  i64
	append_bytes         i64
	fetch_count          i64
	fetch_record_count   i64
	delete_records_count i64
	// Offset operation metrics
	offset_commit_count i64
	offset_fetch_count  i64
	// Group operation metrics
	group_save_count   i64
	group_load_count   i64
	group_delete_count i64
	// Error metrics
	error_count i64
}

/// reset resets all metrics to zero.
fn (mut m MemoryMetrics) reset() {
	stdatomic.store_i64(&m.topic_create_count, 0)
	stdatomic.store_i64(&m.topic_delete_count, 0)
	stdatomic.store_i64(&m.topic_lookup_count, 0)
	stdatomic.store_i64(&m.append_count, 0)
	stdatomic.store_i64(&m.append_record_count, 0)
	stdatomic.store_i64(&m.append_bytes, 0)
	stdatomic.store_i64(&m.fetch_count, 0)
	stdatomic.store_i64(&m.fetch_record_count, 0)
	stdatomic.store_i64(&m.delete_records_count, 0)
	stdatomic.store_i64(&m.offset_commit_count, 0)
	stdatomic.store_i64(&m.offset_fetch_count, 0)
	stdatomic.store_i64(&m.group_save_count, 0)
	stdatomic.store_i64(&m.group_load_count, 0)
	stdatomic.store_i64(&m.group_delete_count, 0)
	stdatomic.store_i64(&m.error_count, 0)
}

/// get_snapshot returns a point-in-time copy of current metric values.
fn (m &MemoryMetrics) get_snapshot() MemoryMetrics {
	return MemoryMetrics{
		topic_create_count:   stdatomic.load_i64(&m.topic_create_count)
		topic_delete_count:   stdatomic.load_i64(&m.topic_delete_count)
		topic_lookup_count:   stdatomic.load_i64(&m.topic_lookup_count)
		append_count:         stdatomic.load_i64(&m.append_count)
		append_record_count:  stdatomic.load_i64(&m.append_record_count)
		append_bytes:         stdatomic.load_i64(&m.append_bytes)
		fetch_count:          stdatomic.load_i64(&m.fetch_count)
		fetch_record_count:   stdatomic.load_i64(&m.fetch_record_count)
		delete_records_count: stdatomic.load_i64(&m.delete_records_count)
		offset_commit_count:  stdatomic.load_i64(&m.offset_commit_count)
		offset_fetch_count:   stdatomic.load_i64(&m.offset_fetch_count)
		group_save_count:     stdatomic.load_i64(&m.group_save_count)
		group_load_count:     stdatomic.load_i64(&m.group_load_count)
		group_delete_count:   stdatomic.load_i64(&m.group_delete_count)
		error_count:          stdatomic.load_i64(&m.error_count)
	}
}

/// get_summary returns a metrics summary as a string.
fn (m &MemoryMetrics) get_summary() string {
	return '[Memory Metrics]
  Topics: create=${m.topic_create_count}, delete=${m.topic_delete_count}, lookup=${m.topic_lookup_count}
  Records: append=${m.append_count} (${m.append_record_count} records, ${m.append_bytes} bytes), fetch=${m.fetch_count} (${m.fetch_record_count} records), delete=${m.delete_records_count}
  Offsets: commit=${m.offset_commit_count}, fetch=${m.offset_fetch_count}
  Groups: save=${m.group_save_count}, load=${m.group_load_count}, delete=${m.group_delete_count}
  Errors: ${m.error_count}'
}

/// MemoryStorageAdapter implements port.StoragePort.
/// Memory-based storage that controls concurrency via per-partition locking.
/// Lock hierarchy (acquire in this order to avoid deadlock):
///   topics_lock -> groups_lock -> share_lock
pub struct MemoryStorageAdapter {
pub mut:
	config MemoryConfig
mut:
	topics         map[string]&TopicStore
	topic_id_index map[string]string
	// topics_lock guards: topics, topic_id_index
	topics_lock sync.RwMutex
	groups      map[string]domain.ConsumerGroup
	offsets     map[string]map[string]i64
	// groups_lock guards: groups, offsets
	groups_lock sync.RwMutex
	// Share Group Partition states keyed by "group_id:topic:partition"
	share_partition_states map[string]domain.SharePartitionState
	// Index for O(k) group-scoped lookup: group_id -> []key
	share_partition_by_group map[string][]string
	// share_lock guards: share_partition_states, share_partition_by_group
	share_lock sync.RwMutex
	// Metrics fields are accessed via sync.stdatomic - no separate lock needed.
	metrics MemoryMetrics
}

/// MemoryConfig holds the memory storage configuration.
pub struct MemoryConfig {
pub:
	max_messages_per_partition int = 1000000
	max_bytes_per_partition    i64 = -1
	retention_ms               i64 = 604800000
	// mmap configuration (v0.33.0)
	use_mmap       bool
	mmap_dir       string = '/tmp/datacore'
	segment_size   i64    = 1073741824
	sync_on_append bool
}

/// memory_capability defines the storage capabilities of the memory adapter.
pub const memory_capability = domain.StorageCapability{
	name:                  'memory'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   false
	is_persistent:         false
	is_distributed:        false
}

/// TopicStore stores topic data.
struct TopicStore {
pub mut:
	metadata        domain.TopicMetadata
	config          domain.TopicConfig
	partitions      []&PartitionStore
	mmap_partitions []&MmapPartitionStore
	use_mmap        bool
	lock            sync.RwMutex
}

/// PartitionStore stores partition data and supports fine-grained locking.
struct PartitionStore {
mut:
	records        []domain.Record
	base_offset    i64
	high_watermark i64
	lock           sync.RwMutex
}

/// new_memory_adapter creates a new memory storage adapter.
pub fn new_memory_adapter() &MemoryStorageAdapter {
	return new_memory_adapter_with_config(MemoryConfig{})
}

/// new_memory_adapter_with_config creates an adapter with custom configuration.
pub fn new_memory_adapter_with_config(config MemoryConfig) &MemoryStorageAdapter {
	return &MemoryStorageAdapter{
		config:         config
		topics:         map[string]&TopicStore{}
		topic_id_index: map[string]string{}
		groups:         map[string]domain.ConsumerGroup{}
		offsets:        map[string]map[string]i64{}
	}
}

// --- Cross-category metrics helper ---

fn (mut a MemoryStorageAdapter) inc_error() {
	stdatomic.add_i64(&a.metrics.error_count, 1)
}

// --- Topic lookup helpers ---
// These helpers encapsulate the rlock/runlock pattern for topic map access,
// ensuring the read lock is always released even when the map lookup fails.

// lookup_topic_read returns a reference to the TopicStore under a read lock.
// The caller must NOT hold topics_lock when calling this function.
fn (mut a MemoryStorageAdapter) lookup_topic_read(topic_name string) !&TopicStore {
	a.topics_lock.rlock()
	topic := a.topics[topic_name] or {
		a.topics_lock.runlock()
		return error('topic not found')
	}
	a.topics_lock.runlock()
	return topic
}

// --- Core interface methods ---

/// health_check checks the storage health status.
pub fn (mut a MemoryStorageAdapter) health_check() !port.HealthStatus {
	return .healthy
}

/// get_storage_capability returns storage capability information.
pub fn (a &MemoryStorageAdapter) get_storage_capability() domain.StorageCapability {
	return memory_capability
}

/// get_cluster_metadata_port returns the cluster metadata port.
/// Memory storage does not support multi-broker, so returns none.
pub fn (a &MemoryStorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

/// StorageStats provides storage statistics.
pub struct StorageStats {
pub:
	topic_count      int
	total_partitions int
	total_records    i64
	total_bytes      i64
	group_count      int
}

/// get_stats returns the current storage statistics.
fn (mut a MemoryStorageAdapter) get_stats() StorageStats {
	mut total_partitions := 0
	mut total_records := i64(0)
	mut total_bytes := i64(0)

	a.topics_lock.rlock()
	topic_count := a.topics.len
	for _, topic in a.topics {
		total_partitions += topic.partitions.len

		for i in 0 .. topic.partitions.len {
			part := topic.partitions[i]
			total_records += part.high_watermark - part.base_offset
			// Note: total_bytes is approximate without iterating records
		}
	}
	a.topics_lock.runlock()

	a.groups_lock.rlock()
	group_count := a.groups.len
	a.groups_lock.runlock()

	return StorageStats{
		topic_count:      topic_count
		total_partitions: total_partitions
		total_records:    total_records
		total_bytes:      total_bytes
		group_count:      group_count
	}
}

/// get_metrics returns the current metrics snapshot.
fn (mut a MemoryStorageAdapter) get_metrics() MemoryMetrics {
	return a.metrics.get_snapshot()
}

/// get_metrics_summary returns the metrics summary string.
fn (mut a MemoryStorageAdapter) get_metrics_summary() string {
	snap := a.metrics.get_snapshot()
	return snap.get_summary()
}

/// reset_metrics resets all metrics to zero.
fn (mut a MemoryStorageAdapter) reset_metrics() {
	a.metrics.reset()
}

/// clear deletes all data (for testing).
fn (mut a MemoryStorageAdapter) clear() {
	a.topics_lock.@lock()
	a.topics.clear()
	a.topic_id_index.clear()
	a.topics_lock.unlock()

	a.groups_lock.@lock()
	a.groups.clear()
	a.offsets.clear()
	a.groups_lock.unlock()

	a.share_lock.@lock()
	a.share_partition_states.clear()
	a.share_partition_by_group.clear()
	a.share_lock.unlock()
}
