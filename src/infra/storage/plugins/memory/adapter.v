// Infra Layer - in-memory storage adapter
// In-memory storage implementation using partition-level locking
// Suitable for testing and single-broker environments
module memory

import domain
import service.port
import sync
import time
import rand
import infra.observability

/// MemoryMetrics tracks metrics for memory storage operations.
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
	m.topic_create_count = 0
	m.topic_delete_count = 0
	m.topic_lookup_count = 0
	m.append_count = 0
	m.append_record_count = 0
	m.append_bytes = 0
	m.fetch_count = 0
	m.fetch_record_count = 0
	m.delete_records_count = 0
	m.offset_commit_count = 0
	m.offset_fetch_count = 0
	m.group_save_count = 0
	m.group_load_count = 0
	m.group_delete_count = 0
	m.error_count = 0
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
pub struct MemoryStorageAdapter {
pub mut:
	config MemoryConfig
mut:
	topics         map[string]&TopicStore
	topic_id_index map[string]string
	groups         map[string]domain.ConsumerGroup
	offsets        map[string]map[string]i64
	global_lock    sync.RwMutex
	// Metrics
	metrics      MemoryMetrics
	metrics_lock sync.Mutex
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

/// create_topic creates a new topic.
/// Automatically generates a UUID v4 format topic_id.
pub fn (mut a MemoryStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// Metrics: topic creation start
	a.metrics_lock.@lock()
	a.metrics.topic_create_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if name in a.topics {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		observability.log_with_context('memory', .error, 'Topic', 'Topic already exists',
			{
			'topic': name
		})
		return error('topic already exists')
	}

	// Generate UUID v4 for topic_id - initialize array at once
	mut topic_id := []u8{len: 16, init: u8(rand.intn(256) or { 0 })}
	// Set UUID version 4 (random)
	topic_id[6] = (topic_id[6] & 0x0f) | 0x40
	topic_id[8] = (topic_id[8] & 0x3f) | 0x80

	// Create partition stores (differs based on mmap mode)
	mut partition_stores := []&PartitionStore{}
	mut mmap_partition_stores := []&MmapPartitionStore{}

	if a.config.use_mmap {
		// mmap mode: create MmapPartitionStore (v0.33.0)
		for i in 0 .. partitions {
			mmap_config := MmapPartitionConfig{
				topic_name:    name
				partition:     i
				base_dir:      a.config.mmap_dir
				segment_size:  a.config.segment_size
				sync_on_write: a.config.sync_on_append
			}
			mmap_part := new_mmap_partition(mmap_config) or {
				// Metrics: error
				a.metrics_lock.@lock()
				a.metrics.error_count++
				a.metrics_lock.unlock()
				observability.log_with_context('memory', .error, 'Topic', 'Failed to create mmap partition',
					{
					'topic':     name
					'partition': i.str()
					'error':     err.msg()
				})
				return error('failed to create mmap partition: ${err}')
			}
			mmap_partition_stores << mmap_part
		}
	} else {
		// In-memory mode: create PartitionStore
		partition_stores = []&PartitionStore{cap: partitions}
		for _ in 0 .. partitions {
			partition_stores << &PartitionStore{
				records:        []domain.Record{}
				base_offset:    0
				high_watermark: 0
			}
		}
	}

	metadata := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     name.starts_with('__')
	}

	a.topics[name] = &TopicStore{
		metadata:        metadata
		config:          config
		partitions:      partition_stores
		mmap_partitions: mmap_partition_stores
		use_mmap:        a.config.use_mmap
	}

	// Cache topic_id -> name mapping for O(1) lookup
	a.topic_id_index[topic_id.hex()] = name

	observability.log_with_context('memory', .info, 'Topic', 'Topic created', {
		'topic':      name
		'partitions': partitions.str()
		'use_mmap':   a.config.use_mmap.str()
	})

	return metadata
}

/// delete_topic deletes a topic.
pub fn (mut a MemoryStorageAdapter) delete_topic(name string) ! {
	// Metrics: topic deletion
	a.metrics_lock.@lock()
	a.metrics.topic_delete_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	topic := a.topics[name] or {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}

	// Remove from topic_id_index cache
	a.topic_id_index.delete(topic.metadata.topic_id.hex())

	a.topics.delete(name)

	observability.log_with_context('memory', .info, 'Topic', 'Topic deleted', {
		'topic': name
	})
}

/// list_topics returns a list of all topics.
pub fn (mut a MemoryStorageAdapter) list_topics() ![]domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, topic in a.topics {
		result << topic.metadata
	}
	return result
}

/// get_topic retrieves topic metadata.
pub fn (mut a MemoryStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	// Metrics: topic lookup
	a.metrics_lock.@lock()
	a.metrics.topic_lookup_count++
	a.metrics_lock.unlock()

	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	if topic := a.topics[name] {
		return topic.metadata
	}

	// Metrics: error
	a.metrics_lock.@lock()
	a.metrics.error_count++
	a.metrics_lock.unlock()
	return error('topic not found')
}

/// get_topic_by_id retrieves a topic by topic_id.
/// Uses O(1) cache lookup.
pub fn (mut a MemoryStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	// O(1) lookup using topic_id_index cache
	topic_id_hex := topic_id.hex()
	if topic_name := a.topic_id_index[topic_id_hex] {
		if topic := a.topics[topic_name] {
			return topic.metadata
		}
	}

	return error('topic not found')
}

/// add_partitions adds partitions to a topic.
pub fn (mut a MemoryStorageAdapter) add_partitions(name string, new_count int) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	mut topic := a.topics[name] or { return error('topic not found') }

	// Check current partition count based on mmap mode
	current := if topic.use_mmap { topic.mmap_partitions.len } else { topic.partitions.len }
	if new_count <= current {
		return error('new partition count must be greater than current')
	}

	topic.lock.@lock()
	defer { topic.lock.unlock() }

	if topic.use_mmap {
		// mmap mode: add MmapPartitionStore (v0.33.0)
		for i in current .. new_count {
			mmap_config := MmapPartitionConfig{
				topic_name:    name
				partition:     i
				base_dir:      a.config.mmap_dir
				segment_size:  a.config.segment_size
				sync_on_write: a.config.sync_on_append
			}
			mmap_part := new_mmap_partition(mmap_config) or {
				return error('failed to create mmap partition: ${err}')
			}
			topic.mmap_partitions << mmap_part
		}
	} else {
		// In-memory mode
		for _ in current .. new_count {
			topic.partitions << &PartitionStore{
				records:        []domain.Record{}
				base_offset:    0
				high_watermark: 0
			}
		}
	}

	topic.metadata = domain.TopicMetadata{
		...topic.metadata
		partition_count: new_count
	}
}

/// append adds records to a partition.
/// Controls concurrency via partition-level locking.
pub fn (mut a MemoryStorageAdapter) append(topic_name string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	// Metrics: append start
	a.metrics_lock.@lock()
	a.metrics.append_count++
	a.metrics.append_record_count += i64(records.len)
	a.metrics_lock.unlock()

	// Look up topic with read lock
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	// mmap mode branch (v0.33.0)
	if topic.use_mmap {
		return a.append_mmap(topic, partition, records)
	}

	if partition < 0 || partition >= topic.partitions.len {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition out of range')
	}

	// Write lock only for the specific partition
	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	base_offset := part.high_watermark
	now := time.now()

	// Calculate bytes written
	mut bytes_written := i64(0)

	// Append records with timestamps
	for record in records {
		mut r := record
		if r.timestamp.unix() == 0 {
			r = domain.Record{
				...r
				timestamp: now
			}
		}
		part.records << r
		bytes_written += i64(r.key.len + r.value.len)
	}
	part.high_watermark += i64(records.len)

	// Metrics: add byte count
	a.metrics_lock.@lock()
	a.metrics.append_bytes += bytes_written
	a.metrics_lock.unlock()

	// Apply retention policy (max message count)
	if a.config.max_messages_per_partition > 0 {
		excess := part.records.len - a.config.max_messages_per_partition
		if excess > 0 {
			// Move elements in-place using slice without clone
			part.records = part.records[excess..]
			part.base_offset += i64(excess)
			observability.log_with_context('memory', .debug, 'Append', 'Applied retention policy',
				{
				'topic':         topic_name
				'partition':     partition.str()
				'deleted_count': excess.str()
			})
		}
	}

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: part.base_offset
		record_count:     records.len
	}
}

/// append_mmap appends records in mmap mode. (v0.33.0)
fn (mut a MemoryStorageAdapter) append_mmap(topic &TopicStore, partition int, records []domain.Record) !domain.AppendResult {
	if partition < 0 || partition >= topic.mmap_partitions.len {
		return error('partition out of range')
	}

	mut mmap_part := topic.mmap_partitions[partition]
	now := time.now()

	// Convert records to byte arrays
	mut record_bytes := [][]u8{cap: records.len}
	for record in records {
		// Simple serialization: key_len(4) + key + value_len(4) + value
		mut data := []u8{}
		// key length
		key_len := record.key.len
		data << u8(key_len >> 24)
		data << u8(key_len >> 16)
		data << u8(key_len >> 8)
		data << u8(key_len)
		data << record.key
		// value length
		value_len := record.value.len
		data << u8(value_len >> 24)
		data << u8(value_len >> 16)
		data << u8(value_len >> 8)
		data << u8(value_len)
		data << record.value
		record_bytes << data
	}

	base_offset, written := mmap_part.append(record_bytes)!

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: mmap_part.get_base_offset()
		record_count:     written
	}
}

/// fetch retrieves records from a partition.
pub fn (mut a MemoryStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	// Metrics: fetch start
	a.metrics_lock.@lock()
	a.metrics.fetch_count++
	a.metrics_lock.unlock()

	// Look up topic with read lock
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	// mmap mode branch (v0.33.0)
	if topic.use_mmap {
		return a.fetch_mmap(topic, partition, offset, max_bytes)
	}

	if partition < 0 || partition >= topic.partitions.len {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('partition out of range')
	}

	// Partition read lock
	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	// Return empty result if offset is out of range
	if offset < part.base_offset {
		return domain.FetchResult{
			records:            []
			first_offset:       part.base_offset
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	start_idx := int(offset - part.base_offset)
	if start_idx >= part.records.len {
		return domain.FetchResult{
			records:            []
			first_offset:       part.high_watermark
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	// Calculate end index based on max_bytes
	mut end_idx := start_idx
	mut total_bytes := 0
	max_fetch_bytes := if max_bytes <= 0 { 1048576 } else { max_bytes }

	for end_idx < part.records.len {
		record_size := part.records[end_idx].key.len + part.records[end_idx].value.len + 50
		if total_bytes + record_size > max_fetch_bytes && end_idx > start_idx {
			break
		}
		total_bytes += record_size
		end_idx++

		if end_idx - start_idx >= 1000 {
			break
		}
	}

	fetched_records := part.records[start_idx..end_idx]

	// Metrics: number of fetched records
	a.metrics_lock.@lock()
	a.metrics.fetch_record_count += i64(fetched_records.len)
	a.metrics_lock.unlock()

	// Calculate the offset of the first record actually returned
	actual_first_offset := part.base_offset + i64(start_idx)

	return domain.FetchResult{
		records:            fetched_records
		first_offset:       actual_first_offset
		high_watermark:     part.high_watermark
		last_stable_offset: part.high_watermark
		log_start_offset:   part.base_offset
	}
}

/// fetch_mmap retrieves records in mmap mode. (v0.33.0)
fn (mut a MemoryStorageAdapter) fetch_mmap(topic &TopicStore, partition int, offset i64, max_bytes int) !domain.FetchResult {
	if partition < 0 || partition >= topic.mmap_partitions.len {
		return error('partition out of range')
	}

	mut mmap_part := topic.mmap_partitions[partition]
	base_offset := mmap_part.get_base_offset()
	high_watermark := mmap_part.get_high_watermark()

	// Return empty result if offset is out of range
	if offset < base_offset || offset >= high_watermark {
		return domain.FetchResult{
			records:            []
			first_offset:       if offset < base_offset { base_offset } else { high_watermark }
			high_watermark:     high_watermark
			last_stable_offset: high_watermark
			log_start_offset:   base_offset
		}
	}

	// Calculate max records based on max_bytes
	max_records := if max_bytes <= 0 { 1000 } else { max_bytes / 100 }
	record_bytes := mmap_part.read(offset, max_records)!

	// Convert byte arrays to domain.Record
	mut records := []domain.Record{cap: record_bytes.len}
	for data in record_bytes {
		if data.len < 8 {
			continue
		}

		// Deserialize key_len(4) + key + value_len(4) + value
		key_len := int(u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3]))
		if 4 + key_len + 4 > data.len {
			continue
		}

		key := data[4..4 + key_len].clone()
		value_start := 4 + key_len
		value_len := int(u32(data[value_start]) << 24 | u32(data[value_start + 1]) << 16 | u32(data[
			value_start + 2]) << 8 | u32(data[value_start + 3]))

		if value_start + 4 + value_len > data.len {
			continue
		}

		value := data[value_start + 4..value_start + 4 + value_len].clone()

		records << domain.Record{
			key:       key
			value:     value
			timestamp: time.now()
		}
	}

	return domain.FetchResult{
		records:            records
		first_offset:       offset
		high_watermark:     high_watermark
		last_stable_offset: high_watermark
		log_start_offset:   base_offset
	}
}

/// delete_records deletes records before the specified offset.
pub fn (mut a MemoryStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	delete_count := int(before_offset - part.base_offset)
	if delete_count > 0 && delete_count <= part.records.len {
		// Use slice assignment instead of clone
		part.records = part.records[delete_count..]
		part.base_offset = before_offset
	}
}

/// get_partition_info retrieves partition information.
pub fn (mut a MemoryStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	// mmap mode branch (v0.33.0)
	if topic.use_mmap {
		if partition < 0 || partition >= topic.mmap_partitions.len {
			return error('partition out of range')
		}

		mmap_part := topic.mmap_partitions[partition]
		return domain.PartitionInfo{
			topic:           topic_name
			partition:       partition
			earliest_offset: mmap_part.get_base_offset()
			latest_offset:   mmap_part.get_high_watermark()
			high_watermark:  mmap_part.get_high_watermark()
		}
	}

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: part.base_offset
		latest_offset:   part.high_watermark
		high_watermark:  part.high_watermark
	}
}

/// save_group saves a consumer group.
pub fn (mut a MemoryStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	// Metrics: group save
	a.metrics_lock.@lock()
	a.metrics.group_save_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	a.groups[group.group_id] = group
}

/// load_group loads a consumer group.
pub fn (mut a MemoryStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	// Metrics: group load
	a.metrics_lock.@lock()
	a.metrics.group_load_count++
	a.metrics_lock.unlock()

	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	if group := a.groups[group_id] {
		return group
	}

	// Metrics: error
	a.metrics_lock.@lock()
	a.metrics.error_count++
	a.metrics_lock.unlock()
	return error('group not found')
}

/// delete_group deletes a consumer group.
pub fn (mut a MemoryStorageAdapter) delete_group(group_id string) ! {
	// Metrics: group delete
	a.metrics_lock.@lock()
	a.metrics.group_delete_count++
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if group_id !in a.groups {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('group not found')
	}
	a.groups.delete(group_id)
	a.offsets.delete(group_id)

	observability.log_with_context('memory', .info, 'Group', 'Group deleted', {
		'group_id': group_id
	})
}

/// list_groups returns a list of all consumer groups.
pub fn (mut a MemoryStorageAdapter) list_groups() ![]domain.GroupInfo {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut result := []domain.GroupInfo{}
	for _, group in a.groups {
		result << domain.GroupInfo{
			group_id:      group.group_id
			protocol_type: group.protocol_type
			state:         match group.state {
				.empty { 'Empty' }
				.preparing_rebalance { 'PreparingRebalance' }
				.completing_rebalance { 'CompletingRebalance' }
				.stable { 'Stable' }
				.dead { 'Dead' }
			}
		}
	}
	return result
}

/// commit_offsets commits offsets.
pub fn (mut a MemoryStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	// Metrics: offset commit
	a.metrics_lock.@lock()
	a.metrics.offset_commit_count += i64(offsets.len)
	a.metrics_lock.unlock()

	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if group_id !in a.offsets {
		a.offsets[group_id] = map[string]i64{}
	}

	for offset in offsets {
		key := '${offset.topic}:${offset.partition}'
		a.offsets[group_id][key] = offset.offset
	}

	observability.log_with_context('memory', .debug, 'Offset', 'Offsets committed', {
		'group_id': group_id
		'count':    offsets.len.str()
	})
}

/// fetch_offsets retrieves committed offsets.
pub fn (mut a MemoryStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	// Metrics: offset fetch
	a.metrics_lock.@lock()
	a.metrics.offset_fetch_count += i64(partitions.len)
	a.metrics_lock.unlock()

	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut results := []domain.OffsetFetchResult{}

	if group_id !in a.offsets {
		for part in partitions {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
		return results
	}

	for part in partitions {
		key := '${part.topic}:${part.partition}'
		offset := a.offsets[group_id][key] or { -1 }
		results << domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset
			metadata:   ''
			error_code: 0
		}
	}

	return results
}

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
pub fn (mut a MemoryStorageAdapter) get_stats() StorageStats {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut total_partitions := 0
	mut total_records := i64(0)
	mut total_bytes := i64(0)

	for _, topic in a.topics {
		total_partitions += topic.partitions.len

		for i in 0 .. topic.partitions.len {
			part := topic.partitions[i]
			total_records += part.high_watermark - part.base_offset
			// Note: total_bytes is approximate without iterating records
		}
	}

	return StorageStats{
		topic_count:      a.topics.len
		total_partitions: total_partitions
		total_records:    total_records
		total_bytes:      total_bytes
		group_count:      a.groups.len
	}
}

/// get_metrics returns the current metrics snapshot.
pub fn (mut a MemoryStorageAdapter) get_metrics() MemoryMetrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary returns the metrics summary string.
pub fn (mut a MemoryStorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics resets all metrics to zero.
pub fn (mut a MemoryStorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}

/// clear deletes all data (for testing).
pub fn (mut a MemoryStorageAdapter) clear() {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	a.topics.clear()
	a.topic_id_index.clear()
	a.groups.clear()
	a.offsets.clear()
}
