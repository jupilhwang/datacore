// S3 Storage Plugin for DataCore
// Implements StoragePort interface with S3 backend
// Uses conditional writes (ETag) for concurrency control
module s3

import domain
import service.port
import time
import json
import crypto.md5
import sync

// Note: S3 HTTP client functions (sign_request, get_object, put_object, etc.) are in s3_client.v
// Note: PartitionIndex, LogSegment, CachedPartitionIndex are defined in partition_index.v
// Note: StoredRecord, TopicPartitionBuffer are defined in s3_record_codec.v and buffer_manager.v
// Note: Buffer management functions are in buffer_manager.v
// Note: Compaction functions are in compaction.v

// Storage capability for S3 adapter
pub const s3_capability = domain.StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

// S3Config holds S3 storage configuration
pub struct S3Config {
pub:
	bucket_name    string = 'datacore-storage'
	region         string = 'us-east-1'
	endpoint       string // Optional: for MinIO/LocalStack
	access_key     string
	secret_key     string
	prefix         string = 'datacore/'
	max_retries    int    = 3
	retry_delay_ms int    = 100
	use_path_style bool   = true  // For MinIO compatibility
	timezone       string = 'UTC' // Added for SigV4 compatibility
	// Batching
	batch_timeout_ms int
	batch_max_bytes  i64
	// Compaction
	compaction_interval_ms int
	target_segment_bytes   i64
	index_cache_ttl_ms     int // Added for config-based TTL
}

// S3StorageAdapter implements StoragePort for S3 storage
pub struct S3StorageAdapter {
mut:
	config S3Config
	// Local caches with TTL
	topic_cache       map[string]CachedTopic
	topic_id_cache    map[string]string // topic_id (hex) -> topic_name for O(1) lookup
	group_cache       map[string]CachedGroup
	offset_cache      map[string]map[string]i64
	topic_index_cache map[string]CachedPartitionIndex // Added index cache
	// Locks for thread safety
	topic_lock  sync.RwMutex
	group_lock  sync.RwMutex
	offset_lock sync.RwMutex
	// Flushing buffer for batched S3 writes
	topic_partition_buffers map[string]TopicPartitionBuffer // Key: "topic:partition"
	buffer_lock             sync.Mutex
	index_update_lock       sync.Mutex // Lock for S3 index updates to prevent race conditions
	is_flushing             bool
	// Compaction settings
	min_segment_count_to_compact int = 5
	compactor_running            bool
}

struct CachedTopic {
	meta      domain.TopicMetadata
	etag      string
	cached_at time.Time
}

struct CachedGroup {
	group     domain.ConsumerGroup
	etag      string
	cached_at time.Time
}

// Note: CachedPartitionIndex is defined in partition_index.v

// S3 object keys structure:
// {prefix}/topics/{topic_name}/metadata.json
// {prefix}/topics/{topic_name}/partitions/{partition}/log-{start_offset}-{end_offset}.bin
// {prefix}/topics/{topic_name}/partitions/{partition}/index.json
// {prefix}/groups/{group_id}/state.json
// {prefix}/offsets/{group_id}/{topic}:{partition}.json

// new_s3_adapter creates a new S3 storage adapter
pub fn new_s3_adapter(config S3Config) !&S3StorageAdapter {
	return &S3StorageAdapter{
		config:                  config
		topic_cache:             map[string]CachedTopic{}
		group_cache:             map[string]CachedGroup{}
		offset_cache:            map[string]map[string]i64{}
		topic_index_cache:       map[string]CachedPartitionIndex{} // Initialized cache
		topic_partition_buffers: map[string]TopicPartitionBuffer{}
	}
}

// ============================================================
// Topic Operations
// ============================================================

// create_topic creates a new topic in S3
pub fn (mut a S3StorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// Check if topic already exists
	existing := a.get_topic(name) or { domain.TopicMetadata{} }
	if existing.name.len > 0 {
		return error('Topic already exists: ${name}')
	}

	topic_id := generate_topic_id(name)

	// Convert TopicConfig to map[string]string
	mut config_map := map[string]string{}
	config_map['retention.ms'] = config.retention_ms.str()
	config_map['retention.bytes'] = config.retention_bytes.str()
	config_map['segment.bytes'] = config.segment_bytes.str()
	config_map['cleanup.policy'] = config.cleanup_policy

	meta := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          config_map
		is_internal:     name.starts_with('_')
	}

	// Write metadata to S3 with conditional write (If-None-Match: *)
	key := a.topic_metadata_key(name)
	data := json.encode(meta)
	a.put_object_if_not_exists(key, data.bytes())!

	// Initialize partition indices
	for p in 0 .. partitions {
		index_key := a.partition_index_key(name, p)
		index := PartitionIndex{
			topic:           name
			partition:       p
			earliest_offset: 0
			high_watermark:  0
			log_segments:    []
		}
		a.put_object(index_key, json.encode(index).bytes())!
	}

	// Cache the topic
	a.topic_lock.@lock()
	a.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      ''
		cached_at: time.now()
	}
	// Also cache topic_id -> name mapping for O(1) lookup
	a.topic_id_cache[meta.topic_id.hex()] = name
	a.topic_lock.unlock()

	return meta
}

// delete_topic deletes a topic from S3
pub fn (mut a S3StorageAdapter) delete_topic(name string) ! {
	// Get topic metadata to remove from id cache
	if cached := a.topic_cache[name] {
		a.topic_lock.@lock()
		a.topic_id_cache.delete(cached.meta.topic_id.hex())
		a.topic_lock.unlock()
	}

	// List and delete all objects with topic prefix
	prefix := '${a.config.prefix}topics/${name}/'
	a.delete_objects_with_prefix(prefix)!

	// Remove from cache
	a.topic_lock.@lock()
	a.topic_cache.delete(name)
	a.topic_lock.unlock()
}

// list_topics lists all topics from S3
pub fn (mut a S3StorageAdapter) list_topics() ![]domain.TopicMetadata {
	prefix := '${a.config.prefix}topics/'
	objects := a.list_objects(prefix)!

	mut topics := []domain.TopicMetadata{}
	mut seen := map[string]bool{}

	// First, add all cached topics
	a.topic_lock.rlock()
	for name, cached in a.topic_cache {
		if name !in seen {
			seen[name] = true
			topics << cached.meta
		}
	}
	a.topic_lock.runlock()

	// Then, add topics from S3 that are not already in cache
	for obj in objects {
		// Extract topic name from path like "datacore/topics/my-topic/metadata.json"
		if obj.key.ends_with('/metadata.json') {
			parts := obj.key.split('/')
			if parts.len >= 3 {
				topic_name := parts[parts.len - 2]
				if topic_name !in seen {
					seen[topic_name] = true
					if meta := a.get_topic(topic_name) {
						topics << meta
					}
				}
			}
		}
	}

	return topics
}

// get_topic retrieves topic metadata from S3
pub fn (mut a S3StorageAdapter) get_topic(name string) !domain.TopicMetadata {
	// Check cache first
	a.topic_lock.rlock()
	if cached := a.topic_cache[name] {
		if time.since(cached.cached_at) < time.minute * 5 {
			a.topic_lock.runlock()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// Fetch from S3
	key := a.topic_metadata_key(name)
	data, etag := a.get_object(key, -1, -1)!

	meta := json.decode(domain.TopicMetadata, data.bytestr())!

	// Update cache
	a.topic_lock.@lock()
	a.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      etag
		cached_at: time.now()
	}
	a.topic_lock.unlock()

	return meta
}

// get_topic_by_id retrieves topic by ID using O(1) cache lookup
pub fn (mut a S3StorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	// Convert topic_id to hex string for map lookup
	topic_id_hex := topic_id.hex()

	// Check cache first (O(1) lookup)
	a.topic_lock.rlock()
	if topic_name := a.topic_id_cache[topic_id_hex] {
		if cached := a.topic_cache[topic_name] {
			a.topic_lock.runlock()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// Cache miss - fetch from S3 and populate cache
	topics := a.list_topics()!
	for t in topics {
		// Populate topic_id_cache for future lookups
		a.topic_lock.@lock()
		a.topic_id_cache[t.topic_id.hex()] = t.name
		a.topic_lock.unlock()

		if t.topic_id == topic_id {
			return t
		}
	}
	return error('Topic not found')
}

// add_partitions adds partitions to a topic
pub fn (mut a S3StorageAdapter) add_partitions(name string, new_count int) ! {
	meta := a.get_topic(name)!
	if new_count <= meta.partition_count {
		return error('New partition count must be greater than current')
	}

	// Initialize new partition indices
	for p in meta.partition_count .. new_count {
		index_key := a.partition_index_key(name, p)
		index := PartitionIndex{
			topic:           name
			partition:       p
			earliest_offset: 0
			high_watermark:  0
			log_segments:    []
		}
		a.put_object(index_key, json.encode(index).bytes())!
	}

	// Update topic metadata with conditional write
	updated_meta := domain.TopicMetadata{
		...meta
		partition_count: new_count
	}

	key := a.topic_metadata_key(name)
	a.put_object(key, json.encode(updated_meta).bytes())!

	// Invalidate cache
	a.topic_lock.@lock()
	a.topic_cache.delete(name)
	a.topic_lock.unlock()
}

// ============================================================
// Record Operations
// ============================================================

// append appends records to a partition (buffers in memory, flushes when full)
pub fn (mut a S3StorageAdapter) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	if records.len == 0 {
		return domain.AppendResult{
			base_offset:      0
			log_append_time:  time.now().unix_milli()
			log_start_offset: 0
		}
	}

	// 1. Get current partition index
	mut index := a.get_partition_index(topic, partition)!
	base_offset := index.high_watermark
	partition_key := '${topic}:${partition}'

	// 2. Add records to the partition buffer
	mut bytes_to_add := i64(0)
	mut stored_records := []StoredRecord{}

	for i, rec in records {
		srec := StoredRecord{
			offset:    base_offset + i64(i)
			timestamp: if rec.timestamp.unix_milli() == 0 { time.now() } else { rec.timestamp }
			key:       rec.key
			value:     rec.value
			headers:   rec.headers
		}
		stored_records << srec
		// Simple size estimate (Actual size is larger due to metadata encoding)
		bytes_to_add += i64(srec.value.len + srec.key.len + 30) // 30 is rough overhead for offset, timestamp, lengths, etc.
	}

	// 3. Update in-memory buffer and check for flush
	mut should_flush := false // For fast path flush
	a.buffer_lock.lock()
	mut tp_buffer := a.topic_partition_buffers[partition_key] or {
		TopicPartitionBuffer{
			records:            []
			current_size_bytes: 0
		}
	}

	tp_buffer.records << stored_records
	tp_buffer.current_size_bytes += bytes_to_add

	if tp_buffer.current_size_bytes >= a.config.batch_max_bytes {
		should_flush = true
	}

	a.topic_partition_buffers[partition_key] = tp_buffer
	a.buffer_lock.unlock()

	// 4. Fast path flush is DISABLED to avoid race conditions with flush_worker
	// All flushes are now handled by the periodic flush_worker
	// if should_flush {
	// 	go a.async_flush_partition(partition_key)
	// }
	_ = should_flush // Suppress unused variable warning

	// 5. Update in-memory high_watermark immediately for response
	// Also update the cache so the next append gets the correct offset
	new_high_watermark := base_offset + i64(records.len)
	a.topic_lock.@lock()
	if cached := a.topic_index_cache[partition_key] {
		mut updated_index := cached.index
		updated_index.high_watermark = new_high_watermark
		a.topic_index_cache[partition_key] = CachedPartitionIndex{
			index:     updated_index
			etag:      cached.etag
			cached_at: cached.cached_at
		}
	}
	a.topic_lock.unlock()

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  time.now().unix_milli()
		log_start_offset: index.earliest_offset
	}
}

// fetch retrieves records from a partition
pub fn (mut a S3StorageAdapter) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	partition_key := '${topic}:${partition}'

	// Get S3 index for segments info
	index := a.get_partition_index(topic, partition)!

	if offset < index.earliest_offset {
		return error('Offset out of range (too old): ${offset} < ${index.earliest_offset}')
	}

	// Find relevant segments
	mut all_records := []domain.Record{}
	mut bytes_read := 0
	mut highest_offset_read := offset - 1

	// 1. First, try to read from S3 segments
	for seg in index.log_segments {
		if seg.end_offset < offset {
			continue
		}
		if seg.start_offset > offset + i64(max_bytes / 100) { // Rough estimate
			break
		}

		// Fetch segment from S3
		// Optimization: If reading from start of segment, use Range Request
		mut data := []u8{}
		if offset == seg.start_offset && max_bytes > 0 {
			mut fetch_size := i64(max_bytes) * 2
			if fetch_size > seg.size_bytes {
				fetch_size = -1 // Read full
			}
			range_end := if fetch_size > 0 { fetch_size } else { -1 }
			data, _ = a.get_object(seg.key, 0, range_end) or { continue }
		} else {
			// Random access without index: must download full segment
			data, _ = a.get_object(seg.key, -1, -1) or { continue }
		}

		stored_records := decode_stored_records(data)

		for rec in stored_records {
			if rec.offset >= offset && bytes_read < max_bytes {
				// Convert StoredRecord to domain.Record
				all_records << domain.Record{
					key:       rec.key
					value:     rec.value
					headers:   rec.headers
					timestamp: rec.timestamp
				}
				bytes_read += rec.value.len + rec.key.len
				if rec.offset > highest_offset_read {
					highest_offset_read = rec.offset
				}
			}
		}

		if bytes_read >= max_bytes {
			break
		}
	}

	// 2. Also read from in-memory buffer (not yet flushed to S3)
	// This is critical for data that hasn't been persisted yet
	if bytes_read < max_bytes {
		a.buffer_lock.lock()
		if tp_buffer := a.topic_partition_buffers[partition_key] {
			for rec in tp_buffer.records {
				// Read records that are at or after the requested offset
				// and haven't been read from S3 segments yet
				if rec.offset >= offset && rec.offset > highest_offset_read
					&& bytes_read < max_bytes {
					all_records << domain.Record{
						key:       rec.key
						value:     rec.value
						headers:   rec.headers
						timestamp: rec.timestamp
					}
					bytes_read += rec.value.len + rec.key.len
					if rec.offset > highest_offset_read {
						highest_offset_read = rec.offset
					}
				}
			}
		}
		a.buffer_lock.unlock()
	}

	return domain.FetchResult{
		records:            all_records
		high_watermark:     index.high_watermark
		last_stable_offset: index.high_watermark
		log_start_offset:   index.earliest_offset
	}
}

// delete_records deletes records before a given offset
pub fn (mut a S3StorageAdapter) delete_records(topic string, partition int, before_offset i64) ! {
	mut index := a.get_partition_index(topic, partition)!

	// Find segments to delete
	mut segments_to_delete := []string{}
	mut remaining_segments := []LogSegment{}

	for seg in index.log_segments {
		if seg.end_offset < before_offset {
			segments_to_delete << seg.key
		} else {
			remaining_segments << seg
		}
	}

	// Delete segments from S3
	for key in segments_to_delete {
		a.delete_object(key) or {}
	}

	// Update index
	index.earliest_offset = before_offset
	index.log_segments = remaining_segments

	index_key := a.partition_index_key(topic, partition)
	a.put_object(index_key, json.encode(index).bytes())!
}

// ============================================================
// Partition Info
// ============================================================

pub fn (mut a S3StorageAdapter) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	index := a.get_partition_index(topic, partition)!

	return domain.PartitionInfo{
		topic:           topic
		partition:       partition
		earliest_offset: index.earliest_offset
		latest_offset:   index.high_watermark
		high_watermark:  index.high_watermark
	}
}

// ============================================================
// Consumer Group Operations
// ============================================================

pub fn (mut a S3StorageAdapter) save_group(group domain.ConsumerGroup) ! {
	key := a.group_key(group.group_id)
	data := json.encode(group)
	a.put_object(key, data.bytes())!

	// Update cache
	a.group_lock.@lock()
	a.group_cache[group.group_id] = CachedGroup{
		group:     group
		etag:      ''
		cached_at: time.now()
	}
	a.group_lock.unlock()
}

pub fn (mut a S3StorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	// Check cache
	a.group_lock.rlock()
	if cached := a.group_cache[group_id] {
		if time.since(cached.cached_at) < time.second * 30 {
			a.group_lock.runlock()
			return cached.group
		}
	}
	a.group_lock.runlock()

	key := a.group_key(group_id)
	data, etag := a.get_object(key, -1, -1)!

	group := json.decode(domain.ConsumerGroup, data.bytestr())!

	// Update cache
	a.group_lock.@lock()
	a.group_cache[group_id] = CachedGroup{
		group:     group
		etag:      etag
		cached_at: time.now()
	}
	a.group_lock.unlock()

	return group
}

pub fn (mut a S3StorageAdapter) delete_group(group_id string) ! {
	key := a.group_key(group_id)
	a.delete_object(key)!

	// Also delete offsets
	offsets_prefix := '${a.config.prefix}offsets/${group_id}/'
	a.delete_objects_with_prefix(offsets_prefix)!

	// Remove from cache
	a.group_lock.@lock()
	a.group_cache.delete(group_id)
	a.group_lock.unlock()
}

pub fn (mut a S3StorageAdapter) list_groups() ![]domain.GroupInfo {
	prefix := '${a.config.prefix}groups/'
	objects := a.list_objects(prefix)!

	mut groups := []domain.GroupInfo{}
	mut seen := map[string]bool{}

	for obj in objects {
		if obj.key.ends_with('/state.json') {
			parts := obj.key.split('/')
			if parts.len >= 2 {
				group_id := parts[parts.len - 2]
				if group_id !in seen {
					seen[group_id] = true
					if group := a.load_group(group_id) {
						groups << domain.GroupInfo{
							group_id:      group_id
							protocol_type: group.protocol_type
							state:         group.state.str()
						}
					}
				}
			}
		}
	}

	return groups
}

// ============================================================
// Offset Operations
// ============================================================

pub fn (mut a S3StorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	mut failed := []string{}
	mut succeeded := []domain.PartitionOffset{}

	// Try to commit each offset individually
	for offset in offsets {
		key := a.offset_key(group_id, offset.topic, offset.partition)
		data := json.encode(offset)
		a.put_object(key, data.bytes()) or {
			failed << '${offset.topic}:${offset.partition}'
			eprintln('[WARN] Failed to commit offset for ${offset.topic}:${offset.partition}: ${err}')
			continue
		}
		succeeded << offset
	}

	// Update local cache for successfully committed offsets
	if succeeded.len > 0 {
		a.offset_lock.@lock()
		if group_id !in a.offset_cache {
			a.offset_cache[group_id] = map[string]i64{}
		}
		for offset in succeeded {
			cache_key := '${offset.topic}:${offset.partition}'
			a.offset_cache[group_id][cache_key] = offset.offset
		}
		a.offset_lock.unlock()
	}

	// Return error only if all commits failed
	if failed.len == offsets.len {
		return error('all offset commits failed: ${failed.join(', ')}')
	}

	// Partial success is acceptable
	if failed.len > 0 {
		eprintln('[WARN] Partial offset commit success: ${succeeded.len} succeeded, ${failed.len} failed')
	}
}

pub fn (mut a S3StorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut results := []domain.OffsetFetchResult{}

	for part in partitions {
		key := a.offset_key(group_id, part.topic, part.partition)

		// Try cache first
		a.offset_lock.rlock()
		cache_key := '${part.topic}:${part.partition}'
		cached_offset := if group_id in a.offset_cache {
			a.offset_cache[group_id][cache_key] or { i64(-1) }
		} else {
			i64(-1)
		}
		a.offset_lock.runlock()

		if cached_offset >= 0 {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     cached_offset
				metadata:   ''
				error_code: 0
			}
			continue
		}

		// Fetch from S3
		data, _ := a.get_object(key, -1, -1) or {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
			continue
		}

		offset_data := json.decode(domain.PartitionOffset, data.bytestr()) or {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
			continue
		}

		results << domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset_data.offset
			metadata:   offset_data.metadata
			error_code: 0
		}
	}

	return results
}

// ============================================================
// Health Check
// ============================================================

pub fn (mut a S3StorageAdapter) health_check() !port.HealthStatus {
	// Try to list a small number of objects
	_ := a.list_objects(a.config.prefix) or { return .unhealthy }
	return .healthy
}

// ============================================================
// Multi-Broker Support
// ============================================================

// get_storage_capability returns the storage capability
pub fn (a &S3StorageAdapter) get_storage_capability() domain.StorageCapability {
	return s3_capability
}

// get_cluster_metadata_port returns the cluster metadata interface
// S3 supports multi-broker mode
pub fn (mut a S3StorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	// Return S3-based cluster metadata implementation
	return new_s3_cluster_metadata_adapter(a)
}

// ============================================================
// S3 Key Helpers
// ============================================================

fn (a &S3StorageAdapter) topic_metadata_key(name string) string {
	return '${a.config.prefix}topics/${name}/metadata.json'
}

fn (a &S3StorageAdapter) partition_index_key(topic string, partition int) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/index.json'
}

fn (a &S3StorageAdapter) log_segment_key(topic string, partition int, start i64, end i64) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/log-${start:016}-${end:016}.bin'
}

fn (a &S3StorageAdapter) group_key(group_id string) string {
	return '${a.config.prefix}groups/${group_id}/state.json'
}

fn (a &S3StorageAdapter) offset_key(group_id string, topic string, partition int) string {
	return '${a.config.prefix}offsets/${group_id}/${topic}:${partition}.json'
}

// ============================================================
// S3 Operations (Abstract - to be implemented with actual SDK)
// ============================================================

// Note: S3 HTTP operations (get_object, put_object, delete_object, list_objects, sign_request, etc.)
// have been moved to s3_client.v

// Note: PartitionIndex, LogSegment, get_partition_index have been moved to partition_index.v

// Note: StoredRecord and TopicPartitionBuffer are defined in s3_record_codec.v and buffer_manager.v

// Note: async_flush_partition, flush_worker, flush_buffer_to_s3 have been moved to buffer_manager.v

// ============================================================
// Worker Management
// ============================================================

// start_workers starts the flush and compaction workers
pub fn (mut a S3StorageAdapter) start_workers() {
	if a.compactor_running {
		return
	}
	a.compactor_running = true
	go a.flush_worker()
	go a.compaction_worker()
}

// Note: compaction_worker, compact_all_partitions, compact_partition, merge_segments
// have been moved to compaction.v

// Note: encode_stored_records and decode_stored_records have been moved to s3_record_codec.v

fn generate_topic_id(name string) []u8 {
	hash := md5.sum(name.bytes())
	return hash[0..16]
}
