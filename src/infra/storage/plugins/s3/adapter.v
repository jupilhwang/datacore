// S3 storage adapter with ETag-based concurrency control
module s3

import domain
import service.port
import time
import json
import crypto.md5
import sync
import net.http
import infra.observability

// NOTE: S3 client functions in s3_client.v; index types in partition_index.v;
// buffer types in buffer_manager.v; compaction in compaction.v

// Configuration Constants

const max_topic_name_length = 255
const max_partition_count = 10000
const topic_cache_ttl = 5 * time.minute
const group_cache_ttl = 30 * time.second
const record_overhead_bytes = 30
const fetch_size_multiplier = 2
const fetch_offset_estimate_divisor = 100
const max_offset_commit_buffer = 100
const max_offset_commit_concurrent = 50

/// s3_capability defines the storage capabilities of the S3 adapter.
pub const s3_capability = domain.StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// S3Config holds the S3 storage configuration.
pub struct S3Config {
pub mut:
	bucket_name    string = 'datacore-storage'
	region         string = 'us-east-1'
	endpoint       string
	access_key     string
	secret_key     string
	prefix         string = 'datacore/'
	max_retries    int    = 3
	retry_delay_ms int    = 100
	use_path_style bool   = true
	timezone       string = 'UTC'
	// Batch settings
	batch_timeout_ms int
	batch_max_bytes  i64
	// Compaction settings
	compaction_interval_ms int
	target_segment_bytes   i64
	index_cache_ttl_ms     int
	// Broker settings
	broker_id i32
	// Offset batch settings
	offset_batch_enabled         bool = true
	offset_flush_interval_ms     int  = 100
	offset_flush_threshold_count int  = 50
}

/// S3StorageAdapter implements the StoragePort for S3 storage.
pub struct S3StorageAdapter {
pub mut:
	config S3Config
	// Local cache with TTL
	topic_cache       map[string]CachedTopic
	topic_id_cache    map[string]string
	group_cache       map[string]CachedGroup
	offset_cache      map[string]map[string]i64
	topic_index_cache map[string]CachedPartitionIndex
	// Locks for thread safety
	topic_lock  sync.RwMutex
	group_lock  sync.RwMutex
	offset_lock sync.RwMutex
	// Flush buffers for batched S3 writes
	topic_partition_buffers map[string]TopicPartitionBuffer
	buffer_lock             sync.Mutex
	index_update_lock       sync.Mutex
	is_flushing             bool
	// Compaction settings
	min_segment_count_to_compact int = 5
	compactor_running            bool
	// Metrics
	metrics      S3Metrics
	metrics_lock sync.Mutex
	// Offset batch buffers
	offset_buffers     map[string]OffsetGroupBuffer
	offset_buffer_lock sync.Mutex
	// Iceberg table writers (when Iceberg is enabled)
	iceberg_writers map[string]&IcebergWriter
	iceberg_lock    sync.RwMutex
}

/// CachedTopic holds cached topic information.
struct CachedTopic {
	meta      domain.TopicMetadata
	etag      string
	cached_at time.Time
}

/// CachedGroup holds cached group information.
struct CachedGroup {
	group     domain.ConsumerGroup
	etag      string
	cached_at time.Time
}

/// CachedSignature holds cached signature information.
struct CachedSignature {
	header    http.Header
	cached_at time.Time
}

/// log_message prints a structured log message.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	mut logger := observability.get_named_logger('s3.${component}')
	match level {
		.trace { logger.debug_map(message, context) }
		.debug { logger.debug_map(message, context) }
		.info { logger.info_map(message, context) }
		.warn { logger.warn_map(message, context) }
		.error { logger.error_map(message, context) }
		.fatal { logger.fatal_map(message, context) }
	}
}

/// S3Metrics tracks metrics for S3 storage operations.
struct S3Metrics {
mut:
	// Flush metrics
	flush_count         i64
	flush_success_count i64
	flush_error_count   i64
	flush_total_ms      i64
	// Compaction metrics
	compaction_count         i64
	compaction_success_count i64
	compaction_error_count   i64
	compaction_total_ms      i64
	compaction_bytes_merged  i64
	// S3 API metrics
	s3_get_count    i64
	s3_put_count    i64
	s3_delete_count i64
	s3_list_count   i64
	s3_error_count  i64
	// Cache metrics
	cache_hit_count  i64
	cache_miss_count i64
	// Offset commit metrics
	offset_commit_count         i64
	offset_commit_success_count i64
	offset_commit_error_count   i64
	// Offset flush metrics
	offset_flush_count         i64
	offset_flush_success_count i64
	offset_flush_error_count   i64
	// Sync append metrics (acks != 0)
	sync_append_count         i64
	sync_append_success_count i64
	sync_append_error_count   i64
	sync_append_total_ms      i64
}

/// reset_metrics resets all metrics to zero.
fn (mut m S3Metrics) reset() {
	m.flush_count = 0
	m.flush_success_count = 0
	m.flush_error_count = 0
	m.flush_total_ms = 0
	m.compaction_count = 0
	m.compaction_success_count = 0
	m.compaction_error_count = 0
	m.compaction_total_ms = 0
	m.compaction_bytes_merged = 0
	m.s3_get_count = 0
	m.s3_put_count = 0
	m.s3_delete_count = 0
	m.s3_list_count = 0
	m.s3_error_count = 0
	m.cache_hit_count = 0
	m.cache_miss_count = 0
	m.offset_commit_count = 0
	m.offset_commit_success_count = 0
	m.offset_commit_error_count = 0
	m.sync_append_count = 0
	m.sync_append_success_count = 0
	m.sync_append_error_count = 0
	m.sync_append_total_ms = 0
}

/// get_summary returns a string summary of the metrics.
fn (m &S3Metrics) get_summary() string {
	flush_success_rate := if m.flush_count > 0 {
		f64(m.flush_success_count) / f64(m.flush_count) * 100.0
	} else {
		0.0
	}
	compaction_success_rate := if m.compaction_count > 0 {
		f64(m.compaction_success_count) / f64(m.compaction_count) * 100.0
	} else {
		0.0
	}
	cache_hit_rate := if m.cache_hit_count + m.cache_miss_count > 0 {
		f64(m.cache_hit_count) / f64(m.cache_hit_count + m.cache_miss_count) * 100.0
	} else {
		0.0
	}
	offset_commit_success_rate := if m.offset_commit_count > 0 {
		f64(m.offset_commit_success_count) / f64(m.offset_commit_count) * 100.0
	} else {
		0.0
	}
	sync_append_success_rate := if m.sync_append_count > 0 {
		f64(m.sync_append_success_count) / f64(m.sync_append_count) * 100.0
	} else {
		0.0
	}

	return '[S3 Metrics]
  Flush: ${m.flush_count} total, ${m.flush_success_count} success (${flush_success_rate:.1f}%), ${m.flush_total_ms}ms
  Compaction: ${m.compaction_count} total, ${m.compaction_success_count} success (${compaction_success_rate:.1f}%), ${m.compaction_bytes_merged} bytes merged, ${m.compaction_total_ms}ms
  S3 API: GET=${m.s3_get_count}, PUT=${m.s3_put_count}, DELETE=${m.s3_delete_count}, LIST=${m.s3_list_count}, errors=${m.s3_error_count}
  Cache: ${m.cache_hit_count} hits, ${m.cache_miss_count} misses (${cache_hit_rate:.1f}% hit rate)
  Offset Commit: ${m.offset_commit_count} total, ${m.offset_commit_success_count} success (${offset_commit_success_rate:.1f}%)
  Sync Append: ${m.sync_append_count} total, ${m.sync_append_success_count} success (${sync_append_success_rate:.1f}%), ${m.sync_append_total_ms}ms'
}

// Note: CachedPartitionIndex is defined in partition_index.v.

// S3 object key structure:
// {prefix}/topics/{topic_name}/metadata.json
// {prefix}/topics/{topic_name}/partitions/{partition}/log-{start_offset}-{end_offset}.bin
// {prefix}/topics/{topic_name}/partitions/{partition}/index.json
// {prefix}/groups/{group_id}/state.json
// {prefix}/offsets/{group_id}/{topic}:{partition}.json

/// new_s3_adapter creates a new S3 storage adapter.
pub fn new_s3_adapter(config S3Config) !&S3StorageAdapter {
	return &S3StorageAdapter{
		config:                  config
		topic_cache:             map[string]CachedTopic{}
		group_cache:             map[string]CachedGroup{}
		offset_cache:            map[string]map[string]i64{}
		topic_index_cache:       map[string]CachedPartitionIndex{}
		topic_partition_buffers: map[string]TopicPartitionBuffer{}
		offset_buffers:          map[string]OffsetGroupBuffer{}
		iceberg_writers:         map[string]&IcebergWriter{}
	}
}

/// create_topic creates a new topic in S3.
pub fn (mut a S3StorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// Input validation
	if name == '' {
		return error('Topic name cannot be empty')
	}
	if name.len > max_topic_name_length {
		return error('Topic name too long: ${name.len} > ${max_topic_name_length}')
	}
	// Check for disallowed characters in topic name
	for ch in name {
		if !ch.is_alnum() && ch != `_` && ch != `-` && ch != `.` {
			return error('Invalid character in topic name: ${ch.ascii_str()}')
		}
	}
	if partitions <= 0 {
		return error('Partition count must be positive: ${partitions}')
	}
	if partitions > max_partition_count {
		return error('Partition count too large: ${partitions} > ${max_partition_count}')
	}

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

	// Save metadata to S3 with conditional write (If-None-Match: *)
	key := a.topic_metadata_key(name)
	data := json.encode(meta)
	a.put_object_if_not_exists(key, data.bytes())!

	// Initialize partition indexes
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

	// Store in topic cache
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

/// delete_topic deletes a topic from S3.
pub fn (mut a S3StorageAdapter) delete_topic(name string) ! {
	// Look up topic metadata to remove from id cache
	if cached := a.topic_cache[name] {
		a.topic_lock.@lock()
		a.topic_id_cache.delete(cached.meta.topic_id.hex())
		a.topic_lock.unlock()
	}

	// Delete all objects starting with topic prefix
	prefix := '${a.config.prefix}topics/${name}/'
	a.delete_objects_with_prefix(prefix)!

	// Remove from cache
	a.topic_lock.@lock()
	a.topic_cache.delete(name)
	a.topic_lock.unlock()
}

/// list_topics retrieves all topics from S3.
pub fn (mut a S3StorageAdapter) list_topics() ![]domain.TopicMetadata {
	prefix := '${a.config.prefix}topics/'
	objects := a.list_objects(prefix)!

	mut topics := []domain.TopicMetadata{}
	mut seen := map[string]bool{}

	// First add all cached topics
	a.topic_lock.rlock()
	for name, cached in a.topic_cache {
		if name !in seen {
			seen[name] = true
			topics << cached.meta
		}
	}
	a.topic_lock.runlock()

	// Add topics from S3 that are not in cache
	for obj in objects {
		// Extract topic name from path in format "datacore/topics/my-topic/metadata.json"
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

/// get_topic retrieves topic metadata from S3.
pub fn (mut a S3StorageAdapter) get_topic(name string) !domain.TopicMetadata {
	// Check cache first
	a.topic_lock.rlock()
	if cached := a.topic_cache[name] {
		if time.since(cached.cached_at) < topic_cache_ttl {
			a.topic_lock.runlock()
			// Cache hit metric
			a.metrics_lock.@lock()
			a.metrics.cache_hit_count++
			a.metrics_lock.unlock()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// Cache miss metric
	a.metrics_lock.@lock()
	a.metrics.cache_miss_count++
	a.metrics.s3_get_count++
	a.metrics_lock.unlock()

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

/// get_topic_by_id retrieves a topic by topic_id.
/// Uses O(1) cache lookup.
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

	// Acquire lock once for all cache updates
	a.topic_lock.@lock()
	for t in topics {
		// Populate topic_id_cache for future lookups
		a.topic_id_cache[t.topic_id.hex()] = t.name
	}
	a.topic_lock.unlock()

	// Find matching topic
	for t in topics {
		if t.topic_id == topic_id {
			return t
		}
	}
	return error('Topic not found')
}

/// add_partitions adds partitions to a topic.
pub fn (mut a S3StorageAdapter) add_partitions(name string, new_count int) ! {
	meta := a.get_topic(name)!
	if new_count <= meta.partition_count {
		return error('New partition count must be greater than current')
	}

	// Initialize new partition indexes
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

	// Update cache directly (instead of invalidating)
	a.topic_lock.@lock()
	if cached := a.topic_cache[name] {
		a.topic_cache[name] = CachedTopic{
			meta:      updated_meta
			etag:      cached.etag
			cached_at: time.now()
		}
	}
	a.topic_lock.unlock()
}

/// append appends records to a partition.
/// Selects sync/async path based on required_acks:
///   acks=0: appends to in-memory buffer and returns immediately (best-effort)
///   acks=1/-1: returns after S3 PUT + index update completes (durability guaranteed)
pub fn (mut a S3StorageAdapter) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	if records.len == 0 {
		return domain.AppendResult{
			base_offset:      0
			log_append_time:  time.now().unix_milli()
			log_start_offset: 0
		}
	}

	// 1. Fetch current partition index
	mut index := a.get_partition_index(topic, partition)!
	base_offset := index.high_watermark
	partition_key := '${topic}:${partition}'

	// 2. Create StoredRecords
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
		bytes_to_add += i64(srec.value.len + srec.key.len + record_overhead_bytes)
	}

	if required_acks == 0 {
		// === acks=0: async path (existing behavior) ===
		// Append to in-memory buffer and return immediately.
		// flush_worker drains the buffer every batch_timeout_ms automatically.
		a.buffer_lock.lock()
		mut tp_buffer := a.topic_partition_buffers[partition_key] or {
			TopicPartitionBuffer{
				records:            []
				current_size_bytes: 0
			}
		}

		tp_buffer.records << stored_records
		tp_buffer.current_size_bytes += bytes_to_add

		a.topic_partition_buffers[partition_key] = tp_buffer
		a.buffer_lock.unlock()
	} else {
		// === acks=1/-1: sync path (durability guarantee) ===
		// Skip in-memory buffer and write directly to S3
		start_time := time.now()
		base_offset_val := stored_records[0].offset
		end_offset := stored_records[stored_records.len - 1].offset
		segment_data := encode_stored_records(stored_records)
		segment_key := a.log_segment_key(topic, partition, base_offset_val, end_offset)

		// S3 PUT (synchronous wait)
		a.put_object(segment_key, segment_data) or {
			a.metrics_lock.@lock()
			a.metrics.sync_append_error_count++
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error('durable append failed (acks=${required_acks}): S3 PUT error: ${err}')
		}

		a.metrics_lock.@lock()
		a.metrics.s3_put_count++
		a.metrics.sync_append_count++
		a.metrics_lock.unlock()

		// Index update (synchronous wait)
		a.index_update_lock.lock()
		a.update_partition_index_with_segment(topic, partition, segment_key, base_offset_val,
			end_offset, segment_data.len) or {
			a.index_update_lock.unlock()
			a.metrics_lock.@lock()
			a.metrics.sync_append_error_count++
			a.metrics_lock.unlock()
			return error('durable append failed (acks=${required_acks}): index update error: ${err}')
		}
		a.index_update_lock.unlock()

		elapsed_ms := time.since(start_time).milliseconds()
		a.metrics_lock.@lock()
		a.metrics.sync_append_success_count++
		a.metrics.sync_append_total_ms += elapsed_ms
		a.metrics_lock.unlock()
	}

	// Append records to Iceberg table (when Iceberg is enabled)
	if a.is_iceberg_enabled() {
		a.append_to_iceberg(topic, partition, records, base_offset) or {
			log_message(.warn, 'IcebergAppend', 'Failed to append to Iceberg', {
				'topic':     topic
				'partition': partition.str()
				'error':     err.str()
			})
		}
	}

	// Update in-memory high_watermark so the next append gets a correct offset.
	// Lock order: index_update_lock -> topic_lock
	// All code paths MUST acquire locks in this order to prevent deadlocks.
	new_high_watermark := base_offset + i64(records.len)
	a.index_update_lock.@lock()
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
	a.index_update_lock.unlock()

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  time.now().unix_milli()
		log_start_offset: index.earliest_offset
	}
}

/// fetch retrieves records from a partition.
pub fn (mut a S3StorageAdapter) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	partition_key := '${topic}:${partition}'

	// Fetch S3 index for segment information
	index := a.get_partition_index(topic, partition)!

	if offset < index.earliest_offset {
		return error('Offset out of range (too old): ${offset} < ${index.earliest_offset}')
	}

	// Find relevant segments
	mut all_records := []domain.Record{}
	mut bytes_read := 0
	mut highest_offset_read := offset - 1

	// 1. Try reading from S3 segments first
	for seg in index.log_segments {
		if seg.end_offset < offset {
			continue
		}
		if seg.start_offset > offset + i64(max_bytes / fetch_offset_estimate_divisor) {
			break
		}

		// Fetch segment from S3
		// Optimization: use Range Request when reading from start of segment
		mut data := []u8{}
		if offset == seg.start_offset && max_bytes > 0 {
			mut fetch_size := i64(max_bytes) * fetch_size_multiplier
			if fetch_size > seg.size_bytes {
				fetch_size = -1
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

	// 2. Also read from in-memory buffer (data not yet flushed to S3)
	// Important for data not yet persisted
	if bytes_read < max_bytes {
		a.buffer_lock.lock()
		if tp_buffer := a.topic_partition_buffers[partition_key] {
			for rec in tp_buffer.records {
				// Read records at or above requested offset not already read from S3 segments
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

	// Offset of the first record actually returned
	actual_first_offset := if all_records.len > 0 { offset } else { index.high_watermark }

	return domain.FetchResult{
		records:            all_records
		first_offset:       actual_first_offset
		high_watermark:     index.high_watermark
		last_stable_offset: index.high_watermark
		log_start_offset:   index.earliest_offset
	}
}

/// delete_records deletes records before the specified offset.
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

/// get_partition_info retrieves partition information.
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

/// save_group saves a consumer group.
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

/// load_group loads a consumer group.
pub fn (mut a S3StorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	// Check cache
	a.group_lock.rlock()
	if cached := a.group_cache[group_id] {
		if time.since(cached.cached_at) < group_cache_ttl {
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

/// delete_group deletes a consumer group.
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

/// list_groups returns all consumer groups.
pub fn (mut a S3StorageAdapter) list_groups() ![]domain.GroupInfo {
	prefix := '${a.config.prefix}groups/'
	objects := a.list_objects(prefix)!

	mut groups := []domain.GroupInfo{}
	mut group_ids := []string{}
	mut seen := map[string]bool{}

	// First pass: collect unique group IDs
	for obj in objects {
		if obj.key.ends_with('/state.json') {
			parts := obj.key.split('/')
			if parts.len >= 2 {
				group_id := parts[parts.len - 2]
				if group_id !in seen {
					seen[group_id] = true
					group_ids << group_id
				}
			}
		}
	}

	// Batch load groups (parallel lookup for better performance)
	// Lower threshold to 1 to enable parallel processing in almost all cases
	if group_ids.len == 0 {
		return groups
	} else if group_ids.len == 1 {
		// Single group: sequential load
		group_id := group_ids[0]
		if group := a.load_group(group_id) {
			groups << domain.GroupInfo{
				group_id:      group_id
				protocol_type: group.protocol_type
				state:         group.state.str()
			}
		}
	} else {
		// Large batch: parallel load using channels
		// Channels don't support optional types, so use domain.GroupInfo directly
		ch := chan domain.GroupInfo{cap: group_ids.len}

		for group_id in group_ids {
			spawn fn [mut a, group_id, ch] () {
				if group := a.load_group(group_id) {
					ch <- domain.GroupInfo{
						group_id:      group.group_id
						protocol_type: group.protocol_type
						state:         group.state.str()
					}
				} else {
					// Send empty GroupInfo for failed loads (filtered out)
					ch <- domain.GroupInfo{
						group_id: ''
					}
				}
			}()
		}

		// Collect results
		for _ in 0 .. group_ids.len {
			info := <-ch
			if info.group_id.len > 0 {
				groups << info
			}
		}
	}

	return groups
}

/// commit_offsets commits offsets.
/// Optimizes performance through parallel processing.
pub fn (mut a S3StorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	if offsets.len == 0 {
		return
	}

	a.record_commit_start_metrics(offsets.len)
	mut failed := []string{}
	mut succeeded := []domain.PartitionOffset{}

	ch := a.create_commit_channel(offsets.len)
	mut active := 0

	for offset in offsets {
		active = a.wait_for_slot(ch, active, mut succeeded, mut failed, group_id)
		active++
		spawn a.commit_single_offset(group_id, offset, ch)
	}

	a.collect_remaining_results(ch, active, mut succeeded, mut failed, group_id)
	a.update_offset_cache_and_metrics(group_id, succeeded, failed)
	a.handle_commit_result(offsets.len, failed, group_id) or {
		// Partial failures are allowed; only log the error
		log_message(.warn, 'OffsetCommit', 'Some offsets failed to commit', {
			'group_id': group_id
			'failed':   failed.len.str()
		})
	}
}

/// CommitResult holds the result of an offset commit.
struct CommitResult {
	offset  domain.PartitionOffset
	success bool
	error   string
}

/// record_commit_start_metrics records commit start metrics.
fn (mut a S3StorageAdapter) record_commit_start_metrics(count int) {
	a.metrics_lock.@lock()
	a.metrics.offset_commit_count += i64(count)
	a.metrics_lock.unlock()
}

/// create_commit_channel creates a commit result channel.
fn (a &S3StorageAdapter) create_commit_channel(offset_count int) chan CommitResult {
	cap_size := if offset_count > max_offset_commit_buffer {
		max_offset_commit_buffer
	} else {
		offset_count
	}
	return chan CommitResult{cap: cap_size}
}

/// wait_for_slot waits for an available slot.
fn (mut a S3StorageAdapter) wait_for_slot(ch chan CommitResult, active int, mut succeeded []domain.PartitionOffset, mut failed []string, group_id string) int {
	mut current_active := active
	for current_active >= max_offset_commit_concurrent {
		result := <-ch
		current_active--
		if result.success {
			succeeded << result.offset
		} else {
			failed << '${result.offset.topic}:${result.offset.partition}'
			a.log_commit_error(result, group_id)
		}
	}
	return current_active
}

/// log_commit_error logs a commit error.
fn (a &S3StorageAdapter) log_commit_error(result CommitResult, group_id string) {
	log_message(.error, 'OffsetCommit', 'Offset commit failed', {
		'group_id':  group_id
		'topic':     result.offset.topic
		'partition': result.offset.partition.str()
		'error':     result.error
	})
}

/// commit_single_offset commits a single offset.
fn (mut a S3StorageAdapter) commit_single_offset(group_id string, offset domain.PartitionOffset, ch chan CommitResult) {
	key := a.offset_key(group_id, offset.topic, offset.partition)
	data := json.encode(offset)

	a.put_object(key, data.bytes()) or {
		error_msg := 'S3 put failed for offset ${offset.topic}:${offset.partition}: ${err}'
		a.log_single_commit_error(group_id, offset, error_msg)
		ch <- CommitResult{
			offset:  offset
			success: false
			error:   error_msg
		}
		return
	}

	ch <- CommitResult{
		offset:  offset
		success: true
		error:   ''
	}
}

/// log_single_commit_error logs a single commit error.
fn (a &S3StorageAdapter) log_single_commit_error(group_id string, offset domain.PartitionOffset, error_msg string) {
	log_message(.error, 'OffsetCommit', error_msg, {
		'group_id':  group_id
		'topic':     offset.topic
		'partition': offset.partition.str()
		'offset':    offset.offset.str()
	})
}

/// collect_remaining_results collects remaining results.
fn (mut a S3StorageAdapter) collect_remaining_results(ch chan CommitResult, active int, mut succeeded []domain.PartitionOffset, mut failed []string, group_id string) {
	for _ in 0 .. active {
		result := <-ch
		if result.success {
			succeeded << result.offset
		} else {
			failed << '${result.offset.topic}:${result.offset.partition}'
			a.log_commit_error(result, group_id)
		}
	}
}

/// update_offset_cache_and_metrics updates cache and metrics.
fn (mut a S3StorageAdapter) update_offset_cache_and_metrics(group_id string, succeeded []domain.PartitionOffset, failed []string) {
	if succeeded.len > 0 {
		a.update_offset_cache(group_id, succeeded)
		a.record_commit_success_metrics(succeeded.len)
	}

	if failed.len > 0 {
		a.record_commit_failure_metrics(failed.len)
	}
}

/// update_offset_cache updates the offset cache.
fn (mut a S3StorageAdapter) update_offset_cache(group_id string, succeeded []domain.PartitionOffset) {
	a.offset_lock.@lock()
	if group_id !in a.offset_cache {
		a.offset_cache[group_id] = map[string]i64{}
	}
	for offset in succeeded {
		cache_key := '${offset.topic}:${offset.partition}'
		a.offset_cache[group_id][cache_key] = offset.offset
	}
	a.offset_lock.unlock()

	log_message(.info, 'OffsetCommit', 'Successfully committed offsets', {
		'group_id': group_id
		'count':    succeeded.len.str()
	})
}

/// record_commit_success_metrics records success metrics.
fn (mut a S3StorageAdapter) record_commit_success_metrics(count int) {
	a.metrics_lock.@lock()
	a.metrics.offset_commit_success_count += i64(count)
	a.metrics.s3_put_count += i64(count)
	a.metrics_lock.unlock()
}

/// record_commit_failure_metrics records failure metrics.
fn (mut a S3StorageAdapter) record_commit_failure_metrics(count int) {
	a.metrics_lock.@lock()
	a.metrics.offset_commit_error_count += i64(count)
	a.metrics.s3_error_count += i64(count)
	a.metrics_lock.unlock()
}

/// handle_commit_result handles the commit result.
fn (a &S3StorageAdapter) handle_commit_result(total_count int, failed []string, group_id string) ! {
	if failed.len == total_count {
		return error('All ${total_count} offset commits failed for group ${group_id}')
	}

	if failed.len > 0 {
		log_message(.warn, 'OffsetCommit', 'Partial offset commit', {
			'group_id': group_id
			'failed':   failed.len.str()
		})
	}
}

/// fetch_offsets retrieves committed offsets.
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

/// health_check checks the storage health status.
pub fn (mut a S3StorageAdapter) health_check() !port.HealthStatus {
	// Attempt to list a small number of objects
	_ := a.list_objects(a.config.prefix) or { return .unhealthy }
	return .healthy
}

/// get_storage_capability returns storage capability information.
pub fn (a &S3StorageAdapter) get_storage_capability() domain.StorageCapability {
	return s3_capability
}

/// get_cluster_metadata_port returns the cluster metadata interface.
/// S3 supports multi-broker mode.
pub fn (mut a S3StorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	// Pass original adapter directly to use config copy + original pointer
	return new_s3_cluster_metadata_adapter(&a)
}

/// topic_metadata_key returns the S3 key for topic metadata.
fn (a &S3StorageAdapter) topic_metadata_key(name string) string {
	return '${a.config.prefix}topics/${name}/metadata.json'
}

/// partition_index_key returns the S3 key for a partition index.
fn (a &S3StorageAdapter) partition_index_key(topic string, partition int) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/index.json'
}

/// log_segment_key returns the S3 key for a log segment.
fn (a &S3StorageAdapter) log_segment_key(topic string, partition int, start i64, end i64) string {
	return '${a.config.prefix}topics/${topic}/partitions/${partition}/log-${start:016}-${end:016}.bin'
}

/// group_key returns the S3 key for a consumer group.
fn (a &S3StorageAdapter) group_key(group_id string) string {
	return '${a.config.prefix}groups/${group_id}/state.json'
}

/// offset_key returns the S3 key for an offset.
fn (a &S3StorageAdapter) offset_key(group_id string, topic string, partition int) string {
	return '${a.config.prefix}offsets/${group_id}/${topic}:${partition}.json'
}

// Moved to s3_client.v.

// Note: PartitionIndex, LogSegment, get_partition_index have been moved to partition_index.v.

// Note: StoredRecord and TopicPartitionBuffer are defined in s3_record_codec.v and buffer_manager.v.

// Note: async_flush_partition, flush_worker, flush_buffer_to_s3 have been moved to buffer_manager.v.

/// start_workers starts flush and compaction workers.
pub fn (mut a S3StorageAdapter) start_workers() {
	if a.compactor_running {
		return
	}
	a.compactor_running = true
	go a.flush_worker()
	go a.compaction_worker()
}

/// get_metrics returns the current metrics snapshot.
pub fn (mut a S3StorageAdapter) get_metrics() S3Metrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary returns the metrics summary string.
pub fn (mut a S3StorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics resets all metrics to zero.
pub fn (mut a S3StorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}

// Note: compaction_worker, compact_all_partitions, compact_partition, merge_segments have been moved to compaction.v.

// Note: encode_stored_records and decode_stored_records have been moved to s3_record_codec.v.

/// generate_topic_id generates a topic_id from the topic name.
fn generate_topic_id(name string) []u8 {
	hash := md5.sum(name.bytes())
	return hash[0..16]
}
