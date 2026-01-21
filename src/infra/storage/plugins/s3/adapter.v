// S3 Storage Plugin for DataCore
// Implements StoragePort interface with S3 backend
// Uses conditional writes (ETag) for concurrency control
module s3

import domain
import service.port
import time
import json
import crypto.md5
import crypto.sha256
import crypto.hmac
import net.http
import sync
import strconv

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

struct CachedPartitionIndex {
	index     PartitionIndex
	etag      string
	cached_at time.Time
}

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
	a.topic_lock.unlock()

	return meta
}

// delete_topic deletes a topic from S3
pub fn (mut a S3StorageAdapter) delete_topic(name string) ! {
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

// get_topic_by_id retrieves topic by ID
pub fn (mut a S3StorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	topics := a.list_topics()!
	for t in topics {
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
				if rec.offset >= offset && rec.offset > highest_offset_read && bytes_read < max_bytes {
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
	for offset in offsets {
		key := a.offset_key(group_id, offset.topic, offset.partition)
		data := json.encode(offset)
		a.put_object(key, data.bytes())!
	}

	// Update local cache
	a.offset_lock.@lock()
	if group_id !in a.offset_cache {
		a.offset_cache[group_id] = map[string]i64{}
	}
	for offset in offsets {
		cache_key := '${offset.topic}:${offset.partition}'
		a.offset_cache[group_id][cache_key] = offset.offset
	}
	a.offset_lock.unlock()
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

struct S3Object {
	key  string
	size i64
	etag string
}

// Partition index stored in S3
struct PartitionIndex {
mut:
	topic           string
	partition       int
	earliest_offset i64
	high_watermark  i64
	log_segments    []LogSegment
}

struct LogSegment {
	start_offset i64
	end_offset   i64
	key          string
	size_bytes   i64
	created_at   time.Time
}

fn (mut a S3StorageAdapter) get_partition_index(topic string, partition int) !PartitionIndex {
	key := '${topic}:${partition}'
	// 1. Check cache
	a.topic_lock.rlock()
	cached_exists := key in a.topic_index_cache
	mut cached_index := PartitionIndex{}
	mut cached_etag := ''
	if cached_exists {
		cached := a.topic_index_cache[key]
		cached_index = cached.index
		cached_etag = cached.etag
		if time.since(cached.cached_at).milliseconds() < a.config.index_cache_ttl_ms {
			a.topic_lock.runlock()
			return cached.index
		}
	}
	a.topic_lock.runlock()

	// 2. Fetch from S3
	index_key := a.partition_index_key(topic, partition)
	data, etag := a.get_object(index_key, -1, -1) or {
		// Index not found in S3
		// If we have a cached version (even if stale), prefer it over creating a new empty one
		if cached_exists {
			// Refresh cache timestamp but keep the data
			a.topic_lock.@lock()
			a.topic_index_cache[key] = CachedPartitionIndex{
				index:     cached_index
				etag:      cached_etag
				cached_at: time.now()
			}
			a.topic_lock.unlock()
			return cached_index
		}
		// No cache exists, create new empty index
		a.topic_lock.@lock()
		a.topic_index_cache[key] = CachedPartitionIndex{
			index:     PartitionIndex{
				topic:           topic
				partition:       partition
				earliest_offset: 0
				high_watermark:  0
				log_segments:    []
			}
			etag:      ''
			cached_at: time.now()
		}
		a.topic_lock.unlock()
		return a.topic_index_cache[key].index // Return the newly cached empty index
	}

	// 3. Decode S3 index
	s3_index := json.decode(PartitionIndex, data.bytestr())!

	// 4. Merge with cached index - keep higher high_watermark
	// This handles the case where append updated the cache but flush hasn't written to S3 yet
	mut final_index := s3_index
	if cached_exists && cached_index.high_watermark > s3_index.high_watermark {
		final_index.high_watermark = cached_index.high_watermark
	}

	a.topic_lock.@lock()
	a.topic_index_cache[key] = CachedPartitionIndex{
		index:     final_index
		etag:      etag
		cached_at: time.now()
	}
	a.topic_lock.unlock()

	return final_index
}

// S3 HTTP operations using signature v4
fn (mut a S3StorageAdapter) get_object(key string, start i64, end i64) !([]u8, string) {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	// Prepare headers
	mut headers := a.sign_request('GET', key, '', []u8{})

	// Add Range header if start is non-negative
	if start >= 0 {
		range_val := if end > start { 'bytes=${start}-${end}' } else { 'bytes=${start}-' }
		headers.add_custom('Range', range_val) or {}
		// Note: AWS SigV4 might require Range header to be signed if added.
		// Our sign_request signs "SignedHeaders". If we add Range AFTER signing, it might fail if we included it in signed headers list.
		// But our sign_request currently only signs host, x-amz-content-sha256, x-amz-date.
		// So adding Range afterwards is fine unless we change sign_request to sign all headers.
	}

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .get
		header: headers
	}) or { return error('S3 GET failed: ${err}') }

	if resp.status_code == 404 {
		return error('Object not found: ${key}')
	}
	// 206 Partial Content is success for Range requests
	if resp.status_code != 200 && resp.status_code != 206 {
		return error('S3 GET failed with status ${resp.status_code}')
	}

	etag := resp.header.get(.etag) or { '' }
	return resp.body.bytes(), etag
}

fn (mut a S3StorageAdapter) put_object(key string, data []u8) ! {
	a.put_object_with_retry(key, data, 3)!
}

// put_object_with_retry attempts to put an object with exponential backoff retry
fn (mut a S3StorageAdapter) put_object_with_retry(key string, data []u8, max_retries int) ! {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut last_err := ''
	for attempt in 0 .. max_retries {
		headers := a.sign_request('PUT', key, '', data)

		resp := http.fetch(http.FetchConfig{
			url:    url
			method: .put
			header: headers
			data:   data.bytestr()
		}) or {
			last_err = 'S3 PUT failed: ${err}'
			if attempt < max_retries - 1 {
				// Exponential backoff: 100ms, 200ms, 400ms...
				time.sleep(time.Duration(100 * (1 << attempt)) * time.millisecond)
				continue
			}
			return error(last_err)
		}

		if resp.status_code in [200, 201, 204] {
			return // Success
		}

		// Retry on 503 (Service Unavailable / Throttling) and 500 (Server Error)
		if resp.status_code in [500, 503] && attempt < max_retries - 1 {
			last_err = 'S3 PUT failed with status ${resp.status_code}'
			// Exponential backoff with jitter
			backoff_ms := 100 * (1 << attempt) + int(time.now().unix_milli() % 50)
			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		return error('S3 PUT failed with status ${resp.status_code}')
	}
	return error(last_err)
}

fn (mut a S3StorageAdapter) put_object_if_not_exists(key string, data []u8) ! {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut headers := a.sign_request('PUT', key, '', data)
	headers.add_custom('If-None-Match', '*') or {}

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .put
		header: headers
		data:   data.bytestr()
	}) or { return error('S3 PUT failed: ${err}') }

	if resp.status_code == 412 {
		return error('Object already exists (precondition failed)')
	}
	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

fn (mut a S3StorageAdapter) delete_object(key string) ! {
	endpoint := a.get_endpoint()
	url := '${endpoint}/${a.config.bucket_name}/${key}'

	headers := a.sign_request('DELETE', key, '', []u8{})

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .delete
		header: headers
	}) or { return error('S3 DELETE failed: ${err}') }

	if resp.status_code !in [200, 204] {
		return error('S3 DELETE failed with status ${resp.status_code}')
	}
}

fn (mut a S3StorageAdapter) delete_objects_with_prefix(prefix string) ! {
	objects := a.list_objects(prefix)!
	for obj in objects {
		a.delete_object(obj.key) or {}
	}
}

fn (mut a S3StorageAdapter) list_objects(prefix string) ![]S3Object {
	endpoint := a.get_endpoint()
	query := 'prefix=${prefix}&list-type=2'
	url := '${endpoint}/${a.config.bucket_name}?${query}'

	headers := a.sign_request('GET', '', query, []u8{})

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .get
		header: headers
	}) or { return error('S3 LIST failed: ${err}') }

	if resp.status_code != 200 {
		return error('S3 LIST failed with status ${resp.status_code}')
	}

	// Parse XML response (simplified)
	return parse_list_objects_response(resp.body)
}

fn (a &S3StorageAdapter) get_endpoint() string {
	if a.config.endpoint.len > 0 {
		return a.config.endpoint
	}
	if a.config.use_path_style {
		return 'https://s3.${a.config.region}.amazonaws.com'
	} else {
		return 'https://${a.config.bucket_name}.s3.${a.config.region}.amazonaws.com'
	}
}

fn (a &S3StorageAdapter) canonicalize_query(query string) string {
	if query == '' {
		return ''
	}

	// Parse query string into map
	mut params := map[string]string{}
	for pair in query.split('&') {
		parts := pair.split_nth('=', 2)
		if parts.len == 2 {
			// URL decode key and value, then re-encode properly for AWS SigV4
			key := url_decode(parts[0])
			value := url_decode(parts[1])
			params[key] = value
		}
	}

	// Sort keys alphabetically
	mut keys := []string{}
	for k in params.keys() {
		keys << k
	}
	keys.sort()

	// Build canonical query string
	mut result := []string{}
	for key in keys {
		// AWS SigV4 requires specific encoding
		encoded_key := url_encode_for_sigv4(key)
		encoded_value := url_encode_for_sigv4(params[key])
		result << '${encoded_key}=${encoded_value}'
	}

	return result.join('&')
}

fn url_decode(s string) string {
	mut result := s
	mut i := 0
	for i < result.len {
		if result[i] == u8(`%`) && i + 2 < result.len {
			hex_str := result[i + 1..i + 3]
			if is_hex_char(hex_str[0]) && is_hex_char(hex_str[1]) {
				c := hex_to_u8(hex_str)
				result = result[0..i] + c.ascii_str() + result[i + 3..]
			} else {
				i++
			}
		} else {
			i++
		}
	}
	return result
}

fn is_hex_char(c u8) bool {
	return (c >= `0` && c <= `9`) || (c >= `A` && c <= `F`) || (c >= `a` && c <= `f`)
}

fn hex_to_u8(s string) u8 {
	mut result := u8(0)
	for c in s {
		result <<= 4
		if c >= `0` && c <= `9` {
			result += c - `0`
		} else if c >= `A` && c <= `F` {
			result += c - `A` + 10
		} else if c >= `a` && c <= `f` {
			result += c - `a` + 10
		}
	}
	return result
}

fn u8_to_hex(c u8) string {
	high := (c >> 4) & 0x0F
	low := c & 0x0F
	mut high_hex := '0'
	mut low_hex := '0'

	if high < 10 {
		high_hex = (u8(`0`) + high).ascii_str()
	} else {
		high_hex = (u8(`A`) + high - 10).ascii_str()
	}

	if low < 10 {
		low_hex = (u8(`0`) + low).ascii_str()
	} else {
		low_hex = (u8(`A`) + low - 10).ascii_str()
	}

	return high_hex + low_hex
}

fn url_encode_for_sigv4(s string) string {
	mut result := []u8{}
	for c in s {
		match c {
			`A`...`Z`, `a`...`z`, `0`...`9`, `-`, `.`, `_`, `~` {
				result << c
			}
			else {
				result << u8(`%`)
				hex := u8_to_hex(c)
				result << hex[0]
				result << hex[1]
			}
		}
	}
	return result.bytestr()
}

fn (a &S3StorageAdapter) sign_request(method string, key string, query string, body []u8) http.Header {
	mut h := http.Header{}
	now := time.now().as_utc()

	// Manual formatting to ensure UTC time
	date_day := now.custom_format('YYYYMMDD')
	hours := now.hour
	minutes := now.minute
	seconds := now.second
	date_str := '${date_day}T${hours:02}${minutes:02}${seconds:02}Z'

	h.add_custom('x-amz-date', date_str) or {}
	host := a.get_host()
	h.add_custom('Host', host) or {}

	payload_hash := sha256.sum(body).hex()
	h.add_custom('x-amz-content-sha256', payload_hash) or {}

	if body.len > 0 {
		h.add_custom('Content-Length', body.len.str()) or {}
	}

	if a.config.access_key.len == 0 || a.config.secret_key.len == 0 {
		return h
	}

	// Canonical Request
	canonical_uri := if key == '' {
		'/${a.config.bucket_name}'
	} else if key.starts_with('/') {
		'/${a.config.bucket_name}${key}'
	} else {
		'/${a.config.bucket_name}/${key}'
	}
	canonical_querystring := a.canonicalize_query(query)

	canonical_headers := 'host:${host}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${date_str}\n'
	signed_headers := 'host;x-amz-content-sha256;x-amz-date'

	canonical_request := '${method}\n${canonical_uri}\n${canonical_querystring}\n${canonical_headers}\n${signed_headers}\n${payload_hash}'

	// String to Sign
	algorithm := 'AWS4-HMAC-SHA256'
	credential_scope := '${date_day}/${a.config.region}/s3/aws4_request'
	canonical_request_hash := sha256.sum(canonical_request.bytes()).hex()

	string_to_sign := '${algorithm}\n${date_str}\n${credential_scope}\n${canonical_request_hash}'

	// Signing Key
	k_date := hmac.new(('AWS4' + a.config.secret_key).bytes(), date_day.bytes(), sha256.sum,
		64)
	k_region := hmac.new(k_date, a.config.region.bytes(), sha256.sum, 64)
	k_service := hmac.new(k_region, 's3'.bytes(), sha256.sum, 64)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, 64)

	// Signature
	signature := hmac.new(k_signing, string_to_sign.bytes(), sha256.sum, 64).hex()

	auth_header := '${algorithm} Credential=${a.config.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
	h.add_custom('Authorization', auth_header) or {}

	return h
}

fn (a &S3StorageAdapter) get_host() string {
	if a.config.endpoint.len > 0 {
		return a.config.endpoint.replace('http://', '').replace('https://', '').split('/')[0]
	}
	return 's3.${a.config.region}.amazonaws.com'
}

// ============================================================
// Internal Record Storage Type
// ============================================================

// StoredRecord is the internal representation with offset for storage
struct StoredRecord {
	offset    i64
	timestamp time.Time
	key       []u8
	value     []u8
	headers   map[string][]u8
}

// TopicPartitionBuffer holds records for a specific partition before flushing to S3
struct TopicPartitionBuffer {
mut:
	records            []StoredRecord
	current_size_bytes i64 // Current total size of all records in this buffer
}

// async_flush_partition performs the S3 put and index update for a single partition batch.
fn (mut a S3StorageAdapter) async_flush_partition(partition_key string) ! {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// 1. Acquire lock, copy buffer, and clear buffer in memory
	a.buffer_lock.lock()

	mut tp_buffer := a.topic_partition_buffers[partition_key] or {
		a.buffer_lock.unlock()
		return
	}

	if tp_buffer.records.len == 0 {
		a.buffer_lock.unlock()
		return
	}

	buffer_data := tp_buffer.records.clone()
	tp_buffer.records.clear()
	tp_buffer.current_size_bytes = 0
	a.topic_partition_buffers[partition_key] = tp_buffer
	a.buffer_lock.unlock()

	// 2. Calculate offsets for batch
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// 3. Encode and Write segment to S3
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// Write segment to S3
	a.put_object(segment_key, segment_data) or {
		// Failure means data loss if not retried. For now, log and return error.
		eprintln('[S3] ASYNC FLUSH FAILED for ${partition_key}: Segment put failed: ${err}')
		return error('Segment put failed during async flush: ${err}')
	}

	// 4. Update partition index with new segment (MUST be atomic/safe from concurrent updates)

	// Get the current index from S3 directly (bypass cache) to ensure we have the latest persisted state
	index_key := a.partition_index_key(topic, partition)
	mut index := PartitionIndex{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		high_watermark:  0
		log_segments:    []
	}

	if data, _ := a.get_object(index_key, -1, -1) {
		if decoded := json.decode(PartitionIndex, data.bytestr()) {
			index = decoded
		}
	}

	// Update index with new segment
	// The segment is added if it extends beyond the current S3 high_watermark
	// This is based on S3 persisted state, not in-memory cache
	if base_offset >= index.high_watermark {
		// High watermark is calculated based on the data that has been successfully stored to S3.
		index.high_watermark = end_offset + 1 // New high watermark
		index.log_segments << LogSegment{
			start_offset: base_offset
			end_offset:   end_offset
			key:          segment_key
			size_bytes:   i64(segment_data.len)
			created_at:   time.now()
		}

		// Write updated index to S3
		a.put_object(index_key, json.encode(index).bytes()) or {
			eprintln('[S3] ASYNC FLUSH FAILED for ${partition_key}: Index put failed: ${err}')
			return error('Index put failed during async flush: ${err}')
		}

		// Update local cache with new index (preserving the higher high_watermark from in-memory)
		cache_key := '${topic}:${partition}'
		a.topic_lock.@lock()
		if cached := a.topic_index_cache[cache_key] {
			// Keep the higher high_watermark between cache and S3
			mut final_index := index
			if cached.index.high_watermark > index.high_watermark {
				final_index.high_watermark = cached.index.high_watermark
			}
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     final_index
				cached_at: time.now()
			}
		} else {
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     index
				cached_at: time.now()
			}
		}
		a.topic_lock.unlock()
	} else {
		// This segment overlaps with already stored data, skip updating index
		eprintln('[S3] ASYNC FLUSH WARNING: Segment base_offset ${base_offset} < high_watermark ${index.high_watermark}. Index not updated.')
	}
}

// flush_worker periodically flushes all buffers that have accumulated data.
// Uses sequential processing per partition to avoid index conflicts.
fn (mut a S3StorageAdapter) flush_worker() {
	for a.compactor_running { // Use compactor_running flag to stop both workers
		time.sleep(a.config.batch_timeout_ms)

		// Process each partition's buffer while holding the lock
		// This prevents race conditions where append modifies the buffer
		// between collecting keys and flushing
		a.buffer_lock.lock()

		// Collect all partition keys that have data
		mut keys_to_flush := []string{}
		for key, tp_buffer in a.topic_partition_buffers {
			if tp_buffer.records.len > 0 {
				keys_to_flush << key
			}
		}

		// For each key, extract buffer data while still holding the lock
		mut flush_batches := map[string][]StoredRecord{}
		for key in keys_to_flush {
			if mut tp_buffer := a.topic_partition_buffers[key] {
				if tp_buffer.records.len > 0 {
					// Clone and clear the buffer atomically
					flush_batches[key] = tp_buffer.records.clone()
					tp_buffer.records.clear()
					tp_buffer.current_size_bytes = 0
					a.topic_partition_buffers[key] = tp_buffer
				}
			}
		}

		a.buffer_lock.unlock()

		// Now flush each batch to S3 (without holding the lock)
		for key, buffer_data in flush_batches {
			if buffer_data.len == 0 {
				continue
			}
			a.flush_buffer_to_s3(key, buffer_data) or {
				eprintln('[S3] Flush failed for ${key}: ${err}')
				// On failure, restore the buffer data to prevent data loss
				a.buffer_lock.lock()
				if mut tp_buffer := a.topic_partition_buffers[key] {
					// Prepend the failed data to the existing buffer
					mut restored := buffer_data.clone()
					restored << tp_buffer.records
					tp_buffer.records = restored
					// Recalculate size
					mut size := i64(0)
					for rec in tp_buffer.records {
						size += i64(rec.value.len + rec.key.len + 30)
					}
					tp_buffer.current_size_bytes = size
					a.topic_partition_buffers[key] = tp_buffer
				} else {
					// Buffer was deleted, recreate it
					mut size := i64(0)
					for rec in buffer_data {
						size += i64(rec.value.len + rec.key.len + 30)
					}
					a.topic_partition_buffers[key] = TopicPartitionBuffer{
						records:            buffer_data.clone()
						current_size_bytes: size
					}
				}
				a.buffer_lock.unlock()
				eprintln('[S3] Buffer restored for ${key} with ${buffer_data.len} records')
			}
		}
	}
}

// flush_buffer_to_s3 flushes a specific buffer batch to S3
fn (mut a S3StorageAdapter) flush_buffer_to_s3(partition_key string, buffer_data []StoredRecord) ! {
	parts := partition_key.split(':')
	if parts.len != 2 {
		return error('Invalid partition key for flush: ${partition_key}')
	}
	topic := parts[0]
	partition_i64 := strconv.atoi64(parts[1]) or {
		return error('Invalid partition number in key: ${parts[1]}')
	}
	partition := int(partition_i64)

	// Calculate offsets for batch
	base_offset := buffer_data[0].offset
	end_offset := buffer_data[buffer_data.len - 1].offset

	// Encode and Write segment to S3
	segment_data := encode_stored_records(buffer_data)
	segment_key := a.log_segment_key(topic, partition, base_offset, end_offset)

	// Write segment to S3
	a.put_object(segment_key, segment_data) or {
		eprintln('[S3] FLUSH FAILED for ${partition_key}: Segment put failed: ${err}')
		return error('Segment put failed during flush: ${err}')
	}

	// Update partition index with new segment
	// Use lock to prevent concurrent index updates from corrupting the index
	a.index_update_lock.lock()
	defer {
		a.index_update_lock.unlock()
	}

	// Get the current index from S3 directly (bypass cache)
	index_key := a.partition_index_key(topic, partition)
	mut index := PartitionIndex{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		high_watermark:  0
		log_segments:    []
	}

	if data, _ := a.get_object(index_key, -1, -1) {
		if decoded := json.decode(PartitionIndex, data.bytestr()) {
			index = decoded
		}
	}

	// Always add the segment if it doesn't already exist
	// Check for duplicate by comparing segment key
	mut segment_exists := false
	for seg in index.log_segments {
		if seg.key == segment_key {
			segment_exists = true
			break
		}
	}

	if !segment_exists {
		index.log_segments << LogSegment{
			start_offset: base_offset
			end_offset:   end_offset
			key:          segment_key
			size_bytes:   i64(segment_data.len)
			created_at:   time.now()
		}

		// Sort segments by start_offset to maintain order
		index.log_segments.sort(a.start_offset < b.start_offset)

		// Update high_watermark to the maximum end_offset + 1
		for seg in index.log_segments {
			if seg.end_offset + 1 > index.high_watermark {
				index.high_watermark = seg.end_offset + 1
			}
		}

		// Write updated index to S3
		a.put_object(index_key, json.encode(index).bytes()) or {
			eprintln('[S3] FLUSH FAILED for ${partition_key}: Index put failed: ${err}')
			return error('Index put failed during flush: ${err}')
		}

		// Update local cache
		cache_key := '${topic}:${partition}'
		a.topic_lock.@lock()
		if cached := a.topic_index_cache[cache_key] {
			// Keep the higher high_watermark between cache and S3
			mut final_index := index
			if cached.index.high_watermark > index.high_watermark {
				final_index.high_watermark = cached.index.high_watermark
			}
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     final_index
				cached_at: time.now()
			}
		} else {
			a.topic_index_cache[cache_key] = CachedPartitionIndex{
				index:     index
				cached_at: time.now()
			}
		}
		a.topic_lock.unlock()
	}
}

// ============================================================
// Compaction Logic
// ============================================================

pub fn (mut a S3StorageAdapter) start_workers() {
	if a.compactor_running {
		return
	}
	a.compactor_running = true
	go a.flush_worker()
	go a.compaction_worker()
}

// compaction_worker periodically checks for segments to merge and performs compaction.
fn (mut a S3StorageAdapter) compaction_worker() {
	for a.compactor_running {
		time.sleep(a.config.compaction_interval_ms)

		a.compact_all_partitions() or {
			// In production, use structured logging here
			eprintln('[S3] Compaction failed: ${err}')
			continue
		}
	}
}

// compact_all_partitions iterates over all topics and partitions and attempts to merge small segments.
fn (mut a S3StorageAdapter) compact_all_partitions() ! {
	topics := a.list_topics()!

	// Use map/set to track active partitions to compact
	// For simplicity, we iterate over all known topics/partitions.

	for t in topics {
		for p in 0 .. t.partition_count {
			a.compact_partition(t.name, p)!
		}
	}
}

fn (mut a S3StorageAdapter) compact_partition(topic string, partition int) ! {
	// 1. Get current index
	mut index := a.get_partition_index(topic, partition)!

	// 2. Identify segments for compaction
	mut segments_to_compact := []LogSegment{}
	mut total_size := i64(0)

	for seg in index.log_segments {
		if total_size >= a.config.target_segment_bytes {
			break
		}

		// Only consider segments smaller than the target size
		if seg.size_bytes < a.config.target_segment_bytes {
			segments_to_compact << seg
			total_size += seg.size_bytes
		}
	}

	// Check if enough small segments were found
	if segments_to_compact.len < a.min_segment_count_to_compact
		|| total_size < a.config.target_segment_bytes / 2 {
		return
	}

	// 3. Perform Compaction
	// Merge segments and upload new large segment to S3
	a.merge_segments(topic, partition, mut index, segments_to_compact)!
}

fn (mut a S3StorageAdapter) merge_segments(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	if segments.len == 0 {
		return
	}

	// Download all segment data in parallel (simplified to sequential for now)
	mut merged_data := []u8{}

	for seg in segments {
		data, _ := a.get_object(seg.key, -1, -1) or {
			// Log error and continue to next segment set, or return error
			// We return error to be safe.
			return error('Failed to download segment ${seg.key}: ${err}')
		}
		merged_data << data
	}

	// New segment metadata
	new_start_offset := segments[0].start_offset
	new_end_offset := segments[segments.len - 1].end_offset
	new_key := a.log_segment_key(topic, partition, new_start_offset, new_end_offset)

	// Upload new merged segment to S3
	a.put_object(new_key, merged_data)!

	// 4. Update index and delete old segments (Atomic Index Update)

	// Find the range of offsets covered by the merged segments
	start_index := index.log_segments.index(segments[0])
	if start_index < 0 {
		return error('Compaction internal error: start segment not found in index')
	}
	end_index := index.log_segments.index(segments[segments.len - 1])
	if end_index < 0 {
		return error('Compaction internal error: end segment not found in index')
	}

	// New list of log segments (excluding merged ones)
	mut new_log_segments := []LogSegment{}

	// Segments before the merged block
	if start_index > 0 {
		new_log_segments << index.log_segments[0..start_index]
	}

	// Add the new merged segment
	new_log_segments << LogSegment{
		start_offset: new_start_offset
		end_offset:   new_end_offset
		key:          new_key
		size_bytes:   i64(merged_data.len)
		created_at:   time.now()
	}

	// Segments after the merged block
	if end_index < index.log_segments.len - 1 {
		new_log_segments << index.log_segments[end_index + 1..]
	}

	// Update index object
	index.log_segments = new_log_segments
	index_key := a.partition_index_key(topic, partition)

	// Atomically write the new index (overwrite old one)
	a.put_object(index_key, json.encode(index).bytes())!

	// 5. Delete old segments (Non-critical step after index update)
	for seg in segments {
		a.delete_object(seg.key) or {
			eprintln('[S3] Failed to delete old segment ${seg.key}: ${err}')
		}
	}
}

// ============================================================
// Record Encoding/Decoding
// ============================================================

fn encode_stored_records(records []StoredRecord) []u8 {
	mut buf := []u8{}
	record_count := records.len
	buf << u8(record_count >> 24)
	buf << u8(record_count >> 16)
	buf << u8(record_count >> 8)
	buf << u8(record_count)

	for rec in records {
		// Offset (8 bytes)
		for i := 7; i >= 0; i-- {
			buf << u8(rec.offset >> (i * 8))
		}

		// Timestamp (8 bytes)
		ts := rec.timestamp.unix_milli()
		for i := 7; i >= 0; i-- {
			buf << u8(ts >> (i * 8))
		}

		// Key
		key_len := rec.key.len
		buf << u8(key_len >> 24)
		buf << u8(key_len >> 16)
		buf << u8(key_len >> 8)
		buf << u8(key_len)
		buf << rec.key

		// Value
		value_len := rec.value.len
		buf << u8(value_len >> 24)
		buf << u8(value_len >> 16)
		buf << u8(value_len >> 8)
		buf << u8(value_len)
		buf << rec.value

		// Headers (map[string][]u8)
		headers_count := rec.headers.len
		buf << u8(headers_count >> 24)
		buf << u8(headers_count >> 16)
		buf << u8(headers_count >> 8)
		buf << u8(headers_count)

		for h_key, h_val in rec.headers {
			// Header key length and value
			buf << u8(h_key.len >> 8)
			buf << u8(h_key.len)
			buf << h_key.bytes()

			// Header value length and value
			buf << u8(h_val.len >> 8)
			buf << u8(h_val.len)
			buf << h_val
		}
	}

	return buf
}

fn decode_stored_records(data []u8) []StoredRecord {
	if data.len < 4 {
		return []
	}

	mut pos := 0
	record_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
		pos + 3])
	pos += 4

	mut records := []StoredRecord{}

	for _ in 0 .. record_count {
		if pos + 20 > data.len {
			break
		}

		// Offset
		mut offset := i64(0)
		for i := 0; i < 8; i++ {
			offset = i64((u64(offset) << 8) | u64(data[pos + i]))
		}
		pos += 8

		// Timestamp
		mut ts := i64(0)
		for i := 0; i < 8; i++ {
			ts = i64((u64(ts) << 8) | u64(data[pos + i]))
		}
		pos += 8

		// Key
		key_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4
		key := data[pos..pos + int(key_len)].clone()
		pos += int(key_len)

		// Value
		value_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4
		value := data[pos..pos + int(value_len)].clone()
		pos += int(value_len)

		// Headers
		headers_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | (u32(data[pos + 2]) << 8) | u32(data[
			pos + 3])
		pos += 4

		mut headers := map[string][]u8{}
		for _ in 0 .. headers_count {
			h_key_len := (u32(data[pos]) << 8) | u32(data[pos + 1])
			pos += 2
			h_key := data[pos..pos + int(h_key_len)].bytestr()
			pos += int(h_key_len)

			h_val_len := (u32(data[pos]) << 8) | u32(data[pos + 1])
			pos += 2
			h_val := data[pos..pos + int(h_val_len)].clone()
			pos += int(h_val_len)

			headers[h_key] = h_val
		}

		records << StoredRecord{
			offset:    offset
			timestamp: time.unix_milli(ts)
			key:       key
			value:     value
			headers:   headers
		}
	}

	return records
}

fn parse_list_objects_response(body string) []S3Object {
	// Simplified XML parsing - in production use proper XML parser
	mut objects := []S3Object{}

	mut remaining := body
	for {
		key_start := remaining.index('<Key>') or { break }
		key_end := remaining.index('</Key>') or { break }

		key := remaining[key_start + 5..key_end]
		objects << S3Object{
			key:  key
			size: 0
			etag: ''
		}

		remaining = remaining[key_end + 6..]
	}

	return objects
}

fn generate_topic_id(name string) []u8 {
	hash := md5.sum(name.bytes())
	return hash[0..16]
}
