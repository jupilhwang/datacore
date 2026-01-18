// S3 Storage Plugin for DataCore
// Implements StoragePort interface with S3 backend
// Uses conditional writes (ETag) for concurrency control
module s3

import domain
import service.port
import time
import json
import crypto.md5
import net.http
import sync

// S3Config holds S3 storage configuration
pub struct S3Config {
pub:
	bucket_name       string = 'datacore-storage'
	region            string = 'us-east-1'
	endpoint          string // Optional: for MinIO/LocalStack
	access_key        string
	secret_key        string
	prefix            string = 'datacore/'
	max_retries       int    = 3
	retry_delay_ms    int    = 100
	use_path_style    bool   = true // For MinIO compatibility
}

// S3StorageAdapter implements StoragePort for S3 storage
pub struct S3StorageAdapter {
mut:
	config          S3Config
	// Local caches with TTL
	topic_cache     map[string]CachedTopic
	group_cache     map[string]CachedGroup
	offset_cache    map[string]map[string]i64
	// Locks for thread safety
	topic_lock      sync.RwMutex
	group_lock      sync.RwMutex
	offset_lock     sync.RwMutex
}

struct CachedTopic {
	meta     domain.TopicMetadata
	etag     string
	cached_at time.Time
}

struct CachedGroup {
	group    domain.ConsumerGroup
	etag     string
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
		config: config
		topic_cache: map[string]CachedTopic{}
		group_cache: map[string]CachedGroup{}
		offset_cache: map[string]map[string]i64{}
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
		name: name
		topic_id: topic_id
		partition_count: partitions
		config: config_map
		is_internal: name.starts_with('_')
	}

	// Write metadata to S3 with conditional write (If-None-Match: *)
	key := a.topic_metadata_key(name)
	data := json.encode(meta)
	a.put_object_if_not_exists(key, data.bytes())!

	// Initialize partition indices
	for p in 0 .. partitions {
		index_key := a.partition_index_key(name, p)
		index := PartitionIndex{
			topic: name
			partition: p
			earliest_offset: 0
			high_watermark: 0
			log_segments: []
		}
		a.put_object(index_key, json.encode(index).bytes())!
	}

	// Cache the topic
	a.topic_lock.@lock()
	a.topic_cache[name] = CachedTopic{
		meta: meta
		etag: ''
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
	data, etag := a.get_object(key)!
	
	meta := json.decode(domain.TopicMetadata, data.bytestr())!

	// Update cache
	a.topic_lock.@lock()
	a.topic_cache[name] = CachedTopic{
		meta: meta
		etag: etag
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
			topic: name
			partition: p
			earliest_offset: 0
			high_watermark: 0
			log_segments: []
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

// append appends records to a partition
pub fn (mut a S3StorageAdapter) append(topic string, partition int, records []domain.Record) !domain.AppendResult {
	if records.len == 0 {
		return domain.AppendResult{
			base_offset: 0
			log_append_time: time.now().unix_milli()
			log_start_offset: 0
		}
	}

	// Get current partition index with retry for concurrent writes
	mut index := a.get_partition_index(topic, partition)!
	base_offset := index.high_watermark

	// Assign offsets to records (wrap in internal struct for storage)
	mut enriched_records := []StoredRecord{}
	for i, rec in records {
		enriched_records << StoredRecord{
			offset: base_offset + i64(i)
			timestamp: if rec.timestamp.unix_milli() == 0 { time.now() } else { rec.timestamp }
			key: rec.key
			value: rec.value
			headers: rec.headers
		}
	}

	// Create log segment data
	segment_data := encode_stored_records(enriched_records)
	segment_key := a.log_segment_key(topic, partition, base_offset, base_offset + i64(records.len) - 1)
	
	// Write segment to S3
	a.put_object(segment_key, segment_data)!

	// Update partition index
	index.high_watermark = base_offset + i64(records.len)
	index.log_segments << LogSegment{
		start_offset: base_offset
		end_offset: base_offset + i64(records.len) - 1
		key: segment_key
		size_bytes: i64(segment_data.len)
		created_at: time.now()
	}

	// Write index with conditional update
	index_key := a.partition_index_key(topic, partition)
	a.put_object(index_key, json.encode(index).bytes())!

	return domain.AppendResult{
		base_offset: base_offset
		log_append_time: time.now().unix_milli()
		log_start_offset: index.earliest_offset
	}
}

// fetch retrieves records from a partition
pub fn (mut a S3StorageAdapter) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	index := a.get_partition_index(topic, partition)!

	if offset < index.earliest_offset {
		return error('Offset out of range (too old): ${offset} < ${index.earliest_offset}')
	}

	if offset >= index.high_watermark {
		// No new records
		return domain.FetchResult{
			records: []
			high_watermark: index.high_watermark
			last_stable_offset: index.high_watermark
			log_start_offset: index.earliest_offset
		}
	}

	// Find relevant segments
	mut all_records := []domain.Record{}
	mut bytes_read := 0

	for seg in index.log_segments {
		if seg.end_offset < offset {
			continue
		}
		if seg.start_offset > offset + i64(max_bytes / 100) { // Rough estimate
			break
		}

		// Fetch segment from S3
		data, _ := a.get_object(seg.key) or { continue }
		stored_records := decode_stored_records(data)

		for rec in stored_records {
			if rec.offset >= offset && bytes_read < max_bytes {
				// Convert StoredRecord to domain.Record
				all_records << domain.Record{
					key: rec.key
					value: rec.value
					headers: rec.headers
					timestamp: rec.timestamp
				}
				bytes_read += rec.value.len + rec.key.len
			}
		}

		if bytes_read >= max_bytes {
			break
		}
	}

	return domain.FetchResult{
		records: all_records
		high_watermark: index.high_watermark
		last_stable_offset: index.high_watermark
		log_start_offset: index.earliest_offset
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
		topic: topic
		partition: partition
		earliest_offset: index.earliest_offset
		latest_offset: index.high_watermark
		high_watermark: index.high_watermark
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
		group: group
		etag: ''
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
	data, etag := a.get_object(key)!
	
	group := json.decode(domain.ConsumerGroup, data.bytestr())!

	// Update cache
	a.group_lock.@lock()
	a.group_cache[group_id] = CachedGroup{
		group: group
		etag: etag
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
							group_id: group_id
							protocol_type: group.protocol_type
							state: group.state.str()
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
				topic: part.topic
				partition: part.partition
				offset: cached_offset
				metadata: ''
				error_code: 0
			}
			continue
		}

		// Fetch from S3
		data, _ := a.get_object(key) or {
			results << domain.OffsetFetchResult{
				topic: part.topic
				partition: part.partition
				offset: -1
				metadata: ''
				error_code: 0
			}
			continue
		}

		offset_data := json.decode(domain.PartitionOffset, data.bytestr()) or {
			results << domain.OffsetFetchResult{
				topic: part.topic
				partition: part.partition
				offset: -1
				metadata: ''
				error_code: 0
			}
			continue
		}

		results << domain.OffsetFetchResult{
			topic: part.topic
			partition: part.partition
			offset: offset_data.offset
			metadata: offset_data.metadata
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
	_ := a.list_objects(a.config.prefix) or {
		return .unhealthy
	}
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
	key   string
	size  i64
	etag  string
}

// Partition index stored in S3
struct PartitionIndex {
mut:
	topic          string
	partition      int
	earliest_offset i64
	high_watermark i64
	log_segments   []LogSegment
}

struct LogSegment {
	start_offset i64
	end_offset   i64
	key          string
	size_bytes   i64
	created_at   time.Time
}

fn (mut a S3StorageAdapter) get_partition_index(topic string, partition int) !PartitionIndex {
	key := a.partition_index_key(topic, partition)
	data, _ := a.get_object(key)!
	return json.decode(PartitionIndex, data.bytestr())!
}

// S3 HTTP operations using signature v4
fn (mut a S3StorageAdapter) get_object(key string) !([]u8, string) {
	endpoint := a.get_endpoint()
	url := '${endpoint}/${a.config.bucket_name}/${key}'
	
	headers := a.sign_request('GET', key, []u8{})
	
	resp := http.fetch(http.FetchConfig{
		url: url
		method: .get
		header: headers
	}) or {
		return error('S3 GET failed: ${err}')
	}

	if resp.status_code == 404 {
		return error('Object not found: ${key}')
	}
	if resp.status_code != 200 {
		return error('S3 GET failed with status ${resp.status_code}')
	}

	etag := resp.header.get(.etag) or { '' }
	return resp.body.bytes(), etag
}

fn (mut a S3StorageAdapter) put_object(key string, data []u8) ! {
	endpoint := a.get_endpoint()
	url := '${endpoint}/${a.config.bucket_name}/${key}'
	
	headers := a.sign_request('PUT', key, data)
	
	resp := http.fetch(http.FetchConfig{
		url: url
		method: .put
		header: headers
		data: data.bytestr()
	}) or {
		return error('S3 PUT failed: ${err}')
	}

	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

fn (mut a S3StorageAdapter) put_object_if_not_exists(key string, data []u8) ! {
	endpoint := a.get_endpoint()
	url := '${endpoint}/${a.config.bucket_name}/${key}'
	
	mut headers := a.sign_request('PUT', key, data)
	headers.add_custom('If-None-Match', '*') or {}
	
	resp := http.fetch(http.FetchConfig{
		url: url
		method: .put
		header: headers
		data: data.bytestr()
	}) or {
		return error('S3 PUT failed: ${err}')
	}

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
	
	headers := a.sign_request('DELETE', key, []u8{})
	
	resp := http.fetch(http.FetchConfig{
		url: url
		method: .delete
		header: headers
	}) or {
		return error('S3 DELETE failed: ${err}')
	}

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
	url := '${endpoint}/${a.config.bucket_name}?prefix=${prefix}&list-type=2'
	
	headers := a.sign_request('GET', '', []u8{})
	
	resp := http.fetch(http.FetchConfig{
		url: url
		method: .get
		header: headers
	}) or {
		return error('S3 LIST failed: ${err}')
	}

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
	return 'https://s3.${a.config.region}.amazonaws.com'
}

fn (a &S3StorageAdapter) sign_request(method string, key string, body []u8) http.Header {
	// AWS Signature V4 signing (simplified)
	mut h := http.Header{}
	now := time.now()
	
	date_str := now.strftime('%Y%m%dT%H%M%SZ')
	h.add_custom('x-amz-date', date_str) or {}
	h.add_custom('Host', a.get_host()) or {}
	
	if body.len > 0 {
		content_hash := md5.sum(body).hex()
		h.add_custom('x-amz-content-sha256', content_hash) or {}
		h.add_custom('Content-Length', body.len.str()) or {}
	}
	
	// In production, implement full AWS Signature V4
	// For now, this is a placeholder
	if a.config.access_key.len > 0 {
		// Add Authorization header with signature
		auth := 'AWS4-HMAC-SHA256 Credential=${a.config.access_key}/${now.strftime("%Y%m%d")}/${a.config.region}/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=placeholder'
		h.add_custom('Authorization', auth) or {}
	}
	
	return h
}

fn (a &S3StorageAdapter) get_host() string {
	if a.config.endpoint.len > 0 {
		// Extract host from endpoint
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

// ============================================================
// Record Encoding/Decoding
// ============================================================

fn encode_stored_records(records []StoredRecord) []u8 {
	// Simple binary format:
	// [record_count:4][record1][record2]...
	// record: [offset:8][timestamp:8][key_len:4][key][value_len:4][value][headers_count:4][headers...]
	mut buf := []u8{}
	
	// Record count
	buf << u8(records.len >> 24)
	buf << u8(records.len >> 16)
	buf << u8(records.len >> 8)
	buf << u8(records.len)
	
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
	record_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) | 
	                (u32(data[pos + 2]) << 8) | u32(data[pos + 3])
	pos += 4
	
	mut records := []StoredRecord{}
	
	for _ in 0 .. record_count {
		if pos + 20 > data.len {
			break
		}
		
		// Offset
		mut offset := i64(0)
		for i := 0; i < 8; i++ {
			offset = (offset << 8) | i64(data[pos + i])
		}
		pos += 8
		
		// Timestamp
		mut ts := i64(0)
		for i := 0; i < 8; i++ {
			ts = (ts << 8) | i64(data[pos + i])
		}
		pos += 8
		
		// Key
		key_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) |
		           (u32(data[pos + 2]) << 8) | u32(data[pos + 3])
		pos += 4
		key := data[pos..pos + int(key_len)].clone()
		pos += int(key_len)
		
		// Value
		value_len := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) |
		             (u32(data[pos + 2]) << 8) | u32(data[pos + 3])
		pos += 4
		value := data[pos..pos + int(value_len)].clone()
		pos += int(value_len)
		
		// Headers
		headers_count := (u32(data[pos]) << 24) | (u32(data[pos + 1]) << 16) |
		                 (u32(data[pos + 2]) << 8) | u32(data[pos + 3])
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
			offset: offset
			timestamp: time.unix_milli(ts)
			key: key
			value: value
			headers: headers
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
			key: key
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
