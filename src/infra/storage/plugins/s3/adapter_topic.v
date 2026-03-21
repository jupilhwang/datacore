// Topic CRUD operations for S3StorageAdapter.
module s3

import domain
import time
import json

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
	defer { a.topic_lock.unlock() }
	a.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      ''
		cached_at: time.now()
	}
	// Also cache topic_id -> name mapping for O(1) lookup
	a.topic_id_cache[meta.topic_id.hex()] = name

	return meta
}

/// delete_topic deletes a topic from S3.
pub fn (mut a S3StorageAdapter) delete_topic(name string) ! {
	a.topic_lock.@lock()
	defer { a.topic_lock.unlock() }

	// Look up topic metadata to remove from id cache
	if cached := a.topic_cache[name] {
		a.topic_id_cache.delete(cached.meta.topic_id.hex())
	}

	// Delete all objects starting with topic prefix (S3 operation, no lock needed)
	prefix := '${a.config.prefix}topics/${name}/'
	a.delete_objects_with_prefix(prefix)!

	// Remove from cache
	a.topic_cache.delete(name)
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
			a.record_cache_hit()
			return cached.meta
		}
	}
	a.topic_lock.runlock()

	// Cache miss metric
	a.record_cache_miss()

	// Fetch from S3
	key := a.topic_metadata_key(name)
	data, etag := a.get_object(key, -1, -1)!

	meta := json.decode(domain.TopicMetadata, data.bytestr())!

	// Update cache
	a.topic_lock.@lock()
	defer { a.topic_lock.unlock() }
	a.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      etag
		cached_at: time.now()
	}

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
	defer { a.topic_lock.unlock() }
	for t in topics {
		// Populate topic_id_cache for future lookups
		a.topic_id_cache[t.topic_id.hex()] = t.name
	}

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
	defer { a.topic_lock.unlock() }
	if cached := a.topic_cache[name] {
		a.topic_cache[name] = CachedTopic{
			meta:      updated_meta
			etag:      cached.etag
			cached_at: time.now()
		}
	}
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
