// Topic CRUD operations for S3StorageAdapter.
module s3

import domain
import infra.observability
import time
import json

/// create_topic creates a new topic in S3.
pub fn (mut a S3StorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	// Input validation
	validate_identifier(name, 'topic')!
	if partitions <= 0 {
		return error('Partition count must be positive: ${partitions}')
	}
	if partitions > max_partition_count {
		return error('Partition count too large: ${partitions} > ${max_partition_count}')
	}

	topic_id := generate_topic_id(name)
	meta := build_topic_metadata(name, topic_id, partitions, config)

	// Save metadata to S3 with conditional write (If-None-Match: *)
	// S3 returns 412 Precondition Failed if the object already exists.
	key := a.topic_metadata_key(name)
	data := json.encode(meta)
	a.put_object_if_not_exists(key, data.bytes()) or {
		if err.msg().contains('precondition failed') {
			return error('Topic already exists: ${name}')
		}
		return err
	}

	a.initialize_partition_indices(name, partitions)!

	// Store in topic cache
	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	a.cache.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      ''
		cached_at: time.now()
	}
	a.cache.topic_id_cache[meta.topic_id.hex()] = name
	a.cache.topic_id_reverse_cache[meta.topic_id.hex()] = name

	// Persist topic_id -> name mapping to S3 for cold-start recovery
	a.update_topic_id_index_on_create(meta.topic_id.hex(), name)

	return meta
}

/// initialize_partition_indices creates partition index files on S3.
fn (mut a S3StorageAdapter) initialize_partition_indices(topic_name string, num_partitions int) ! {
	for p in 0 .. num_partitions {
		index_key := a.partition_index_key(topic_name, p)
		index := PartitionIndex{
			topic:           topic_name
			partition:       p
			earliest_offset: 0
			high_watermark:  0
			log_segments:    []
		}
		a.put_object(index_key, json.encode(index).bytes())!
	}
}

/// clear_topic_caches removes all cache entries for the given topic under exclusive lock.
fn (mut a S3StorageAdapter) clear_topic_caches(name string) {
	a.cache.topic_lock.@lock()

	mut topic_id_hex := ''
	if cached := a.cache.topic_cache[name] {
		topic_id_hex = cached.meta.topic_id.hex()
	}

	a.cache.topic_cache.delete(name)
	if topic_id_hex.len > 0 {
		a.cache.topic_id_cache.delete(topic_id_hex)
		a.cache.topic_id_reverse_cache.delete(topic_id_hex)
	}

	mut index_keys_to_delete := []string{}
	for key, _ in a.cache.topic_index_cache {
		if key.starts_with('${name}:') {
			index_keys_to_delete << key
		}
	}
	for key in index_keys_to_delete {
		a.cache.topic_index_cache.delete(key)
	}

	a.cache.topic_lock.unlock()
}

/// clear_topic_partition_buffers removes partition buffers for the given topic.
fn (mut a S3StorageAdapter) clear_topic_partition_buffers(name string) {
	a.buffer_lock.@lock()
	defer { a.buffer_lock.unlock() }
	mut keys_to_delete := []string{}
	for key, _ in a.topic_partition_buffers {
		if key.starts_with('${name}:') {
			keys_to_delete << key
		}
	}
	for key in keys_to_delete {
		a.topic_partition_buffers.delete(key)
	}
}

/// delete_topic deletes a topic from S3.
pub fn (mut a S3StorageAdapter) delete_topic(name string) ! {
	validate_identifier(name, 'topic')!

	topic_id_hex := a.get_cached_topic_id_hex(name)

	a.clear_topic_caches(name)
	a.clear_topic_partition_buffers(name)

	prefix := '${a.config.prefix}topics/${name}/'
	a.delete_objects_with_prefix(prefix) or {
		observability.log_with_context('s3', .error, 'TopicAdapter', 'failed to delete topic objects from S3',
			{
			'topic':  name
			'prefix': prefix
			'error':  err.str()
		})
		return
	}

	if topic_id_hex.len > 0 {
		a.update_topic_id_index_on_delete(topic_id_hex)
	}
}

/// list_topics retrieves all topics from S3.
pub fn (mut a S3StorageAdapter) list_topics() ![]domain.TopicMetadata {
	prefix := '${a.config.prefix}topics/'
	objects := a.list_objects(prefix)!

	mut topics := []domain.TopicMetadata{}
	mut seen := map[string]bool{}

	// First add all cached topics
	a.cache.topic_lock.rlock()
	for name, cached in a.cache.topic_cache {
		if name !in seen {
			seen[name] = true
			topics << cached.meta
		}
	}
	a.cache.topic_lock.runlock()

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
	validate_identifier(name, 'topic')!
	// Check cache first
	a.cache.topic_lock.rlock()
	if cached := a.cache.topic_cache[name] {
		if time.since(cached.cached_at) < topic_cache_ttl {
			a.cache.topic_lock.runlock()
			a.record_cache_hit()
			return cached.meta
		}
	}
	a.cache.topic_lock.runlock()

	// Cache miss metric
	a.record_cache_miss()

	// Fetch from S3
	key := a.topic_metadata_key(name)
	data, etag := a.get_object(key, -1, -1)!

	meta := json.decode(domain.TopicMetadata, data.bytestr())!

	// Update cache
	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	a.cache.topic_cache[name] = CachedTopic{
		meta:      meta
		etag:      etag
		cached_at: time.now()
	}

	return meta
}

/// get_topic_by_id retrieves a topic by topic_id.
/// Uses O(1) reverse cache lookup, then persistent S3 index,
/// then falls back to list_topics on miss.
pub fn (mut a S3StorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	topic_id_hex := topic_id.hex()

	// 1. Check reverse cache (O(1) in-memory lookup)
	a.cache.topic_lock.rlock()
	reverse_hit := a.cache.topic_id_reverse_cache[topic_id_hex] or { '' }
	a.cache.topic_lock.runlock()

	if reverse_hit.len > 0 {
		return a.get_topic(reverse_hit)
	}

	// 2. Try persistent S3 index (single GET, avoids full scan)
	persistent_hit := a.lookup_topic_name_from_persistent_index(topic_id_hex)
	if persistent_hit.len > 0 {
		return a.get_topic(persistent_hit)
	}

	// 3. Fallback: full scan via list_topics
	topics := a.list_topics()!

	a.cache.topic_lock.@lock()
	for t in topics {
		tid_hex := t.topic_id.hex()
		a.cache.topic_id_cache[tid_hex] = t.name
		a.cache.topic_id_reverse_cache[tid_hex] = t.name
	}
	a.cache.topic_lock.unlock()

	for t in topics {
		if t.topic_id == topic_id {
			return t
		}
	}
	return error('Topic not found')
}

/// add_partitions adds partitions to a topic.
pub fn (mut a S3StorageAdapter) add_partitions(name string, new_count int) ! {
	validate_identifier(name, 'topic')!
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
	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	if cached := a.cache.topic_cache[name] {
		a.cache.topic_cache[name] = CachedTopic{
			meta:      updated_meta
			etag:      cached.etag
			cached_at: time.now()
		}
	}
}

/// get_partition_info retrieves partition information.
pub fn (mut a S3StorageAdapter) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	validate_identifier(topic, 'topic')!
	index := a.get_partition_index(topic, partition)!

	return domain.PartitionInfo{
		topic:           topic
		partition:       partition
		earliest_offset: index.earliest_offset
		latest_offset:   index.high_watermark
		high_watermark:  index.high_watermark
	}
}
