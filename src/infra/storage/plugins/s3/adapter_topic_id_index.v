// Infra Layer - Persistent topic_id Reverse Index
// Maintains a JSON file on S3 mapping topic_id hex -> topic name,
// enabling O(1) lookup on cold start without scanning all topics.
module s3

import domain
import json
import infra.observability

/// build_topic_metadata constructs topic metadata from name, id, partitions, and config.
fn build_topic_metadata(name string, topic_id []u8, partitions int, config domain.TopicConfig) domain.TopicMetadata {
	mut config_map := map[string]string{}
	config_map['retention.ms'] = config.retention_ms.str()
	config_map['retention.bytes'] = config.retention_bytes.str()
	config_map['segment.bytes'] = config.segment_bytes.str()
	config_map['cleanup.policy'] = config.cleanup_policy

	return domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          config_map
		is_internal:     name.starts_with('_')
	}
}

/// topic_id_index_key returns the S3 key for the persistent topic_id reverse index.
fn (a &S3StorageAdapter) topic_id_index_key() string {
	return '${a.config.prefix}__cluster/topic_id_index.json'
}

/// encode_topic_id_index serializes a topic_id index map to JSON bytes.
fn encode_topic_id_index(idx map[string]string) []u8 {
	return json.encode(idx).bytes()
}

/// decode_topic_id_index deserializes JSON bytes into a topic_id index map.
/// Returns empty map on invalid or empty input.
fn decode_topic_id_index(data []u8) map[string]string {
	if data.len == 0 {
		return map[string]string{}
	}
	return json.decode(map[string]string, data.bytestr()) or { return map[string]string{} }
}

/// merge_topic_id_index returns a new map with the entry added or updated.
fn merge_topic_id_index(existing map[string]string, topic_id_hex string, topic_name string) map[string]string {
	mut result := existing.clone()
	result[topic_id_hex] = topic_name
	return result
}

/// remove_from_topic_id_index returns a new map with the entry removed.
fn remove_from_topic_id_index(existing map[string]string, topic_id_hex string) map[string]string {
	mut result := existing.clone()
	result.delete(topic_id_hex)
	return result
}

/// save_topic_id_index persists the topic_id index to S3.
fn (mut a S3StorageAdapter) save_topic_id_index(idx map[string]string) {
	key := a.topic_id_index_key()
	a.put_object(key, encode_topic_id_index(idx)) or {
		observability.log_with_context('s3', .warn, 'TopicIdIndex', 'failed to save topic_id index',
			{
			'error': err.msg()
		})
	}
}

/// load_topic_id_index reads the topic_id index from S3.
/// Returns empty map when the file does not exist or cannot be parsed.
fn (mut a S3StorageAdapter) load_topic_id_index() map[string]string {
	key := a.topic_id_index_key()
	data, _ := a.get_object(key, -1, -1) or { return map[string]string{} }
	return decode_topic_id_index(data)
}

/// update_topic_id_index_on_create adds an entry to the persistent index.
/// Loads current index from S3, merges the new entry, and writes back.
fn (mut a S3StorageAdapter) update_topic_id_index_on_create(topic_id_hex string, topic_name string) {
	current := a.load_topic_id_index()
	updated := merge_topic_id_index(current, topic_id_hex, topic_name)
	a.save_topic_id_index(updated)
}

/// update_topic_id_index_on_delete removes an entry from the persistent index.
/// Loads current index from S3, removes the entry, and writes back.
fn (mut a S3StorageAdapter) update_topic_id_index_on_delete(topic_id_hex string) {
	current := a.load_topic_id_index()
	updated := remove_from_topic_id_index(current, topic_id_hex)
	a.save_topic_id_index(updated)
}

/// populate_cache_from_topic_id_index loads the persistent index and
/// populates the in-memory reverse cache. Called on cold start.
fn (mut a S3StorageAdapter) populate_cache_from_topic_id_index() {
	idx := a.load_topic_id_index()
	if idx.len == 0 {
		return
	}

	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	for topic_id_hex, topic_name in idx {
		a.cache.topic_id_cache[topic_id_hex] = topic_name
		a.cache.topic_id_reverse_cache[topic_id_hex] = topic_name
	}
}

/// lookup_topic_name_from_persistent_index looks up a topic name by topic_id
/// from the persistent S3 index. Populates the in-memory cache on hit.
/// Returns the topic name or empty string if not found.
fn (mut a S3StorageAdapter) lookup_topic_name_from_persistent_index(topic_id_hex string) string {
	s3_index := a.load_topic_id_index()
	topic_name := s3_index[topic_id_hex] or { return '' }

	a.cache.topic_lock.@lock()
	for tid_hex, tname in s3_index {
		a.cache.topic_id_cache[tid_hex] = tname
		a.cache.topic_id_reverse_cache[tid_hex] = tname
	}
	a.cache.topic_lock.unlock()

	return topic_name
}

/// get_cached_topic_id_hex returns the hex topic_id for a topic name from cache.
/// Returns empty string if not cached.
fn (mut a S3StorageAdapter) get_cached_topic_id_hex(name string) string {
	a.cache.topic_lock.rlock()
	defer { a.cache.topic_lock.runlock() }
	if cached := a.cache.topic_cache[name] {
		return cached.meta.topic_id.hex()
	}
	return ''
}
