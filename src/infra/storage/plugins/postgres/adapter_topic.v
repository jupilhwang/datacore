// Infra Layer - PostgreSQL storage adapter: topic CRUD operations
module postgres

import domain
import infra.observability
import rand
import time

// --- Topic metrics helpers ---

fn (mut a PostgresStorageAdapter) inc_topic_create() {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.topic_create_count++
}

fn (mut a PostgresStorageAdapter) inc_create_topic_query(elapsed_ms i64) {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
}

// --- Topic batch helpers ---

/// build_partition_metadata_params generates a parameter array for batch partition metadata INSERT.
fn (a &PostgresStorageAdapter) build_partition_metadata_params(topic_name string, partition_count int) []string {
	mut all_params := []string{}
	for p in 0 .. partition_count {
		all_params << topic_name
		all_params << p.str()
		all_params << '0'
		all_params << '0'
	}
	return all_params
}

/// build_partition_metadata_range_params generates a parameter array for batch INSERT of partition metadata within a specific range.
fn (a &PostgresStorageAdapter) build_partition_metadata_range_params(topic_name string, start_partition int, end_partition int) []string {
	mut all_params := []string{}
	for p in start_partition .. end_partition {
		all_params << topic_name
		all_params << p.str()
		all_params << '0'
		all_params << '0'
	}
	return all_params
}

// --- Topic CRUD methods ---

/// create_topic creates a new topic.
/// Automatically generates a UUID v4 format topic_id.
pub fn (mut a PostgresStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	start_time := time.now()

	a.inc_topic_create()

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// Generate UUID for topic_id
	mut topic_id := []u8{len: 16}
	for i in 0 .. 16 {
		topic_id[i] = u8(rand.intn(256) or { 0 })
	}
	// Set UUID version 4 (random)
	topic_id[6] = (topic_id[6] & 0x0f) | 0x40
	topic_id[8] = (topic_id[8] & 0x3f) | 0x80

	is_internal := name.starts_with('__')

	// Begin transaction
	db.begin()!

	// Insert topic
	db.exec_param_many('
		INSERT INTO topics (name, topic_id, partition_count, is_internal)
		VALUES (\$1, \$2, \$3, \$4)
	',
		[name, '\\x${topic_id.hex()}', partitions.str(), is_internal.str()])!

	// Create partition metadata entries (batch INSERT)
	if partitions > 0 {
		all_params := a.build_partition_metadata_params(name, partitions)
		query := build_batch_insert_query('partition_metadata', ['topic_name', 'partition_id',
			'base_offset', 'high_watermark'], 4, partitions)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!

	metadata := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     is_internal
	}

	// Update cache
	a.cache_lock.@lock()
	a.topic_cache[name] = metadata
	a.topic_id_idx[topic_id.hex()] = name
	a.cache_lock.unlock()

	elapsed_ms := time.since(start_time).milliseconds()
	a.inc_create_topic_query(elapsed_ms)

	observability.log_with_context('postgres', .info, 'Topic', 'Topic created', {
		'name':       name
		'partitions': partitions.str()
	})

	return metadata
}

/// delete_topic deletes a topic.
pub fn (mut a PostgresStorageAdapter) delete_topic(name string) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// Delete records
	db.exec_param('DELETE FROM records WHERE topic_name = $1', name)!
	// Delete partition metadata
	db.exec_param('DELETE FROM partition_metadata WHERE topic_name = $1', name)!
	// Delete committed offsets
	db.exec_param('DELETE FROM committed_offsets WHERE topic_name = $1', name)!
	// Delete topic
	db.exec_param('DELETE FROM topics WHERE name = $1', name)!

	db.commit()!

	// Update cache
	a.cache_lock.@lock()
	if topic := a.topic_cache[name] {
		a.topic_id_idx.delete(topic.topic_id.hex())
	}
	a.topic_cache.delete(name)
	a.cache_lock.unlock()
}

/// list_topics returns a list of all topics.
pub fn (mut a PostgresStorageAdapter) list_topics() ![]domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, metadata in a.topic_cache {
		result << metadata
	}
	return result
}

/// get_topic retrieves topic metadata.
pub fn (mut a PostgresStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	if metadata := a.topic_cache[name] {
		return metadata
	}
	return error('topic not found: ${name}')
}

/// get_topic_by_id retrieves a topic by topic_id.
pub fn (mut a PostgresStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	topic_id_hex := topic_id.hex()
	if topic_name := a.topic_id_idx[topic_id_hex] {
		if metadata := a.topic_cache[topic_name] {
			return metadata
		}
	}
	return error('topic not found: ${topic_id_hex}')
}

/// add_partitions adds partitions to a topic.
pub fn (mut a PostgresStorageAdapter) add_partitions(name string, new_count int) ! {
	a.cache_lock.rlock()
	topic := a.topic_cache[name] or {
		a.cache_lock.runlock()
		return error('topic not found: ${name}')
	}
	current := topic.partition_count
	a.cache_lock.runlock()

	if new_count <= current {
		return error('new partition count must be greater than current')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// Update topic partition count
	db.exec_param_many('UPDATE topics SET partition_count = $1, updated_at = NOW() WHERE name = $2',
		[new_count.str(), name])!

	// Create new partition metadata entries (batch INSERT)
	new_partition_count := new_count - current
	if new_partition_count > 0 {
		all_params := a.build_partition_metadata_range_params(name, current, new_count)
		query := build_batch_insert_query('partition_metadata', ['topic_name', 'partition_id',
			'base_offset', 'high_watermark'], 4, new_partition_count)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!

	// Update cache
	a.cache_lock.@lock()
	if mut metadata := a.topic_cache[name] {
		a.topic_cache[name] = domain.TopicMetadata{
			...metadata
			partition_count: new_count
		}
	}
	a.cache_lock.unlock()
}
