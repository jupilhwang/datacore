// Infra Layer - in-memory storage adapter: topic CRUD operations
module memory

import domain
import infra.observability
import rand
import sync.stdatomic

// --- Topic metrics helpers ---

fn (mut a MemoryStorageAdapter) inc_topic_create() {
	stdatomic.add_i64(&a.metrics.topic_create_count, 1)
}

fn (mut a MemoryStorageAdapter) inc_topic_delete() {
	stdatomic.add_i64(&a.metrics.topic_delete_count, 1)
}

fn (mut a MemoryStorageAdapter) inc_topic_lookup() {
	stdatomic.add_i64(&a.metrics.topic_lookup_count, 1)
}

// --- Topic CRUD methods ---

/// create_topic creates a new topic.
/// Automatically generates a UUID v4 format topic_id.
pub fn (mut a MemoryStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	a.inc_topic_create()

	a.topics_lock.@lock()
	defer { a.topics_lock.unlock() }

	if name in a.topics {
		a.inc_error()
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
				a.inc_error()
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
	a.inc_topic_delete()

	a.topics_lock.@lock()
	defer { a.topics_lock.unlock() }

	topic := a.topics[name] or {
		a.inc_error()
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
	a.topics_lock.rlock()
	defer { a.topics_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, topic in a.topics {
		result << topic.metadata
	}
	return result
}

/// get_topic retrieves topic metadata.
pub fn (mut a MemoryStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	a.inc_topic_lookup()

	a.topics_lock.rlock()
	defer { a.topics_lock.runlock() }

	if topic := a.topics[name] {
		return topic.metadata
	}

	a.inc_error()
	return error('topic not found')
}

/// get_topic_by_id retrieves a topic by topic_id.
/// Uses O(1) cache lookup.
pub fn (mut a MemoryStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	a.topics_lock.rlock()
	defer { a.topics_lock.runlock() }

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
	a.topics_lock.@lock()
	defer { a.topics_lock.unlock() }

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
