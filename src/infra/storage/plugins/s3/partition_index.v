// Infra Layer - S3 Partition Index Management
// Handles partition index storage and caching
module s3

import json
import time

/// PartitionIndex is the partition index stored in S3.
struct PartitionIndex {
mut:
	topic           string
	partition       int
	earliest_offset i64
	high_watermark  i64
	log_segments    []LogSegment
}

/// LogSegment represents a log segment stored in S3.
struct LogSegment {
	start_offset i64
	end_offset   i64
	key          string
	size_bytes   i64
	created_at   time.Time
}

/// CachedPartitionIndex holds a cached partition index with metadata.
struct CachedPartitionIndex {
	index     PartitionIndex
	etag      string
	cached_at time.Time
}

/// get_partition_index retrieves a partition index from cache or S3.
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
		// Prefer stale cached version over creating a new empty index if available
		if cached_exists {
			// Refresh cache timestamp while keeping data
			a.topic_lock.@lock()
			a.topic_index_cache[key] = CachedPartitionIndex{
				index:     cached_index
				etag:      cached_etag
				cached_at: time.now()
			}
			a.topic_lock.unlock()
			return cached_index
		}
		// No cache: create new empty index
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
		return a.topic_index_cache[key].index
	}

	// 3. Decode S3 index
	s3_index := json.decode(PartitionIndex, data.bytestr())!

	// 4. Merge with cached index - preserve the higher high_watermark
	// Handles the case where append updated the cache but flush has not yet written to S3
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
