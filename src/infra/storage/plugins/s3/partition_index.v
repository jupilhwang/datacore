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

/// RecordIndex maps a record offset to its byte position within a segment.
/// Used for S3 Range Request optimization to avoid downloading entire segments.
struct RecordIndex {
	offset        i64
	byte_position i64
}

/// LogSegment represents a log segment stored in S3.
struct LogSegment {
	start_offset i64
	end_offset   i64
	key          string
	size_bytes   i64
	created_at   time.Time
	record_index []RecordIndex
}

/// find_byte_range returns the (byte_start, byte_end) for a target offset
/// using binary search on the record index.
/// Returns (-1, -1) when no index is available (fallback to full download).
fn (seg &LogSegment) find_byte_range(target_offset i64, max_bytes int) (i64, i64) {
	if seg.record_index.len == 0 {
		return i64(-1), i64(-1)
	}

	idx := binary_search_record_index(seg.record_index, target_offset)
	if idx < 0 {
		return i64(-1), i64(-1)
	}

	byte_start := seg.record_index[idx].byte_position

	// Determine the upper bound: next record position or segment end
	mut next_pos := seg.size_bytes
	if idx + 1 < seg.record_index.len {
		next_pos = seg.record_index[idx + 1].byte_position
	}

	mut byte_end := next_pos - 1
	if max_bytes > 0 {
		capped := byte_start + i64(max_bytes)
		if capped < byte_end {
			byte_end = capped
		}
	}

	return byte_start, byte_end
}

/// binary_search_record_index finds the index of the entry whose offset
/// matches or is the largest offset <= target_offset.
/// Returns -1 if target_offset is before all entries.
fn binary_search_record_index(index []RecordIndex, target_offset i64) int {
	mut lo := 0
	mut hi := index.len - 1
	mut result := -1

	for lo <= hi {
		mid := lo + (hi - lo) / 2
		if index[mid].offset <= target_offset {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return result
}

/// CachedPartitionIndex holds a cached partition index with metadata.
struct CachedPartitionIndex {
	index     PartitionIndex
	etag      string
	cached_at time.Time
}

/// get_cached_index_if_valid returns the cached index if TTL is not expired.
/// Returns the index and true if cache hit; empty index and false if miss or expired.
fn (mut a S3StorageAdapter) get_cached_index_if_valid(key string) (PartitionIndex, string, bool) {
	a.cache.topic_lock.rlock()
	defer { a.cache.topic_lock.runlock() }
	if cached := a.cache.topic_index_cache[key] {
		if time.since(cached.cached_at).milliseconds() < a.config.index_cache_ttl_ms {
			return cached.index, cached.etag, true
		}
		return cached.index, cached.etag, false
	}
	return PartitionIndex{}, '', false
}

/// refresh_stale_cache re-reads the current cache under write lock to preserve
/// concurrent reserve_offsets updates and extends TTL.
fn (mut a S3StorageAdapter) refresh_stale_cache(key string, stale_index PartitionIndex, etag string) PartitionIndex {
	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	mut refreshed := stale_index
	if current := a.cache.topic_index_cache[key] {
		if current.index.high_watermark > refreshed.high_watermark {
			refreshed = current.index
		}
	}
	a.cache.topic_index_cache[key] = CachedPartitionIndex{
		index:     refreshed
		etag:      etag
		cached_at: time.now()
	}
	return refreshed
}

/// create_empty_partition_index creates and caches a new empty partition index.
fn (mut a S3StorageAdapter) create_empty_partition_index(key string, topic string, partition int) PartitionIndex {
	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	empty := PartitionIndex{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		high_watermark:  0
		log_segments:    []
	}
	a.cache.topic_index_cache[key] = CachedPartitionIndex{
		index:     empty
		etag:      ''
		cached_at: time.now()
	}
	return empty
}

/// merge_s3_index_with_cache merges an S3-fetched index with the current cache
/// to preserve the higher high_watermark.
fn (mut a S3StorageAdapter) merge_s3_index_with_cache(key string, s3_index PartitionIndex, etag string) PartitionIndex {
	a.cache.topic_lock.@lock()
	defer { a.cache.topic_lock.unlock() }
	mut final_index := s3_index
	if current := a.cache.topic_index_cache[key] {
		if current.index.high_watermark > final_index.high_watermark {
			final_index.high_watermark = current.index.high_watermark
		}
	}
	a.cache.topic_index_cache[key] = CachedPartitionIndex{
		index:     final_index
		etag:      etag
		cached_at: time.now()
	}
	return final_index
}

/// get_partition_index retrieves a partition index from cache or S3.
fn (mut a S3StorageAdapter) get_partition_index(topic string, partition int) !PartitionIndex {
	key := '${topic}:${partition}'

	cached_index, cached_etag, valid := a.get_cached_index_if_valid(key)
	if valid {
		return cached_index
	}
	cache_exists := cached_index.topic.len > 0 || cached_etag.len > 0

	index_key := a.partition_index_key(topic, partition)
	data, etag := a.get_object(index_key, -1, -1) or {
		if cache_exists {
			return a.refresh_stale_cache(key, cached_index, cached_etag)
		}
		return a.create_empty_partition_index(key, topic, partition)
	}

	s3_index := json.decode(PartitionIndex, data.bytestr())!
	return a.merge_s3_index_with_cache(key, s3_index, etag)
}
