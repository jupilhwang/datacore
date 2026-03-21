// Consumer group and offset operations for S3StorageAdapter.
module s3

import domain
import time
import json
import sync.stdatomic
import infra.observability

/// save_group saves a consumer group.
pub fn (mut a S3StorageAdapter) save_group(group domain.ConsumerGroup) ! {
	validate_identifier(group.group_id, 'group_id')!
	key := a.group_key(group.group_id)
	data := json.encode(group)
	a.put_object(key, data.bytes())!

	// Update cache
	a.group_lock.@lock()
	defer { a.group_lock.unlock() }
	a.group_cache[group.group_id] = CachedGroup{
		group:     group
		etag:      ''
		cached_at: time.now()
	}
}

/// load_group loads a consumer group.
pub fn (mut a S3StorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	validate_identifier(group_id, 'group_id')!
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
	defer { a.group_lock.unlock() }
	a.group_cache[group_id] = CachedGroup{
		group:     group
		etag:      etag
		cached_at: time.now()
	}

	return group
}

/// delete_group deletes a consumer group.
pub fn (mut a S3StorageAdapter) delete_group(group_id string) ! {
	validate_identifier(group_id, 'group_id')!
	key := a.group_key(group_id)
	a.delete_object(key)!

	// Also delete offsets
	offsets_prefix := '${a.config.prefix}offsets/${group_id}/'
	a.delete_objects_with_prefix(offsets_prefix)!

	// Remove from cache
	a.group_lock.@lock()
	defer { a.group_lock.unlock() }
	a.group_cache.delete(group_id)
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

/// commit_offsets commits offsets via the batch snapshot path.
/// Offsets are buffered in memory and flushed to S3 as a single binary snapshot
/// by flush_pending_offsets (background worker).
pub fn (mut a S3StorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	if offsets.len == 0 {
		return
	}

	validate_identifier(group_id, 'group_id')!
	for offset in offsets {
		validate_identifier(offset.topic, 'topic')!
	}

	a.record_commit_start_metrics(offsets.len)

	// Buffer all offsets into the batch snapshot path
	a.offset_buffer_lock.lock()
	for offset in offsets {
		a.buffer_offset(group_id, offset)
	}
	a.offset_buffer_lock.unlock()

	// Update offset cache for immediate read-after-write consistency
	a.update_offset_cache(group_id, offsets)
	a.record_commit_success_metrics(offsets.len)
}

/// record_commit_start_metrics records commit start metrics.
fn (mut a S3StorageAdapter) record_commit_start_metrics(count int) {
	stdatomic.add_i64(&a.metrics.offset_commit_count, count)
}

/// update_offset_cache updates the offset cache.
fn (mut a S3StorageAdapter) update_offset_cache(group_id string, succeeded []domain.PartitionOffset) {
	a.offset_lock.@lock()
	defer { a.offset_lock.unlock() }
	if group_id !in a.offset_cache {
		a.offset_cache[group_id] = map[string]i64{}
	}
	for offset in succeeded {
		cache_key := '${offset.topic}:${offset.partition}'
		a.offset_cache[group_id][cache_key] = offset.offset
	}

	observability.log_with_context('s3', .info, 'OffsetCommit', 'Successfully committed offsets',
		{
		'group_id': group_id
		'count':    succeeded.len.str()
	})
}

/// record_commit_success_metrics records successful offset commit metrics.
/// Note: s3_put_count is NOT incremented here because the batch path defers
/// the actual S3 PUT to flush_pending_offsets (background worker).
fn (mut a S3StorageAdapter) record_commit_success_metrics(count int) {
	stdatomic.add_i64(&a.metrics.offset_commit_success_count, count)
}

/// OffsetFetchResult holds the result of a single offset fetch operation.
struct OffsetFetchItem {
	result domain.OffsetFetchResult
}

/// fetch_single_offset fetches a single partition offset from S3.
fn (mut a S3StorageAdapter) fetch_single_offset(group_id string, part domain.TopicPartition, ch chan OffsetFetchItem) {
	key := a.offset_key(group_id, part.topic, part.partition)
	data, _ := a.get_object(key, -1, -1) or {
		ch <- OffsetFetchItem{
			result: domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
		return
	}

	offset_data := json.decode(domain.PartitionOffset, data.bytestr()) or {
		ch <- OffsetFetchItem{
			result: domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
		return
	}

	ch <- OffsetFetchItem{
		result: domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset_data.offset
			metadata:   offset_data.metadata
			error_code: 0
		}
	}
}

/// fetch_offsets retrieves committed offsets.
/// Cache hits are resolved immediately; S3 GETs are issued in parallel.
pub fn (mut a S3StorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	validate_identifier(group_id, 'group_id')!
	for part in partitions {
		validate_identifier(part.topic, 'topic')!
	}

	mut results := []domain.OffsetFetchResult{}

	// Separate cache hits from partitions that require S3 GETs
	mut s3_partitions := []domain.TopicPartition{}
	for part in partitions {
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
		} else {
			s3_partitions << part
		}
	}

	// Fetch remaining partitions from S3 in parallel (bounded concurrency)
	if s3_partitions.len > 0 {
		ch := chan OffsetFetchItem{cap: s3_partitions.len}
		mut active := 0

		for part in s3_partitions {
			// Wait for a slot when concurrency limit is reached
			for active >= max_fetch_concurrent {
				item := <-ch
				results << item.result
				active--
			}
			active++
			spawn a.fetch_single_offset(group_id, part, ch)
		}

		// Collect remaining results
		for _ in 0 .. active {
			item := <-ch
			results << item.result
		}
	}

	return results
}
