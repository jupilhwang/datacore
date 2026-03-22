// Infra Layer - in-memory storage adapter: record operations
module memory

import domain
import infra.observability
import sync.stdatomic
import time

// --- Record metrics helpers ---

fn (mut a MemoryStorageAdapter) inc_append(record_count int) {
	stdatomic.add_i64(&a.metrics.append_count, 1)
	stdatomic.add_i64(&a.metrics.append_record_count, record_count)
}

fn (mut a MemoryStorageAdapter) inc_append_bytes(bytes i64) {
	// stdatomic.add_i64 takes int delta; cast i64 to int for the call.
	// Byte counts rarely exceed i32 max per call, so this is safe in practice.
	stdatomic.add_i64(&a.metrics.append_bytes, int(bytes))
}

fn (mut a MemoryStorageAdapter) inc_fetch() {
	stdatomic.add_i64(&a.metrics.fetch_count, 1)
}

fn (mut a MemoryStorageAdapter) inc_fetch_records(count i64) {
	stdatomic.add_i64(&a.metrics.fetch_record_count, int(count))
}

// --- Record methods ---

/// append adds records to a partition.
/// Controls concurrency via partition-level locking.
pub fn (mut a MemoryStorageAdapter) append(topic_name string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	a.inc_append(records.len)

	topic := a.lookup_topic_read(topic_name) or {
		a.inc_error()
		return error('topic not found')
	}

	// mmap mode branch (v0.33.0)
	if topic.use_mmap {
		return a.append_mmap(topic, partition, records)
	}

	if partition < 0 || partition >= topic.partitions.len {
		a.inc_error()
		return error('partition out of range')
	}

	// Write lock only for the specific partition
	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	base_offset := part.high_watermark
	now := time.now()

	// Calculate bytes written
	mut bytes_written := i64(0)

	// Append records with timestamps
	for record in records {
		mut r := record
		if r.timestamp.unix() == 0 {
			r = domain.Record{
				...r
				timestamp: now
			}
		}
		part.records << r
		bytes_written += i64(r.key.len + r.value.len)
	}
	part.high_watermark += i64(records.len)

	a.inc_append_bytes(bytes_written)

	// Apply retention policy (max message count)
	if a.config.max_messages_per_partition > 0 {
		excess := part.records.len - a.config.max_messages_per_partition
		if excess > 0 {
			// Move elements in-place using slice without clone
			part.records = part.records[excess..]
			part.base_offset += i64(excess)
			observability.log_with_context('memory', .debug, 'Append', 'Applied retention policy',
				{
				'topic':         topic_name
				'partition':     partition.str()
				'deleted_count': excess.str()
			})
		}
	}

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: part.base_offset
		record_count:     records.len
	}
}

/// append_mmap appends records in mmap mode. (v0.33.0)
fn (mut a MemoryStorageAdapter) append_mmap(topic &TopicStore, partition int, records []domain.Record) !domain.AppendResult {
	if partition < 0 || partition >= topic.mmap_partitions.len {
		return error('partition out of range')
	}

	mut mmap_part := topic.mmap_partitions[partition]
	now := time.now()

	// Convert records to byte arrays
	mut record_bytes := [][]u8{cap: records.len}
	for record in records {
		// Simple serialization: key_len(4) + key + value_len(4) + value
		// Pre-allocate with estimated capacity: 4 (key_len) + key + 4 (value_len) + value
		estimated_cap := 8 + record.key.len + record.value.len
		mut data := []u8{cap: estimated_cap}
		// key length
		key_len := record.key.len
		data << u8(key_len >> 24)
		data << u8(key_len >> 16)
		data << u8(key_len >> 8)
		data << u8(key_len)
		data << record.key
		// value length
		value_len := record.value.len
		data << u8(value_len >> 24)
		data << u8(value_len >> 16)
		data << u8(value_len >> 8)
		data << u8(value_len)
		data << record.value
		record_bytes << data
	}

	base_offset, written := mmap_part.append(record_bytes)!

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: mmap_part.get_base_offset()
		record_count:     written
	}
}

/// fetch retrieves records from a partition.
pub fn (mut a MemoryStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	a.inc_fetch()

	topic := a.lookup_topic_read(topic_name) or {
		a.inc_error()
		return error('topic not found')
	}

	// mmap mode branch (v0.33.0)
	if topic.use_mmap {
		return a.fetch_mmap(topic, partition, offset, max_bytes)
	}

	if partition < 0 || partition >= topic.partitions.len {
		a.inc_error()
		return error('partition out of range')
	}

	// Partition read lock
	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	// Return empty result if offset is out of range
	if offset < part.base_offset {
		return domain.FetchResult{
			records:            []
			first_offset:       part.base_offset
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	start_idx := int(offset - part.base_offset)
	if start_idx >= part.records.len {
		return domain.FetchResult{
			records:            []
			first_offset:       part.high_watermark
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	// Calculate end index based on max_bytes
	mut end_idx := start_idx
	mut total_bytes := 0
	max_fetch_bytes := if max_bytes <= 0 { 1048576 } else { max_bytes }

	for end_idx < part.records.len {
		record_size := part.records[end_idx].key.len + part.records[end_idx].value.len + 50
		if total_bytes + record_size > max_fetch_bytes && end_idx > start_idx {
			break
		}
		total_bytes += record_size
		end_idx++

		if end_idx - start_idx >= 1000 {
			break
		}
	}

	fetched_records := part.records[start_idx..end_idx]

	a.inc_fetch_records(i64(fetched_records.len))

	// Calculate the offset of the first record actually returned
	actual_first_offset := part.base_offset + i64(start_idx)

	return domain.FetchResult{
		records:            fetched_records
		first_offset:       actual_first_offset
		high_watermark:     part.high_watermark
		last_stable_offset: part.high_watermark
		log_start_offset:   part.base_offset
	}
}

/// fetch_mmap retrieves records in mmap mode. (v0.33.0)
fn (mut a MemoryStorageAdapter) fetch_mmap(topic &TopicStore, partition int, offset i64, max_bytes int) !domain.FetchResult {
	if partition < 0 || partition >= topic.mmap_partitions.len {
		return error('partition out of range')
	}

	mut mmap_part := topic.mmap_partitions[partition]
	base_offset := mmap_part.get_base_offset()
	high_watermark := mmap_part.get_high_watermark()

	// Return empty result if offset is out of range
	if offset < base_offset || offset >= high_watermark {
		return domain.FetchResult{
			records:            []
			first_offset:       if offset < base_offset { base_offset } else { high_watermark }
			high_watermark:     high_watermark
			last_stable_offset: high_watermark
			log_start_offset:   base_offset
		}
	}

	// Calculate max records based on max_bytes
	max_records := if max_bytes <= 0 { 1000 } else { max_bytes / 100 }
	record_bytes := mmap_part.read(offset, max_records)!

	// Convert byte arrays to domain.Record
	mut records := []domain.Record{cap: record_bytes.len}
	for data in record_bytes {
		if data.len < 8 {
			continue
		}

		// Deserialize key_len(4) + key + value_len(4) + value
		key_len := int(u32(data[0]) << 24 | u32(data[1]) << 16 | u32(data[2]) << 8 | u32(data[3]))
		if 4 + key_len + 4 > data.len {
			continue
		}

		key := data[4..4 + key_len].clone()
		value_start := 4 + key_len
		value_len := int(u32(data[value_start]) << 24 | u32(data[value_start + 1]) << 16 | u32(data[
			value_start + 2]) << 8 | u32(data[value_start + 3]))

		if value_start + 4 + value_len > data.len {
			continue
		}

		value := data[value_start + 4..value_start + 4 + value_len].clone()

		records << domain.Record{
			key:       key
			value:     value
			timestamp: time.now()
		}
	}

	return domain.FetchResult{
		records:            records
		first_offset:       offset
		high_watermark:     high_watermark
		last_stable_offset: high_watermark
		log_start_offset:   base_offset
	}
}

/// delete_records deletes records before the specified offset.
pub fn (mut a MemoryStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
	topic := a.lookup_topic_read(topic_name) or { return error('topic not found') }

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	delete_count := int(before_offset - part.base_offset)
	if delete_count > 0 && delete_count <= part.records.len {
		// Use slice assignment instead of clone
		part.records = part.records[delete_count..]
		part.base_offset = before_offset
	}
}

/// get_partition_info retrieves partition information.
pub fn (mut a MemoryStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	topic := a.lookup_topic_read(topic_name) or { return error('topic not found') }

	// mmap mode branch (v0.33.0)
	if topic.use_mmap {
		if partition < 0 || partition >= topic.mmap_partitions.len {
			return error('partition out of range')
		}

		mmap_part := topic.mmap_partitions[partition]
		return domain.PartitionInfo{
			topic:           topic_name
			partition:       partition
			earliest_offset: mmap_part.get_base_offset()
			latest_offset:   mmap_part.get_high_watermark()
			high_watermark:  mmap_part.get_high_watermark()
		}
	}

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: part.base_offset
		latest_offset:   part.high_watermark
		high_watermark:  part.high_watermark
	}
}
