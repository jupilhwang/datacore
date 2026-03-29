// Infra Layer - PostgreSQL storage adapter: record operations
module postgres

import db.pg
import domain
import encoding.hex
import time

// --- Record metrics helpers ---

fn (mut a PostgresStorageAdapter) inc_append(record_count i64) {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.append_count++
	a.metrics.append_record_count += record_count
}

fn (mut a PostgresStorageAdapter) inc_append_query(elapsed_ms i64) {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
}

fn (mut a PostgresStorageAdapter) inc_fetch() {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.fetch_count++
}

fn (mut a PostgresStorageAdapter) inc_fetch_done(record_count i64, elapsed_ms i64) {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.fetch_record_count += record_count
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
}

// --- Record batch helpers ---

/// build_record_insert_params generates a parameter array for batch record INSERT.
fn (a &PostgresStorageAdapter) build_record_insert_params(topic_name string, partition int, start_offset i64, records []domain.Record, default_time time.Time) []string {
	mut all_params := []string{}
	for i, record in records {
		ts := if record.timestamp.unix() == 0 { default_time } else { record.timestamp }
		key_hex := if record.key.len > 0 { '\\x${record.key.hex()}' } else { '' }
		value_hex := if record.value.len > 0 { '\\x${record.value.hex()}' } else { '' }
		current_offset := start_offset + i64(i)

		all_params << topic_name
		all_params << partition.str()
		all_params << current_offset.str()
		all_params << key_hex
		all_params << value_hex
		all_params << ts.format_rfc3339()
	}
	return all_params
}

/// decode_record_rows decodes PostgreSQL query result rows into a domain.Record array.
fn (a &PostgresStorageAdapter) decode_record_rows(rows []pg.Row) []domain.Record {
	mut records := []domain.Record{}
	for row in rows {
		key_str := get_row_str(&row, 1, '')
		value_str := get_row_str(&row, 2, '')
		ts_str := get_row_str(&row, 3, '')

		// Decode key from hex
		mut key := []u8{}
		if key_str.len > 0 && key_str.starts_with('\\x') {
			key = hex.decode(key_str[2..]) or { []u8{} }
		}

		// Decode value from hex
		mut value := []u8{}
		if value_str.len > 0 && value_str.starts_with('\\x') {
			value = hex.decode(value_str[2..]) or { []u8{} }
		}

		ts := time.parse_rfc3339(ts_str) or { time.now() }

		records << domain.Record{
			key:       key
			value:     value
			timestamp: ts
			headers:   map[string][]u8{}
		}
	}
	return records
}

// --- Record methods ---

/// append adds records to a partition.
/// Controls concurrency using row locks (FOR UPDATE).
pub fn (mut a PostgresStorageAdapter) append(topic_name string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	start_time := time.now()

	a.inc_append(i64(records.len))

	// Check topic exists
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		a.inc_error()
		return error('topic not found: ${topic_name}')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		a.inc_error()
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// Lock partition row for update (row lock)
	rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
		FOR UPDATE
	',
		[topic_name, partition.str()])!

	if rows.len == 0 {
		db.rollback()!
		a.inc_error()
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&rows[0], 0, 0)
	mut high_watermark := get_row_i64(&rows[0], 1, 0)

	now := time.now()
	start_offset := high_watermark

	// Batch INSERT records
	if records.len > 0 {
		all_params := a.build_record_insert_params(topic_name, partition, high_watermark,
			records, now)
		query := build_batch_insert_query('records', ['topic_name', 'partition_id', 'offset_id',
			'record_key', 'record_value', 'timestamp'], 6, records.len)
		db.exec_param_many(query, all_params)!

		high_watermark += i64(records.len)
	}

	// Update high watermark
	db.exec_param_many('
		UPDATE partition_metadata SET high_watermark = \$1 WHERE topic_name = \$2 AND partition_id = \$3
	',
		[high_watermark.str(), topic_name, partition.str()])!

	db.commit()!

	elapsed_ms := time.since(start_time).milliseconds()
	a.inc_append_query(elapsed_ms)

	return domain.AppendResult{
		base_offset:      start_offset
		log_append_time:  now.unix()
		log_start_offset: base_offset
		record_count:     records.len
	}
}

/// fetch retrieves records from a partition.
pub fn (mut a PostgresStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	start_time := time.now()

	a.inc_fetch()

	// Check topic exists
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		a.inc_error()
		return error('topic not found: ${topic_name}')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		a.inc_error()
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// Retrieve partition metadata
	meta_rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
	',
		[topic_name, partition.str()])!

	if meta_rows.len == 0 {
		a.inc_error()
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&meta_rows[0], 0, 0)
	high_watermark := get_row_i64(&meta_rows[0], 1, 0)

	// Return empty result if offset is before base
	if offset < base_offset {
		return domain.FetchResult{
			records:            []
			first_offset:       base_offset
			high_watermark:     high_watermark
			last_stable_offset: high_watermark
			log_start_offset:   base_offset
		}
	}

	// Calculate limit based on max_bytes (rough estimate: 1KB per record)
	max_records := if max_bytes <= 0 { 1000 } else { max_bytes / 1024 }
	limit := if max_records > 1000 { 1000 } else { max_records }

	// Retrieve records
	rows := db.exec_param_many('
		SELECT offset_id, record_key, record_value, timestamp FROM records
		WHERE topic_name = \$1 AND partition_id = \$2 AND offset_id >= \$3
		ORDER BY offset_id ASC
		LIMIT \$4
	',
		[topic_name, partition.str(), offset.str(), limit.str()])!

	// Decode records
	fetched_records := a.decode_record_rows(rows)

	elapsed_ms := time.since(start_time).milliseconds()
	a.inc_fetch_done(i64(fetched_records.len), elapsed_ms)

	// Actual offset of the first returned record
	actual_first_offset := if fetched_records.len > 0 { offset } else { high_watermark }

	return domain.FetchResult{
		records:            fetched_records
		first_offset:       actual_first_offset
		high_watermark:     high_watermark
		last_stable_offset: high_watermark
		log_start_offset:   base_offset
	}
}

/// delete_records deletes records before the specified offset.
pub fn (mut a PostgresStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		return error('topic not found: ${topic_name}')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// Delete records before the offset
	db.exec_param_many('
		DELETE FROM records WHERE topic_name = \$1 AND partition_id = \$2 AND offset_id < \$3
	',
		[topic_name, partition.str(), before_offset.str()])!

	// Update base offset
	db.exec_param_many('
		UPDATE partition_metadata SET base_offset = \$1
		WHERE topic_name = \$2 AND partition_id = \$3 AND base_offset < \$1
	',
		[before_offset.str(), topic_name, partition.str()])!

	db.commit()!
}

/// get_partition_info retrieves partition information.
pub fn (mut a PostgresStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		return error('topic not found: ${topic_name}')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		return error('partition out of range')
	}

	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
	',
		[topic_name, partition.str()])!

	if rows.len == 0 {
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&rows[0], 0, 0)
	high_watermark := get_row_i64(&rows[0], 1, 0)

	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: base_offset
		latest_offset:   high_watermark
		high_watermark:  high_watermark
	}
}
