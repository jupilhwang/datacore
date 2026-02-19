// Infra Layer - PostgreSQL storage adapter
// PostgreSQL-based storage implementation using transactions + row locks
// Utilizes PostgreSQL FOR UPDATE locks for concurrency control
module postgres

import db.pg
import domain
import service.port
import time
import rand
import sync
import encoding.hex

/// LogLevel defines log levels.
enum LogLevel {
	debug
	info
	warn
	error
}

/// log_message prints a structured log message.
fn log_message(level LogLevel, component string, message string, context map[string]string) {
	level_str := match level {
		.debug { '[DEBUG]' }
		.info { '[INFO]' }
		.warn { '[WARN]' }
		.error { '[ERROR]' }
	}

	timestamp := time.now().format_ss()
	mut ctx_str := ''
	if context.len > 0 {
		mut parts := []string{}
		for key, value in context {
			parts << '${key}=${value}'
		}
		ctx_str = ' {${parts.join(', ')}}'
	}

	eprintln('${timestamp} ${level_str} [Postgres:${component}] ${message}${ctx_str}')
}

/// PostgresMetrics tracks metrics for PostgreSQL storage operations.
struct PostgresMetrics {
mut:
	// Query metrics
	query_count       i64
	query_error_count i64
	query_total_ms    i64
	// Topic operation metrics
	topic_create_count i64
	topic_delete_count i64
	topic_lookup_count i64
	// Record operation metrics
	append_count        i64
	append_record_count i64
	fetch_count         i64
	fetch_record_count  i64
	// Offset operation metrics
	offset_commit_count i64
	offset_fetch_count  i64
	// Group operation metrics
	group_save_count   i64
	group_load_count   i64
	group_delete_count i64
	// Error metrics
	error_count i64
}

/// reset_metrics resets all metrics to zero.
fn (mut m PostgresMetrics) reset() {
	m.query_count = 0
	m.query_error_count = 0
	m.query_total_ms = 0
	m.topic_create_count = 0
	m.topic_delete_count = 0
	m.topic_lookup_count = 0
	m.append_count = 0
	m.append_record_count = 0
	m.fetch_count = 0
	m.fetch_record_count = 0
	m.offset_commit_count = 0
	m.offset_fetch_count = 0
	m.group_save_count = 0
	m.group_load_count = 0
	m.group_delete_count = 0
	m.error_count = 0
}

/// get_summary returns a metrics summary as a string.
fn (m &PostgresMetrics) get_summary() string {
	return '[Postgres Metrics]
  Queries: ${m.query_count} total, ${m.query_error_count} errors, ${m.query_total_ms}ms
  Topics: create=${m.topic_create_count}, delete=${m.topic_delete_count}, lookup=${m.topic_lookup_count}
  Records: append=${m.append_count} (${m.append_record_count} records), fetch=${m.fetch_count} (${m.fetch_record_count} records)
  Offsets: commit=${m.offset_commit_count}, fetch=${m.offset_fetch_count}
  Groups: save=${m.group_save_count}, load=${m.group_load_count}, delete=${m.group_delete_count}
  Errors: ${m.error_count}'
}

/// PostgresStorageAdapter implements port.StoragePort.
/// Stores topics, records, and consumer groups using PostgreSQL.
/// Controls concurrency using transactions and row locks (FOR UPDATE).
pub struct PostgresStorageAdapter {
pub mut:
	config PostgresConfig
mut:
	pool             &pg.ConnectionPool
	cluster_metadata &PostgresClusterMetadataPort = unsafe { nil }
	topic_cache      map[string]domain.TopicMetadata
	topic_id_idx     map[string]string
	cache_lock       sync.RwMutex
	initialized      bool
	// Metrics
	metrics      PostgresMetrics
	metrics_lock sync.Mutex
}

/// PostgresConfig holds the PostgreSQL storage configuration.
pub struct PostgresConfig {
pub:
	host      string = 'localhost'
	port      int    = 5432
	user      string = 'datacore'
	password  string
	database  string = 'datacore'
	pool_size int    = 10
	sslmode   string = 'disable'
}

/// postgres_capability defines the storage capabilities of the PostgreSQL adapter.
pub const postgres_capability = domain.StorageCapability{
	name:                  'postgresql'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// new_postgres_adapter creates a new PostgreSQL storage adapter.
/// Initializes the connection pool, creates the schema, and loads the topic cache.
pub fn new_postgres_adapter(config PostgresConfig) !&PostgresStorageAdapter {
	// Build PostgreSQL connection string (including sslmode)
	conninfo := 'host=${config.host} port=${config.port} user=${config.user} password=${config.password} dbname=${config.database} sslmode=${config.sslmode}'

	// Test single connection using connection string
	test_conn := pg.connect_with_conninfo(conninfo)!
	test_conn.close() or {}

	// Create pg.Config (for pool creation)
	pg_config := pg.Config{
		host:     config.host
		port:     config.port
		user:     config.user
		password: config.password
		dbname:   config.database
	}

	pool := pg.new_connection_pool(pg_config, config.pool_size)!

	mut adapter := &PostgresStorageAdapter{
		config:           config
		pool:             &pool
		cluster_metadata: unsafe { nil }
		topic_cache:      map[string]domain.TopicMetadata{}
		topic_id_idx:     map[string]string{}
		initialized:      false
	}

	adapter.init_schema()!
	adapter.load_topic_cache()!

	// Initialize cluster metadata port for multi-broker support
	cluster_id := 'datacore-cluster' // TODO: make configurable
	adapter.cluster_metadata = new_cluster_metadata_port(adapter.pool, cluster_id)!

	adapter.initialized = true

	return adapter
}

/// get_row_str is a helper function that safely retrieves a string from a row value.
/// Returns the default value if the index is out of range or the value is none.
fn get_row_str(row &pg.Row, idx int, default_val string) string {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	return val_opt or { default_val }
}

/// get_row_int is a helper function that safely retrieves an integer from a row value.
/// Returns the default value if the index is out of range or the value is none.
fn get_row_int(row &pg.Row, idx int, default_val int) int {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	val := val_opt or { return default_val }
	return val.int()
}

/// get_row_i64 is a helper function that safely retrieves an i64 from a row value.
/// Returns the default value if the index is out of range or the value is none.
fn get_row_i64(row &pg.Row, idx int, default_val i64) i64 {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	val := val_opt or { return default_val }
	return val.i64()
}

/// build_batch_insert_query is a helper function that generates a batch INSERT query.
/// Takes the number of parameters and rows and generates the VALUES clause.
fn build_batch_insert_query(table string, columns []string, params_per_row int, row_count int) string {
	mut values_parts := []string{}
	for i in 0 .. row_count {
		param_idx := i * params_per_row + 1
		mut placeholders := []string{}
		for j in 0 .. params_per_row {
			placeholders << '\$${param_idx + j}'
		}
		values_parts << '(${placeholders.join(', ')})'
	}
	values_clause := values_parts.join(', ')
	columns_clause := columns.join(', ')
	return 'INSERT INTO ${table} (${columns_clause}) VALUES ${values_clause}'
}

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

/// build_offset_commit_params generates a parameter array for batch offset commit UPSERT.
fn (a &PostgresStorageAdapter) build_offset_commit_params(group_id string, offsets []domain.PartitionOffset) []string {
	mut all_params := []string{}
	for offset in offsets {
		all_params << group_id
		all_params << offset.topic
		all_params << offset.partition.str()
		all_params << offset.offset.str()
		all_params << offset.metadata
	}
	return all_params
}

/// build_offset_upsert_query generates a batch UPSERT query for offset commits.
fn (a &PostgresStorageAdapter) build_offset_upsert_query(offset_count int) string {
	mut values_parts := []string{}
	for i in 0 .. offset_count {
		param_idx := i * 5 + 1
		values_parts << '(\$${param_idx}, \$${param_idx + 1}, \$${param_idx + 2}, \$${param_idx + 3}, \$${
			param_idx + 4})'
	}
	values_clause := values_parts.join(', ')
	return 'INSERT INTO committed_offsets (group_id, topic_name, partition_id, committed_offset, metadata) VALUES ${values_clause} ON CONFLICT (group_id, topic_name, partition_id) DO UPDATE SET committed_offset = EXCLUDED.committed_offset, metadata = EXCLUDED.metadata, committed_at = NOW()'
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

/// init_schema initializes the database schema.
/// Creates topics, records, partition_metadata, consumer_groups, and committed_offsets tables.
/// Skips tables that already exist (IF NOT EXISTS).
fn (mut a PostgresStorageAdapter) init_schema() ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	// Topics table
	db.exec("
		CREATE TABLE IF NOT EXISTS topics (
			name VARCHAR(255) PRIMARY KEY,
			topic_id BYTEA NOT NULL UNIQUE,
			partition_count INT NOT NULL,
			is_internal BOOLEAN DEFAULT FALSE,
			config JSONB DEFAULT '{}',
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)
	")!

	// Records table with partitioning support
	db.exec("
		CREATE TABLE IF NOT EXISTS records (
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			offset_id BIGINT NOT NULL,
			record_key BYTEA,
			record_value BYTEA,
			headers JSONB DEFAULT '[]',
			timestamp TIMESTAMPTZ NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (topic_name, partition_id, offset_id)
		)
	")!

	// Partition metadata table
	db.exec('
		CREATE TABLE IF NOT EXISTS partition_metadata (
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			base_offset BIGINT DEFAULT 0,
			high_watermark BIGINT DEFAULT 0,
			PRIMARY KEY (topic_name, partition_id)
		)
	')!

	// Consumer groups table
	db.exec("
		CREATE TABLE IF NOT EXISTS consumer_groups (
			group_id VARCHAR(255) PRIMARY KEY,
			protocol_type VARCHAR(50),
			state VARCHAR(50) NOT NULL,
			generation_id INT DEFAULT 0,
			leader VARCHAR(255),
			protocol VARCHAR(255),
			members JSONB DEFAULT '[]',
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW()
		)
	")!

	// Committed offsets table
	db.exec("
		CREATE TABLE IF NOT EXISTS committed_offsets (
			group_id VARCHAR(255) NOT NULL,
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			committed_offset BIGINT NOT NULL,
			metadata VARCHAR(255) DEFAULT '',
			committed_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (group_id, topic_name, partition_id)
		)
	")!

	// Create indexes
	db.exec('CREATE INDEX IF NOT EXISTS idx_records_topic_partition ON records(topic_name, partition_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_records_offset ON records(topic_name, partition_id, offset_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_committed_offsets_group ON committed_offsets(group_id)')!
}

/// load_topic_cache loads all topics into the in-memory cache.
/// Called once at startup to optimize topic lookup performance.
fn (mut a PostgresStorageAdapter) load_topic_cache() ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec('SELECT name, topic_id, partition_count, is_internal FROM topics')!

	a.cache_lock.@lock()
	defer { a.cache_lock.unlock() }

	for row in rows {
		name := get_row_str(&row, 0, '')
		if name == '' {
			continue
		}
		topic_id_str := get_row_str(&row, 1, '')
		partition_count := get_row_int(&row, 2, 0)
		is_internal_str := get_row_str(&row, 3, 'f')
		is_internal := is_internal_str == 't' || is_internal_str == 'true'

		// Decode topic_id from hex (remove \x prefix)
		clean_id := topic_id_str.replace('\\x', '')
		topic_id := hex.decode(clean_id) or { []u8{} }

		metadata := domain.TopicMetadata{
			name:            name
			topic_id:        topic_id
			partition_count: partition_count
			is_internal:     is_internal
			config:          map[string]string{}
		}

		a.topic_cache[name] = metadata
		a.topic_id_idx[topic_id.hex()] = name
	}
}

/// create_topic creates a new topic.
/// Automatically generates a UUID v4 format topic_id.
pub fn (mut a PostgresStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	start_time := time.now()

	// Metrics: topic creation start
	a.metrics_lock.@lock()
	a.metrics.topic_create_count++
	a.metrics_lock.unlock()

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

	// Metrics: record query time
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
	a.metrics_lock.unlock()

	log_message(.info, 'Topic', 'Topic created', {
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
	return error('topic not found')
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
	return error('topic not found')
}

/// add_partitions adds partitions to a topic.
pub fn (mut a PostgresStorageAdapter) add_partitions(name string, new_count int) ! {
	a.cache_lock.rlock()
	topic := a.topic_cache[name] or {
		a.cache_lock.runlock()
		return error('topic not found')
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

/// append adds records to a partition.
/// Controls concurrency using row locks (FOR UPDATE).
pub fn (mut a PostgresStorageAdapter) append(topic_name string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	start_time := time.now()

	// Metrics: append start
	a.metrics_lock.@lock()
	a.metrics.append_count++
	a.metrics.append_record_count += i64(records.len)
	a.metrics_lock.unlock()

	// Check topic exists
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
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
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
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

	// Metrics: record query time
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
	a.metrics_lock.unlock()

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

	// Metrics: fetch start
	a.metrics_lock.@lock()
	a.metrics.fetch_count++
	a.metrics_lock.unlock()

	// Check topic exists
	a.cache_lock.rlock()
	topic := a.topic_cache[topic_name] or {
		a.cache_lock.runlock()
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
		return error('topic not found')
	}
	a.cache_lock.runlock()

	if partition < 0 || partition >= topic.partition_count {
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
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
		// Metrics: error
		a.metrics_lock.@lock()
		a.metrics.error_count++
		a.metrics_lock.unlock()
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

	// Metrics: fetched record count and query time
	elapsed_ms := time.since(start_time).milliseconds()
	a.metrics_lock.@lock()
	a.metrics.fetch_record_count += i64(fetched_records.len)
	a.metrics.query_count++
	a.metrics.query_total_ms += elapsed_ms
	a.metrics_lock.unlock()

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
		return error('topic not found')
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
		return error('topic not found')
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

/// save_group saves a consumer group.
pub fn (mut a PostgresStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	state_str := match group.state {
		.empty { 'empty' }
		.preparing_rebalance { 'preparing_rebalance' }
		.completing_rebalance { 'completing_rebalance' }
		.stable { 'stable' }
		.dead { 'dead' }
	}

	// Upsert group
	db.exec_param_many('
		INSERT INTO consumer_groups (group_id, protocol_type, state, generation_id, leader, protocol)
		VALUES (\$1, \$2, \$3, \$4, \$5, \$6)
		ON CONFLICT (group_id) DO UPDATE SET
			protocol_type = EXCLUDED.protocol_type,
			state = EXCLUDED.state,
			generation_id = EXCLUDED.generation_id,
			leader = EXCLUDED.leader,
			protocol = EXCLUDED.protocol,
			updated_at = NOW()
	',
		[
		group.group_id,
		group.protocol_type,
		state_str,
		group.generation_id.str(),
		group.leader,
		group.protocol,
	])!
}

/// load_group loads a consumer group.
pub fn (mut a PostgresStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec_param('
		SELECT group_id, protocol_type, state, generation_id, leader, protocol
		FROM consumer_groups WHERE group_id = \$1
	',
		group_id)!

	if rows.len == 0 {
		return error('group not found')
	}

	row := rows[0]
	state_str := get_row_str(&row, 2, 'empty')
	state := match state_str {
		'preparing_rebalance' { domain.GroupState.preparing_rebalance }
		'completing_rebalance' { domain.GroupState.completing_rebalance }
		'stable' { domain.GroupState.stable }
		'dead' { domain.GroupState.dead }
		else { domain.GroupState.empty }
	}

	return domain.ConsumerGroup{
		group_id:      get_row_str(&row, 0, '')
		protocol_type: get_row_str(&row, 1, '')
		state:         state
		generation_id: get_row_int(&row, 3, 0)
		leader:        get_row_str(&row, 4, '')
		protocol:      get_row_str(&row, 5, '')
		members:       []domain.GroupMember{}
	}
}

/// delete_group deletes a consumer group.
pub fn (mut a PostgresStorageAdapter) delete_group(group_id string) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!
	db.exec_param('DELETE FROM committed_offsets WHERE group_id = $1', group_id)!
	db.exec_param('DELETE FROM consumer_groups WHERE group_id = $1', group_id)!
	db.commit()!
}

/// list_groups returns a list of all consumer groups.
pub fn (mut a PostgresStorageAdapter) list_groups() ![]domain.GroupInfo {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	rows := db.exec('SELECT group_id, protocol_type, state FROM consumer_groups')!

	mut result := []domain.GroupInfo{}
	for row in rows {
		state_str := get_row_str(&row, 2, 'Empty')
		state := match state_str {
			'preparing_rebalance' { 'PreparingRebalance' }
			'completing_rebalance' { 'CompletingRebalance' }
			'stable' { 'Stable' }
			'dead' { 'Dead' }
			else { 'Empty' }
		}

		result << domain.GroupInfo{
			group_id:      get_row_str(&row, 0, '')
			protocol_type: get_row_str(&row, 1, '')
			state:         state
		}
	}
	return result
}

/// commit_offsets commits offsets.
pub fn (mut a PostgresStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	// Perform batch UPSERT
	if offsets.len > 0 {
		all_params := a.build_offset_commit_params(group_id, offsets)
		query := a.build_offset_upsert_query(offsets.len)
		db.exec_param_many(query, all_params)!
	}

	db.commit()!
}

/// fetch_offsets retrieves committed offsets.
pub fn (mut a PostgresStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	mut results := []domain.OffsetFetchResult{}

	if partitions.len == 0 {
		return results
	}

	// Retrieve offsets for all partitions in a single query (using OR conditions)
	mut topic_partition_pairs := []string{}
	mut all_params := []string{}
	all_params << group_id

	for i, part in partitions {
		param_idx := i * 2 + 2
		topic_partition_pairs << '(topic_name = \$${param_idx} AND partition_id = \$${param_idx + 1})'
		all_params << part.topic
		all_params << part.partition.str()
	}

	where_clause := topic_partition_pairs.join(' OR ')
	query := 'SELECT topic_name, partition_id, committed_offset, metadata FROM committed_offsets WHERE group_id = \$1 AND (${where_clause})'
	rows := db.exec_param_many(query, all_params)!

	// Convert results to map
	mut offset_map := map[string]domain.OffsetFetchResult{}
	for row in rows {
		topic := get_row_str(&row, 0, '')
		partition := get_row_int(&row, 1, 0)
		key := '${topic}:${partition}'
		offset_map[key] = domain.OffsetFetchResult{
			topic:      topic
			partition:  partition
			offset:     get_row_i64(&row, 2, -1)
			metadata:   get_row_str(&row, 3, '')
			error_code: 0
		}
	}

	// Generate results for all requested partitions
	for part in partitions {
		key := '${part.topic}:${part.partition}'
		if result := offset_map[key] {
			results << result
		} else {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
	}

	return results
}

/// health_check checks the storage health status.
pub fn (mut a PostgresStorageAdapter) health_check() !port.HealthStatus {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.exec('SELECT 1')!
	return .healthy
}

/// get_storage_capability returns storage capability information.
pub fn (a &PostgresStorageAdapter) get_storage_capability() domain.StorageCapability {
	return postgres_capability
}

/// get_cluster_metadata_port returns the cluster metadata port.
/// PostgreSQL supports multi-broker mode.
pub fn (a &PostgresStorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	if a.cluster_metadata != unsafe { nil } {
		return a.cluster_metadata
	}
	return none
}

/// StorageStats provides storage statistics.
/// Contains a summary of the current database state.
pub struct StorageStats {
pub:
	topic_count      int
	total_partitions int
	total_records    i64
	group_count      int
}

/// get_stats returns current storage statistics.
/// Queries topic count, partition count, record count, and consumer group count.
pub fn (mut a PostgresStorageAdapter) get_stats() !StorageStats {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	topic_count := db.q_int('SELECT COUNT(*) FROM topics')!
	total_partitions := db.q_int('SELECT COUNT(*) FROM partition_metadata')!
	total_records := db.q_int('SELECT COUNT(*) FROM records')!
	group_count := db.q_int('SELECT COUNT(*) FROM consumer_groups')!

	return StorageStats{
		topic_count:      topic_count
		total_partitions: total_partitions
		total_records:    total_records
		group_count:      group_count
	}
}

/// close closes all connections in the pool.
/// Must be called when the adapter is no longer needed to release resources.
pub fn (mut a PostgresStorageAdapter) close() {
	a.pool.close()
}

/// get_metrics returns a snapshot of current metrics.
pub fn (mut a PostgresStorageAdapter) get_metrics() PostgresMetrics {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics
}

/// get_metrics_summary returns a metrics summary string.
pub fn (mut a PostgresStorageAdapter) get_metrics_summary() string {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	return a.metrics.get_summary()
}

/// reset_metrics resets all metrics to zero.
pub fn (mut a PostgresStorageAdapter) reset_metrics() {
	a.metrics_lock.@lock()
	defer {
		a.metrics_lock.unlock()
	}
	a.metrics.reset()
}
