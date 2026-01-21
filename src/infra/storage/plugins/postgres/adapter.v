// Infra Layer - PostgreSQL Storage Adapter
// PostgreSQL-based storage with Transaction + Row Lock for concurrency control
module postgres

import db.pg
import domain
import service.port
import time
import rand
import sync
import encoding.hex

// PostgresStorageAdapter implements port.StoragePort
pub struct PostgresStorageAdapter {
pub mut:
	config PostgresConfig
mut:
	pool             &pg.ConnectionPool
	cluster_metadata &PostgresClusterMetadataPort = unsafe { nil }
	topic_cache      map[string]domain.TopicMetadata // topic_name -> metadata
	topic_id_idx     map[string]string               // topic_id (hex) -> topic_name
	cache_lock       sync.RwMutex
	initialized      bool
}

// PostgresConfig holds PostgreSQL storage configuration
pub struct PostgresConfig {
pub:
	host      string = 'localhost'
	port      int    = 5432
	user      string = 'datacore'
	password  string
	database  string = 'datacore'
	pool_size int    = 10
}

// Storage capability for PostgreSQL adapter
pub const postgres_capability = domain.StorageCapability{
	name:                  'postgresql'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

// new_postgres_adapter creates a new PostgreSQL storage adapter
pub fn new_postgres_adapter(config PostgresConfig) !&PostgresStorageAdapter {
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
	cluster_id := 'datacore-cluster' // TODO: Make configurable
	adapter.cluster_metadata = new_cluster_metadata_port(adapter.pool, cluster_id)!

	adapter.initialized = true

	return adapter
}

// Helper function to safely get string from row value
fn get_row_str(row &pg.Row, idx int, default_val string) string {
	if idx >= row.vals.len {
		return default_val
	}
	val := row.vals[idx] or { return default_val }
	return val
}

// Helper function to safely get int from row value
fn get_row_int(row &pg.Row, idx int, default_val int) int {
	if idx >= row.vals.len {
		return default_val
	}
	val := row.vals[idx] or { return default_val }
	return val.int()
}

// Helper function to safely get i64 from row value
fn get_row_i64(row &pg.Row, idx int, default_val i64) i64 {
	if idx >= row.vals.len {
		return default_val
	}
	val := row.vals[idx] or { return default_val }
	return val.i64()
}

// init_schema initializes the database schema
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

// load_topic_cache loads all topics into memory cache
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

		// Decode topic_id from hex (removing \x prefix if present)
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

// ============================================================
// Topic Operations
// ============================================================

pub fn (mut a PostgresStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
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

	// Create partition metadata entries
	for p in 0 .. partitions {
		db.exec_param_many('
			INSERT INTO partition_metadata (topic_name, partition_id, base_offset, high_watermark)
			VALUES (\$1, \$2, 0, 0)
		',
			[name, p.str()])!
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

	return metadata
}

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

pub fn (mut a PostgresStorageAdapter) list_topics() ![]domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, metadata in a.topic_cache {
		result << metadata
	}
	return result
}

pub fn (mut a PostgresStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	a.cache_lock.rlock()
	defer { a.cache_lock.runlock() }

	if metadata := a.topic_cache[name] {
		return metadata
	}
	return error('topic not found')
}

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

	// Create new partition metadata entries
	for p in current .. new_count {
		db.exec_param_many('
			INSERT INTO partition_metadata (topic_name, partition_id, base_offset, high_watermark)
			VALUES (\$1, \$2, 0, 0)
		',
			[name, p.str()])!
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

// ============================================================
// Record Operations
// ============================================================

pub fn (mut a PostgresStorageAdapter) append(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
	// Verify topic exists
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

	// Lock partition row for update (Row Lock)
	rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
		FOR UPDATE
	',
		[topic_name, partition.str()])!

	if rows.len == 0 {
		db.rollback()!
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&rows[0], 0, 0)
	mut high_watermark := get_row_i64(&rows[0], 1, 0)

	now := time.now()
	start_offset := high_watermark

	// Insert records
	for record in records {
		ts := if record.timestamp.unix() == 0 { now } else { record.timestamp }

		key_hex := if record.key.len > 0 { '\\x${record.key.hex()}' } else { '' }
		value_hex := if record.value.len > 0 { '\\x${record.value.hex()}' } else { '' }

		db.exec_param_many('
			INSERT INTO records (topic_name, partition_id, offset_id, record_key, record_value, timestamp)
			VALUES (\$1, \$2, \$3, \$4, \$5, \$6)
		',
			[
			topic_name,
			partition.str(),
			high_watermark.str(),
			key_hex,
			value_hex,
			ts.format_rfc3339(),
		])!

		high_watermark++
	}

	// Update high watermark
	db.exec_param_many('
		UPDATE partition_metadata SET high_watermark = \$1 WHERE topic_name = \$2 AND partition_id = \$3
	',
		[high_watermark.str(), topic_name, partition.str()])!

	db.commit()!

	return domain.AppendResult{
		base_offset:      start_offset
		log_append_time:  now.unix()
		log_start_offset: base_offset
		record_count:     records.len
	}
}

pub fn (mut a PostgresStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	// Verify topic exists
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

	// Get partition metadata
	meta_rows := db.exec_param_many('
		SELECT base_offset, high_watermark FROM partition_metadata
		WHERE topic_name = \$1 AND partition_id = \$2
	',
		[topic_name, partition.str()])!

	if meta_rows.len == 0 {
		return error('partition metadata not found')
	}

	base_offset := get_row_i64(&meta_rows[0], 0, 0)
	high_watermark := get_row_i64(&meta_rows[0], 1, 0)

	// Return empty if offset is before base
	if offset < base_offset {
		return domain.FetchResult{
			records:            []
			high_watermark:     high_watermark
			last_stable_offset: high_watermark
			log_start_offset:   base_offset
		}
	}

	// Calculate limit based on max_bytes (rough estimate: 1KB per record)
	max_records := if max_bytes <= 0 { 1000 } else { max_bytes / 1024 }
	limit := if max_records > 1000 { 1000 } else { max_records }

	// Fetch records
	rows := db.exec_param_many('
		SELECT offset_id, record_key, record_value, timestamp FROM records
		WHERE topic_name = \$1 AND partition_id = \$2 AND offset_id >= \$3
		ORDER BY offset_id ASC
		LIMIT \$4
	',
		[topic_name, partition.str(), offset.str(), limit.str()])!

	mut fetched_records := []domain.Record{}
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

		fetched_records << domain.Record{
			key:       key
			value:     value
			timestamp: ts
			headers:   map[string][]u8{}
		}
	}

	return domain.FetchResult{
		records:            fetched_records
		high_watermark:     high_watermark
		last_stable_offset: high_watermark
		log_start_offset:   base_offset
	}
}

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

	// Delete records before offset
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

// ============================================================
// Offset Operations
// ============================================================

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

// ============================================================
// Consumer Group Operations
// ============================================================

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

pub fn (mut a PostgresStorageAdapter) delete_group(group_id string) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!
	db.exec_param('DELETE FROM committed_offsets WHERE group_id = $1', group_id)!
	db.exec_param('DELETE FROM consumer_groups WHERE group_id = $1', group_id)!
	db.commit()!
}

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

// ============================================================
// Offset Commit/Fetch
// ============================================================

pub fn (mut a PostgresStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.begin()!

	for offset in offsets {
		db.exec_param_many('
			INSERT INTO committed_offsets (group_id, topic_name, partition_id, committed_offset, metadata)
			VALUES (\$1, \$2, \$3, \$4, \$5)
			ON CONFLICT (group_id, topic_name, partition_id) DO UPDATE SET
				committed_offset = EXCLUDED.committed_offset,
				metadata = EXCLUDED.metadata,
				committed_at = NOW()
		',
			[group_id, offset.topic, offset.partition.str(), offset.offset.str(), offset.metadata])!
	}

	db.commit()!
}

pub fn (mut a PostgresStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	mut results := []domain.OffsetFetchResult{}

	for part in partitions {
		rows := db.exec_param_many('
			SELECT committed_offset, metadata FROM committed_offsets
			WHERE group_id = \$1 AND topic_name = \$2 AND partition_id = \$3
		',
			[group_id, part.topic, part.partition.str()])!

		if rows.len > 0 {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     get_row_i64(&rows[0], 0, -1)
				metadata:   get_row_str(&rows[0], 1, '')
				error_code: 0
			}
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

// ============================================================
// Health Check
// ============================================================

pub fn (mut a PostgresStorageAdapter) health_check() !port.HealthStatus {
	mut db := a.pool.acquire()!
	defer { a.pool.release(db) }

	db.exec('SELECT 1')!
	return .healthy
}

// ============================================================
// Multi-Broker Support
// ============================================================

pub fn (a &PostgresStorageAdapter) get_storage_capability() domain.StorageCapability {
	return postgres_capability
}

pub fn (a &PostgresStorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	// PostgreSQL supports multi-broker mode
	if a.cluster_metadata != unsafe { nil } {
		return a.cluster_metadata
	}
	return none
}

// ============================================================
// Stats and Utilities
// ============================================================

// StorageStats provides storage statistics
pub struct StorageStats {
pub:
	topic_count      int
	total_partitions int
	total_records    i64
	group_count      int
}

// get_stats returns current storage statistics
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

// close closes all connections in the pool
pub fn (mut a PostgresStorageAdapter) close() {
	a.pool.close()
}
