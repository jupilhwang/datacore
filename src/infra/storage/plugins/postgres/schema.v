// Infra Layer - PostgreSQL storage adapter: schema initialization
module postgres

import domain
import encoding.hex

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

	// Share partition states table for share group persistence
	db.exec("
		CREATE TABLE IF NOT EXISTS share_partition_states (
			group_id VARCHAR(255) NOT NULL,
			topic_name VARCHAR(255) NOT NULL,
			partition_id INT NOT NULL,
			start_offset BIGINT DEFAULT 0,
			end_offset BIGINT DEFAULT 0,
			record_states JSONB DEFAULT '{}',
			acquired_records JSONB DEFAULT '{}',
			delivery_counts JSONB DEFAULT '{}',
			total_acquired BIGINT DEFAULT 0,
			total_acknowledged BIGINT DEFAULT 0,
			total_released BIGINT DEFAULT 0,
			total_rejected BIGINT DEFAULT 0,
			created_at TIMESTAMPTZ DEFAULT NOW(),
			updated_at TIMESTAMPTZ DEFAULT NOW(),
			PRIMARY KEY (group_id, topic_name, partition_id)
		)
	")!

	// Create indexes
	db.exec('CREATE INDEX IF NOT EXISTS idx_records_topic_partition ON records(topic_name, partition_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_records_offset ON records(topic_name, partition_id, offset_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_committed_offsets_group ON committed_offsets(group_id)')!
	db.exec('CREATE INDEX IF NOT EXISTS idx_share_partition_states_group ON share_partition_states(group_id)')!
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
