// Infra Layer - PostgreSQL storage adapter
// PostgreSQL-based storage implementation using transactions + row locks
// Utilizes PostgreSQL FOR UPDATE locks for concurrency control
module postgres

import db.pg
import domain
import service.port
import sync

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
	// Share group state metrics
	share_save_count   i64
	share_load_count   i64
	share_delete_count i64
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
	m.share_save_count = 0
	m.share_load_count = 0
	m.share_delete_count = 0
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
  ShareGroups: save=${m.share_save_count}, load=${m.share_load_count}, delete=${m.share_delete_count}
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
	host       string = 'localhost'
	port       int    = 5432
	user       string = 'datacore'
	password   string
	database   string = 'datacore'
	pool_size  int    = 10
	sslmode    string = 'disable'
	cluster_id string = 'datacore-cluster'
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
	adapter.cluster_metadata = new_cluster_metadata_port(adapter.pool, config.cluster_id)!

	adapter.initialized = true

	return adapter
}

// --- Cross-category helpers ---

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

// --- Cross-category metrics helper ---

fn (mut a PostgresStorageAdapter) inc_error() {
	a.metrics_lock.@lock()
	defer { a.metrics_lock.unlock() }
	a.metrics.error_count++
}

// --- Core interface methods ---

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
