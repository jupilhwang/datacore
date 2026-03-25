/// Infrastructure layer - PostgreSQL-based transaction store
module transaction

import db.pg
import domain
import json
import sync

/// PgTransactionConfig holds configuration for PostgreSQL-based transaction storage.
pub struct PgTransactionConfig {
pub:
	table_name string = 'transaction_metadata'
}

/// PostgresTransactionStore implements TransactionStore using PostgreSQL.
/// Transaction state is stored in a relational table with JSON-encoded partitions.
pub struct PostgresTransactionStore {
mut:
	pool   &pg.ConnectionPool
	config PgTransactionConfig
	lock   sync.Mutex
}

/// new_postgres_transaction_store creates a new PostgreSQL-based transaction store.
/// Initializes the transaction_metadata table on creation.
pub fn new_postgres_transaction_store(pool &pg.ConnectionPool, config PgTransactionConfig) !&PostgresTransactionStore {
	mut store := &PostgresTransactionStore{
		pool:   pool
		config: config
	}
	store.init_schema()!
	return store
}

/// Initializes the transaction metadata table.
fn (mut s PostgresTransactionStore) init_schema() ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	mut db := s.pool.acquire()!
	defer { s.pool.release(db) }

	db.exec("
		CREATE TABLE IF NOT EXISTS ${s.config.table_name} (
			transactional_id TEXT PRIMARY KEY,
			producer_id BIGINT NOT NULL DEFAULT 0,
			producer_epoch SMALLINT NOT NULL DEFAULT 0,
			state TEXT NOT NULL DEFAULT 'Empty',
			partitions TEXT NOT NULL DEFAULT '[]',
			timeout_ms INTEGER NOT NULL DEFAULT 0,
			start_time_ms BIGINT NOT NULL DEFAULT 0,
			last_update_ms BIGINT NOT NULL DEFAULT 0
		)
	")!
}

/// get_transaction retrieves transaction metadata from PostgreSQL.
pub fn (mut s PostgresTransactionStore) get_transaction(transactional_id string) !domain.TransactionMetadata {
	s.lock.@lock()
	defer { s.lock.unlock() }

	mut db := s.pool.acquire()!
	defer { s.pool.release(db) }

	rows := db.exec("
		SELECT transactional_id, producer_id, producer_epoch,
		       state, partitions, timeout_ms,
		       start_time_ms, last_update_ms
		FROM ${s.config.table_name}
		WHERE transactional_id = '${transactional_id}'
	")!

	if rows.len == 0 {
		return error('transactional_id not found: ${transactional_id}')
	}

	return parse_pg_transaction_row(&rows[0])
}

/// save_transaction saves transaction metadata to PostgreSQL (upsert).
pub fn (mut s PostgresTransactionStore) save_transaction(metadata domain.TransactionMetadata) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	mut db := s.pool.acquire()!
	defer { s.pool.release(db) }

	partitions_json := json.encode(metadata.topic_partitions)
	state_str := metadata.state.str()

	db.exec("
		INSERT INTO ${s.config.table_name}
			(transactional_id, producer_id, producer_epoch,
			 state, partitions, timeout_ms,
			 start_time_ms, last_update_ms)
		VALUES
			('${metadata.transactional_id}', ${metadata.producer_id},
			 ${metadata.producer_epoch}, '${state_str}',
			 '${partitions_json}', ${metadata.txn_timeout_ms},
			 ${metadata.txn_start_timestamp},
			 ${metadata.txn_last_update_timestamp})
		ON CONFLICT (transactional_id) DO UPDATE SET
			producer_id = ${metadata.producer_id},
			producer_epoch = ${metadata.producer_epoch},
			state = '${state_str}',
			partitions = '${partitions_json}',
			timeout_ms = ${metadata.txn_timeout_ms},
			start_time_ms = ${metadata.txn_start_timestamp},
			last_update_ms = ${metadata.txn_last_update_timestamp}
	")!
}

/// delete_transaction deletes transaction metadata from PostgreSQL.
pub fn (mut s PostgresTransactionStore) delete_transaction(transactional_id string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	mut db := s.pool.acquire()!
	defer { s.pool.release(db) }

	check := db.exec("
		SELECT 1 FROM ${s.config.table_name}
		WHERE transactional_id = '${transactional_id}'
	")!

	if check.len == 0 {
		return error('transactional_id not found: ${transactional_id}')
	}

	db.exec("
		DELETE FROM ${s.config.table_name}
		WHERE transactional_id = '${transactional_id}'
	")!
}

/// list_transactions returns a list of all transactions from PostgreSQL.
pub fn (mut s PostgresTransactionStore) list_transactions() ![]domain.TransactionMetadata {
	s.lock.@lock()
	defer { s.lock.unlock() }

	mut db := s.pool.acquire()!
	defer { s.pool.release(db) }

	rows := db.exec('
		SELECT transactional_id, producer_id, producer_epoch,
		       state, partitions, timeout_ms,
		       start_time_ms, last_update_ms
		FROM ${s.config.table_name}
	')!

	mut result := []domain.TransactionMetadata{cap: rows.len}
	for row in rows {
		meta := parse_pg_transaction_row(&row) or { continue }
		result << meta
	}
	return result
}

// --- Row parsing helpers ---

/// Parses a PostgreSQL row into TransactionMetadata.
fn parse_pg_transaction_row(row &pg.Row) !domain.TransactionMetadata {
	txn_id := get_pg_str(row, 0, '')
	if txn_id == '' {
		return error('empty transactional_id in row')
	}

	state_str := get_pg_str(row, 3, 'Empty')
	partitions_json := get_pg_str(row, 4, '[]')
	partitions := json.decode([]domain.TopicPartition, partitions_json) or {
		[]domain.TopicPartition{}
	}

	return domain.TransactionMetadata{
		transactional_id:          txn_id
		producer_id:               get_pg_i64(row, 1, 0)
		producer_epoch:            i16(get_pg_int(row, 2, 0))
		state:                     domain.transaction_state_from_string(state_str)
		topic_partitions:          partitions
		txn_timeout_ms:            i32(get_pg_int(row, 5, 0))
		txn_start_timestamp:       get_pg_i64(row, 6, 0)
		txn_last_update_timestamp: get_pg_i64(row, 7, 0)
	}
}

/// Safely retrieves a string from a pg.Row at the given index.
fn get_pg_str(row &pg.Row, idx int, default_val string) string {
	if idx >= row.vals.len {
		return default_val
	}
	val_opt := row.vals[idx]
	if val_opt == none {
		return default_val
	}
	return val_opt or { default_val }
}

/// Safely retrieves an int from a pg.Row at the given index.
fn get_pg_int(row &pg.Row, idx int, default_val int) int {
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

/// Safely retrieves an i64 from a pg.Row at the given index.
fn get_pg_i64(row &pg.Row, idx int, default_val i64) i64 {
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
