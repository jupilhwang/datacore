module transaction

import db.pg
import domain
import os

fn create_pg_test_pool() ?&pg.ConnectionPool {
	host := os.getenv_opt('DATACORE_PG_HOST') or { return none }
	port_str := os.getenv_opt('DATACORE_PG_PORT') or { '5432' }
	user := os.getenv_opt('DATACORE_PG_USER') or { return none }
	password := os.getenv_opt('DATACORE_PG_PASSWORD') or { '' }
	database := os.getenv_opt('DATACORE_PG_DATABASE') or { 'datacore_test' }

	pg_config := pg.Config{
		host:     host
		port:     port_str.int()
		user:     user
		password: password
		dbname:   database
	}

	pool := pg.new_connection_pool(pg_config, 2) or { return none }
	return &pool
}

fn test_postgres_store_save_and_get() {
	pool := create_pg_test_pool() or {
		println('Skipping PostgreSQL transaction store tests - environment not configured')
		return
	}

	mut store := new_postgres_transaction_store(pool, PgTransactionConfig{}) or {
		assert false, 'Failed to create store: ${err}'
		return
	}

	metadata := domain.TransactionMetadata{
		transactional_id:          'pg-txn-1'
		producer_id:               2000
		producer_epoch:            2
		txn_timeout_ms:            30000
		state:                     .ongoing
		topic_partitions:          [
			domain.TopicPartition{
				topic:     'pg-topic'
				partition: 1
			},
		]
		txn_start_timestamp:       5000000
		txn_last_update_timestamp: 5000001
	}

	store.save_transaction(metadata) or {
		assert false, 'save failed: ${err}'
		return
	}

	result := store.get_transaction('pg-txn-1') or {
		assert false, 'get failed: ${err}'
		return
	}

	assert result.transactional_id == 'pg-txn-1'
	assert result.producer_id == 2000
	assert result.producer_epoch == 2
	assert result.state == .ongoing
	assert result.topic_partitions.len == 1

	store.delete_transaction('pg-txn-1') or {}
}

fn test_postgres_store_delete() {
	pool := create_pg_test_pool() or {
		println('Skipping PostgreSQL transaction store tests')
		return
	}

	mut store := new_postgres_transaction_store(pool, PgTransactionConfig{}) or {
		assert false, 'Failed to create store: ${err}'
		return
	}

	metadata := domain.TransactionMetadata{
		transactional_id:          'pg-txn-del'
		producer_id:               3000
		producer_epoch:            1
		txn_timeout_ms:            60000
		state:                     .ongoing
		topic_partitions:          []
		txn_start_timestamp:       6000000
		txn_last_update_timestamp: 6000001
	}

	store.save_transaction(metadata) or {
		assert false, 'save failed: ${err}'
		return
	}

	store.delete_transaction('pg-txn-del') or {
		assert false, 'delete failed: ${err}'
		return
	}

	store.get_transaction('pg-txn-del') or {
		assert true
		return
	}
	assert false, 'expected error after delete'
}

fn test_postgres_store_list() {
	pool := create_pg_test_pool() or {
		println('Skipping PostgreSQL transaction store tests')
		return
	}

	mut store := new_postgres_transaction_store(pool, PgTransactionConfig{}) or {
		assert false, 'Failed to create store: ${err}'
		return
	}

	store.save_transaction(domain.TransactionMetadata{
		transactional_id:          'pg-list-a'
		producer_id:               4000
		state:                     .ongoing
		topic_partitions:          []
		txn_start_timestamp:       7000000
		txn_last_update_timestamp: 7000001
	}) or {
		assert false, 'save a failed: ${err}'
		return
	}
	store.save_transaction(domain.TransactionMetadata{
		transactional_id:          'pg-list-b'
		producer_id:               4001
		state:                     .prepare_commit
		topic_partitions:          []
		txn_start_timestamp:       7000002
		txn_last_update_timestamp: 7000003
	}) or {
		assert false, 'save b failed: ${err}'
		return
	}

	result := store.list_transactions() or {
		assert false, 'list failed: ${err}'
		return
	}

	assert result.len >= 2

	store.delete_transaction('pg-list-a') or {}
	store.delete_transaction('pg-list-b') or {}
}

fn test_postgres_store_get_nonexistent() {
	pool := create_pg_test_pool() or {
		println('Skipping PostgreSQL transaction store tests')
		return
	}

	mut store := new_postgres_transaction_store(pool, PgTransactionConfig{}) or {
		assert false, 'Failed to create store: ${err}'
		return
	}

	store.get_transaction('nonexistent-pg-txn') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'expected error for nonexistent transaction'
}

fn test_postgres_store_state_transition() {
	pool := create_pg_test_pool() or {
		println('Skipping PostgreSQL transaction store tests')
		return
	}

	mut store := new_postgres_transaction_store(pool, PgTransactionConfig{}) or {
		assert false, 'Failed to create store: ${err}'
		return
	}

	initial := domain.TransactionMetadata{
		transactional_id:          'pg-txn-state'
		producer_id:               5000
		producer_epoch:            1
		txn_timeout_ms:            60000
		state:                     .ongoing
		topic_partitions:          [
			domain.TopicPartition{
				topic:     'topic-1'
				partition: 0
			},
		]
		txn_start_timestamp:       7000000
		txn_last_update_timestamp: 7000001
	}

	store.save_transaction(initial) or {
		assert false, 'save failed: ${err}'
		return
	}

	updated := domain.TransactionMetadata{
		transactional_id:          initial.transactional_id
		producer_id:               initial.producer_id
		producer_epoch:            initial.producer_epoch
		txn_timeout_ms:            initial.txn_timeout_ms
		state:                     .complete_commit
		topic_partitions:          initial.topic_partitions
		txn_start_timestamp:       initial.txn_start_timestamp
		txn_last_update_timestamp: 8000000
	}

	store.save_transaction(updated) or {
		assert false, 'update failed: ${err}'
		return
	}

	result := store.get_transaction('pg-txn-state') or {
		assert false, 'get failed: ${err}'
		return
	}

	assert result.state == .complete_commit
	assert result.txn_last_update_timestamp == 8000000

	store.delete_transaction('pg-txn-state') or {}
}
