// Service Layer - Transaction Coordinator
module transaction

import domain
import service.port
import time
import rand

// TransactionCoordinator manages transactional producers and transactions
pub struct TransactionCoordinator {
mut:
	store port.TransactionStore
}

// new_transaction_coordinator creates a new transaction coordinator
pub fn new_transaction_coordinator(store port.TransactionStore) &TransactionCoordinator {
	return &TransactionCoordinator{
		store: store
	}
}

// init_producer_id initializes a producer ID for transactional or idempotent producers
pub fn (mut c TransactionCoordinator) init_producer_id(transactional_id ?string, transaction_timeout_ms i32, producer_id i64, producer_epoch i16) !domain.InitProducerIdResult {
	// 1. Idempotent producer (no transactional_id)
	if transactional_id == none {
		// Generate new producer ID
		new_pid := if producer_id == -1 {
			rand.i64()
		} else {
			producer_id
		}
		// Ensure positive
		final_pid := if new_pid < 0 { -new_pid } else { new_pid }
		
		return domain.InitProducerIdResult{
			producer_id:    final_pid
			producer_epoch: 0
		}
	}

	// 2. Transactional producer
	tid := transactional_id or { return error('transactional_id is required') }
	
	// Check if transaction metadata exists
	mut metadata := c.store.get_transaction(tid) or {
		// Create new metadata
		new_pid := rand.i64()
		final_pid := if new_pid < 0 { -new_pid } else { new_pid }
		
		meta := domain.TransactionMetadata{
			transactional_id:          tid
			producer_id:               final_pid
			producer_epoch:            0
			txn_timeout_ms:            transaction_timeout_ms
			state:                     .empty
			topic_partitions:          []
			txn_start_timestamp:       time.now().unix_milli()
			txn_last_update_timestamp: time.now().unix_milli()
		}
		c.store.save_transaction(meta)!
		return domain.InitProducerIdResult{
			producer_id:    meta.producer_id
			producer_epoch: meta.producer_epoch
		}
	}

	// Existing transaction - increment epoch
	// If transaction is in progress, we should abort it (implicit abort)
	// For now, we just increment epoch and reset state
	new_epoch := metadata.producer_epoch + 1
	
	updated_meta := domain.TransactionMetadata{
		...metadata
		producer_epoch:            new_epoch
		state:                     .empty
		topic_partitions:          []
		txn_last_update_timestamp: time.now().unix_milli()
	}
	
	c.store.save_transaction(updated_meta)!
	
	return domain.InitProducerIdResult{
		producer_id:    updated_meta.producer_id
		producer_epoch: updated_meta.producer_epoch
	}
}

// add_partitions_to_txn adds partitions to a transaction
pub fn (mut c TransactionCoordinator) add_partitions_to_txn(transactional_id string, producer_id i64, producer_epoch i16, partitions []domain.TopicPartition) ! {
	// 1. Get transaction metadata
	mut meta := c.store.get_transaction(transactional_id) or {
		return error('transactional_id not found: ${transactional_id}')
	}

	// 2. Validate producer ID and epoch
	if meta.producer_id != producer_id {
		return error('invalid producer id')
	}
	if meta.producer_epoch != producer_epoch {
		return error('invalid producer epoch')
	}

	// 3. Validate state
	if meta.state != .empty && meta.state != .ongoing {
		return error('invalid transaction state: ${meta.state}')
	}

	// 4. Add partitions
	mut new_partitions := meta.topic_partitions.clone()
	for p in partitions {
		// Check if already exists
		mut exists := false
		for existing in new_partitions {
			if existing.topic == p.topic && existing.partition == p.partition {
				exists = true
				break
			}
		}
		if !exists {
			new_partitions << p
		}
	}

	// 5. Update state
	updated_meta := domain.TransactionMetadata{
		...meta
		state:                     .ongoing
		topic_partitions:          new_partitions
		txn_last_update_timestamp: time.now().unix_milli()
	}

	c.store.save_transaction(updated_meta)!
}

// end_txn ends a transaction (commit or abort)
pub fn (mut c TransactionCoordinator) end_txn(transactional_id string, producer_id i64, producer_epoch i16, result domain.TransactionResult) ! {
	// 1. Get transaction metadata
	mut meta := c.store.get_transaction(transactional_id) or {
		return error('transactional_id not found: ${transactional_id}')
	}

	// 2. Validate producer ID and epoch
	if meta.producer_id != producer_id {
		return error('invalid producer id')
	}
	if meta.producer_epoch != producer_epoch {
		return error('invalid producer epoch')
	}

	// 3. Validate state
	if meta.state != .ongoing {
		// If already empty and trying to commit/abort, it's fine (idempotent)
		if meta.state == .empty {
			return
		}
		return error('invalid transaction state: ${meta.state}')
	}

	// 4. Transition state
	// In a real implementation, we would write markers to the log
	// For now, we just update the state to CompleteCommit/CompleteAbort then Empty
	
	// Transition to Prepare
	prepare_state := if result == .commit { domain.TransactionState.prepare_commit } else { domain.TransactionState.prepare_abort }
	meta_prepare := domain.TransactionMetadata{
		...meta
		state: prepare_state
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_prepare)!

	// Write markers (TODO: Implement WriteTxnMarkers)
	// For now, assume markers are written successfully

	// Transition to Complete
	complete_state := if result == .commit { domain.TransactionState.complete_commit } else { domain.TransactionState.complete_abort }
	meta_complete := domain.TransactionMetadata{
		...meta_prepare
		state: complete_state
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_complete)!

	// Transition to Empty (Transaction finished)
	meta_empty := domain.TransactionMetadata{
		...meta_complete
		state:            .empty
		topic_partitions: []
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_empty)!
}
