// Manages the lifecycle of Kafka transactions.
// Handles transaction state transitions to guarantee exactly-once semantics.
module transaction

import domain
import service.port
import time
import rand

/// TransactionCoordinator manages transactional producers and transactions.
/// Responsible for beginning transactions, adding partitions, and commit/rollback.
pub struct TransactionCoordinator {
mut:
	store port.TransactionStore
}

/// new_transaction_coordinator creates a new transaction coordinator.
pub fn new_transaction_coordinator(store port.TransactionStore) &TransactionCoordinator {
	return &TransactionCoordinator{
		store: store
	}
}

/// get_transaction returns transaction metadata for the given transactional_id.
pub fn (mut c TransactionCoordinator) get_transaction(transactional_id string) !domain.TransactionMetadata {
	return c.store.get_transaction(transactional_id)
}

/// init_producer_id initializes a producer ID for a transactional or idempotent producer.
/// transactional_id: transaction ID (none when using idempotency only)
/// transaction_timeout_ms: transaction timeout in milliseconds
/// producer_id: existing producer ID (-1 to create a new one)
/// producer_epoch: existing producer epoch
pub fn (mut c TransactionCoordinator) init_producer_id(transactional_id ?string, transaction_timeout_ms i32, producer_id i64, producer_epoch i16) !domain.InitProducerIdResult {
	// 1. Idempotent producer (no transactional_id)
	if transactional_id == none {
		// Generate a new producer ID
		new_pid := if producer_id == -1 {
			rand.i64()
		} else {
			producer_id
		}
		// Ensure positive value
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
	// If a transaction is in progress, it must be rolled back (implicit rollback)
	// Currently only increments the epoch and resets the state
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

/// add_partitions_to_txn adds partitions to a transaction.
/// Registers the list of topic/partitions to be included in the transaction.
pub fn (mut c TransactionCoordinator) add_partitions_to_txn(transactional_id string, producer_id i64, producer_epoch i16, partitions []domain.TopicPartition) ! {
	// 1. Retrieve transaction metadata
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
		// Check for duplicates
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

/// add_offsets_to_txn adds consumer group offsets to a transaction.
/// Offsets will be committed together when the transaction is committed.
pub fn (mut c TransactionCoordinator) add_offsets_to_txn(transactional_id string, producer_id i64, producer_epoch i16, group_id string) ! {
	// 1. Retrieve transaction metadata
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

	// 4. Add the __consumer_offsets partition for this group to the transaction
	// Partition is determined by: hash(group_id) % 50 (default __consumer_offsets partition count)
	group_partition := hash_group_id(group_id) % 50

	// Add __consumer_offsets partition to the transaction
	mut new_partitions := meta.topic_partitions.clone()
	consumer_offsets_partition := domain.TopicPartition{
		topic:     '__consumer_offsets'
		partition: group_partition
	}

	// Check if already added
	mut already_added := false
	for tp in new_partitions {
		if tp.topic == '__consumer_offsets' && tp.partition == group_partition {
			already_added = true
			break
		}
	}

	if !already_added {
		new_partitions << consumer_offsets_partition
	}

	// Update state to ongoing and add partition
	updated_meta := domain.TransactionMetadata{
		...meta
		state:                     .ongoing
		topic_partitions:          new_partitions
		txn_last_update_timestamp: time.now().unix_milli()
	}

	c.store.save_transaction(updated_meta)!
}

/// hash_group_id computes a hash of group_id to determine the __consumer_offsets partition.
/// Uses a hash function equivalent to Java's String.hashCode().
fn hash_group_id(group_id string) int {
	mut hash := u32(0)
	for c in group_id {
		hash = hash * 31 + u32(c)
	}
	// Convert to positive value
	return int(hash & 0x7fffffff)
}

/// write_txn_markers validates and processes transaction markers for commit/abort.
/// For each marker, validates the producer_id against known transactions.
/// Returns per-partition results indicating success or failure.
pub fn (mut c TransactionCoordinator) write_txn_markers(markers []domain.WriteTxnMarker) []domain.WriteTxnMarkerResult {
	mut results := []domain.WriteTxnMarkerResult{}
	for marker in markers {
		results << c.process_single_marker(marker)
	}
	return results
}

/// process_single_marker processes one marker entry and returns per-partition results.
fn (mut c TransactionCoordinator) process_single_marker(marker domain.WriteTxnMarker) domain.WriteTxnMarkerResult {
	error_code := c.validate_marker_producer(marker)

	mut topic_results := []domain.WriteTxnMarkerTopicResult{}
	for topic in marker.topics {
		topic_results << build_marker_topic_result(topic, error_code)
	}

	return domain.WriteTxnMarkerResult{
		producer_id: marker.producer_id
		topics:      topic_results
	}
}

/// validate_marker_producer checks if the marker's producer_id/epoch is valid.
/// Returns 0 on success, or an error code on validation failure.
fn (mut c TransactionCoordinator) validate_marker_producer(marker domain.WriteTxnMarker) i16 {
	meta := c.find_transaction_by_producer_id(marker.producer_id) or {
		// Producer not found - acceptable for inter-broker WriteTxnMarkers
		return 0
	}

	if meta.producer_epoch != marker.producer_epoch {
		// Epoch mismatch indicates stale producer
		return 1 // INVALID_PRODUCER_EPOCH
	}

	return 0
}

/// find_transaction_by_producer_id scans all transactions to find one matching the producer_id.
fn (mut c TransactionCoordinator) find_transaction_by_producer_id(producer_id i64) !domain.TransactionMetadata {
	transactions := c.store.list_transactions()!
	for meta in transactions {
		if meta.producer_id == producer_id {
			return meta
		}
	}
	return error('producer_id not found: ${producer_id}')
}

/// build_marker_topic_result builds results for a single topic's partitions.
fn build_marker_topic_result(topic domain.WriteTxnMarkerTopic, error_code i16) domain.WriteTxnMarkerTopicResult {
	mut partition_results := []domain.WriteTxnMarkerPartitionResult{}
	for p in topic.partitions {
		partition_results << domain.WriteTxnMarkerPartitionResult{
			partition:  p
			error_code: error_code
		}
	}
	return domain.WriteTxnMarkerTopicResult{
		name:       topic.name
		partitions: partition_results
	}
}

/// build_end_txn_markers generates WriteTxnMarker entries from transaction metadata.
/// Used during end_txn to produce markers for all partitions in the transaction.
fn build_end_txn_markers(meta domain.TransactionMetadata, result domain.TransactionResult) []domain.WriteTxnMarker {
	// Group partitions by topic
	mut topic_map := map[string][]int{}
	for tp in meta.topic_partitions {
		topic_map[tp.topic] << tp.partition
	}

	mut topics := []domain.WriteTxnMarkerTopic{}
	for name, partitions in topic_map {
		topics << domain.WriteTxnMarkerTopic{
			name:       name
			partitions: partitions
		}
	}

	if topics.len == 0 {
		return []domain.WriteTxnMarker{}
	}

	return [
		domain.WriteTxnMarker{
			producer_id:        meta.producer_id
			producer_epoch:     meta.producer_epoch
			transaction_result: result
			topics:             topics
		},
	]
}

/// end_txn ends a transaction (commit or rollback).
/// Transitions the transaction state: Prepare -> Complete -> Empty.
pub fn (mut c TransactionCoordinator) end_txn(transactional_id string, producer_id i64, producer_epoch i16, result domain.TransactionResult) ! {
	// 1. Retrieve transaction metadata
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
		// OK if commit/rollback is attempted on an already empty state (idempotency)
		if meta.state == .empty {
			return
		}
		return error('invalid transaction state: ${meta.state}')
	}

	// 4. State transition
	// In a real implementation, a marker must be written to the log
	// Currently only updates state: CompleteCommit/CompleteAbort -> Empty

	// Transition to prepare state
	prepare_state := if result == .commit {
		domain.TransactionState.prepare_commit
	} else {
		domain.TransactionState.prepare_abort
	}
	meta_prepare := domain.TransactionMetadata{
		...meta
		state:                     prepare_state
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_prepare)!

	// Write transaction markers to all partitions in the transaction.
	// The coordinator validates marker metadata; physical record writing
	// is handled at the infrastructure layer (handler).
	markers := build_end_txn_markers(meta, result)
	c.write_txn_markers(markers)

	// Transition to complete state
	complete_state := if result == .commit {
		domain.TransactionState.complete_commit
	} else {
		domain.TransactionState.complete_abort
	}
	meta_complete := domain.TransactionMetadata{
		...meta_prepare
		state:                     complete_state
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_complete)!

	// Transition to empty state (transaction complete)
	meta_empty := domain.TransactionMetadata{
		...meta_complete
		state:                     .empty
		topic_partitions:          []
		txn_last_update_timestamp: time.now().unix_milli()
	}
	c.store.save_transaction(meta_empty)!
}
