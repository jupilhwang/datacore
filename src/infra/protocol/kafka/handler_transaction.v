// Infra Layer - Kafka Protocol Handler - Transaction Operations
// InitProducerId, AddPartitionsToTxn, EndTxn, AddOffsetsToTxn, TxnOffsetCommit handlers
module kafka

import domain
import rand

// Handle InitProducerId (API Key 22)
// Returns a producer ID for idempotent/transactional producers
fn (mut h Handler) handle_init_producer_id(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.init_producer_id, version)
	mut reader := new_reader(body)
	req := parse_init_producer_id_request(mut reader, version, is_flexible)!

	// Use TransactionCoordinator if available
	if mut txn_coord := h.txn_coordinator {
		result := txn_coord.init_producer_id(req.transactional_id, req.transaction_timeout_ms,
			req.producer_id, req.producer_epoch) or {
			// Handle error
			resp := InitProducerIdResponse{
				throttle_time_ms: 0
				error_code:       i16(ErrorCode.unknown_server_error)
				producer_id:      -1
				producer_epoch:   -1
			}
			return resp.encode(version)
		}

		resp := InitProducerIdResponse{
			throttle_time_ms: 0
			error_code:       0
			producer_id:      result.producer_id
			producer_epoch:   result.producer_epoch
		}
		return resp.encode(version)
	}

	// Fallback for idempotent producers (no transaction coordinator)
	mut producer_id := req.producer_id
	mut producer_epoch := req.producer_epoch
	mut error_code := i16(ErrorCode.none)

	// For new producers (producer_id == -1), generate a new producer ID
	if producer_id == -1 {
		// Generate a unique producer ID using random number
		// In production, this should be coordinated across brokers
		producer_id = rand.i64()
		if producer_id < 0 {
			producer_id = -producer_id // Ensure positive
		}
		producer_epoch = 0
	} else {
		// Existing producer - increment epoch
		producer_epoch += 1
	}

	// Note: Transactional ID handling would require additional state management
	// For now, we support idempotent producers only
	if transactional_id := req.transactional_id {
		if transactional_id.len > 0 {
			// Transactional producers require coordinator support
			// Return error if coordinator not configured
			error_code = i16(ErrorCode.coordinator_not_available)
		}
	}

	resp := InitProducerIdResponse{
		throttle_time_ms: 0
		error_code:       error_code
		producer_id:      producer_id
		producer_epoch:   producer_epoch
	}

	return resp.encode(version)
}

// Handle AddPartitionsToTxn (API Key 24)
fn (mut h Handler) handle_add_partitions_to_txn(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.add_partitions_to_txn, version)
	mut reader := new_reader(body)
	req := parse_add_partitions_to_txn_request(mut reader, version, is_flexible)!

	if mut txn_coord := h.txn_coordinator {
		// Convert request topics to TopicPartition list
		mut partitions := []domain.TopicPartition{}
		for t in req.topics {
			for p in t.partitions {
				partitions << domain.TopicPartition{
					topic:     t.name
					partition: int(p)
				}
			}
		}

		txn_coord.add_partitions_to_txn(req.transactional_id, req.producer_id, req.producer_epoch,
			partitions) or {
			// Global error (e.g. invalid transaction state)
			// We need to return error for all partitions
			mut results := []AddPartitionsToTxnResult{}
			for t in req.topics {
				mut p_results := []AddPartitionsToTxnPartitionResult{}
				for p in t.partitions {
					p_results << AddPartitionsToTxnPartitionResult{
						partition_index: p
						error_code:      i16(ErrorCode.invalid_txn_state) // Simplified error mapping
					}
				}
				results << AddPartitionsToTxnResult{
					name:       t.name
					partitions: p_results
				}
			}
			return AddPartitionsToTxnResponse{
				throttle_time_ms: 0
				results:          results
			}.encode(version)
		}

		// Success
		mut results := []AddPartitionsToTxnResult{}
		for t in req.topics {
			mut p_results := []AddPartitionsToTxnPartitionResult{}
			for p in t.partitions {
				p_results << AddPartitionsToTxnPartitionResult{
					partition_index: p
					error_code:      0
				}
			}
			results << AddPartitionsToTxnResult{
				name:       t.name
				partitions: p_results
			}
		}
		return AddPartitionsToTxnResponse{
			throttle_time_ms: 0
			results:          results
		}.encode(version)
	}

	// Coordinator not available
	mut results := []AddPartitionsToTxnResult{}
	for t in req.topics {
		mut p_results := []AddPartitionsToTxnPartitionResult{}
		for p in t.partitions {
			p_results << AddPartitionsToTxnPartitionResult{
				partition_index: p
				error_code:      i16(ErrorCode.coordinator_not_available)
			}
		}
		results << AddPartitionsToTxnResult{
			name:       t.name
			partitions: p_results
		}
	}
	return AddPartitionsToTxnResponse{
		throttle_time_ms: 0
		results:          results
	}.encode(version)
}

// Handle EndTxn (API Key 26)
fn (mut h Handler) handle_end_txn(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.end_txn, version)
	mut reader := new_reader(body)
	req := parse_end_txn_request(mut reader, version, is_flexible)!

	if mut txn_coord := h.txn_coordinator {
		result := if req.transaction_result {
			domain.TransactionResult.commit
		} else {
			domain.TransactionResult.abort
		}

		txn_coord.end_txn(req.transactional_id, req.producer_id, req.producer_epoch, result) or {
			return EndTxnResponse{
				throttle_time_ms: 0
				error_code:       i16(ErrorCode.invalid_txn_state) // Simplified error mapping
			}.encode(version)
		}

		return EndTxnResponse{
			throttle_time_ms: 0
			error_code:       0
		}.encode(version)
	}

	return EndTxnResponse{
		throttle_time_ms: 0
		error_code:       i16(ErrorCode.coordinator_not_available)
	}.encode(version)
}

// Handle AddOffsetsToTxn (API Key 25)
fn (mut h Handler) handle_add_offsets_to_txn(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.add_offsets_to_txn, version)
	mut reader := new_reader(body)
	req := parse_add_offsets_to_txn_request(mut reader, version, is_flexible)!

	if mut txn_coord := h.txn_coordinator {
		txn_coord.add_offsets_to_txn(req.transactional_id, req.producer_id, req.producer_epoch,
			req.group_id) or {
			return AddOffsetsToTxnResponse{
				throttle_time_ms: 0
				error_code:       i16(ErrorCode.invalid_txn_state) // Simplified error mapping
			}.encode(version)
		}

		return AddOffsetsToTxnResponse{
			throttle_time_ms: 0
			error_code:       0
		}.encode(version)
	}

	return AddOffsetsToTxnResponse{
		throttle_time_ms: 0
		error_code:       i16(ErrorCode.coordinator_not_available)
	}.encode(version)
}

// Handle TxnOffsetCommit (API Key 28)
fn (mut h Handler) handle_txn_offset_commit(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.txn_offset_commit, version)
	mut reader := new_reader(body)
	req := parse_txn_offset_commit_request(mut reader, version, is_flexible)!

	// Validate transaction coordinator exists
	if mut txn_coord := h.txn_coordinator {
		// Verify transaction exists and is valid
		meta := txn_coord.get_transaction(req.transactional_id) or {
			// Transaction not found
			mut topics := []TxnOffsetCommitResponseTopic{}
			for t in req.topics {
				mut partitions := []TxnOffsetCommitResponsePartition{}
				for p in t.partitions {
					partitions << TxnOffsetCommitResponsePartition{
						partition_index: p.partition_index
						error_code:      i16(ErrorCode.transactional_id_not_found)
					}
				}
				topics << TxnOffsetCommitResponseTopic{
					name:       t.name
					partitions: partitions
				}
			}
			return TxnOffsetCommitResponse{
				throttle_time_ms: 0
				topics:           topics
			}.encode(version)
		}

		// Validate producer ID and epoch
		if meta.producer_id != req.producer_id {
			mut topics := []TxnOffsetCommitResponseTopic{}
			for t in req.topics {
				mut partitions := []TxnOffsetCommitResponsePartition{}
				for p in t.partitions {
					partitions << TxnOffsetCommitResponsePartition{
						partition_index: p.partition_index
						error_code:      i16(ErrorCode.invalid_producer_id_mapping)
					}
				}
				topics << TxnOffsetCommitResponseTopic{
					name:       t.name
					partitions: partitions
				}
			}
			return TxnOffsetCommitResponse{
				throttle_time_ms: 0
				topics:           topics
			}.encode(version)
		}

		if meta.producer_epoch != req.producer_epoch {
			mut topics := []TxnOffsetCommitResponseTopic{}
			for t in req.topics {
				mut partitions := []TxnOffsetCommitResponsePartition{}
				for p in t.partitions {
					partitions << TxnOffsetCommitResponsePartition{
						partition_index: p.partition_index
						error_code:      i16(ErrorCode.invalid_producer_epoch)
					}
				}
				topics << TxnOffsetCommitResponseTopic{
					name:       t.name
					partitions: partitions
				}
			}
			return TxnOffsetCommitResponse{
				throttle_time_ms: 0
				topics:           topics
			}.encode(version)
		}

		// Validate transaction state - must be ongoing
		if meta.state != .ongoing {
			mut topics := []TxnOffsetCommitResponseTopic{}
			for t in req.topics {
				mut partitions := []TxnOffsetCommitResponsePartition{}
				for p in t.partitions {
					partitions << TxnOffsetCommitResponsePartition{
						partition_index: p.partition_index
						error_code:      i16(ErrorCode.invalid_txn_state)
					}
				}
				topics << TxnOffsetCommitResponseTopic{
					name:       t.name
					partitions: partitions
				}
			}
			return TxnOffsetCommitResponse{
				throttle_time_ms: 0
				topics:           topics
			}.encode(version)
		}

		// Transaction is valid - commit offsets
		mut all_offsets := []domain.PartitionOffset{}
		for t in req.topics {
			for p in t.partitions {
				all_offsets << domain.PartitionOffset{
					topic:        t.name
					partition:    int(p.partition_index)
					offset:       p.committed_offset
					leader_epoch: p.committed_leader_epoch
					metadata:     p.committed_metadata
				}
			}
		}

		// Commit offsets to storage
		h.storage.commit_offsets(req.group_id, all_offsets) or {
			// Build error response for all partitions
			mut topics := []TxnOffsetCommitResponseTopic{}
			for t in req.topics {
				mut partitions := []TxnOffsetCommitResponsePartition{}
				for p in t.partitions {
					partitions << TxnOffsetCommitResponsePartition{
						partition_index: p.partition_index
						error_code:      i16(ErrorCode.unknown_server_error)
					}
				}
				topics << TxnOffsetCommitResponseTopic{
					name:       t.name
					partitions: partitions
				}
			}
			return TxnOffsetCommitResponse{
				throttle_time_ms: 0
				topics:           topics
			}.encode(version)
		}

		// Build success response
		mut topics := []TxnOffsetCommitResponseTopic{}
		for t in req.topics {
			mut partitions := []TxnOffsetCommitResponsePartition{}
			for p in t.partitions {
				partitions << TxnOffsetCommitResponsePartition{
					partition_index: p.partition_index
					error_code:      0
				}
			}
			topics << TxnOffsetCommitResponseTopic{
				name:       t.name
				partitions: partitions
			}
		}
		return TxnOffsetCommitResponse{
			throttle_time_ms: 0
			topics:           topics
		}.encode(version)
	}

	// Coordinator not available - return error for all partitions
	mut topics := []TxnOffsetCommitResponseTopic{}
	for t in req.topics {
		mut partitions := []TxnOffsetCommitResponsePartition{}
		for p in t.partitions {
			partitions << TxnOffsetCommitResponsePartition{
				partition_index: p.partition_index
				error_code:      i16(ErrorCode.coordinator_not_available)
			}
		}
		topics << TxnOffsetCommitResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}
	return TxnOffsetCommitResponse{
		throttle_time_ms: 0
		topics:           topics
	}.encode(version)
}

// Process InitProducerId request (Frame-based)
fn (mut h Handler) process_init_producer_id(req InitProducerIdRequest, version i16) !InitProducerIdResponse {
	// Use TransactionCoordinator if available
	if mut txn_coord := h.txn_coordinator {
		result := txn_coord.init_producer_id(req.transactional_id, req.transaction_timeout_ms,
			req.producer_id, req.producer_epoch) or {
			return InitProducerIdResponse{
				throttle_time_ms: 0
				error_code:       i16(ErrorCode.unknown_server_error)
				producer_id:      -1
				producer_epoch:   -1
			}
		}

		return InitProducerIdResponse{
			throttle_time_ms: 0
			error_code:       0
			producer_id:      result.producer_id
			producer_epoch:   result.producer_epoch
		}
	}

	// Fallback for idempotent producers
	mut producer_id := req.producer_id
	mut producer_epoch := req.producer_epoch
	mut error_code := i16(ErrorCode.none)

	if producer_id == -1 {
		producer_id = rand.i64()
		if producer_id < 0 {
			producer_id = -producer_id
		}
		producer_epoch = 0
	} else {
		producer_epoch += 1
	}

	if transactional_id := req.transactional_id {
		if transactional_id.len > 0 {
			error_code = i16(ErrorCode.coordinator_not_available)
		}
	}

	return InitProducerIdResponse{
		throttle_time_ms: 0
		error_code:       error_code
		producer_id:      producer_id
		producer_epoch:   producer_epoch
	}
}
