// Kafka Protocol - Transaction Operations
// InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain
import rand

// InitProducerId Request (API Key 22)
// Used by idempotent/transactional producers to obtain a producer ID
pub struct InitProducerIdRequest {
pub:
	transactional_id       ?string // Nullable - null for non-transactional producer
	transaction_timeout_ms i32     // Timeout for transactions
	producer_id            i64     // Existing producer ID or -1 for new
	producer_epoch         i16     // Existing epoch or -1 for new
}

fn parse_init_producer_id_request(mut reader BinaryReader, version i16, is_flexible bool) !InitProducerIdRequest {
	// transactional_id: NULLABLE_STRING (v0-v1) / COMPACT_NULLABLE_STRING (v2+)
	raw_transactional_id := reader.read_flex_nullable_string(is_flexible)!
	// Convert empty string to none for optional type
	transactional_id := if raw_transactional_id.len > 0 {
		?string(raw_transactional_id)
	} else {
		?string(none)
	}

	// transaction_timeout_ms: INT32
	transaction_timeout_ms := reader.read_i32()!

	// producer_id and producer_epoch added in v3
	mut producer_id := i64(-1)
	mut producer_epoch := i16(-1)
	if version >= 3 {
		producer_id = reader.read_i64()!
		producer_epoch = reader.read_i16()!
	}

	return InitProducerIdRequest{
		transactional_id:       transactional_id
		transaction_timeout_ms: transaction_timeout_ms
		producer_id:            producer_id
		producer_epoch:         producer_epoch
	}
}

// ============================================================================
// AddPartitionsToTxn Request (API Key 24)
// ============================================================================

pub struct AddPartitionsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	topics           []AddPartitionsToTxnTopic
}

pub struct AddPartitionsToTxnTopic {
pub:
	name       string
	partitions []i32
}

fn parse_add_partitions_to_txn_request(mut reader BinaryReader, version i16, is_flexible bool) !AddPartitionsToTxnRequest {
	transactional_id := reader.read_flex_string(is_flexible)!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!

	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topics := []AddPartitionsToTxnTopic{}
	for _ in 0 .. topic_count {
		name := reader.read_flex_string(is_flexible)!

		partition_count := reader.read_flex_array_len(is_flexible)!

		mut partitions := []i32{}
		for _ in 0 .. partition_count {
			partitions << reader.read_i32()!
		}

		topics << AddPartitionsToTxnTopic{
			name:       name
			partitions: partitions
		}

		reader.skip_flex_tagged_fields(is_flexible)!
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return AddPartitionsToTxnRequest{
		transactional_id: transactional_id
		producer_id:      producer_id
		producer_epoch:   producer_epoch
		topics:           topics
	}
}

// ============================================================================
// AddOffsetsToTxn Request (API Key 25)
// ============================================================================
// Adds consumer group offsets to a transaction

pub struct AddOffsetsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	group_id         string
}

fn parse_add_offsets_to_txn_request(mut reader BinaryReader, version i16, is_flexible bool) !AddOffsetsToTxnRequest {
	transactional_id := reader.read_flex_string(is_flexible)!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!
	group_id := reader.read_flex_string(is_flexible)!

	reader.skip_flex_tagged_fields(is_flexible)!

	return AddOffsetsToTxnRequest{
		transactional_id: transactional_id
		producer_id:      producer_id
		producer_epoch:   producer_epoch
		group_id:         group_id
	}
}

// ============================================================================
// EndTxn Request (API Key 26)
// ============================================================================

pub struct EndTxnRequest {
pub:
	transactional_id   string
	producer_id        i64
	producer_epoch     i16
	transaction_result bool // false=ABORT, true=COMMIT
}

fn parse_end_txn_request(mut reader BinaryReader, version i16, is_flexible bool) !EndTxnRequest {
	transactional_id := reader.read_flex_string(is_flexible)!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!
	transaction_result := reader.read_i8()! != 0

	reader.skip_flex_tagged_fields(is_flexible)!

	return EndTxnRequest{
		transactional_id:   transactional_id
		producer_id:        producer_id
		producer_epoch:     producer_epoch
		transaction_result: transaction_result
	}
}

// ============================================================================
// TxnOffsetCommit Request (API Key 28)
// ============================================================================
// Commits offsets within a transaction

pub struct TxnOffsetCommitRequest {
pub:
	transactional_id  string
	group_id          string
	producer_id       i64
	producer_epoch    i16
	generation_id     i32
	member_id         string
	group_instance_id ?string
	topics            []TxnOffsetCommitRequestTopic
}

pub struct TxnOffsetCommitRequestTopic {
pub:
	name       string
	partitions []TxnOffsetCommitRequestPartition
}

pub struct TxnOffsetCommitRequestPartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     string
}

fn parse_txn_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !TxnOffsetCommitRequest {
	transactional_id := reader.read_flex_string(is_flexible)!
	group_id := reader.read_flex_string(is_flexible)!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!

	// v3+: generation_id, member_id, group_instance_id
	mut generation_id := i32(-1)
	mut member_id := ''
	mut group_instance_id := ?string(none)
	if version >= 3 {
		generation_id = reader.read_i32()!
		member_id = reader.read_flex_string(is_flexible)!
		raw_group_instance_id := reader.read_flex_nullable_string(is_flexible)!
		group_instance_id = if raw_group_instance_id.len > 0 { raw_group_instance_id } else { none }
	}

	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topics := []TxnOffsetCommitRequestTopic{}
	for _ in 0 .. topic_count {
		name := reader.read_flex_string(is_flexible)!

		partition_count := reader.read_flex_array_len(is_flexible)!

		mut partitions := []TxnOffsetCommitRequestPartition{}
		for _ in 0 .. partition_count {
			partition_index := reader.read_i32()!
			committed_offset := reader.read_i64()!
			// v2+: committed_leader_epoch
			committed_leader_epoch := if version >= 2 { reader.read_i32()! } else { i32(-1) }
			committed_metadata := if is_flexible {
				reader.read_compact_nullable_string() or { '' }
			} else {
				reader.read_nullable_string() or { '' }
			}

			partitions << TxnOffsetCommitRequestPartition{
				partition_index:        partition_index
				committed_offset:       committed_offset
				committed_leader_epoch: committed_leader_epoch
				committed_metadata:     committed_metadata
			}

			reader.skip_flex_tagged_fields(is_flexible)!
		}

		topics << TxnOffsetCommitRequestTopic{
			name:       name
			partitions: partitions
		}

		reader.skip_flex_tagged_fields(is_flexible)!
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return TxnOffsetCommitRequest{
		transactional_id:  transactional_id
		group_id:          group_id
		producer_id:       producer_id
		producer_epoch:    producer_epoch
		generation_id:     generation_id
		member_id:         member_id
		group_instance_id: group_instance_id
		topics:            topics
	}
}

// ============================================================================
// InitProducerId Response (API Key 22)
// ============================================================================
// Returns a producer ID for idempotent/transactional producers

pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32 // Throttle time in milliseconds
	error_code       i16 // Error code (0 = success)
	producer_id      i64 // Assigned producer ID
	producer_epoch   i16 // Producer epoch
}

pub fn (r InitProducerIdResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms: INT32 (v0+)
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16 (v0+)
	writer.write_i16(r.error_code)

	// producer_id: INT64 (v0+)
	writer.write_i64(r.producer_id)

	// producer_epoch: INT16 (v0+)
	writer.write_i16(r.producer_epoch)

	// Tagged fields for flexible versions
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// AddPartitionsToTxn Response (API Key 24)
// ============================================================================

pub struct AddPartitionsToTxnResponse {
pub:
	throttle_time_ms i32
	results          []AddPartitionsToTxnResult
}

pub struct AddPartitionsToTxnResult {
pub:
	name       string
	partitions []AddPartitionsToTxnPartitionResult
}

pub struct AddPartitionsToTxnPartitionResult {
pub:
	partition_index i32
	error_code      i16
}

pub fn (r AddPartitionsToTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		if is_flexible {
			writer.write_compact_string(res.name)
			writer.write_compact_array_len(res.partitions.len)
		} else {
			writer.write_string(res.name)
			writer.write_array_len(res.partitions.len)
		}

		for p in res.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// AddOffsetsToTxn Response (API Key 25)
// ============================================================================

pub struct AddOffsetsToTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

pub fn (r AddOffsetsToTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// EndTxn Response (API Key 26)
// ============================================================================

pub struct EndTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

pub fn (r EndTxnResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// TxnOffsetCommit Response (API Key 28)
// ============================================================================

pub struct TxnOffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []TxnOffsetCommitResponseTopic
}

pub struct TxnOffsetCommitResponseTopic {
pub:
	name       string
	partitions []TxnOffsetCommitResponsePartition
}

pub struct TxnOffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

pub fn (r TxnOffsetCommitResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_string(t.name)
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// Transaction Handlers
// ============================================================================

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
