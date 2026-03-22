// Kafka protocol - Transaction handlers
// Handler methods for transaction-related operations:
// InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit, WriteTxnMarkers
module kafka

import domain
import infra.observability
import rand
import time

// handle_init_producer_id - handles InitProducerId (API Key 22)
// Returns a producer ID for idempotent/transactional producers
fn (mut h Handler) handle_init_producer_id(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := is_flexible_version(.init_producer_id, version)
	mut reader := new_reader(body)
	req := parse_init_producer_id_request(mut reader, version, is_flexible)!

	txn_id := req.transactional_id or { '' }
	h.logger.debug('Processing init producer id', observability.field_string('txn_id',
		txn_id), observability.field_int('producer_id', req.producer_id), observability.field_int('producer_epoch',
		req.producer_epoch))

	// Use TransactionCoordinator if available
	if mut txn_coord := h.txn_coordinator {
		result := txn_coord.init_producer_id(req.transactional_id, req.transaction_timeout_ms,
			req.producer_id, req.producer_epoch) or {
			// Handle error
			resp := InitProducerIdResponse{
				throttle_time_ms: default_throttle_time_ms
				error_code:       i16(ErrorCode.unknown_server_error)
				producer_id:      -1
				producer_epoch:   -1
			}
			return resp.encode(version)
		}

		resp := InitProducerIdResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       0
			producer_id:      result.producer_id
			producer_epoch:   result.producer_epoch
		}
		return resp.encode(version)
	}

	// Fallback for idempotent producers (no transaction coordinator)
	resp := build_fallback_producer_id_response(req)

	elapsed := time.since(start_time)
	h.logger.debug('Init producer id completed', observability.field_int('producer_id',
		resp.producer_id), observability.field_int('error_code', resp.error_code), observability.field_duration('latency',
		elapsed))

	return resp.encode(version)
}

// handle_add_partitions_to_txn - handles AddPartitionsToTxn (API Key 24)
fn (mut h Handler) handle_add_partitions_to_txn(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := is_flexible_version(.add_partitions_to_txn, version)
	mut reader := new_reader(body)
	req := parse_add_partitions_to_txn_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing add partitions to txn', observability.field_string('txn_id',
		req.transactional_id), observability.field_int('producer_id', req.producer_id),
		observability.field_int('topics', req.topics.len))

	if mut txn_coord := h.txn_coordinator {
		// Convert request topics to a TopicPartition list
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
			return build_add_partitions_error_response(req, .invalid_txn_state).encode(version)
		}

		return build_add_partitions_error_response(req, .none).encode(version)
	}

	elapsed := time.since(start_time)
	h.logger.debug('Add partitions to txn completed', observability.field_string('txn_id',
		req.transactional_id), observability.field_duration('latency', elapsed))

	// Coordinator not available
	return build_add_partitions_error_response(req, .coordinator_not_available).encode(version)
}

// handle_end_txn - handles EndTxn (API Key 26)
fn (mut h Handler) handle_end_txn(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := is_flexible_version(.end_txn, version)
	mut reader := new_reader(body)
	req := parse_end_txn_request(mut reader, version, is_flexible)!

	h.logger.info('Processing end txn', observability.field_string('txn_id', req.transactional_id),
		observability.field_int('producer_id', req.producer_id), observability.field_bool('commit',
		req.transaction_result))

	if mut txn_coord := h.txn_coordinator {
		result := if req.transaction_result {
			domain.TransactionResult.commit
		} else {
			domain.TransactionResult.abort
		}

		txn_coord.end_txn(req.transactional_id, req.producer_id, req.producer_epoch, result) or {
			return EndTxnResponse{
				throttle_time_ms: default_throttle_time_ms
				error_code:       i16(ErrorCode.invalid_txn_state)
			}.encode(version)
		}

		return EndTxnResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       0
		}.encode(version)
	}

	elapsed := time.since(start_time)
	h.logger.warn('End txn failed: coordinator not available', observability.field_string('txn_id',
		req.transactional_id), observability.field_duration('latency', elapsed))

	return EndTxnResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       i16(ErrorCode.coordinator_not_available)
	}.encode(version)
}

// handle_add_offsets_to_txn - handles AddOffsetsToTxn (API Key 25)
fn (mut h Handler) handle_add_offsets_to_txn(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := is_flexible_version(.add_offsets_to_txn, version)
	mut reader := new_reader(body)
	req := parse_add_offsets_to_txn_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing add offsets to txn', observability.field_string('txn_id',
		req.transactional_id), observability.field_string('group_id', req.group_id), observability.field_int('producer_id',
		req.producer_id))

	if mut txn_coord := h.txn_coordinator {
		txn_coord.add_offsets_to_txn(req.transactional_id, req.producer_id, req.producer_epoch,
			req.group_id) or {
			return AddOffsetsToTxnResponse{
				throttle_time_ms: default_throttle_time_ms
				error_code:       i16(ErrorCode.invalid_txn_state)
			}.encode(version)
		}

		return AddOffsetsToTxnResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       0
		}.encode(version)
	}

	elapsed := time.since(start_time)
	h.logger.warn('Add offsets to txn failed: coordinator not available', observability.field_string('txn_id',
		req.transactional_id), observability.field_duration('latency', elapsed))

	return AddOffsetsToTxnResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       i16(ErrorCode.coordinator_not_available)
	}.encode(version)
}

// WriteTxnMarkers handler (API Key 27)
// Used by the transaction coordinator to write commit/abort markers to partition leaders.
// This is an inter-broker communication API.
fn (mut h Handler) handle_write_txn_markers(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.write_txn_markers, version)
	mut reader := new_reader(body)
	req := parse_write_txn_markers_request(mut reader, version, is_flexible)!

	// Validate via coordinator if available, then write physical records
	if mut txn_coord := h.txn_coordinator {
		domain_markers := convert_to_domain_markers(req.markers)
		txn_coord.write_txn_markers(domain_markers)
	}

	// Write physical records to partitions
	mut results := []WriteTxnMarkerResult{}
	for marker in req.markers {
		results << h.write_single_txn_marker(marker)
	}

	return WriteTxnMarkersResponse{
		markers: results
	}.encode(version)
}

// write_single_txn_marker writes markers for a single producer to all target partitions.
fn (mut h Handler) write_single_txn_marker(marker WriteTxnMarker) WriteTxnMarkerResult {
	mut topic_results := []WriteTxnMarkerTopicResult{}

	for topic in marker.topics {
		mut partition_results := []WriteTxnMarkerPartitionResult{}
		for partition_index in topic.partition_indexes {
			error_code := h.write_txn_marker_to_partition(topic.name, partition_index,
				marker.producer_id, marker.producer_epoch, marker.transaction_result,
				marker.coordinator_epoch)
			partition_results << WriteTxnMarkerPartitionResult{
				partition_index: partition_index
				error_code:      error_code
			}
		}
		topic_results << WriteTxnMarkerTopicResult{
			name:       topic.name
			partitions: partition_results
		}
	}

	return WriteTxnMarkerResult{
		producer_id: marker.producer_id
		topics:      topic_results
	}
}

// convert_to_domain_markers converts kafka protocol markers to domain markers.
fn convert_to_domain_markers(markers []WriteTxnMarker) []domain.WriteTxnMarker {
	mut result := []domain.WriteTxnMarker{}
	for m in markers {
		mut topics := []domain.WriteTxnMarkerTopic{}
		for t in m.topics {
			mut partitions := []int{}
			for p in t.partition_indexes {
				partitions << int(p)
			}
			topics << domain.WriteTxnMarkerTopic{
				name:       t.name
				partitions: partitions
			}
		}
		txn_result := if m.transaction_result {
			domain.TransactionResult.commit
		} else {
			domain.TransactionResult.abort
		}
		result << domain.WriteTxnMarker{
			producer_id:        m.producer_id
			producer_epoch:     m.producer_epoch
			transaction_result: txn_result
			topics:             topics
		}
	}
	return result
}

// write_txn_marker_to_partition writes a transaction marker to a specific partition
fn (mut h Handler) write_txn_marker_to_partition(topic string, partition_index i32, producer_id i64, producer_epoch i16, committed bool, coordinator_epoch i32) i16 {
	// Check if topic exists
	topic_meta := h.storage.get_topic(topic) or { return i16(ErrorCode.unknown_topic_or_partition) }

	// Check if partition exists
	if partition_index < 0 || partition_index >= i32(topic_meta.partition_count) {
		return i16(ErrorCode.unknown_topic_or_partition)
	}

	// Write control record (transaction marker) to the partition
	// Control records have special attributes to indicate commit/abort
	control_records := build_txn_control_records(producer_id, producer_epoch, committed)

	// Append the control record to storage
	h.storage.append(topic, int(partition_index), control_records, i16(1)) or {
		return i16(ErrorCode.unknown_server_error)
	}

	return i16(ErrorCode.none)
}

// build_txn_control_records builds control records for a transaction commit/abort.
// Kafka control record format:
// - Key: version (INT16) + type (INT16), where type is 0=ABORT, 1=COMMIT
// - Value: version (INT16) + type (INT16) + coordinator_epoch (INT32)
// - Records are marked with is_control_record=true and the appropriate control_type
// - producer_id and producer_epoch are stored in Record metadata for RecordBatch attributes
fn build_txn_control_records(producer_id i64, producer_epoch i16, committed bool) []domain.Record {
	control_type := if committed {
		domain.ControlRecordType.commit
	} else {
		domain.ControlRecordType.abort
	}
	marker_type := if committed { i16(1) } else { i16(0) }

	// Control record key format (Kafka standard):
	// - version: INT16 (0)
	// - type: INT16 (0=ABORT, 1=COMMIT)
	mut key_writer := new_writer()
	key_writer.write_i16(0)
	key_writer.write_i16(marker_type)

	// Control record value format (Kafka standard):
	// - version: INT16 (0)
	// - type: INT16 (0=ABORT, 1=COMMIT)
	// - coordinator_epoch: INT32 (optional, set to 0)
	mut value_writer := new_writer()
	value_writer.write_i16(0)
	value_writer.write_i16(marker_type)
	value_writer.write_i32(0)

	return [
		domain.Record{
			key:               key_writer.bytes()
			value:             value_writer.bytes()
			headers:           map[string][]u8{}
			timestamp:         time.now()
			is_control_record: true
			control_type:      control_type
			producer_id:       producer_id
			producer_epoch:    producer_epoch
		},
	]
}

// TxnOffsetCommit handler (API Key 28)
fn (mut h Handler) handle_txn_offset_commit(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.txn_offset_commit, version)
	mut reader := new_reader(body)
	req := parse_txn_offset_commit_request(mut reader, version, is_flexible)!

	// Check if transaction coordinator is present
	if mut txn_coord := h.txn_coordinator {
		// Verify transaction existence and validity
		meta := txn_coord.get_transaction(req.transactional_id) or {
			// Transaction not found
			return build_txn_offset_commit_error_response(req, i16(ErrorCode.transactional_id_not_found),
				version)
		}

		// Validate producer ID and epoch
		if meta.producer_id != req.producer_id {
			return build_txn_offset_commit_error_response(req, i16(ErrorCode.invalid_producer_id_mapping),
				version)
		}

		if meta.producer_epoch != req.producer_epoch {
			return build_txn_offset_commit_error_response(req, i16(ErrorCode.invalid_producer_epoch),
				version)
		}

		// Validate transaction state - must be ongoing
		if meta.state != .ongoing {
			return build_txn_offset_commit_error_response(req, i16(ErrorCode.invalid_txn_state),
				version)
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
			return build_txn_offset_commit_error_response(req, i16(ErrorCode.unknown_server_error),
				version)
		}

		// Build success response
		return build_txn_offset_commit_success_response(req, version)
	}

	// Coordinator not available - return error for all partitions
	return build_txn_offset_commit_error_response(req, i16(ErrorCode.coordinator_not_available),
		version)
}

// build_txn_offset_commit_error_response builds error response for all partitions
fn build_txn_offset_commit_error_response(req TxnOffsetCommitRequest, error_code i16, version i16) []u8 {
	mut topics := []TxnOffsetCommitResponseTopic{}
	for t in req.topics {
		mut partitions := []TxnOffsetCommitResponsePartition{}
		for p in t.partitions {
			partitions << TxnOffsetCommitResponsePartition{
				partition_index: p.partition_index
				error_code:      error_code
			}
		}
		topics << TxnOffsetCommitResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}
	return TxnOffsetCommitResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}.encode(version)
}

// build_txn_offset_commit_success_response builds success response for all partitions
fn build_txn_offset_commit_success_response(req TxnOffsetCommitRequest, version i16) []u8 {
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
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}.encode(version)
}

// build_add_partitions_error_response builds AddPartitionsToTxn response for all
// partitions with the given error code. Used for both error and success cases.
fn build_add_partitions_error_response(req AddPartitionsToTxnRequest, error_code ErrorCode) AddPartitionsToTxnResponse {
	mut results := []AddPartitionsToTxnResult{}
	for t in req.topics {
		mut p_results := []AddPartitionsToTxnPartitionResult{}
		for p in t.partitions {
			p_results << AddPartitionsToTxnPartitionResult{
				partition_index: p
				error_code:      i16(error_code)
			}
		}
		results << AddPartitionsToTxnResult{
			name:       t.name
			partitions: p_results
		}
	}
	return AddPartitionsToTxnResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}
}

// build_fallback_producer_id_response generates a producer ID response for
// idempotent producers when no transaction coordinator is available.
fn build_fallback_producer_id_response(req InitProducerIdRequest) InitProducerIdResponse {
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
		throttle_time_ms: default_throttle_time_ms
		error_code:       error_code
		producer_id:      producer_id
		producer_epoch:   producer_epoch
	}
}

// InitProducerId request processor (frame-based)
fn (mut h Handler) process_init_producer_id(req InitProducerIdRequest, version i16) !InitProducerIdResponse {
	// Use TransactionCoordinator if available
	if mut txn_coord := h.txn_coordinator {
		result := txn_coord.init_producer_id(req.transactional_id, req.transaction_timeout_ms,
			req.producer_id, req.producer_epoch) or {
			return InitProducerIdResponse{
				throttle_time_ms: default_throttle_time_ms
				error_code:       i16(ErrorCode.unknown_server_error)
				producer_id:      -1
				producer_epoch:   -1
			}
		}

		return InitProducerIdResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       0
			producer_id:      result.producer_id
			producer_epoch:   result.producer_epoch
		}
	}

	// Fallback for idempotent producers
	return build_fallback_producer_id_response(req)
}
