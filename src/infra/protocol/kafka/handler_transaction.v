// Kafka 프로토콜 - Transaction 핸들러
// 트랜잭션 관련 작업을 위한 핸들러 메서드:
// InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit, WriteTxnMarkers
module kafka

import domain
import infra.observability
import rand
import time

// InitProducerId 처리 (API Key 22)
// 멱등성/트랜잭션 프로듀서를 위한 프로듀서 ID 반환
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
		throttle_time_ms: default_throttle_time_ms
		error_code:       error_code
		producer_id:      producer_id
		producer_epoch:   producer_epoch
	}

	elapsed := time.since(start_time)
	h.logger.debug('Init producer id completed', observability.field_int('producer_id',
		producer_id), observability.field_int('error_code', error_code), observability.field_duration('latency',
		elapsed))

	return resp.encode(version)
}

// AddPartitionsToTxn 처리 (API Key 24)
fn (mut h Handler) handle_add_partitions_to_txn(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := is_flexible_version(.add_partitions_to_txn, version)
	mut reader := new_reader(body)
	req := parse_add_partitions_to_txn_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing add partitions to txn', observability.field_string('txn_id',
		req.transactional_id), observability.field_int('producer_id', req.producer_id),
		observability.field_int('topics', req.topics.len))

	if mut txn_coord := h.txn_coordinator {
		// 요청 토픽을 TopicPartition 리스트로 변환
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
				throttle_time_ms: default_throttle_time_ms
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
			throttle_time_ms: default_throttle_time_ms
			results:          results
		}.encode(version)
	}

	elapsed := time.since(start_time)
	h.logger.debug('Add partitions to txn completed', observability.field_string('txn_id',
		req.transactional_id), observability.field_duration('latency', elapsed))

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
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}.encode(version)
}

// EndTxn 처리 (API Key 26)
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
				error_code:       i16(ErrorCode.invalid_txn_state) // Simplified error mapping
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

// AddOffsetsToTxn 처리 (API Key 25)
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
				error_code:       i16(ErrorCode.invalid_txn_state) // Simplified error mapping
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

// WriteTxnMarkers 처리 (API Key 27)
// 이 API는 트랜잭션 코디네이터가 파티션 리더에 commit/abort 마커를 쓰는 데 사용됩니다.
// 브로커 간 통신 API입니다.
fn (mut h Handler) handle_write_txn_markers(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.write_txn_markers, version)
	mut reader := new_reader(body)
	req := parse_write_txn_markers_request(mut reader, version, is_flexible)!

	mut results := []WriteTxnMarkerResult{}

	for marker in req.markers {
		mut topic_results := []WriteTxnMarkerTopicResult{}

		for topic in marker.topics {
			mut partition_results := []WriteTxnMarkerPartitionResult{}

			for partition_index in topic.partition_indexes {
				// 파티션에 트랜잭션 마커 (commit/abort) 쓰기
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

		results << WriteTxnMarkerResult{
			producer_id: marker.producer_id
			topics:      topic_results
		}
	}

	return WriteTxnMarkersResponse{
		markers: results
	}.encode(version)
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
	h.storage.append(topic, int(partition_index), control_records) or {
		return i16(ErrorCode.unknown_server_error)
	}

	return i16(ErrorCode.none)
}

// build_txn_control_records는 트랜잭션 commit/abort를 위한 컨트롤 레코드를 생성합니다.
// Kafka 컨트롤 레코드 형식:
// - Key: version (INT16) + type (INT16), type은 0=ABORT, 1=COMMIT
// - Value: version (INT16) + type (INT16) + coordinator_epoch (INT32)
// - 레코드는 is_control_record=true와 적절한 control_type으로 표시됨
// - producer_id와 producer_epoch는 RecordBatch attributes를 위해 Record 메타데이터에 저장됨
fn build_txn_control_records(producer_id i64, producer_epoch i16, committed bool) []domain.Record {
	control_type := if committed {
		domain.ControlRecordType.commit
	} else {
		domain.ControlRecordType.abort
	}
	marker_type := if committed { i16(1) } else { i16(0) }

	// 컨트롤 레코드 키 형식 (Kafka 표준):
	// - version: INT16 (0)
	// - type: INT16 (0=ABORT, 1=COMMIT)
	mut key_writer := new_writer()
	key_writer.write_i16(0) // version
	key_writer.write_i16(marker_type)

	// 컨트롤 레코드 값 형식 (Kafka 표준):
	// - version: INT16 (0)
	// - type: INT16 (0=ABORT, 1=COMMIT)
	// - coordinator_epoch: INT32 (선택사항, 0으로 설정)
	mut value_writer := new_writer()
	value_writer.write_i16(0) // version
	value_writer.write_i16(marker_type)
	value_writer.write_i32(0) // coordinator_epoch

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

// TxnOffsetCommit 처리 (API Key 28)
fn (mut h Handler) handle_txn_offset_commit(body []u8, version i16) ![]u8 {
	is_flexible := is_flexible_version(.txn_offset_commit, version)
	mut reader := new_reader(body)
	req := parse_txn_offset_commit_request(mut reader, version, is_flexible)!

	// 트랜잭션 코디네이터 존재 확인
	if mut txn_coord := h.txn_coordinator {
		// 트랜잭션 존재 및 유효성 확인
		meta := txn_coord.get_transaction(req.transactional_id) or {
			// 트랜잭션을 찾을 수 없음
			return build_txn_offset_commit_error_response(req, i16(ErrorCode.transactional_id_not_found),
				version)
		}

		// 프로듀서 ID 및 epoch 유효성 검사
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

// InitProducerId 요청 처리 (Frame 기반)
fn (mut h Handler) process_init_producer_id(req InitProducerIdRequest, version i16) !InitProducerIdResponse {
	// 가능하면 TransactionCoordinator 사용
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
