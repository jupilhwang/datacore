// Kafka 프로토콜 - 트랜잭션 파서
// 트랜잭션 관련 요청의 파싱 함수
module kafka

/// parse_init_producer_id_request는 InitProducerId 요청을 파싱합니다 (API Key 22).
fn parse_init_producer_id_request(mut reader BinaryReader, version i16, is_flexible bool) !InitProducerIdRequest {
	// transactional_id: NULLABLE_STRING (v0-v1) / COMPACT_NULLABLE_STRING (v2+)
	raw_transactional_id := reader.read_flex_nullable_string(is_flexible)!
	// 빈 문자열을 optional 타입의 none으로 변환
	transactional_id := if raw_transactional_id.len > 0 {
		?string(raw_transactional_id)
	} else {
		?string(none)
	}

	// transaction_timeout_ms: INT32 - 트랜잭션 타임아웃
	transaction_timeout_ms := reader.read_i32()!

	// v3+: producer_id와 producer_epoch 추가됨
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

/// parse_add_partitions_to_txn_request는 AddPartitionsToTxn 요청을 파싱합니다 (API Key 24).
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

/// parse_add_offsets_to_txn_request는 AddOffsetsToTxn 요청을 파싱합니다 (API Key 25).
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

/// parse_end_txn_request는 EndTxn 요청을 파싱합니다 (API Key 26).
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

/// parse_write_txn_markers_request는 WriteTxnMarkers 요청을 파싱합니다 (API Key 27).
fn parse_write_txn_markers_request(mut reader BinaryReader, version i16, is_flexible bool) !WriteTxnMarkersRequest {
	marker_count := reader.read_flex_array_len(is_flexible)!

	mut markers := []WriteTxnMarker{}
	for _ in 0 .. marker_count {
		producer_id := reader.read_i64()!
		producer_epoch := reader.read_i16()!
		transaction_result := reader.read_i8()! != 0

		topic_count := reader.read_flex_array_len(is_flexible)!

		mut topics := []WriteTxnMarkerTopic{}
		for _ in 0 .. topic_count {
			name := reader.read_flex_string(is_flexible)!

			partition_count := reader.read_flex_array_len(is_flexible)!

			mut partition_indexes := []i32{}
			for _ in 0 .. partition_count {
				partition_indexes << reader.read_i32()!
			}

			topics << WriteTxnMarkerTopic{
				name:              name
				partition_indexes: partition_indexes
			}

			reader.skip_flex_tagged_fields(is_flexible)!
		}

		coordinator_epoch := reader.read_i32()!

		markers << WriteTxnMarker{
			producer_id:        producer_id
			producer_epoch:     producer_epoch
			transaction_result: transaction_result
			topics:             topics
			coordinator_epoch:  coordinator_epoch
		}

		reader.skip_flex_tagged_fields(is_flexible)!
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return WriteTxnMarkersRequest{
		markers: markers
	}
}

/// parse_txn_offset_commit_request는 TxnOffsetCommit 요청을 파싱합니다 (API Key 28).
fn parse_txn_offset_commit_request(mut reader BinaryReader, version i16, is_flexible bool) !TxnOffsetCommitRequest {
	transactional_id := reader.read_flex_string(is_flexible)!
	group_id := reader.read_flex_string(is_flexible)!
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!

	// v3+: generation_id, member_id, group_instance_id 추가됨
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
			// v2+: committed_leader_epoch 추가됨
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
