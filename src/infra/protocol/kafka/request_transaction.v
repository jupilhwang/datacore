// Infra Layer - Kafka Request Parsing - Transaction Operations
// InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit request parsing
module kafka

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
	raw_transactional_id := if is_flexible {
		reader.read_compact_nullable_string()!
	} else {
		reader.read_nullable_string()!
	}
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
	transactional_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut topics := []AddPartitionsToTxnTopic{}
	for _ in 0 .. topic_count {
		name := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}

		partition_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		mut partitions := []i32{}
		for _ in 0 .. partition_count {
			partitions << reader.read_i32()!
		}

		topics << AddPartitionsToTxnTopic{
			name:       name
			partitions: partitions
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

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
	transactional_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!
	group_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

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
	transactional_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!
	transaction_result := reader.read_i8()! != 0

	if is_flexible {
		reader.skip_tagged_fields()!
	}

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
	transactional_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}
	group_id := if is_flexible {
		reader.read_compact_string()!
	} else {
		reader.read_string()!
	}
	producer_id := reader.read_i64()!
	producer_epoch := reader.read_i16()!

	// v3+: generation_id, member_id, group_instance_id
	mut generation_id := i32(-1)
	mut member_id := ''
	mut group_instance_id := ?string(none)
	if version >= 3 {
		generation_id = reader.read_i32()!
		member_id = if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
		raw_group_instance_id := if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
		group_instance_id = if raw_group_instance_id.len > 0 { raw_group_instance_id } else { none }
	}

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut topics := []TxnOffsetCommitRequestTopic{}
	for _ in 0 .. topic_count {
		name := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}

		partition_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

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

			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topics << TxnOffsetCommitRequestTopic{
			name:       name
			partitions: partitions
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

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
