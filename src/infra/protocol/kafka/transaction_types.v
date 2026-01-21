// Kafka Protocol - Transaction Types
// Struct definitions for transaction-related requests and responses
module kafka

// ============================================================================
// InitProducerId (API Key 22)
// ============================================================================

// InitProducerIdRequest is used by idempotent/transactional producers to obtain a producer ID
pub struct InitProducerIdRequest {
pub:
	transactional_id       ?string // Nullable - null for non-transactional producer
	transaction_timeout_ms i32     // Timeout for transactions
	producer_id            i64     // Existing producer ID or -1 for new
	producer_epoch         i16     // Existing epoch or -1 for new
}

// InitProducerIdResponse returns a producer ID for idempotent/transactional producers
pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32 // Throttle time in milliseconds
	error_code       i16 // Error code (0 = success)
	producer_id      i64 // Assigned producer ID
	producer_epoch   i16 // Producer epoch
}

// ============================================================================
// AddPartitionsToTxn (API Key 24)
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

// ============================================================================
// AddOffsetsToTxn (API Key 25)
// ============================================================================

// AddOffsetsToTxnRequest adds consumer group offsets to a transaction
pub struct AddOffsetsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	group_id         string
}

pub struct AddOffsetsToTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

// ============================================================================
// EndTxn (API Key 26)
// ============================================================================

pub struct EndTxnRequest {
pub:
	transactional_id   string
	producer_id        i64
	producer_epoch     i16
	transaction_result bool // false=ABORT, true=COMMIT
}

pub struct EndTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

// ============================================================================
// TxnOffsetCommit (API Key 28)
// ============================================================================

// TxnOffsetCommitRequest commits offsets within a transaction
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
