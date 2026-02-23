// Transaction-related request/response struct definitions
//
// Used for producer ID initialization, transaction begin/end, offset commit, etc.
module kafka

/// InitProducerId request - request by idempotent/transactional producer to obtain a producer ID
///
/// Idempotent producers require a producer ID to prevent duplicate messages.
/// Transactional producers must additionally provide a transactional_id.
pub struct InitProducerIdRequest {
pub:
	transactional_id       ?string
	transaction_timeout_ms i32
	producer_id            i64
	producer_epoch         i16
}

/// InitProducerId response - returns producer ID to idempotent/transactional producer
pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	producer_id      i64
	producer_epoch   i16
}

/// AddPartitionsToTxn request - request to add partitions to a transaction
///
/// Before a transactional producer writes to a new partition,
/// that partition must be registered with the transaction.
pub struct AddPartitionsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	topics           []AddPartitionsToTxnTopic
}

/// AddPartitionsToTxn topic - topic to add to the transaction
pub struct AddPartitionsToTxnTopic {
pub:
	name       string
	partitions []i32
}

/// AddPartitionsToTxn response - result of adding partitions
pub struct AddPartitionsToTxnResponse {
pub:
	throttle_time_ms i32
	results          []AddPartitionsToTxnResult
}

/// AddPartitionsToTxn result - per-topic partition add result
pub struct AddPartitionsToTxnResult {
pub:
	name       string
	partitions []AddPartitionsToTxnPartitionResult
}

/// AddPartitionsToTxn partition result - per-partition add result
pub struct AddPartitionsToTxnPartitionResult {
pub:
	partition_index i32
	error_code      i16
}

// AddOffsetsToTxn (API Key 25) - add offsets to transaction

/// AddOffsetsToTxn request - request to add consumer group offsets to a transaction
///
/// Registers the group with the transaction before committing offsets within it.
/// This enables "exactly-once" semantics.
pub struct AddOffsetsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	group_id         string
}

/// AddOffsetsToTxn response - result of adding offsets
pub struct AddOffsetsToTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

/// EndTxn request - request to commit or abort a transaction
///
/// transaction_result == true means COMMIT, false means ABORT.
pub struct EndTxnRequest {
pub:
	transactional_id   string
	producer_id        i64
	producer_epoch     i16
	transaction_result bool
}

/// EndTxn response - result of ending the transaction
pub struct EndTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

/// TxnOffsetCommit request - request to commit offsets within a transaction
///
/// Offsets are only actually committed when the transaction is committed.
/// If the transaction is aborted, offset commits are also rolled back.
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

/// TxnOffsetCommit request topic - topic to commit
pub struct TxnOffsetCommitRequestTopic {
pub:
	name       string
	partitions []TxnOffsetCommitRequestPartition
}

/// TxnOffsetCommit request partition - partition offset to commit
pub struct TxnOffsetCommitRequestPartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     string
}

/// TxnOffsetCommit response - result of transactional offset commit
pub struct TxnOffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []TxnOffsetCommitResponseTopic
}

/// TxnOffsetCommit response topic - per-topic commit result
pub struct TxnOffsetCommitResponseTopic {
pub:
	name       string
	partitions []TxnOffsetCommitResponsePartition
}

/// TxnOffsetCommit response partition - per-partition commit result
pub struct TxnOffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

// WriteTxnMarkers (API Key 27) - write transaction markers

/// WriteTxnMarkers request - request by transaction coordinator to write markers to partition leaders
///
/// This is a broker-to-broker communication API.
/// The transaction coordinator writes markers to each partition when ending a transaction.
pub struct WriteTxnMarkersRequest {
pub:
	markers []WriteTxnMarker
}

/// WriteTxnMarker - a single transaction marker to write
pub struct WriteTxnMarker {
pub:
	producer_id        i64
	producer_epoch     i16
	transaction_result bool
	topics             []WriteTxnMarkerTopic
	coordinator_epoch  i32
}

/// WriteTxnMarker topic - topic in a WriteTxnMarkers request
pub struct WriteTxnMarkerTopic {
pub:
	name              string
	partition_indexes []i32
}

/// WriteTxnMarkers response - response to a WriteTxnMarkers request
pub struct WriteTxnMarkersResponse {
pub:
	markers []WriteTxnMarkerResult
}

/// WriteTxnMarker result - result for a single producer ID
pub struct WriteTxnMarkerResult {
pub:
	producer_id i64
	topics      []WriteTxnMarkerTopicResult
}

/// WriteTxnMarker topic result - result for a topic
pub struct WriteTxnMarkerTopicResult {
pub:
	name       string
	partitions []WriteTxnMarkerPartitionResult
}

/// WriteTxnMarker partition result - result for a partition
pub struct WriteTxnMarkerPartitionResult {
pub:
	partition_index i32
	error_code      i16
}
