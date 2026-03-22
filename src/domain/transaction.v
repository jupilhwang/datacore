// Supports exactly-once semantics.
module domain

/// TransactionState represents the state of a transaction.
/// empty: initial state
/// ongoing: in progress
/// prepare_commit: preparing to commit
/// prepare_abort: preparing to roll back
/// complete_commit: commit complete
/// complete_abort: rollback complete
/// dead: terminated
/// prepare_epoch_fence: preparing epoch fencing
pub enum TransactionState {
	empty               = 0
	ongoing             = 1
	prepare_commit      = 2
	prepare_abort       = 3
	complete_commit     = 4
	complete_abort      = 5
	dead                = 6
	prepare_epoch_fence = 7
}

/// TransactionResult represents the outcome of a transaction.
/// commit: commit (success)
/// abort: rollback (cancelled)
pub enum TransactionResult {
	commit = 0
	abort  = 1
}

/// TransactionMetadata holds the state of a transactional producer.
/// transactional_id: transaction ID
/// producer_id: producer ID
/// producer_epoch: producer epoch
/// txn_timeout_ms: transaction timeout (milliseconds)
/// state: transaction state
/// topic_partitions: list of topic-partitions included in the transaction
/// txn_start_timestamp: transaction start time
/// txn_last_update_timestamp: last update time
pub struct TransactionMetadata {
pub:
	transactional_id          string
	producer_id               i64
	producer_epoch            i16
	txn_timeout_ms            i32
	state                     TransactionState
	topic_partitions          []TopicPartition
	txn_start_timestamp       i64
	txn_last_update_timestamp i64
}

// Helper methods

/// str converts TransactionState to a string.
pub fn (s TransactionState) str() string {
	return match s {
		.empty { 'Empty' }
		.ongoing { 'Ongoing' }
		.prepare_commit { 'PrepareCommit' }
		.prepare_abort { 'PrepareAbort' }
		.complete_commit { 'CompleteCommit' }
		.complete_abort { 'CompleteAbort' }
		.dead { 'Dead' }
		.prepare_epoch_fence { 'PrepareEpochFence' }
	}
}

/// transaction_state_from_string converts a string to a TransactionState.
pub fn transaction_state_from_string(s string) TransactionState {
	return match s {
		'Empty' { .empty }
		'Ongoing' { .ongoing }
		'PrepareCommit' { .prepare_commit }
		'PrepareAbort' { .prepare_abort }
		'CompleteCommit' { .complete_commit }
		'CompleteAbort' { .complete_abort }
		'Dead' { .dead }
		'PrepareEpochFence' { .prepare_epoch_fence }
		else { .empty }
	}
}

/// boolean returns whether the TransactionResult is a commit.
pub fn (r TransactionResult) boolean() bool {
	return r == .commit
}

/// InitProducerIdResult is the result of the InitProducerId API.
/// producer_id: assigned producer ID
/// producer_epoch: producer epoch
pub struct InitProducerIdResult {
pub:
	producer_id    i64
	producer_epoch i16
}

// WriteTxnMarkers domain types
// Used by the coordinator to validate and process transaction markers
// during commit/abort flows.

/// WriteTxnMarker represents a single transaction marker to write.
/// Contains producer identity and the target partitions.
pub struct WriteTxnMarker {
pub:
	producer_id        i64
	producer_epoch     i16
	transaction_result TransactionResult
	topics             []WriteTxnMarkerTopic
}

/// WriteTxnMarkerTopic represents a topic and its partitions for marker writing.
pub struct WriteTxnMarkerTopic {
pub:
	name       string
	partitions []int
}

/// WriteTxnMarkerResult holds the result of writing markers for a single producer.
pub struct WriteTxnMarkerResult {
pub:
	producer_id i64
	topics      []WriteTxnMarkerTopicResult
}

/// WriteTxnMarkerTopicResult holds per-topic marker writing results.
pub struct WriteTxnMarkerTopicResult {
pub:
	name       string
	partitions []WriteTxnMarkerPartitionResult
}

/// WriteTxnMarkerPartitionResult holds per-partition marker writing result.
pub struct WriteTxnMarkerPartitionResult {
pub:
	partition  int
	error_code i16
}
