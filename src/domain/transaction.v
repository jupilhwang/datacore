module domain

// TransactionState represents the state of a transaction
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

// TransactionResult represents the result of a transaction
pub enum TransactionResult {
	commit = 0
	abort  = 1
}

// TransactionMetadata holds the state of a transactional producer
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

// Helper methods for enums

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
		else { .empty } // Default
	}
}

pub fn (r TransactionResult) boolean() bool {
	return r == .commit
}

pub struct InitProducerIdResult {
pub:
	producer_id    i64
	producer_epoch i16
}
