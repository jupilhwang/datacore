// Kafka protocol - Share Group State types (KIP-932)
// InitializeShareGroupState (API Key 83), ReadShareGroupState (API Key 84),
// WriteShareGroupState (API Key 85), DeleteShareGroupState (API Key 86)
// Request/response struct definitions for Share Group state persistence.
module kafka

// --- Common types ---

/// StateBatch represents a batch of record states within a share partition.
/// Used by ReadShareGroupState and WriteShareGroupState APIs.
pub struct StateBatch {
pub:
	first_offset   i64
	last_offset    i64
	delivery_state i8
	delivery_count i16
}

// --- InitializeShareGroupState (API Key 83) ---

/// InitializeShareGroupStateRequest initializes share partition state
/// when a share group first accesses a partition.
pub struct InitializeShareGroupStateRequest {
pub:
	group_id string
	topics   []InitializeShareGroupStateTopicData
}

/// InitializeShareGroupStateTopicData contains per-topic data for initialization.
pub struct InitializeShareGroupStateTopicData {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []InitializeShareGroupStatePartitionData
}

/// InitializeShareGroupStatePartitionData contains per-partition data for initialization.
pub struct InitializeShareGroupStatePartitionData {
pub:
	partition    i32
	state_epoch  i32
	start_offset i64
}

/// InitializeShareGroupStateResponse is the response to an InitializeShareGroupState request.
pub struct InitializeShareGroupStateResponse {
pub:
	throttle_time_ms i32
	results          []InitializeShareGroupStateResult
}

/// InitializeShareGroupStateResult contains per-partition results.
pub struct InitializeShareGroupStateResult {
pub:
	partition     i32
	error_code    i16
	error_message string
}

// --- ReadShareGroupState (API Key 84) ---

/// ReadShareGroupStateRequest reads the current state of share partitions.
pub struct ReadShareGroupStateRequest {
pub:
	group_id string
	topics   []ReadShareGroupStateTopicData
}

/// ReadShareGroupStateTopicData contains per-topic data for reading state.
pub struct ReadShareGroupStateTopicData {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []ReadShareGroupStatePartitionData
}

/// ReadShareGroupStatePartitionData contains per-partition data for reading state.
pub struct ReadShareGroupStatePartitionData {
pub:
	partition    i32
	leader_epoch i32
}

/// ReadShareGroupStateResponse is the response to a ReadShareGroupState request.
pub struct ReadShareGroupStateResponse {
pub:
	throttle_time_ms i32
	results          []ReadShareGroupStateTopicResult
}

/// ReadShareGroupStateTopicResult contains per-topic results for reading state.
pub struct ReadShareGroupStateTopicResult {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []ReadShareGroupStatePartitionResult
}

/// ReadShareGroupStatePartitionResult contains per-partition results for reading state.
pub struct ReadShareGroupStatePartitionResult {
pub:
	partition     i32
	state_epoch   i32
	start_offset  i64
	state_batches []StateBatch
	error_code    i16
	error_message string
}

// --- WriteShareGroupState (API Key 85) ---

/// WriteShareGroupStateRequest writes updated share partition state.
pub struct WriteShareGroupStateRequest {
pub:
	group_id string
	topics   []WriteShareGroupStateTopicData
}

/// WriteShareGroupStateTopicData contains per-topic data for writing state.
pub struct WriteShareGroupStateTopicData {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []WriteShareGroupStatePartitionData
}

/// WriteShareGroupStatePartitionData contains per-partition data for writing state.
pub struct WriteShareGroupStatePartitionData {
pub:
	partition               i32
	state_epoch             i32
	leader_epoch            i32
	start_offset            i64
	delivery_complete_count i32
	state_batches           []StateBatch
}

/// WriteShareGroupStateResponse is the response to a WriteShareGroupState request.
pub struct WriteShareGroupStateResponse {
pub:
	throttle_time_ms i32
	results          []WriteShareGroupStateResult
}

/// WriteShareGroupStateResult contains per-partition results for writing state.
pub struct WriteShareGroupStateResult {
pub:
	partition     i32
	error_code    i16
	error_message string
}

// --- DeleteShareGroupState (API Key 86) ---

/// DeleteShareGroupStateRequest deletes share partition state when cleaning up a share group.
pub struct DeleteShareGroupStateRequest {
pub:
	group_id string
	topics   []DeleteShareGroupStateTopicData
}

/// DeleteShareGroupStateTopicData contains per-topic data for deletion.
pub struct DeleteShareGroupStateTopicData {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

/// DeleteShareGroupStateResponse is the response to a DeleteShareGroupState request.
pub struct DeleteShareGroupStateResponse {
pub:
	throttle_time_ms i32
	results          []DeleteShareGroupStateResult
}

/// DeleteShareGroupStateResult contains per-partition results for deletion.
pub struct DeleteShareGroupStateResult {
pub:
	partition     i32
	error_code    i16
	error_message string
}
