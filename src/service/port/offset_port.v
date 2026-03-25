// Offset management interface following DIP.
// Abstracts offset commit/fetch operations from the concrete OffsetManager.
module port

import domain

/// OffsetCommitRequest represents an offset commit request.
pub struct OffsetCommitRequest {
pub:
	group_id string
	offsets  []domain.PartitionOffset
}

/// OffsetCommitResult represents the result of an offset commit.
pub struct OffsetCommitResult {
pub:
	topic         string
	partition     int
	error_code    i16
	error_message string
}

/// OffsetCommitResponse represents an offset commit response.
pub struct OffsetCommitResponse {
pub:
	results []OffsetCommitResult
}

/// OffsetFetchRequest represents an offset fetch request.
pub struct OffsetFetchRequest {
pub:
	group_id       string
	partitions     []domain.TopicPartition
	require_stable bool
}

/// OffsetFetchResult represents the result of an offset fetch.
pub struct OffsetFetchResult {
pub:
	topic                  string
	topic_id               ?[]u8
	partition              int
	committed_offset       i64
	committed_leader_epoch i32
	metadata               string
	error_code             i16
}

/// OffsetFetchResponse represents an offset fetch response.
pub struct OffsetFetchResponse {
pub:
	results    []OffsetFetchResult
	error_code i16
}

/// OffsetManagerPort abstracts consumer offset management.
/// Implemented by service/offset OffsetManager.
pub interface OffsetManagerPort {
mut:
	/// Commits consumer group offsets to storage.
	commit_offsets(req OffsetCommitRequest) !OffsetCommitResponse

	/// Fetches committed offsets for a consumer group.
	fetch_offsets(req OffsetFetchRequest) !OffsetFetchResponse
}
