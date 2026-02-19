module offset

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
	topic_id               ?[]u8 // Topic ID (v10+, UUID)
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

/// OffsetError represents errors that occur during offset operations.
pub enum OffsetError {
	none
	invalid_group_id
	group_not_found
	invalid_topic
	topic_not_found
	partition_out_of_range
	storage_error
	unknown_error
}

/// to_error_code converts an OffsetError to a Kafka error code.
pub fn (e OffsetError) to_error_code() i16 {
	return match e {
		.none { 0 }
		.invalid_group_id { i16(domain.ErrorCode.invalid_group_id) }
		.group_not_found { i16(domain.ErrorCode.group_id_not_found) }
		.invalid_topic { i16(domain.ErrorCode.invalid_topic_exception) }
		.topic_not_found { i16(domain.ErrorCode.unknown_topic_or_partition) }
		.partition_out_of_range { i16(domain.ErrorCode.unknown_topic_or_partition) }
		.storage_error { i16(domain.ErrorCode.unknown_server_error) }
		.unknown_error { i16(domain.ErrorCode.unknown_server_error) }
	}
}
