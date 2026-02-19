module offset

import domain

/// OffsetCommitRequest는 오프셋 커밋 요청을 나타냅니다.
pub struct OffsetCommitRequest {
pub:
	group_id string
	offsets  []domain.PartitionOffset
}

/// OffsetCommitResult는 오프셋 커밋 결과를 나타냅니다.
pub struct OffsetCommitResult {
pub:
	topic         string
	partition     int
	error_code    i16
	error_message string
}

/// OffsetCommitResponse는 오프셋 커밋 응답을 나타냅니다.
pub struct OffsetCommitResponse {
pub:
	results []OffsetCommitResult
}

/// OffsetFetchRequest는 오프셋 조회 요청을 나타냅니다.
pub struct OffsetFetchRequest {
pub:
	group_id       string
	partitions     []domain.TopicPartition
	require_stable bool
}

/// OffsetFetchResult는 오프셋 조회 결과를 나타냅니다.
pub struct OffsetFetchResult {
pub:
	topic                  string
	topic_id               ?[]u8 // 토픽 ID (v10+, UUID)
	partition              int
	committed_offset       i64
	committed_leader_epoch i32
	metadata               string
	error_code             i16
}

/// OffsetFetchResponse는 오프셋 조회 응답을 나타냅니다.
pub struct OffsetFetchResponse {
pub:
	results    []OffsetFetchResult
	error_code i16
}

/// OffsetError는 오프셋 작업 중 발생하는 오류를 나타냅니다.
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

/// to_error_code는 OffsetError를 Kafka 에러 코드로 변환합니다.
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
