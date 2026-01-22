// 서비스 레이어 - Offset 유스케이스 타입
// OffsetCommit, OffsetFetch 요청/응답의 서비스 레벨 타입을 정의합니다.
module offset

import domain

/// OffsetCommitRequest는 오프셋 커밋 요청을 나타냅니다.
pub struct OffsetCommitRequest {
pub:
	group_id string                   // 컨슈머 그룹 ID
	offsets  []domain.PartitionOffset // 커밋할 오프셋 목록
}

/// OffsetCommitResult는 오프셋 커밋 결과를 나타냅니다.
pub struct OffsetCommitResult {
pub:
	topic         string // 토픽 이름
	partition     int    // 파티션 번호
	error_code    i16    // 오류 코드 (0이면 성공)
	error_message string // 오류 메시지 (있는 경우)
}

/// OffsetCommitResponse는 오프셋 커밋 응답을 나타냅니다.
pub struct OffsetCommitResponse {
pub:
	results []OffsetCommitResult // 각 파티션별 결과
}

/// OffsetFetchRequest는 오프셋 조회 요청을 나타냅니다.
pub struct OffsetFetchRequest {
pub:
	group_id       string                  // 컨슈머 그룹 ID
	partitions     []domain.TopicPartition // 조회할 파티션 목록
	require_stable bool                    // 안정적인 오프셋만 반환할지 여부 (v7+)
}

/// OffsetFetchResult는 오프셋 조회 결과를 나타냅니다.
pub struct OffsetFetchResult {
pub:
	topic                  string // 토픽 이름
	topic_id               ?[]u8  // 토픽 ID (v10+, UUID)
	partition              int    // 파티션 번호
	committed_offset       i64    // 커밋된 오프셋
	committed_leader_epoch i32    // 커밋된 리더 에포크
	metadata               string // 메타데이터
	error_code             i16    // 오류 코드 (0이면 성공)
}

/// OffsetFetchResponse는 오프셋 조회 응답을 나타냅니다.
pub struct OffsetFetchResponse {
pub:
	results    []OffsetFetchResult // 각 파티션별 결과
	error_code i16                 // 전체 오류 코드 (v2+)
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
