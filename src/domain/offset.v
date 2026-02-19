// Kafka 컨슈머 오프셋 관련 데이터 구조를 정의합니다.
module domain

/// PartitionOffset은 파티션에 대해 커밋된 오프셋을 나타냅니다.
/// topic: 토픽 이름
/// partition: 파티션 번호
/// offset: 커밋된 오프셋
/// leader_epoch: 리더 에포크 (기본 -1)
/// metadata: 사용자 정의 메타데이터
pub struct PartitionOffset {
pub:
	topic        string
	partition    int
	offset       i64
	leader_epoch i32 = -1
	metadata     string
}

/// OffsetCommit은 오프셋 커밋 요청을 나타냅니다.
/// group_id: 컨슈머 그룹 ID
/// generation_id: 세대 ID
/// member_id: 멤버 ID
/// offsets: 커밋할 파티션 오프셋 목록
pub struct OffsetCommit {
pub:
	group_id      string
	generation_id int
	member_id     string
	offsets       []PartitionOffset
}

/// OffsetFetchResult는 오프셋 조회 결과를 나타냅니다.
/// topic: 토픽 이름
/// partition: 파티션 번호
/// offset: 조회된 오프셋
/// metadata: 사용자 정의 메타데이터
/// error_code: 에러 코드 (0 = 성공)
pub struct OffsetFetchResult {
pub:
	topic      string
	partition  int
	offset     i64
	metadata   string
	error_code i16
}
