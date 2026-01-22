// 인프라 레이어 - Kafka 트랜잭션 타입 정의
// 트랜잭션 관련 요청/응답 구조체 정의
//
// 이 모듈은 Kafka 트랜잭션 프로토콜의 핵심 타입들을 정의합니다.
// 프로듀서 ID 초기화, 트랜잭션 시작/종료, 오프셋 커밋 등에 사용됩니다.
module kafka

// ============================================================================
// InitProducerId (API Key 22) - 프로듀서 ID 초기화
// ============================================================================

/// InitProducerId 요청 - 멱등성/트랜잭션 프로듀서가 프로듀서 ID를 얻기 위한 요청
///
/// 멱등성 프로듀서는 중복 메시지 방지를 위해 프로듀서 ID가 필요합니다.
/// 트랜잭션 프로듀서는 추가로 transactional_id를 제공해야 합니다.
pub struct InitProducerIdRequest {
pub:
	transactional_id       ?string // 트랜잭션 ID (nullable - 비트랜잭션 프로듀서는 null)
	transaction_timeout_ms i32     // 트랜잭션 타임아웃 (밀리초)
	producer_id            i64     // 기존 프로듀서 ID 또는 -1 (신규)
	producer_epoch         i16     // 기존 에포크 또는 -1 (신규)
}

/// InitProducerId 응답 - 멱등성/트랜잭션 프로듀서에게 프로듀서 ID 반환
pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32 // 스로틀링 시간 (밀리초)
	error_code       i16 // 에러 코드 (0 = 성공)
	producer_id      i64 // 할당된 프로듀서 ID
	producer_epoch   i16 // 프로듀서 에포크
}

// ============================================================================
// AddPartitionsToTxn (API Key 24) - 트랜잭션에 파티션 추가
// ============================================================================

/// AddPartitionsToTxn 요청 - 트랜잭션에 파티션을 추가하는 요청
///
/// 트랜잭션 프로듀서가 새로운 파티션에 메시지를 쓰기 전에
/// 해당 파티션을 트랜잭션에 등록해야 합니다.
pub struct AddPartitionsToTxnRequest {
pub:
	transactional_id string                    // 트랜잭션 ID
	producer_id      i64                       // 프로듀서 ID
	producer_epoch   i16                       // 프로듀서 에포크
	topics           []AddPartitionsToTxnTopic // 추가할 토픽/파티션 목록
}

/// AddPartitionsToTxn 토픽 - 트랜잭션에 추가할 토픽
pub struct AddPartitionsToTxnTopic {
pub:
	name       string // 토픽 이름
	partitions []i32  // 파티션 인덱스 목록
}

/// AddPartitionsToTxn 응답 - 파티션 추가 결과
pub struct AddPartitionsToTxnResponse {
pub:
	throttle_time_ms i32                        // 스로틀링 시간 (밀리초)
	results          []AddPartitionsToTxnResult // 토픽별 결과
}

/// AddPartitionsToTxn 결과 - 토픽별 파티션 추가 결과
pub struct AddPartitionsToTxnResult {
pub:
	name       string // 토픽 이름
	partitions []AddPartitionsToTxnPartitionResult // 파티션별 결과
}

/// AddPartitionsToTxn 파티션 결과 - 파티션별 추가 결과
pub struct AddPartitionsToTxnPartitionResult {
pub:
	partition_index i32 // 파티션 인덱스
	error_code      i16 // 에러 코드
}

// ============================================================================
// AddOffsetsToTxn (API Key 25) - 트랜잭션에 오프셋 추가
// ============================================================================

/// AddOffsetsToTxn 요청 - 트랜잭션에 컨슈머 그룹 오프셋을 추가하는 요청
///
/// 트랜잭션 내에서 오프셋을 커밋하기 전에 그룹을 트랜잭션에 등록합니다.
/// 이를 통해 "exactly-once" 시맨틱을 구현할 수 있습니다.
pub struct AddOffsetsToTxnRequest {
pub:
	transactional_id string // 트랜잭션 ID
	producer_id      i64    // 프로듀서 ID
	producer_epoch   i16    // 프로듀서 에포크
	group_id         string // 컨슈머 그룹 ID
}

/// AddOffsetsToTxn 응답 - 오프셋 추가 결과
pub struct AddOffsetsToTxnResponse {
pub:
	throttle_time_ms i32 // 스로틀링 시간 (밀리초)
	error_code       i16 // 에러 코드
}

// ============================================================================
// EndTxn (API Key 26) - 트랜잭션 종료
// ============================================================================

/// EndTxn 요청 - 트랜잭션을 커밋하거나 중단하는 요청
///
/// transaction_result가 true이면 COMMIT, false이면 ABORT입니다.
pub struct EndTxnRequest {
pub:
	transactional_id   string // 트랜잭션 ID
	producer_id        i64    // 프로듀서 ID
	producer_epoch     i16    // 프로듀서 에포크
	transaction_result bool   // 트랜잭션 결과 (false=ABORT, true=COMMIT)
}

/// EndTxn 응답 - 트랜잭션 종료 결과
pub struct EndTxnResponse {
pub:
	throttle_time_ms i32 // 스로틀링 시간 (밀리초)
	error_code       i16 // 에러 코드
}

// ============================================================================
// TxnOffsetCommit (API Key 28) - 트랜잭션 오프셋 커밋
// ============================================================================

/// TxnOffsetCommit 요청 - 트랜잭션 내에서 오프셋을 커밋하는 요청
///
/// 트랜잭션이 커밋될 때만 오프셋이 실제로 커밋됩니다.
/// 트랜잭션이 중단되면 오프셋 커밋도 롤백됩니다.
pub struct TxnOffsetCommitRequest {
pub:
	transactional_id  string                        // 트랜잭션 ID
	group_id          string                        // 컨슈머 그룹 ID
	producer_id       i64                           // 프로듀서 ID
	producer_epoch    i16                           // 프로듀서 에포크
	generation_id     i32                           // 그룹 세대 ID
	member_id         string                        // 멤버 ID
	group_instance_id ?string                       // 정적 멤버십 인스턴스 ID
	topics            []TxnOffsetCommitRequestTopic // 커밋할 토픽/오프셋
}

/// TxnOffsetCommit 요청 토픽 - 커밋할 토픽
pub struct TxnOffsetCommitRequestTopic {
pub:
	name       string // 토픽 이름
	partitions []TxnOffsetCommitRequestPartition // 파티션별 오프셋
}

/// TxnOffsetCommit 요청 파티션 - 커밋할 파티션 오프셋
pub struct TxnOffsetCommitRequestPartition {
pub:
	partition_index        i32    // 파티션 인덱스
	committed_offset       i64    // 커밋할 오프셋
	committed_leader_epoch i32    // 커밋 시점의 리더 에포크
	committed_metadata     string // 커밋 메타데이터
}

/// TxnOffsetCommit 응답 - 트랜잭션 오프셋 커밋 결과
pub struct TxnOffsetCommitResponse {
pub:
	throttle_time_ms i32 // 스로틀링 시간 (밀리초)
	topics           []TxnOffsetCommitResponseTopic // 토픽별 결과
}

/// TxnOffsetCommit 응답 토픽 - 토픽별 커밋 결과
pub struct TxnOffsetCommitResponseTopic {
pub:
	name       string // 토픽 이름
	partitions []TxnOffsetCommitResponsePartition // 파티션별 결과
}

/// TxnOffsetCommit 응답 파티션 - 파티션별 커밋 결과
pub struct TxnOffsetCommitResponsePartition {
pub:
	partition_index i32 // 파티션 인덱스
	error_code      i16 // 에러 코드
}

// ============================================================================
// WriteTxnMarkers (API Key 27) - 트랜잭션 마커 쓰기
// ============================================================================

/// WriteTxnMarkers 요청 - 트랜잭션 코디네이터가 파티션 리더에게 마커를 쓰는 요청
///
/// 이것은 브로커 간 통신 API입니다.
/// 트랜잭션 코디네이터가 트랜잭션 종료 시 각 파티션에 마커를 기록합니다.
pub struct WriteTxnMarkersRequest {
pub:
	markers []WriteTxnMarker // 쓸 마커 목록
}

/// WriteTxnMarker - 쓸 단일 트랜잭션 마커
pub struct WriteTxnMarker {
pub:
	producer_id        i64                   // 현재 프로듀서 ID
	producer_epoch     i16                   // 프로듀서 ID에 연결된 현재 에포크
	transaction_result bool                  // 트랜잭션 결과 (false=ABORT, true=COMMIT)
	topics             []WriteTxnMarkerTopic // 마커를 쓸 토픽 목록
	coordinator_epoch  i32                   // 트랜잭션 코디네이터 에포크
}

/// WriteTxnMarker 토픽 - WriteTxnMarkers 요청의 토픽
pub struct WriteTxnMarkerTopic {
pub:
	name              string // 토픽 이름
	partition_indexes []i32  // 마커를 쓸 파티션 인덱스 목록
}

/// WriteTxnMarkers 응답 - WriteTxnMarkers 요청에 대한 응답
pub struct WriteTxnMarkersResponse {
pub:
	markers []WriteTxnMarkerResult // 프로듀서 ID별 결과
}

/// WriteTxnMarker 결과 - 단일 프로듀서 ID에 대한 결과
pub struct WriteTxnMarkerResult {
pub:
	producer_id i64                         // 현재 프로듀서 ID
	topics      []WriteTxnMarkerTopicResult // 토픽별 결과
}

/// WriteTxnMarker 토픽 결과 - 토픽에 대한 결과
pub struct WriteTxnMarkerTopicResult {
pub:
	name       string // 토픽 이름
	partitions []WriteTxnMarkerPartitionResult // 파티션별 결과
}

/// WriteTxnMarker 파티션 결과 - 파티션에 대한 결과
pub struct WriteTxnMarkerPartitionResult {
pub:
	partition_index i32 // 파티션 인덱스
	error_code      i16 // 에러 코드 (0 = 성공)
}
