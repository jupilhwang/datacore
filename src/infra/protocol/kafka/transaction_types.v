// 트랜잭션 관련 요청/응답 구조체 정의
//
// 프로듀서 ID 초기화, 트랜잭션 시작/종료, 오프셋 커밋 등에 사용됩니다.
module kafka

/// InitProducerId 요청 - 멱등성/트랜잭션 프로듀서가 프로듀서 ID를 얻기 위한 요청
///
/// 멱등성 프로듀서는 중복 메시지 방지를 위해 프로듀서 ID가 필요합니다.
/// 트랜잭션 프로듀서는 추가로 transactional_id를 제공해야 합니다.
pub struct InitProducerIdRequest {
pub:
	transactional_id       ?string
	transaction_timeout_ms i32
	producer_id            i64
	producer_epoch         i16
}

/// InitProducerId 응답 - 멱등성/트랜잭션 프로듀서에게 프로듀서 ID 반환
pub struct InitProducerIdResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	producer_id      i64
	producer_epoch   i16
}

/// AddPartitionsToTxn 요청 - 트랜잭션에 파티션을 추가하는 요청
///
/// 트랜잭션 프로듀서가 새로운 파티션에 메시지를 쓰기 전에
/// 해당 파티션을 트랜잭션에 등록해야 합니다.
pub struct AddPartitionsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	topics           []AddPartitionsToTxnTopic
}

/// AddPartitionsToTxn 토픽 - 트랜잭션에 추가할 토픽
pub struct AddPartitionsToTxnTopic {
pub:
	name       string
	partitions []i32
}

/// AddPartitionsToTxn 응답 - 파티션 추가 결과
pub struct AddPartitionsToTxnResponse {
pub:
	throttle_time_ms i32
	results          []AddPartitionsToTxnResult
}

/// AddPartitionsToTxn 결과 - 토픽별 파티션 추가 결과
pub struct AddPartitionsToTxnResult {
pub:
	name       string
	partitions []AddPartitionsToTxnPartitionResult
}

/// AddPartitionsToTxn 파티션 결과 - 파티션별 추가 결과
pub struct AddPartitionsToTxnPartitionResult {
pub:
	partition_index i32
	error_code      i16
}

// AddOffsetsToTxn (API Key 25) - 트랜잭션에 오프셋 추가

/// AddOffsetsToTxn 요청 - 트랜잭션에 컨슈머 그룹 오프셋을 추가하는 요청
///
/// 트랜잭션 내에서 오프셋을 커밋하기 전에 그룹을 트랜잭션에 등록합니다.
/// 이를 통해 "exactly-once" 시맨틱을 구현할 수 있습니다.
pub struct AddOffsetsToTxnRequest {
pub:
	transactional_id string
	producer_id      i64
	producer_epoch   i16
	group_id         string
}

/// AddOffsetsToTxn 응답 - 오프셋 추가 결과
pub struct AddOffsetsToTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

/// EndTxn 요청 - 트랜잭션을 커밋하거나 중단하는 요청
///
/// transaction_result가 true이면 COMMIT, false이면 ABORT입니다.
pub struct EndTxnRequest {
pub:
	transactional_id   string
	producer_id        i64
	producer_epoch     i16
	transaction_result bool
}

/// EndTxn 응답 - 트랜잭션 종료 결과
pub struct EndTxnResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

/// TxnOffsetCommit 요청 - 트랜잭션 내에서 오프셋을 커밋하는 요청
///
/// 트랜잭션이 커밋될 때만 오프셋이 실제로 커밋됩니다.
/// 트랜잭션이 중단되면 오프셋 커밋도 롤백됩니다.
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

/// TxnOffsetCommit 요청 토픽 - 커밋할 토픽
pub struct TxnOffsetCommitRequestTopic {
pub:
	name       string
	partitions []TxnOffsetCommitRequestPartition
}

/// TxnOffsetCommit 요청 파티션 - 커밋할 파티션 오프셋
pub struct TxnOffsetCommitRequestPartition {
pub:
	partition_index        i32
	committed_offset       i64
	committed_leader_epoch i32
	committed_metadata     string
}

/// TxnOffsetCommit 응답 - 트랜잭션 오프셋 커밋 결과
pub struct TxnOffsetCommitResponse {
pub:
	throttle_time_ms i32
	topics           []TxnOffsetCommitResponseTopic
}

/// TxnOffsetCommit 응답 토픽 - 토픽별 커밋 결과
pub struct TxnOffsetCommitResponseTopic {
pub:
	name       string
	partitions []TxnOffsetCommitResponsePartition
}

/// TxnOffsetCommit 응답 파티션 - 파티션별 커밋 결과
pub struct TxnOffsetCommitResponsePartition {
pub:
	partition_index i32
	error_code      i16
}

// WriteTxnMarkers (API Key 27) - 트랜잭션 마커 쓰기

/// WriteTxnMarkers 요청 - 트랜잭션 코디네이터가 파티션 리더에게 마커를 쓰는 요청
///
/// 이것은 브로커 간 통신 API입니다.
/// 트랜잭션 코디네이터가 트랜잭션 종료 시 각 파티션에 마커를 기록합니다.
pub struct WriteTxnMarkersRequest {
pub:
	markers []WriteTxnMarker
}

/// WriteTxnMarker - 쓸 단일 트랜잭션 마커
pub struct WriteTxnMarker {
pub:
	producer_id        i64
	producer_epoch     i16
	transaction_result bool
	topics             []WriteTxnMarkerTopic
	coordinator_epoch  i32
}

/// WriteTxnMarker 토픽 - WriteTxnMarkers 요청의 토픽
pub struct WriteTxnMarkerTopic {
pub:
	name              string
	partition_indexes []i32
}

/// WriteTxnMarkers 응답 - WriteTxnMarkers 요청에 대한 응답
pub struct WriteTxnMarkersResponse {
pub:
	markers []WriteTxnMarkerResult
}

/// WriteTxnMarker 결과 - 단일 프로듀서 ID에 대한 결과
pub struct WriteTxnMarkerResult {
pub:
	producer_id i64
	topics      []WriteTxnMarkerTopicResult
}

/// WriteTxnMarker 토픽 결과 - 토픽에 대한 결과
pub struct WriteTxnMarkerTopicResult {
pub:
	name       string
	partitions []WriteTxnMarkerPartitionResult
}

/// WriteTxnMarker 파티션 결과 - 파티션에 대한 결과
pub struct WriteTxnMarkerPartitionResult {
pub:
	partition_index i32
	error_code      i16
}
