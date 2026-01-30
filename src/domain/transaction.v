// 도메인 레이어 - 트랜잭션 도메인 모델
// Kafka 트랜잭션 관련 데이터 구조를 정의합니다.
// Exactly-once 시맨틱을 지원합니다.
module domain

/// TransactionState는 트랜잭션의 상태를 나타냅니다.
/// empty: 초기 상태
/// ongoing: 진행 중
/// prepare_commit: 커밋 준비
/// prepare_abort: 롤백 준비
/// complete_commit: 커밋 완료
/// complete_abort: 롤백 완료
/// dead: 종료됨
/// prepare_epoch_fence: 에포크 펜싱 준비
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

/// TransactionResult는 트랜잭션의 결과를 나타냅니다.
/// commit: 커밋 (성공)
/// abort: 롤백 (취소)
pub enum TransactionResult {
	commit = 0
	abort  = 1
}

/// TransactionMetadata는 트랜잭션 프로듀서의 상태를 보관합니다.
/// transactional_id: 트랜잭션 ID
/// producer_id: 프로듀서 ID
/// producer_epoch: 프로듀서 에포크
/// txn_timeout_ms: 트랜잭션 타임아웃 (밀리초)
/// state: 트랜잭션 상태
/// topic_partitions: 트랜잭션에 포함된 토픽-파티션 목록
/// txn_start_timestamp: 트랜잭션 시작 시간
/// txn_last_update_timestamp: 마지막 업데이트 시간
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

// 헬퍼 메서드

/// str은 TransactionState를 문자열로 변환합니다.
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

/// transaction_state_from_string은 문자열을 TransactionState로 변환합니다.
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
		else { .empty } // 기본값
	}
}

/// boolean은 TransactionResult가 커밋인지 여부를 반환합니다.
pub fn (r TransactionResult) boolean() bool {
	return r == .commit
}

/// InitProducerIdResult는 InitProducerId API의 결과입니다.
/// producer_id: 할당된 프로듀서 ID
/// producer_epoch: 프로듀서 에포크
pub struct InitProducerIdResult {
pub:
	producer_id    i64
	producer_epoch i16
}
