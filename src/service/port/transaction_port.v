// 트랜잭션 메타데이터 저장을 위한 인터페이스를 정의합니다.
// Kafka의 트랜잭션 기능을 지원하기 위해 사용됩니다.
module port

import domain

/// TransactionStore는 트랜잭션 메타데이터 저장을 위한 인터페이스입니다.
/// 트랜잭션 코디네이터에서 트랜잭션 상태를 영구 저장할 때 사용합니다.
pub interface TransactionStore {
mut:
	/// transactional_id로 트랜잭션 메타데이터를 조회합니다.
	get_transaction(transactional_id string) !domain.TransactionMetadata

	/// 트랜잭션 메타데이터를 저장합니다 (생성 또는 업데이트).
	save_transaction(metadata domain.TransactionMetadata) !

	/// 트랜잭션 메타데이터를 삭제합니다.
	delete_transaction(transactional_id string) !

	/// 모든 트랜잭션 목록을 반환합니다 (디버깅/모니터링용).
	list_transactions() ![]domain.TransactionMetadata
}
