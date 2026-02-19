module domain

/// Partition은 토픽 파티션을 나타냅니다.
/// 참고: DataCore Stateless 아키텍처
/// - leader_id: 항상 응답하는 브로커 (모든 브로커가 동등)
/// - leader_epoch: 항상 0 (Stateless 모델에서는 리더 선출 없음)
/// - replica_nodes/isr_nodes: 항상 [broker_id] (복제 없음, 공유 스토리지)
/// 이 필드들은 Kafka 프로토콜 호환성을 위해서만 유지됩니다.
pub struct Partition {
pub:
	topic         string
	index         int
	leader_id     i32
	leader_epoch  i32
	replica_nodes []i32
	isr_nodes     []i32
}

/// PartitionInfo는 파티션 오프셋 정보를 포함합니다.
/// topic: 토픽 이름
/// partition: 파티션 번호
/// earliest_offset: 가장 이른 오프셋
/// latest_offset: 가장 최근 오프셋
/// high_watermark: 하이 워터마크
pub struct PartitionInfo {
pub:
	topic           string
	partition       int
	earliest_offset i64
	latest_offset   i64
	high_watermark  i64
}

/// TopicPartition은 토픽-파티션 쌍입니다.
/// 토픽과 파티션을 함께 식별하는 데 사용됩니다.
pub struct TopicPartition {
pub:
	topic     string
	partition int
}
