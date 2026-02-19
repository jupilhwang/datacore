// 브로커 간 데이터 복제 작업을 정의합니다.
// infra/replication/Manager에서 구현됩니다 (V duck typing).
module port

import domain

/// ReplicationPort는 브로커 간 데이터 복제를 위한 인터페이스입니다.
/// infra/replication/Manager가 이 인터페이스를 구현합니다.
pub interface ReplicationPort {
mut:
	/// 복제 매니저를 시작합니다.
	start() !

	/// 복제 매니저를 중지합니다.
	stop() !

	/// 특정 파티션의 레코드 데이터를 레플리카 브로커들에게 전송합니다.
	/// topic: 토픽 이름
	/// partition: 파티션 인덱스
	/// offset: 레코드 오프셋
	/// records_data: 직렬화된 레코드 바이트
	send_replicate(topic string, partition i32, offset i64, records_data []u8) !

	/// S3 플러시 완료를 레플리카 브로커들에게 알립니다.
	/// topic: 토픽 이름
	/// partition: 파티션 인덱스
	/// offset: 플러시된 마지막 오프셋
	send_flush_ack(topic string, partition i32, offset i64) !

	/// 복제된 레코드 데이터를 인메모리 버퍼에 저장합니다.
	store_replica_buffer(buffer domain.ReplicaBuffer) !

	/// 지정된 오프셋 이하의 복제 버퍼를 삭제합니다.
	delete_replica_buffer(topic string, partition i32, offset i64) !

	/// 모든 인메모리 복제 버퍼를 반환합니다 (크래시 복구 시 사용).
	get_all_replica_buffers() ![]domain.ReplicaBuffer

	/// 현재 복제 통계 스냅샷을 반환합니다.
	get_stats() domain.ReplicationStats

	/// 특정 브로커의 헬스 상태를 갱신합니다.
	update_broker_health(broker_id string, health domain.ReplicationHealth)
}
