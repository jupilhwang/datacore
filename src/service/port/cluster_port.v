// 분산 스토리지 어댑터(PostgreSQL, etcd 등)에서 구현됩니다.
module port

import domain

/// ClusterMetadataPort는 클러스터 조정을 위한 작업을 정의합니다.
/// 멀티 브로커 모드를 지원하는 스토리지 어댑터에서 구현됩니다.
pub interface ClusterMetadataPort {
mut:
	/// 클러스터에 브로커를 등록합니다.
	/// 반환값: 할당된 broker_id (충돌 시 요청과 다를 수 있음)
	register_broker(info domain.BrokerInfo) !domain.BrokerInfo

	/// 클러스터에서 브로커를 제거합니다.
	deregister_broker(broker_id i32) !

	/// 브로커의 마지막 하트비트 시간을 업데이트합니다.
	update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) !

	/// 특정 브로커의 정보를 반환합니다.
	get_broker(broker_id i32) !domain.BrokerInfo

	/// 등록된 모든 브로커 목록을 반환합니다.
	list_brokers() ![]domain.BrokerInfo

	/// 활성 상태의 브로커만 반환합니다 (dead/shutdown 제외).
	list_active_brokers() ![]domain.BrokerInfo
	/// 현재 클러스터 메타데이터를 반환합니다.
	get_cluster_metadata() !domain.ClusterMetadata

	/// 낙관적 잠금을 사용하여 클러스터 메타데이터를 업데이트합니다.
	/// 버전 불일치 시 (동시 수정) 오류를 반환합니다.
	update_cluster_metadata(metadata domain.ClusterMetadata) !
	/// 특정 파티션의 할당 정보를 반환합니다.
	get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment

	/// 토픽의 모든 파티션 할당 정보를 반환합니다.
	list_partition_assignments(topic_name string) ![]domain.PartitionAssignment

	/// 파티션 할당 정보를 업데이트합니다.
	update_partition_assignment(assignment domain.PartitionAssignment) !
	/// 분산 잠금 획득을 시도합니다.
	/// 반환값: 잠금 획득 성공 시 true, 다른 곳에서 보유 중이면 false
	try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool

	/// 분산 잠금을 해제합니다.
	release_lock(lock_name string, holder_id string) !

	/// 보유 중인 잠금의 TTL을 연장합니다.
	refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool
	/// 브로커를 dead 상태로 표시합니다 (하트비트 누락).
	mark_broker_dead(broker_id i32) !
	/// 스토리지 기능 정보를 반환합니다.
	get_capability() domain.StorageCapability
}
