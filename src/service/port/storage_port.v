// 서비스 레이어 - 스토리지 포트 인터페이스
// 유스케이스 레이어에서 정의하고 어댑터 레이어에서 구현하는 스토리지 작업 인터페이스
// 이 인터페이스는 Clean Architecture의 의존성 역전 원칙을 따릅니다.
module port

import domain

/// StoragePort는 스토리지 작업을 위한 인터페이스를 정의합니다.
/// infra/storage에서 구현되며, 토픽/파티션/레코드 관리 및 컨슈머 그룹 기능을 제공합니다.
pub interface StoragePort {
mut:
	/// 새로운 토픽을 생성합니다.
	/// name: 토픽 이름
	/// partitions: 파티션 수
	/// config: 토픽 설정 (보존 기간, 세그먼트 크기 등)
	create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata

	/// 토픽을 삭제합니다.
	delete_topic(name string) !

	/// 모든 토픽 목록을 반환합니다.
	list_topics() ![]domain.TopicMetadata

	/// 토픽 이름으로 토픽 메타데이터를 조회합니다.
	get_topic(name string) !domain.TopicMetadata

	/// 토픽 ID로 토픽 메타데이터를 조회합니다.
	get_topic_by_id(topic_id []u8) !domain.TopicMetadata

	/// 토픽에 파티션을 추가합니다.
	/// new_count: 새로운 총 파티션 수 (기존보다 커야 함)
	add_partitions(name string, new_count int) !
	/// 레코드를 토픽의 특정 파티션에 추가합니다.
	/// 반환값: 기본 오프셋과 로그 추가 시간을 포함한 결과
	append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult

	/// 토픽의 특정 파티션에서 레코드를 가져옵니다.
	/// offset: 시작 오프셋
	/// max_bytes: 최대 바이트 수
	fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult

	/// 지정된 오프셋 이전의 레코드를 삭제합니다.
	delete_records(topic string, partition int, before_offset i64) !
	/// 파티션 정보를 조회합니다 (최초/최신 오프셋, 하이 워터마크 등).
	get_partition_info(topic string, partition int) !domain.PartitionInfo
	/// 컨슈머 그룹을 저장합니다.
	save_group(group domain.ConsumerGroup) !

	/// 컨슈머 그룹을 로드합니다.
	load_group(group_id string) !domain.ConsumerGroup

	/// 컨슈머 그룹을 삭제합니다.
	delete_group(group_id string) !

	/// 모든 컨슈머 그룹 목록을 반환합니다.
	list_groups() ![]domain.GroupInfo
	/// 컨슈머 그룹의 오프셋을 커밋합니다.
	commit_offsets(group_id string, offsets []domain.PartitionOffset) !

	/// 컨슈머 그룹의 커밋된 오프셋을 조회합니다.
	fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult
	/// 스토리지 상태를 확인합니다.
	health_check() !HealthStatus
	/// 스토리지 기능 정보를 반환합니다.
	get_storage_capability() domain.StorageCapability

	/// 클러스터 메타데이터 포트를 반환합니다 (멀티 브로커 모드에서만 사용).
	get_cluster_metadata_port() ?&ClusterMetadataPort
}

/// HealthStatus는 스토리지의 상태를 나타냅니다.
pub enum HealthStatus {
	healthy   // 정상
	degraded  // 성능 저하
	unhealthy // 비정상
}

/// Lock은 파티션 레벨 잠금을 위한 인터페이스입니다.
pub interface Lock {
	/// 잠금을 해제합니다.
	release() !
}

/// LockableStorage는 잠금 기능이 추가된 StoragePort입니다.
/// 동시성 제어가 필요한 경우 사용합니다.
pub interface LockableStorage {
	StoragePort /// 특정 토픽/파티션에 대한 잠금을 획득합니다.

	acquire_lock(topic string, partition int) !Lock
}
