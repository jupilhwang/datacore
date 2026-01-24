// Infra Layer - S3 클러스터 메타데이터 어댑터
// S3를 사용한 멀티 브로커 조정을 위한 ClusterMetadataPort 구현
module s3

import domain
import time
import json

/// S3ClusterMetadataAdapter는 S3를 백엔드로 사용하여 ClusterMetadataPort를 구현합니다.
pub struct S3ClusterMetadataAdapter {
pub mut:
	adapter &S3StorageAdapter
}

/// new_s3_cluster_metadata_adapter는 새로운 S3 클러스터 메타데이터 어댑터를 생성합니다.
pub fn new_s3_cluster_metadata_adapter(adapter &S3StorageAdapter) &S3ClusterMetadataAdapter {
	return &S3ClusterMetadataAdapter{
		adapter: adapter
	}
}

// 클러스터 메타데이터용 S3 키 구조:
// {prefix}/__cluster/brokers/{broker_id}.json
// {prefix}/__cluster/metadata.json
// {prefix}/__cluster/partitions/{topic}/{partition}.json
// {prefix}/__cluster/locks/{lock_name}.json

// ============================================================
// 브로커 등록 (Broker Registration)
// ============================================================

/// register_broker는 브로커를 클러스터에 등록합니다.
pub fn (mut a S3ClusterMetadataAdapter) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	key := a.broker_key(info.broker_id)

	// 브로커가 이미 존재하는지 확인
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		// 기존 브로커 업데이트
		mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
			return error('failed to decode existing broker: ${err}')
		}
		broker.status = .active
		broker.last_heartbeat = time.now().unix_milli()

		data := json.encode(broker).bytes()
		a.adapter.put_object(key, data)!
		return broker
	}

	// 새 브로커 등록
	mut new_info := info
	new_info.registered_at = time.now().unix_milli()
	new_info.last_heartbeat = time.now().unix_milli()
	new_info.status = .active

	data := json.encode(new_info).bytes()
	a.adapter.put_object(key, data)!

	return new_info
}

/// deregister_broker는 브로커를 클러스터에서 등록 해제합니다.
pub fn (mut a S3ClusterMetadataAdapter) deregister_broker(broker_id i32) ! {
	key := a.broker_key(broker_id)

	// 기존 브로커 조회
	existing, _ := a.adapter.get_object(key, 0, -1) or { return }
	if existing.len == 0 {
		return
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or { return }
	broker.status = .shutdown

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

/// update_broker_heartbeat는 브로커의 하트비트를 업데이트합니다.
pub fn (mut a S3ClusterMetadataAdapter) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
	key := a.broker_key(heartbeat.broker_id)

	existing, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${heartbeat.broker_id}')
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}

	broker.last_heartbeat = heartbeat.timestamp
	if heartbeat.wants_shutdown {
		broker.status = .draining
	}

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

/// get_broker는 특정 브로커의 정보를 조회합니다.
pub fn (mut a S3ClusterMetadataAdapter) get_broker(broker_id i32) !domain.BrokerInfo {
	key := a.broker_key(broker_id)

	data, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${broker_id}')
	}

	return json.decode(domain.BrokerInfo, data.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}
}

/// list_brokers는 등록된 모든 브로커 목록을 조회합니다.
pub fn (mut a S3ClusterMetadataAdapter) list_brokers() ![]domain.BrokerInfo {
	prefix := a.brokers_prefix()
	objects := a.adapter.list_objects(prefix)!

	mut brokers := []domain.BrokerInfo{}
	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}
		data, _ := a.adapter.get_object(obj.key, 0, -1) or { continue }
		broker := json.decode(domain.BrokerInfo, data.bytestr()) or { continue }
		brokers << broker
	}

	return brokers
}

/// list_active_brokers는 활성 상태인 브로커 목록만 조회합니다.
pub fn (mut a S3ClusterMetadataAdapter) list_active_brokers() ![]domain.BrokerInfo {
	all_brokers := a.list_brokers()!

	mut active := []domain.BrokerInfo{}
	for broker in all_brokers {
		if broker.status == .active {
			active << broker
		}
	}

	return active
}

// ============================================================
// 클러스터 메타데이터 (Cluster Metadata)
// ============================================================

/// get_cluster_metadata는 현재 클러스터 메타데이터를 조회합니다.
pub fn (mut a S3ClusterMetadataAdapter) get_cluster_metadata() !domain.ClusterMetadata {
	key := a.cluster_metadata_key()

	data, _ := a.adapter.get_object(key, 0, -1) or {
		// 존재하지 않으면 기본 메타데이터 반환
		return domain.ClusterMetadata{
			cluster_id:       'datacore-cluster'
			controller_id:    -1
			brokers:          []domain.BrokerInfo{}
			metadata_version: 0
			updated_at:       time.now().unix_milli()
		}
	}

	return json.decode(domain.ClusterMetadata, data.bytestr()) or {
		return error('failed to decode cluster metadata: ${err}')
	}
}

/// update_cluster_metadata는 클러스터 메타데이터를 업데이트합니다.
/// Optimistic locking을 위해 버전을 확인합니다.
pub fn (mut a S3ClusterMetadataAdapter) update_cluster_metadata(metadata domain.ClusterMetadata) ! {
	key := a.cluster_metadata_key()

	// optimistic locking을 위한 버전 확인
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		current := json.decode(domain.ClusterMetadata, existing.bytestr()) or {
			return error('failed to decode existing metadata')
		}
		if current.metadata_version >= metadata.metadata_version {
			return error('metadata version conflict')
		}
	}

	mut updated := metadata
	updated.updated_at = time.now().unix_milli()

	data := json.encode(updated).bytes()
	a.adapter.put_object(key, data)!
}

// ============================================================
// 파티션 할당 (Partition Assignment)
// ============================================================

/// get_partition_assignment는 특정 토픽-파티션의 할당 정보를 조회합니다.
pub fn (mut a S3ClusterMetadataAdapter) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	key := a.partition_assignment_key(topic_name, partition)

	data, _ := a.adapter.get_object(key, 0, -1) or {
		return error('partition assignment not found')
	}

	return json.decode(domain.PartitionAssignment, data.bytestr()) or {
		return error('failed to decode partition assignment: ${err}')
	}
}

/// list_partition_assignments는 특정 토픽의 모든 파티션 할당을 조회합니다.
pub fn (mut a S3ClusterMetadataAdapter) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	prefix := a.partition_assignments_prefix(topic_name)
	objects := a.adapter.list_objects(prefix)!

	mut assignments := []domain.PartitionAssignment{}
	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}
		data, _ := a.adapter.get_object(obj.key, 0, -1) or { continue }
		assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or { continue }
		assignments << assignment
	}

	return assignments
}

/// update_partition_assignment는 파티션 할당을 업데이트합니다.
pub fn (mut a S3ClusterMetadataAdapter) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	key := a.partition_assignment_key(assignment.topic_name, assignment.partition)

	data := json.encode(assignment).bytes()
	a.adapter.put_object(key, data)!
}

// ============================================================
// 분산 락 (Distributed Locking)
// ============================================================

/// LockInfo는 분산 락을 나타냅니다.
struct LockInfo {
	lock_name   string // 락 이름
	holder_id   string // 락 보유자 ID
	expires_at  i64    // 만료 시간
	acquired_at i64    // 획득 시간
}

/// try_acquire_lock은 분산 락 획득을 시도합니다.
/// TTL이 만료되면 락은 자동으로 해제됩니다.
pub fn (mut a S3ClusterMetadataAdapter) try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	key := a.lock_key(lock_name)
	now := time.now().unix_milli()

	// 락이 존재하고 유효한지 확인
	existing, _ := a.adapter.get_object(key, 0, -1) or { []u8{}, '' }
	if existing.len > 0 {
		lock_info := json.decode(LockInfo, existing.bytestr()) or {
			// 손상된 락, 획득 시도
			return a.create_lock(key, lock_name, holder_id, ttl_ms)
		}

		// 락이 만료되었는지 확인
		if lock_info.expires_at > now {
			// 락이 아직 누군가에게 보유됨
			if lock_info.holder_id == holder_id {
				// 이미 락을 보유 중, 갱신
				return a.refresh_lock(lock_name, holder_id, ttl_ms)
			}
			return false
		}
		// 락이 만료됨, 획득 가능
	}

	return a.create_lock(key, lock_name, holder_id, ttl_ms)
}

/// create_lock은 새로운 락을 생성합니다.
fn (mut a S3ClusterMetadataAdapter) create_lock(key string, lock_name string, holder_id string, ttl_ms i64) !bool {
	now := time.now().unix_milli()
	lock_info := LockInfo{
		lock_name:   lock_name
		holder_id:   holder_id
		expires_at:  now + ttl_ms
		acquired_at: now
	}

	data := json.encode(lock_info).bytes()
	a.adapter.put_object(key, data)!
	return true
}

/// release_lock은 분산 락을 해제합니다.
pub fn (mut a S3ClusterMetadataAdapter) release_lock(lock_name string, holder_id string) ! {
	key := a.lock_key(lock_name)

	// 락을 보유하고 있는지 확인
	existing, _ := a.adapter.get_object(key, 0, -1) or { return }
	if existing.len == 0 {
		return
	}

	lock_info := json.decode(LockInfo, existing.bytestr()) or { return }
	if lock_info.holder_id != holder_id {
		return error('lock not held by this holder')
	}

	a.adapter.delete_object(key)!
}

/// refresh_lock은 분산 락의 TTL을 갱신합니다.
pub fn (mut a S3ClusterMetadataAdapter) refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	key := a.lock_key(lock_name)
	now := time.now().unix_milli()

	// 락을 보유하고 있는지 확인
	existing, _ := a.adapter.get_object(key, 0, -1) or { return false }
	if existing.len == 0 {
		return false
	}

	mut lock_info := json.decode(LockInfo, existing.bytestr()) or { return false }
	if lock_info.holder_id != holder_id {
		return false
	}

	// 락 갱신
	lock_info = LockInfo{
		...lock_info
		expires_at: now + ttl_ms
	}

	data := json.encode(lock_info).bytes()
	a.adapter.put_object(key, data)!
	return true
}

// ============================================================
// 상태 모니터링 (Health Monitoring) - ClusterCoordinatorPort용
// ============================================================

/// mark_broker_dead는 브로커를 dead 상태로 표시합니다.
pub fn (mut a S3ClusterMetadataAdapter) mark_broker_dead(broker_id i32) ! {
	key := a.broker_key(broker_id)

	existing, _ := a.adapter.get_object(key, 0, -1) or {
		return error('broker not found: ${broker_id}')
	}

	mut broker := json.decode(domain.BrokerInfo, existing.bytestr()) or {
		return error('failed to decode broker: ${err}')
	}

	broker.status = .dead

	data := json.encode(broker).bytes()
	a.adapter.put_object(key, data)!
}

// ============================================================
// 스토리지 기능 (Capability)
// ============================================================

/// get_capability는 S3 스토리지 기능 정보를 반환합니다.
pub fn (a &S3ClusterMetadataAdapter) get_capability() domain.StorageCapability {
	return s3_capability
}

// ============================================================
// 키 헬퍼 (Key Helpers)
// ============================================================

/// broker_key는 브로커 정보의 S3 키를 반환합니다.
fn (a &S3ClusterMetadataAdapter) broker_key(broker_id i32) string {
	return '${a.adapter.config.prefix}__cluster/brokers/${broker_id}.json'
}

/// brokers_prefix는 브로커 목록 조회용 S3 접두사를 반환합니다.
fn (a &S3ClusterMetadataAdapter) brokers_prefix() string {
	return '${a.adapter.config.prefix}__cluster/brokers/'
}

/// cluster_metadata_key는 클러스터 메타데이터의 S3 키를 반환합니다.
fn (a &S3ClusterMetadataAdapter) cluster_metadata_key() string {
	return '${a.adapter.config.prefix}__cluster/metadata.json'
}

/// partition_assignment_key는 파티션 할당의 S3 키를 반환합니다.
fn (a &S3ClusterMetadataAdapter) partition_assignment_key(topic_name string, partition i32) string {
	return '${a.adapter.config.prefix}__cluster/partitions/${topic_name}/${partition}.json'
}

/// partition_assignments_prefix는 파티션 할당 목록 조회용 S3 접두사를 반환합니다.
fn (a &S3ClusterMetadataAdapter) partition_assignments_prefix(topic_name string) string {
	return '${a.adapter.config.prefix}__cluster/partitions/${topic_name}/'
}

/// lock_key는 분산 락의 S3 키를 반환합니다.
fn (a &S3ClusterMetadataAdapter) lock_key(lock_name string) string {
	return '${a.adapter.config.prefix}__cluster/locks/${lock_name}.json'
}
