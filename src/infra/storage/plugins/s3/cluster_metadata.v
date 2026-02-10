// Infra Layer - S3 클러스터 메타데이터 어댑터
// S3를 사용한 멀티 브로커 조정을 위한 ClusterMetadataPort 구현
module s3

import domain
import time
import json

// V 인터페이스 값 복사 버그 우회: 모듈 레벨에 config 백업 보관
// 구조체에 config 필드를 추가하면 인터페이스 할당 시 segfault가 발생하므로
// 전역 변수를 사용하여 config를 보존합니다.
__global cluster_metadata_config_backup = S3Config{}

/// S3ClusterMetadataAdapter는 S3를 백엔드로 사용하여 ClusterMetadataPort를 구현합니다.
/// V 인터페이스를 통한 호출 시 adapter 포인터의 config 유실 문제를 해결하기 위해
/// 자체 S3StorageAdapter 인스턴스를 보유합니다.
pub struct S3ClusterMetadataAdapter {
pub mut:
	adapter &S3StorageAdapter
}

/// new_s3_cluster_metadata_adapter는 새로운 S3 클러스터 메타데이터 어댑터를 생성합니다.
/// adapter의 config를 전역 백업에 저장하고, 원본 adapter를 직접 사용합니다.
pub fn new_s3_cluster_metadata_adapter(adapter &S3StorageAdapter) &S3ClusterMetadataAdapter {
	// V 인터페이스 값 복사 버그 우회: config를 전역 변수에 백업
	cluster_metadata_config_backup = adapter.config
	log_message(.info, 'ClusterMetadata', 'Creating cluster metadata adapter', {
		'endpoint':    adapter.config.endpoint
		'region':      adapter.config.region
		'bucket_name': adapter.config.bucket_name
		'prefix':      adapter.config.prefix
	})
	return &S3ClusterMetadataAdapter{
		adapter: adapter
	}
}

/// new_s3_cluster_metadata_with_config는 명시적 config로 어댑터를 생성합니다.
/// V 인터페이스 값 복사 시 config 유실을 방지하기 위해
/// config를 자체 보유하고 adapter에도 강제 설정합니다.
pub fn new_s3_cluster_metadata_with_config(config S3Config) !&S3ClusterMetadataAdapter {
	// V 인터페이스 값 복사 버그 우회: config를 전역 변수에 백업
	cluster_metadata_config_backup = config
	mut own_adapter := new_s3_adapter(config)!
	// adapter config를 원본으로 강제 덮어쓰기
	own_adapter.config = config
	return &S3ClusterMetadataAdapter{
		adapter: own_adapter
	}
}

// 클러스터 메타데이터용 S3 키 구조:
// {prefix}/__cluster/brokers/{broker_id}.json
// {prefix}/__cluster/metadata.json
// {prefix}/__cluster/assignments/{topic}/{partition}.json
// {prefix}/__cluster/locks/{lock_name}.json

// ============================================================
// 브로커 등록 (Broker Registration)
// ============================================================

/// register_broker는 브로커를 클러스터에 등록합니다.
pub fn (mut a S3ClusterMetadataAdapter) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
/// S3 키: {prefix}/__cluster/assignments/{topic}/{partition}.json
pub fn (mut a S3ClusterMetadataAdapter) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	a.ensure_adapter_config()
	key := a.partition_assignment_key(topic_name, partition)

	log_message(.debug, 'PartitionAssignment', 'Reading partition assignment from S3',
		{
		'topic':     topic_name
		'partition': partition.str()
		'key':       key
	})

	// 메트릭 수집
	a.adapter.metrics_lock.@lock()
	a.adapter.metrics.s3_get_count++
	a.adapter.metrics_lock.unlock()

	data, _ := a.adapter.get_object(key, 0, -1) or {
		log_message(.warn, 'PartitionAssignment', 'Partition assignment not found', {
			'topic':     topic_name
			'partition': partition.str()
			'key':       key
			'error':     err.str()
		})
		a.adapter.metrics_lock.@lock()
		a.adapter.metrics.s3_error_count++
		a.adapter.metrics_lock.unlock()
		return error('partition assignment not found: ${topic_name}/${partition}')
	}

	assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or {
		log_message(.error, 'PartitionAssignment', 'Failed to decode partition assignment',
			{
			'topic':     topic_name
			'partition': partition.str()
			'key':       key
			'error':     err.str()
		})
		return error('failed to decode partition assignment: ${err}')
	}

	log_message(.debug, 'PartitionAssignment', 'Successfully read partition assignment',
		{
		'topic':            topic_name
		'partition':        partition.str()
		'preferred_broker': assignment.preferred_broker.str()
	})

	return assignment
}

/// list_partition_assignments는 특정 토픽의 모든 파티션 할당을 조회합니다.
/// S3 접두사: {prefix}/__cluster/assignments/{topic}/
pub fn (mut a S3ClusterMetadataAdapter) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	a.ensure_adapter_config()
	prefix := a.partition_assignments_prefix(topic_name)

	log_message(.debug, 'PartitionAssignment', 'Listing partition assignments for topic',
		{
		'topic':  topic_name
		'prefix': prefix
	})

	// 메트릭 수집
	a.adapter.metrics_lock.@lock()
	a.adapter.metrics.s3_list_count++
	a.adapter.metrics_lock.unlock()

	objects := a.adapter.list_objects(prefix)!

	mut assignments := []domain.PartitionAssignment{}
	mut failed_count := 0

	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}

		// 메트릭 수집
		a.adapter.metrics_lock.@lock()
		a.adapter.metrics.s3_get_count++
		a.adapter.metrics_lock.unlock()

		data, _ := a.adapter.get_object(obj.key, 0, -1) or {
			failed_count++
			a.adapter.metrics_lock.@lock()
			a.adapter.metrics.s3_error_count++
			a.adapter.metrics_lock.unlock()
			continue
		}

		assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or {
			failed_count++
			a.adapter.metrics_lock.@lock()
			a.adapter.metrics.s3_error_count++
			a.adapter.metrics_lock.unlock()
			continue
		}

		assignments << assignment
	}

	log_message(.info, 'PartitionAssignment', 'Listed partition assignments for topic',
		{
		'topic':  topic_name
		'count':  assignments.len.str()
		'failed': failed_count.str()
	})

	return assignments
}

/// list_all_partition_assignments는 모든 토픽의 모든 파티션 할당을 조회합니다.
/// S3 접두사: {prefix}/__cluster/assignments/
pub fn (mut a S3ClusterMetadataAdapter) list_all_partition_assignments() ![]domain.PartitionAssignment {
	a.ensure_adapter_config()
	prefix := a.all_assignments_prefix()

	log_message(.debug, 'PartitionAssignment', 'Listing all partition assignments', {
		'prefix': prefix
	})

	// 메트릭 수집
	a.adapter.metrics_lock.@lock()
	a.adapter.metrics.s3_list_count++
	a.adapter.metrics_lock.unlock()

	objects := a.adapter.list_objects(prefix)!

	mut assignments := []domain.PartitionAssignment{}
	mut failed_count := 0

	for obj in objects {
		if !obj.key.ends_with('.json') {
			continue
		}

		// 메트릭 수집
		a.adapter.metrics_lock.@lock()
		a.adapter.metrics.s3_get_count++
		a.adapter.metrics_lock.unlock()

		data, _ := a.adapter.get_object(obj.key, 0, -1) or {
			failed_count++
			a.adapter.metrics_lock.@lock()
			a.adapter.metrics.s3_error_count++
			a.adapter.metrics_lock.unlock()
			continue
		}

		assignment := json.decode(domain.PartitionAssignment, data.bytestr()) or {
			failed_count++
			a.adapter.metrics_lock.@lock()
			a.adapter.metrics.s3_error_count++
			a.adapter.metrics_lock.unlock()
			continue
		}

		assignments << assignment
	}

	log_message(.info, 'PartitionAssignment', 'Listed all partition assignments', {
		'total_count': assignments.len.str()
		'failed':      failed_count.str()
	})

	return assignments
}

/// update_partition_assignment는 파티션 할당을 업데이트합니다.
/// S3 키: {prefix}/__cluster/assignments/{topic}/{partition}.json
/// 재시도 로직을 포함하여 안정적으로 저장합니다.
pub fn (mut a S3ClusterMetadataAdapter) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	a.ensure_adapter_config()
	key := a.partition_assignment_key(assignment.topic_name, assignment.partition)

	log_message(.debug, 'PartitionAssignment', 'Updating partition assignment', {
		'topic':            assignment.topic_name
		'partition':        assignment.partition.str()
		'preferred_broker': assignment.preferred_broker.str()
		'key':              key
	})

	data := json.encode(assignment).bytes()

	// 재시도 로직
	mut last_error := ''
	for retry in 0 .. a.adapter.config.max_retries {
		if retry > 0 {
			log_message(.debug, 'PartitionAssignment', 'Retrying partition assignment update',
				{
				'topic':     assignment.topic_name
				'partition': assignment.partition.str()
				'retry':     retry.str()
			})
			time.sleep(a.adapter.config.retry_delay_ms * time.millisecond)
		}

		a.adapter.put_object(key, data) or {
			last_error = err.str()
			continue
		}

		// 성공 메트릭
		a.adapter.metrics_lock.@lock()
		a.adapter.metrics.s3_put_count++
		a.adapter.metrics_lock.unlock()

		log_message(.info, 'PartitionAssignment', 'Successfully updated partition assignment',
			{
			'topic':     assignment.topic_name
			'partition': assignment.partition.str()
			'retry':     retry.str()
		})
		return
	}

	// 모든 재시도 실패
	a.adapter.metrics_lock.@lock()
	a.adapter.metrics.s3_error_count++
	a.adapter.metrics_lock.unlock()

	log_message(.error, 'PartitionAssignment', 'Failed to update partition assignment after retries',
		{
		'topic':       assignment.topic_name
		'partition':   assignment.partition.str()
		'key':         key
		'error':       last_error
		'max_retries': a.adapter.config.max_retries.str()
	})

	return error('failed to update partition assignment after ${a.adapter.config.max_retries} retries: ${last_error}')
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
	a.ensure_adapter_config()
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
// V 인터페이스 값 복사 버그 대응 (Config Recovery)
// ============================================================

// V 인터페이스 값 복사 버그 대응: adapter의 config가 유실된 경우 전역 백업에서 복구
fn (mut a S3ClusterMetadataAdapter) ensure_adapter_config() {
	if a.adapter.config.endpoint.len == 0 && cluster_metadata_config_backup.endpoint.len > 0 {
		a.adapter.config = cluster_metadata_config_backup
	}
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
/// 형식: {prefix}/__cluster/assignments/{topic_name}/{partition_id}.json
fn (a &S3ClusterMetadataAdapter) partition_assignment_key(topic_name string, partition i32) string {
	return '${a.adapter.config.prefix}__cluster/assignments/${topic_name}/${partition}.json'
}

/// partition_assignments_prefix는 특정 토픽의 파티션 할당 목록 조회용 S3 접두사를 반환합니다.
/// 형식: {prefix}/__cluster/assignments/{topic_name}/
fn (a &S3ClusterMetadataAdapter) partition_assignments_prefix(topic_name string) string {
	return '${a.adapter.config.prefix}__cluster/assignments/${topic_name}/'
}

/// all_assignments_prefix는 모든 파티션 할당 목록 조회용 S3 접두사를 반환합니다.
/// 형식: {prefix}/__cluster/assignments/
fn (a &S3ClusterMetadataAdapter) all_assignments_prefix() string {
	return '${a.adapter.config.prefix}__cluster/assignments/'
}

/// lock_key는 분산 락의 S3 키를 반환합니다.
fn (a &S3ClusterMetadataAdapter) lock_key(lock_name string) string {
	return '${a.adapter.config.prefix}__cluster/locks/${lock_name}.json'
}
