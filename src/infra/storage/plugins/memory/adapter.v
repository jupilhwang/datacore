// Infra Layer - 메모리 스토리지 어댑터
// 파티션 수준 락킹을 사용한 인메모리 스토리지 구현
// 테스트 및 단일 브로커 환경에 적합
module memory

import domain
import service.port
import sync
import time
import rand

/// MemoryStorageAdapter는 port.StoragePort를 구현합니다.
/// 메모리 기반 스토리지로 파티션별 락킹을 통해 동시성을 제어합니다.
pub struct MemoryStorageAdapter {
pub mut:
	config MemoryConfig
mut:
	topics         map[string]&TopicStore
	topic_id_index map[string]string // topic_id (hex) -> topic_name, O(1) 조회용
	groups         map[string]domain.ConsumerGroup
	offsets        map[string]map[string]i64
	global_lock    sync.RwMutex
}

/// MemoryConfig는 메모리 스토리지 설정을 담습니다.
pub struct MemoryConfig {
pub:
	max_messages_per_partition int = 1000000   // 파티션당 최대 메시지 수 (기본 100만)
	max_bytes_per_partition    i64 = -1        // 파티션당 최대 바이트 (-1 = 무제한)
	retention_ms               i64 = 604800000 // 보존 기간 (기본 7일)
}

/// memory_capability는 메모리 어댑터의 스토리지 기능을 정의합니다.
pub const memory_capability = domain.StorageCapability{
	name:                  'memory'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   false
	is_persistent:         false
	is_distributed:        false
}

/// TopicStore는 토픽 데이터를 저장합니다.
struct TopicStore {
pub mut:
	metadata   domain.TopicMetadata
	config     domain.TopicConfig
	partitions []&PartitionStore
	lock       sync.RwMutex
}

/// PartitionStore는 파티션 데이터를 저장하며 세밀한 락킹을 지원합니다.
struct PartitionStore {
mut:
	records        []domain.Record
	base_offset    i64
	high_watermark i64
	lock           sync.RwMutex
}

/// new_memory_adapter는 새로운 메모리 스토리지 어댑터를 생성합니다.
pub fn new_memory_adapter() &MemoryStorageAdapter {
	return new_memory_adapter_with_config(MemoryConfig{})
}

/// new_memory_adapter_with_config는 사용자 정의 설정으로 어댑터를 생성합니다.
pub fn new_memory_adapter_with_config(config MemoryConfig) &MemoryStorageAdapter {
	return &MemoryStorageAdapter{
		config:         config
		topics:         map[string]&TopicStore{}
		topic_id_index: map[string]string{}
		groups:         map[string]domain.ConsumerGroup{}
		offsets:        map[string]map[string]i64{}
	}
}

// ============================================================
// 토픽 작업 (Topic Operations)
// ============================================================

/// create_topic은 새로운 토픽을 생성합니다.
/// UUID v4 형식의 topic_id를 자동 생성합니다.
pub fn (mut a MemoryStorageAdapter) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if name in a.topics {
		return error('topic already exists')
	}

	// topic_id용 UUID v4 생성 - 배열 한 번에 초기화
	mut topic_id := []u8{len: 16, init: u8(rand.intn(256) or { 0 })}
	// UUID 버전 4 (랜덤) 설정
	topic_id[6] = (topic_id[6] & 0x0f) | 0x40
	topic_id[8] = (topic_id[8] & 0x3f) | 0x80

	// 파티션 스토어 생성
	mut partition_stores := []&PartitionStore{cap: partitions}
	for _ in 0 .. partitions {
		partition_stores << &PartitionStore{
			records:        []domain.Record{}
			base_offset:    0
			high_watermark: 0
		}
	}

	metadata := domain.TopicMetadata{
		name:            name
		topic_id:        topic_id
		partition_count: partitions
		config:          map[string]string{}
		is_internal:     name.starts_with('__')
	}

	a.topics[name] = &TopicStore{
		metadata:   metadata
		config:     config
		partitions: partition_stores
	}

	// topic_id -> name 매핑을 캐시에 저장 (O(1) 조회용)
	a.topic_id_index[topic_id.hex()] = name

	return metadata
}

/// delete_topic은 토픽을 삭제합니다.
pub fn (mut a MemoryStorageAdapter) delete_topic(name string) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	topic := a.topics[name] or { return error('topic not found') }

	// topic_id_index 캐시에서 제거
	a.topic_id_index.delete(topic.metadata.topic_id.hex())

	a.topics.delete(name)
}

/// list_topics는 모든 토픽 목록을 반환합니다.
pub fn (mut a MemoryStorageAdapter) list_topics() ![]domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut result := []domain.TopicMetadata{}
	for _, topic in a.topics {
		result << topic.metadata
	}
	return result
}

/// get_topic은 토픽 메타데이터를 조회합니다.
pub fn (mut a MemoryStorageAdapter) get_topic(name string) !domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	if topic := a.topics[name] {
		return topic.metadata
	}
	return error('topic not found')
}

/// get_topic_by_id는 topic_id로 토픽을 조회합니다.
/// O(1) 캐시 조회를 사용합니다.
pub fn (mut a MemoryStorageAdapter) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	// topic_id_index 캐시를 사용한 O(1) 조회
	topic_id_hex := topic_id.hex()
	if topic_name := a.topic_id_index[topic_id_hex] {
		if topic := a.topics[topic_name] {
			return topic.metadata
		}
	}

	return error('topic not found')
}

/// add_partitions는 토픽에 파티션을 추가합니다.
pub fn (mut a MemoryStorageAdapter) add_partitions(name string, new_count int) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	mut topic := a.topics[name] or { return error('topic not found') }

	current := topic.partitions.len
	if new_count <= current {
		return error('new partition count must be greater than current')
	}

	topic.lock.@lock()
	defer { topic.lock.unlock() }

	for _ in current .. new_count {
		topic.partitions << &PartitionStore{
			records:        []domain.Record{}
			base_offset:    0
			high_watermark: 0
		}
	}

	topic.metadata = domain.TopicMetadata{
		...topic.metadata
		partition_count: new_count
	}
}

// ============================================================
// 레코드 작업 (Record Operations) - 파티션 수준 락킹
// ============================================================

/// append는 파티션에 레코드를 추가합니다.
/// 파티션 수준 락킹으로 동시성을 제어합니다.
pub fn (mut a MemoryStorageAdapter) append(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
	// 읽기 락으로 토픽 조회
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	// 특정 파티션만 쓰기 락
	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	base_offset := part.high_watermark
	now := time.now()

	// 타임스탬프와 함께 레코드 추가
	for record in records {
		mut r := record
		if r.timestamp.unix() == 0 {
			r = domain.Record{
				...r
				timestamp: now
			}
		}
		part.records << r
	}
	part.high_watermark += i64(records.len)

	// 보존 정책 적용 (최대 메시지 수)
	if a.config.max_messages_per_partition > 0 {
		excess := part.records.len - a.config.max_messages_per_partition
		if excess > 0 {
			// 슬라이스를 사용하여 clone 없이 제자리에서 요소 이동
			part.records = part.records[excess..]
			part.base_offset += i64(excess)
		}
	}

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  now.unix()
		log_start_offset: part.base_offset
		record_count:     records.len
	}
}

/// fetch는 파티션에서 레코드를 조회합니다.
pub fn (mut a MemoryStorageAdapter) fetch(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	// 읽기 락으로 토픽 조회
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	// 파티션 읽기 락
	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	// 오프셋이 범위를 벗어나면 빈 결과 반환
	if offset < part.base_offset {
		return domain.FetchResult{
			records:            []
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	start_idx := int(offset - part.base_offset)
	if start_idx >= part.records.len {
		return domain.FetchResult{
			records:            []
			high_watermark:     part.high_watermark
			last_stable_offset: part.high_watermark
			log_start_offset:   part.base_offset
		}
	}

	// max_bytes 기반으로 종료 인덱스 계산
	mut end_idx := start_idx
	mut total_bytes := 0
	max_fetch_bytes := if max_bytes <= 0 { 1048576 } else { max_bytes }

	for end_idx < part.records.len {
		record_size := part.records[end_idx].key.len + part.records[end_idx].value.len + 50
		if total_bytes + record_size > max_fetch_bytes && end_idx > start_idx {
			break
		}
		total_bytes += record_size
		end_idx++

		if end_idx - start_idx >= 1000 {
			break
		}
	}

	return domain.FetchResult{
		records:            part.records[start_idx..end_idx]
		high_watermark:     part.high_watermark
		last_stable_offset: part.high_watermark
		log_start_offset:   part.base_offset
	}
}

/// delete_records는 지정된 오프셋 이전의 레코드를 삭제합니다.
pub fn (mut a MemoryStorageAdapter) delete_records(topic_name string, partition int, before_offset i64) ! {
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.@lock()
	defer { part.lock.unlock() }

	delete_count := int(before_offset - part.base_offset)
	if delete_count > 0 && delete_count <= part.records.len {
		// clone 대신 슬라이스 할당 사용
		part.records = part.records[delete_count..]
		part.base_offset = before_offset
	}
}

// ============================================================
// 오프셋 작업 (Offset Operations)
// ============================================================

/// get_partition_info는 파티션 정보를 조회합니다.
pub fn (mut a MemoryStorageAdapter) get_partition_info(topic_name string, partition int) !domain.PartitionInfo {
	a.global_lock.rlock()
	topic := a.topics[topic_name] or {
		a.global_lock.runlock()
		return error('topic not found')
	}
	a.global_lock.runlock()

	if partition < 0 || partition >= topic.partitions.len {
		return error('partition out of range')
	}

	mut part := topic.partitions[partition]
	part.lock.rlock()
	defer { part.lock.runlock() }

	return domain.PartitionInfo{
		topic:           topic_name
		partition:       partition
		earliest_offset: part.base_offset
		latest_offset:   part.high_watermark
		high_watermark:  part.high_watermark
	}
}

// ============================================================
// 컨슈머 그룹 작업 (Consumer Group Operations)
// ============================================================

/// save_group은 컨슈머 그룹을 저장합니다.
pub fn (mut a MemoryStorageAdapter) save_group(group domain.ConsumerGroup) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	a.groups[group.group_id] = group
}

/// load_group은 컨슈머 그룹을 로드합니다.
pub fn (mut a MemoryStorageAdapter) load_group(group_id string) !domain.ConsumerGroup {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	if group := a.groups[group_id] {
		return group
	}
	return error('group not found')
}

/// delete_group은 컨슈머 그룹을 삭제합니다.
pub fn (mut a MemoryStorageAdapter) delete_group(group_id string) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if group_id !in a.groups {
		return error('group not found')
	}
	a.groups.delete(group_id)
	a.offsets.delete(group_id)
}

/// list_groups는 모든 컨슈머 그룹 목록을 반환합니다.
pub fn (mut a MemoryStorageAdapter) list_groups() ![]domain.GroupInfo {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut result := []domain.GroupInfo{}
	for _, group in a.groups {
		result << domain.GroupInfo{
			group_id:      group.group_id
			protocol_type: group.protocol_type
			state:         match group.state {
				.empty { 'Empty' }
				.preparing_rebalance { 'PreparingRebalance' }
				.completing_rebalance { 'CompletingRebalance' }
				.stable { 'Stable' }
				.dead { 'Dead' }
			}
		}
	}
	return result
}

// ============================================================
// 오프셋 커밋/조회 (Offset Commit/Fetch)
// ============================================================

/// commit_offsets는 오프셋을 커밋합니다.
pub fn (mut a MemoryStorageAdapter) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	if group_id !in a.offsets {
		a.offsets[group_id] = map[string]i64{}
	}

	for offset in offsets {
		key := '${offset.topic}:${offset.partition}'
		a.offsets[group_id][key] = offset.offset
	}
}

/// fetch_offsets는 커밋된 오프셋을 조회합니다.
pub fn (mut a MemoryStorageAdapter) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut results := []domain.OffsetFetchResult{}

	if group_id !in a.offsets {
		for part in partitions {
			results << domain.OffsetFetchResult{
				topic:      part.topic
				partition:  part.partition
				offset:     -1
				metadata:   ''
				error_code: 0
			}
		}
		return results
	}

	for part in partitions {
		key := '${part.topic}:${part.partition}'
		offset := a.offsets[group_id][key] or { -1 }
		results << domain.OffsetFetchResult{
			topic:      part.topic
			partition:  part.partition
			offset:     offset
			metadata:   ''
			error_code: 0
		}
	}

	return results
}

// ============================================================
// 헬스 체크 (Health Check)
// ============================================================

/// health_check는 스토리지 상태를 확인합니다.
pub fn (mut a MemoryStorageAdapter) health_check() !port.HealthStatus {
	return .healthy
}

// ============================================================
// 멀티 브로커 지원 (Multi-Broker Support)
// ============================================================

/// get_storage_capability는 스토리지 기능 정보를 반환합니다.
pub fn (a &MemoryStorageAdapter) get_storage_capability() domain.StorageCapability {
	return memory_capability
}

/// get_cluster_metadata_port는 클러스터 메타데이터 포트를 반환합니다.
/// 메모리 스토리지는 멀티 브로커를 지원하지 않으므로 none을 반환합니다.
pub fn (a &MemoryStorageAdapter) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

// ============================================================
// 통계 및 유틸리티 (Stats and Utilities)
// ============================================================

/// StorageStats는 스토리지 통계를 제공합니다.
pub struct StorageStats {
pub:
	topic_count      int
	total_partitions int
	total_records    i64
	total_bytes      i64
	group_count      int
}

/// get_stats는 현재 스토리지 통계를 반환합니다.
pub fn (mut a MemoryStorageAdapter) get_stats() StorageStats {
	a.global_lock.rlock()
	defer { a.global_lock.runlock() }

	mut total_partitions := 0
	mut total_records := i64(0)
	mut total_bytes := i64(0)

	for _, topic in a.topics {
		total_partitions += topic.partitions.len

		for i in 0 .. topic.partitions.len {
			part := topic.partitions[i]
			total_records += part.high_watermark - part.base_offset
			// 참고: total_bytes는 레코드를 순회하지 않으면 대략적인 값
		}
	}

	return StorageStats{
		topic_count:      a.topics.len
		total_partitions: total_partitions
		total_records:    total_records
		total_bytes:      total_bytes
		group_count:      a.groups.len
	}
}

/// clear는 모든 데이터를 삭제합니다 (테스트용).
pub fn (mut a MemoryStorageAdapter) clear() {
	a.global_lock.@lock()
	defer { a.global_lock.unlock() }

	a.topics.clear()
	a.topic_id_index.clear()
	a.groups.clear()
	a.offsets.clear()
}

// ============================================================
// SharedAdapter - 동시성 테스트를 위한 스레드 안전 래퍼
// V 언어는 스레드 간 접근에 'shared' 키워드가 필요합니다.
// ============================================================

/// SharedAdapter는 V에서 동시 접근을 위해 MemoryStorageAdapter를 래핑합니다.
/// 사용법: shared adapter := SharedAdapter{ inner: new_memory_adapter() }
pub struct SharedAdapter {
pub mut:
	inner &MemoryStorageAdapter
}

/// new_shared_adapter는 새로운 SharedAdapter를 생성합니다.
pub fn new_shared_adapter() SharedAdapter {
	return SharedAdapter{
		inner: new_memory_adapter()
	}
}

/// new_shared_adapter_with_config는 사용자 정의 설정으로 SharedAdapter를 생성합니다.
pub fn new_shared_adapter_with_config(config MemoryConfig) SharedAdapter {
	return SharedAdapter{
		inner: new_memory_adapter_with_config(config)
	}
}

/// create_topic_safe는 스레드 안전한 토픽 생성을 수행합니다.
pub fn (shared a SharedAdapter) create_topic_safe(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	lock a {
		return a.inner.create_topic(name, partitions, config)
	}
}

/// append_safe는 스레드 안전한 레코드 추가를 수행합니다.
pub fn (shared a SharedAdapter) append_safe(topic_name string, partition int, records []domain.Record) !domain.AppendResult {
	lock a {
		return a.inner.append(topic_name, partition, records)
	}
}

/// fetch_safe는 스레드 안전한 레코드 조회를 수행합니다.
pub fn (shared a SharedAdapter) fetch_safe(topic_name string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	lock a {
		return a.inner.fetch(topic_name, partition, offset, max_bytes)
	}
}

/// get_partition_info_safe는 스레드 안전한 파티션 정보 조회를 수행합니다.
pub fn (shared a SharedAdapter) get_partition_info_safe(topic_name string, partition int) !domain.PartitionInfo {
	lock a {
		return a.inner.get_partition_info(topic_name, partition)
	}
}

/// commit_offsets_safe는 스레드 안전한 오프셋 커밋을 수행합니다.
pub fn (shared a SharedAdapter) commit_offsets_safe(group_id string, offsets []domain.PartitionOffset) ! {
	lock a {
		return a.inner.commit_offsets(group_id, offsets)
	}
}

/// fetch_offsets_safe는 스레드 안전한 오프셋 조회를 수행합니다.
pub fn (shared a SharedAdapter) fetch_offsets_safe(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	lock a {
		return a.inner.fetch_offsets(group_id, partitions)
	}
}

/// get_stats_safe는 스레드 안전한 통계 조회를 수행합니다.
pub fn (shared a SharedAdapter) get_stats_safe() StorageStats {
	lock a {
		return a.inner.get_stats()
	}
}
