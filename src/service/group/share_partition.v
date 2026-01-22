// 서비스 레이어 - Share 파티션 관리 (KIP-932)
// Share 파티션, 레코드 획득, 확인, 해제를 관리합니다.
// Share 그룹은 여러 컨슈머가 동일한 파티션을 공유하여 메시지를 처리할 수 있게 합니다.
module group

import domain
import service.port
import sync
import time

// ============================================================================
// Share 파티션 매니저
// ============================================================================

/// SharePartitionManager는 share 파티션과 레코드 상태를 관리합니다.
/// 레코드 획득, 확인, 해제 등의 작업을 처리합니다.
pub struct SharePartitionManager {
mut:
	// "group_id:topic:partition" 키로 관리되는 share 파티션
	partitions map[string]&domain.SharePartition
	// 영구 저장을 위한 스토리지
	storage port.StoragePort
	// 스레드 안전성
	lock sync.RwMutex
}

/// new_share_partition_manager는 새로운 share 파티션 매니저를 생성합니다.
pub fn new_share_partition_manager(storage port.StoragePort) &SharePartitionManager {
	return &SharePartitionManager{
		partitions: map[string]&domain.SharePartition{}
		storage:    storage
	}
}

// ============================================================================
// 파티션 관리
// ============================================================================

/// get_or_create_partition은 share 파티션을 가져오거나 생성합니다.
pub fn (mut m SharePartitionManager) get_or_create_partition(group_id string, topic_name string, partition i32) &domain.SharePartition {
	m.lock.@lock()
	defer { m.lock.unlock() }

	return m.get_or_create_partition_internal(group_id, topic_name, partition)
}

/// get_partition은 share 파티션을 반환합니다.
pub fn (mut m SharePartitionManager) get_partition(group_id string, topic_name string, partition i32) ?&domain.SharePartition {
	m.lock.rlock()
	defer { m.lock.runlock() }

	key := '${group_id}:${topic_name}:${partition}'
	return m.partitions[key] or { return none }
}

/// delete_partitions_for_group은 그룹의 모든 파티션을 삭제합니다.
pub fn (mut m SharePartitionManager) delete_partitions_for_group(group_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut to_delete := []string{}
	for key, _ in m.partitions {
		if key.starts_with('${group_id}:') {
			to_delete << key
		}
	}
	for key in to_delete {
		m.partitions.delete(key)
	}
}

/// get_partition_stats는 그룹의 파티션 통계를 반환합니다.
/// 반환값: (파티션 수, 총 획득, 총 확인, 총 해제, 총 거부)
pub fn (mut m SharePartitionManager) get_partition_stats(group_id string) (int, i64, i64, i64, i64) {
	m.lock.rlock()
	defer { m.lock.runlock() }

	mut partition_count := 0
	mut total_acquired := i64(0)
	mut total_acknowledged := i64(0)
	mut total_released := i64(0)
	mut total_rejected := i64(0)

	for key, sp in m.partitions {
		if key.starts_with('${group_id}:') {
			partition_count += 1
			total_acquired += sp.total_acquired
			total_acknowledged += sp.total_acknowledged
			total_released += sp.total_released
			total_rejected += sp.total_rejected
		}
	}

	return partition_count, total_acquired, total_acknowledged, total_released, total_rejected
}

// ============================================================================
// 레코드 획득
// ============================================================================

/// acquire_records는 컨슈머를 위해 레코드를 획득합니다.
/// 획득된 레코드는 지정된 기간 동안 해당 컨슈머에게 잠깁니다.
pub fn (mut m SharePartitionManager) acquire_records(group_id string, member_id string, topic_name string, partition i32, max_records int, lock_duration_ms i64, max_partition_locks int) []domain.AcquiredRecordInfo {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut sp := m.get_or_create_partition_internal(group_id, topic_name, partition)
	now := time.now().unix_milli()

	mut acquired := []domain.AcquiredRecordInfo{}
	mut offset := sp.start_offset

	// 획득할 수 있는 레코드 찾기
	for acquired.len < max_records && offset <= sp.end_offset {
		state := sp.record_states[offset] or { domain.RecordState.available }

		if state == .available {
			// 최대 파티션 잠금 수 확인
			if sp.acquired_records.len >= max_partition_locks {
				break
			}

			// 배달 횟수 가져오기 또는 초기화
			mut delivery_count := i32(1)
			if existing := sp.acquired_records[offset] {
				delivery_count = existing.delivery_count + 1
			}

			// 레코드 획득
			sp.record_states[offset] = .acquired
			sp.acquired_records[offset] = domain.AcquiredRecord{
				offset:          offset
				member_id:       member_id
				delivery_count:  delivery_count
				acquired_at:     now
				lock_expires_at: now + lock_duration_ms
			}

			acquired << domain.AcquiredRecordInfo{
				offset:         offset
				delivery_count: delivery_count
				timestamp:      now
			}

			sp.total_acquired += 1
		}

		offset += 1
	}

	// 필요시 end_offset 업데이트
	if offset > sp.end_offset {
		sp.end_offset = offset
	}

	return acquired
}

// ============================================================================
// 레코드 확인
// ============================================================================

/// acknowledge_records는 레코드에 대한 확인을 처리합니다.
/// accept, release, reject 세 가지 타입을 지원합니다.
pub fn (mut m SharePartitionManager) acknowledge_records(group_id string, member_id string, batch domain.AcknowledgementBatch, delivery_attempt_limit i32) domain.ShareAcknowledgeResult {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut sp := m.get_or_create_partition_internal(group_id, batch.topic_name, batch.partition)

	// 배치의 각 오프셋 처리
	for offset := batch.first_offset; offset <= batch.last_offset; offset++ {
		// 갭 오프셋 건너뛰기
		if offset in batch.gap_offsets {
			continue
		}

		// 이 멤버가 획득한 레코드인지 확인
		acquired := sp.acquired_records[offset] or { continue }

		if acquired.member_id != member_id {
			// 이 멤버가 소유하지 않은 레코드
			continue
		}

		match batch.acknowledge_type {
			.accept {
				// 성공적으로 처리됨
				sp.record_states[offset] = .acknowledged
				sp.acquired_records.delete(offset)
				sp.total_acknowledged += 1
			}
			.release {
				// 재배달을 위해 해제
				if acquired.delivery_count < delivery_attempt_limit {
					sp.record_states[offset] = .available
				} else {
					// 최대 시도 횟수 도달 - 아카이브
					sp.record_states[offset] = .archived
				}
				sp.acquired_records.delete(offset)
				sp.total_released += 1
			}
			.reject {
				// 처리 불가로 거부됨
				sp.record_states[offset] = .archived
				sp.acquired_records.delete(offset)
				sp.total_rejected += 1
			}
		}
	}

	// 가능하면 SPSO(Share Partition Start Offset) 전진
	m.advance_spso_internal(mut sp)

	return domain.ShareAcknowledgeResult{
		topic_name: batch.topic_name
		partition:  batch.partition
		error_code: 0
	}
}

// ============================================================================
// 레코드 해제
// ============================================================================

/// release_expired_locks_with_limits는 획득 잠금이 만료된 레코드를 해제합니다.
/// 제공된 맵의 배달 시도 제한을 사용합니다.
pub fn (mut m SharePartitionManager) release_expired_locks_with_limits(group_limits map[string]i32) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	now := time.now().unix_milli()

	for _, mut sp in m.partitions {
		delivery_limit := group_limits[sp.group_id] or { continue }

		mut expired := []i64{}
		for offset, acquired in sp.acquired_records {
			if now > acquired.lock_expires_at {
				expired << offset
			}
		}

		for offset in expired {
			acquired := sp.acquired_records[offset] or { continue }

			// 배달 횟수 확인
			if acquired.delivery_count >= delivery_limit {
				// 최대 시도 횟수 도달 - 아카이브
				sp.record_states[offset] = .archived
			} else {
				// 재배달을 위해 해제
				sp.record_states[offset] = .available
			}
			sp.acquired_records.delete(offset)
			sp.total_released += 1
		}

		// 가능하면 SPSO 전진
		m.advance_spso_internal(mut sp)
	}
}

/// release_member_records는 멤버가 획득한 모든 레코드를 해제합니다.
pub fn (mut m SharePartitionManager) release_member_records(group_id string, member_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	m.release_member_records_internal(group_id, member_id)
}

/// release_member_records_internal은 잠금 없이 내부적으로 멤버 레코드를 해제합니다.
pub fn (mut m SharePartitionManager) release_member_records_internal(group_id string, member_id string) {
	for _, mut sp in m.partitions {
		if sp.group_id != group_id {
			continue
		}

		mut to_release := []i64{}
		for offset, acquired in sp.acquired_records {
			if acquired.member_id == member_id {
				to_release << offset
			}
		}

		for offset in to_release {
			sp.record_states[offset] = .available
			sp.acquired_records.delete(offset)
		}
	}
}

// ============================================================================
// 내부 헬퍼
// ============================================================================

/// get_or_create_partition_internal은 잠금 없이 파티션을 가져오거나 생성합니다.
fn (mut m SharePartitionManager) get_or_create_partition_internal(group_id string, topic_name string, partition i32) &domain.SharePartition {
	key := '${group_id}:${topic_name}:${partition}'

	if sp := m.partitions[key] {
		return sp
	}

	mut start_offset := i64(0)
	if info := m.storage.get_partition_info(topic_name, partition) {
		start_offset = info.high_watermark
	}

	mut sp := &domain.SharePartition{
		...domain.new_share_partition(topic_name, partition, group_id, start_offset)
	}
	m.partitions[key] = sp
	return sp
}

/// advance_spso_internal은 확인/아카이브된 모든 레코드를 지나 SPSO를 전진시킵니다.
fn (mut m SharePartitionManager) advance_spso_internal(mut sp domain.SharePartition) {
	// 확인/아카이브된 모든 레코드를 지나 SPSO 전진
	mut new_start := sp.start_offset
	for new_start <= sp.end_offset {
		state := sp.record_states[new_start] or { break }
		if state == .acknowledged || state == .archived {
			// 이 오프셋의 상태 정리
			sp.record_states.delete(new_start)
			new_start += 1
		} else {
			break
		}
	}
	sp.start_offset = new_start
}
