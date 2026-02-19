// Infra Layer - S3 오프셋 배치 버퍼
// 컨슈머 그룹 오프셋을 메모리에 버퍼링하고 주기적으로 S3에 플러시합니다.
// flush_worker의 실패 복원 패턴을 따릅니다 (buffer_manager.v 참조).
module s3

import domain
import time

/// OffsetGroupBuffer는 컨슈머 그룹의 오프셋을 메모리에 버퍼링합니다.
/// flush_worker가 주기적으로 dirty 상태인 버퍼를 S3에 플러시합니다.
struct OffsetGroupBuffer {
pub mut:
	offsets       map[string]OffsetEntry
	version       i32
	dirty         bool
	dirty_count   int
	last_flush_at i64
}

/// buffer_offset은 오프셋을 메모리 버퍼에 축적합니다.
/// 호출자가 offset_buffer_lock을 획득한 상태에서 호출해야 합니다.
/// 임계치 초과 시 true를 반환하여 즉시 플러시가 필요함을 알립니다.
fn (mut a S3StorageAdapter) buffer_offset(group_id string, offset domain.PartitionOffset) bool {
	cache_key := '${offset.topic}:${offset.partition}'

	if group_id !in a.offset_buffers {
		a.offset_buffers[group_id] = OffsetGroupBuffer{
			offsets:     map[string]OffsetEntry{}
			dirty:       true
			dirty_count: 1
		}
	}

	mut buf := a.offset_buffers[group_id]
	buf.offsets[cache_key] = OffsetEntry{
		offset:       offset.offset
		leader_epoch: offset.leader_epoch
		metadata:     offset.metadata
		committed_at: time.now().unix_milli()
	}
	buf.dirty = true
	buf.dirty_count++
	a.offset_buffers[group_id] = buf

	return a.config.offset_flush_threshold_count > 0
		&& buf.dirty_count >= a.config.offset_flush_threshold_count
}

/// get_buffered_offset은 버퍼에서 오프셋을 조회합니다.
/// 버퍼에 없으면 none을 반환합니다.
/// 호출자가 offset_buffer_lock을 획득한 상태에서 호출해야 합니다.
fn (a &S3StorageAdapter) get_buffered_offset(group_id string, topic string, partition int) ?i64 {
	cache_key := '${topic}:${partition}'
	if group_id in a.offset_buffers {
		if entry := a.offset_buffers[group_id].offsets[cache_key] {
			return entry.offset
		}
	}
	return none
}

/// flush_pending_offsets는 dirty 상태인 모든 그룹의 오프셋을 S3에 플러시합니다.
/// flush_worker에서 spawn으로 호출됩니다.
fn (mut a S3StorageAdapter) flush_pending_offsets() {
	// 1. offset_buffer_lock 획득, dirty 그룹 추출
	a.offset_buffer_lock.lock()

	mut groups_to_flush := map[string]OffsetGroupBuffer{}
	for group_id, _ in a.offset_buffers {
		buf := a.offset_buffers[group_id]
		if buf.dirty {
			now := time.now().unix_milli()
			// 기존 map을 이동하고 빈 map으로 교체 (clone 회피)
			mut updated := a.offset_buffers[group_id]
			groups_to_flush[group_id] = OffsetGroupBuffer{
				offsets:       updated.offsets
				version:       updated.version
				dirty:         false
				dirty_count:   0
				last_flush_at: now
			}
			updated.offsets = map[string]OffsetEntry{}
			updated.dirty = false
			updated.dirty_count = 0
			updated.last_flush_at = now
			a.offset_buffers[group_id] = updated
		}
	}

	a.offset_buffer_lock.unlock()

	if groups_to_flush.len == 0 {
		return
	}

	// 2. 그룹별 S3 플러시
	for group_id, buf in groups_to_flush {
		a.flush_group_offsets(group_id, buf) or {
			// 실패 시 버퍼 복원 (buffer_manager.v의 flush_worker 패턴 참조)
			a.restore_offset_buffer(group_id, buf)
			// 메트릭: 오프셋 플러시 실패
			a.metrics_lock.@lock()
			a.metrics.offset_flush_error_count++
			a.metrics_lock.unlock()
			log_message(.error, 'OffsetFlush', 'Failed to flush offsets', {
				'group_id': group_id
				'error':    err.msg()
			})
		}
	}
}

/// flush_group_offsets는 단일 그룹의 오프셋 스냅샷을 S3에 저장합니다.
/// merge-on-conflict 전략을 사용합니다.
fn (mut a S3StorageAdapter) flush_group_offsets(group_id string, buf OffsetGroupBuffer) ! {
	snapshot_key := a.offset_snapshot_key(group_id)

	// 1. S3에서 현재 스냅샷 조회 (없으면 빈 스냅샷)
	mut remote := new_offset_snapshot(a.config.broker_id)
	data, _ := a.get_object(snapshot_key, -1, -1) or { []u8{}, '' }
	if data.len > 0 {
		remote = try_decode_offset_data(data) or {
			// 디코딩 실패 시 빈 스냅샷으로 시작
			new_offset_snapshot(a.config.broker_id)
		}
	}

	// 2. 로컬 버퍼를 스냅샷으로 변환
	local := OffsetSnapshot{
		version:   buf.version
		broker_id: a.config.broker_id
		timestamp: time.now().unix_milli()
		offsets:   buf.offsets.clone()
	}

	// 3. 병합 (max offset per partition)
	merged := merge_offset_snapshots(local, remote)

	// 4. 인코딩 및 S3 PUT
	encoded := encode_offset_snapshot(merged)
	a.put_object(snapshot_key, encoded)!

	// 5. 버퍼의 version 업데이트
	a.offset_buffer_lock.lock()
	if group_id in a.offset_buffers {
		mut updated := a.offset_buffers[group_id]
		updated.version = merged.version
		a.offset_buffers[group_id] = updated
	}
	a.offset_buffer_lock.unlock()

	// 메트릭: 오프셋 플러시 성공
	a.metrics_lock.@lock()
	a.metrics.offset_flush_count++
	a.metrics.offset_flush_success_count++
	a.metrics_lock.unlock()
}

/// merge_offset_entries는 source의 오프셋 엔트리를 target에 병합합니다.
/// 동일 키가 존재하면 더 큰 offset을 가진 엔트리를 선택합니다.
fn merge_offset_entries(mut target map[string]OffsetEntry, source map[string]OffsetEntry) {
	for key, entry in source {
		if key in target {
			if entry.offset > target[key].offset {
				target[key] = entry
			}
		} else {
			target[key] = entry
		}
	}
}

/// restore_offset_buffer는 플러시 실패 시 버퍼를 이전 상태로 복원합니다.
/// buffer_manager.v의 flush_worker 실패 복원 패턴을 따릅니다.
fn (mut a S3StorageAdapter) restore_offset_buffer(group_id string, failed_buf OffsetGroupBuffer) {
	a.offset_buffer_lock.lock()
	defer { a.offset_buffer_lock.unlock() }

	if group_id in a.offset_buffers {
		mut current := a.offset_buffers[group_id]
		merge_offset_entries(mut current.offsets, failed_buf.offsets)
		current.dirty = true
		current.dirty_count += failed_buf.dirty_count
		a.offset_buffers[group_id] = current
	} else {
		// 그룹이 삭제된 경우 재생성
		a.offset_buffers[group_id] = OffsetGroupBuffer{
			offsets:       failed_buf.offsets
			version:       failed_buf.version
			dirty:         true
			dirty_count:   failed_buf.dirty_count
			last_flush_at: 0
		}
	}
}

/// offset_snapshot_key는 오프셋 스냅샷의 S3 키를 생성합니다.
fn (a &S3StorageAdapter) offset_snapshot_key(group_id string) string {
	return '${a.config.prefix}offsets/${group_id}/snapshot.bin'
}
