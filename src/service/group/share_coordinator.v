// Share 그룹, 파티션 할당, 진행 중인 레코드 상태를 관리합니다.
// Share 그룹은 전통적인 컨슈머 그룹과 달리 여러 컨슈머가 동일한 파티션을 공유합니다.
module group

import domain
import service.port
import sync
import time
import rand

// Share 그룹 코디네이터

/// ShareGroupCoordinator는 share 그룹을 관리합니다.
/// 멤버십 관리, 파티션 할당, 레코드 상태 추적을 담당합니다.
pub struct ShareGroupCoordinator {
mut:
	// group_id로 키가 지정된 share 그룹
	groups map[string]&domain.ShareGroup
	config domain.ShareGroupConfig
	// 영구 저장을 위한 스토리지
	storage port.StoragePort
	// 스레드 안전성
	lock sync.RwMutex
	// 할당자
	assignor &ShareGroupSimpleAssignor
	// 파티션 매니저
	partition_manager &SharePartitionManager
	// 세션 매니저
	session_manager &ShareSessionManager
}

/// new_share_group_coordinator는 새로운 share 그룹 코디네이터를 생성합니다.
pub fn new_share_group_coordinator(storage port.StoragePort, config domain.ShareGroupConfig) &ShareGroupCoordinator {
	return &ShareGroupCoordinator{
		groups:            map[string]&domain.ShareGroup{}
		config:            config
		storage:           storage
		assignor:          new_simple_assignor()
		partition_manager: new_share_partition_manager(storage)
		session_manager:   new_share_session_manager()
	}
}

// 그룹 관리

/// get_or_create_group은 share 그룹을 가져오거나 생성합니다.
pub fn (mut c ShareGroupCoordinator) get_or_create_group(group_id string) &domain.ShareGroup {
	c.lock.@lock()
	defer { c.lock.unlock() }

	if group := c.groups[group_id] {
		return group
	}

	// 새 그룹 생성
	mut new_group := &domain.ShareGroup{
		...domain.new_share_group(group_id, c.config)
	}
	c.groups[group_id] = new_group
	return new_group
}

/// get_group은 ID로 share 그룹을 반환합니다.
pub fn (mut c ShareGroupCoordinator) get_group(group_id string) ?&domain.ShareGroup {
	c.lock.rlock()
	defer { c.lock.runlock() }
	return c.groups[group_id] or { return none }
}

/// delete_group은 share 그룹을 삭제합니다.
/// 활성 멤버가 있는 그룹은 삭제할 수 없습니다.
pub fn (mut c ShareGroupCoordinator) delete_group(group_id string) ! {
	c.lock.@lock()
	defer { c.lock.unlock() }

	group := c.groups[group_id] or { return error('group not found: ${group_id}') }

	if group.members.len > 0 {
		return error('cannot delete group with active members')
	}

	// 연관된 파티션 삭제
	c.partition_manager.delete_partitions_for_group(group_id)

	// 세션 삭제
	c.session_manager.delete_sessions_for_group(group_id)

	c.groups.delete(group_id)
}

/// list_groups는 모든 share 그룹을 반환합니다.
pub fn (mut c ShareGroupCoordinator) list_groups() []string {
	c.lock.rlock()
	defer { c.lock.runlock() }

	mut groups := []string{}
	for group_id, _ in c.groups {
		groups << group_id
	}
	return groups
}

// 멤버 관리

/// ShareGroupHeartbeatRequest는 하트비트 요청을 나타냅니다.
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

/// ShareGroupHeartbeatResponse는 하트비트 응답을 나타냅니다.
pub struct ShareGroupHeartbeatResponse {
pub:
	error_code                i16
	error_message             string
	member_id                 string
	member_epoch              i32
	heartbeat_interval        i32
	assignment                []domain.SharePartitionAssignment
	should_compute_assignment bool
}

/// heartbeat는 share 그룹 하트비트를 처리합니다.
/// 새 멤버 참가, 구독 변경, 리밸런싱을 처리합니다.
pub fn (mut c ShareGroupCoordinator) heartbeat(req ShareGroupHeartbeatRequest) ShareGroupHeartbeatResponse {
	c.lock.@lock()
	defer { c.lock.unlock() }

	mut group := c.get_or_create_group_internal(req.group_id)
	now := time.now().unix_milli()

	// 새 멤버인 경우 (에포크 = 0) 멤버 ID 생성
	mut member_id := req.member_id
	mut is_new_member := false

	if req.member_epoch == 0 || member_id.len == 0 {
		// 새 멤버 참가
		member_id = generate_member_id()
		is_new_member = true
	}

	// 멤버 가져오기 또는 생성
	mut member := group.members[member_id] or {
		// 새 멤버 생성
		mut new_member := &domain.ShareMember{
			member_id:              member_id
			rack_id:                req.rack_id
			client_id:              ''
			client_host:            ''
			subscribed_topic_names: req.subscribed_topic_names
			member_epoch:           0
			state:                  .joining
			assigned_partitions:    []domain.SharePartitionAssignment{}
			last_heartbeat:         now
			joined_at:              now
		}
		group.members[member_id] = new_member
		is_new_member = true
		new_member
	}

	// 멤버 정보 업데이트
	member.last_heartbeat = now
	member.rack_id = req.rack_id

	// 에포크 확인
	if !is_new_member && req.member_epoch != member.member_epoch {
		// 펜스된 멤버
		return ShareGroupHeartbeatResponse{
			error_code:    22
			error_message: 'member epoch mismatch'
			member_id:     member_id
			member_epoch:  member.member_epoch
		}
	}

	// 구독 변경 확인
	subscriptions_changed := !arrays_equal(member.subscribed_topic_names, req.subscribed_topic_names)
	if subscriptions_changed {
		member.subscribed_topic_names = req.subscribed_topic_names.clone()
		// 그룹의 구독 토픽 업데이트
		for topic in req.subscribed_topic_names {
			group.subscribed_topics[topic] = true
		}
	}

	// 필요시 리밸런싱 트리거
	mut needs_rebalance := is_new_member || subscriptions_changed

	if needs_rebalance {
		group.group_epoch += 1
		c.compute_assignment(mut group)
	}

	// 멤버 에포크 및 상태 업데이트
	if is_new_member || needs_rebalance {
		member.member_epoch = group.assignment_epoch
		member.state = .stable
	}

	// 멤버의 할당 가져오기
	assignment := group.target_assignment[member_id] or { []domain.SharePartitionAssignment{} }
	member.assigned_partitions = assignment

	// 그룹 상태 업데이트
	if group.members.len > 0 {
		group.state = .stable
	}
	group.updated_at = now

	return ShareGroupHeartbeatResponse{
		error_code:         0
		member_id:          member_id
		member_epoch:       member.member_epoch
		heartbeat_interval: group.heartbeat_interval_ms
		assignment:         assignment
	}
}

/// leave_group은 멤버가 그룹을 떠나는 것을 처리합니다.
pub fn (mut c ShareGroupCoordinator) leave_group(group_id string, member_id string) ! {
	c.lock.@lock()
	defer { c.lock.unlock() }

	mut group := c.groups[group_id] or { return error('group not found') }

	if member_id !in group.members {
		return error('member not found')
	}

	// 멤버 제거
	group.members.delete(member_id)
	group.target_assignment.delete(member_id)

	// 에포크 증가 및 할당 재계산
	group.group_epoch += 1
	c.compute_assignment(mut group)

	// 그룹 상태 업데이트
	if group.members.len == 0 {
		group.state = .empty
	}

	// 이 멤버가 획득한 레코드 해제
	c.partition_manager.release_member_records_internal(group_id, member_id)

	// 세션 삭제
	c.session_manager.delete_session(group_id, member_id)
}

/// remove_expired_members는 타임아웃된 멤버를 제거합니다.
pub fn (mut c ShareGroupCoordinator) remove_expired_members() {
	c.lock.@lock()
	defer { c.lock.unlock() }

	now := time.now().unix_milli()

	for group_id, mut group in c.groups {
		mut expired_members := []string{}

		for member_id, member in group.members {
			if now - member.last_heartbeat > group.session_timeout_ms {
				expired_members << member_id
			}
		}

		for member_id in expired_members {
			group.members.delete(member_id)
			group.target_assignment.delete(member_id)
			c.partition_manager.release_member_records_internal(group_id, member_id)

			// 세션 삭제
			c.session_manager.delete_session(group_id, member_id)
		}

		if expired_members.len > 0 {
			group.group_epoch += 1
			c.compute_assignment(mut group)

			if group.members.len == 0 {
				group.state = .empty
			}
		}
	}
}

// 할당

/// compute_assignment는 모든 멤버에 대한 파티션 할당을 계산합니다.
fn (mut c ShareGroupCoordinator) compute_assignment(mut group domain.ShareGroup) {
	// 멤버 구독 수집
	mut member_subs := []ShareMemberSubscription{}
	for _, member in group.members {
		member_subs << ShareMemberSubscription{
			member_id: member.member_id
			rack_id:   member.rack_id
			topics:    member.subscribed_topic_names
		}
	}

	// 토픽 메타데이터 수집
	mut topic_meta := map[string]ShareTopicMetadata{}
	for topic_name, _ in group.subscribed_topics {
		// 스토리지에서 파티션 수 가져오기
		topic := c.storage.get_topic(topic_name) or { continue }
		topic_meta[topic_name] = ShareTopicMetadata{
			topic_id:        topic.topic_id
			topic_name:      topic_name
			partition_count: topic.partition_count
		}
	}

	// SimpleAssignor를 사용하여 할당 계산
	new_assignment := c.assignor.assign(member_subs, topic_meta)

	// 대상 할당 업데이트 - 맵 복제
	group.target_assignment.clear()
	for k, v in new_assignment {
		group.target_assignment[k] = v
	}
	group.assignment_epoch = group.group_epoch
}

/// get_or_create_group_internal은 잠금 없이 그룹을 가져오거나 생성합니다.
fn (mut c ShareGroupCoordinator) get_or_create_group_internal(group_id string) &domain.ShareGroup {
	if group := c.groups[group_id] {
		return group
	}

	mut new_group := &domain.ShareGroup{
		...domain.new_share_group(group_id, c.config)
	}
	c.groups[group_id] = new_group
	return new_group
}

// 파티션 관리 (위임)

/// get_or_create_partition은 share 파티션을 가져오거나 생성합니다.
pub fn (mut c ShareGroupCoordinator) get_or_create_partition(group_id string, topic_name string, partition i32) &domain.SharePartition {
	return c.partition_manager.get_or_create_partition(group_id, topic_name, partition)
}

/// get_partition은 share 파티션을 반환합니다.
pub fn (mut c ShareGroupCoordinator) get_partition(group_id string, topic_name string, partition i32) ?&domain.SharePartition {
	return c.partition_manager.get_partition(group_id, topic_name, partition)
}

// 레코드 획득 (위임)

/// acquire_records는 컨슈머를 위해 레코드를 획득합니다.
pub fn (mut c ShareGroupCoordinator) acquire_records(group_id string, member_id string, topic_name string, partition i32, max_records int) []domain.AcquiredRecordInfo {
	c.lock.rlock()
	group := c.groups[group_id] or {
		c.lock.runlock()
		return []domain.AcquiredRecordInfo{}
	}
	lock_duration := group.record_lock_duration_ms
	max_partition_locks := group.max_partition_locks
	c.lock.runlock()

	return c.partition_manager.acquire_records(group_id, member_id, topic_name, partition,
		max_records, lock_duration, max_partition_locks)
}

/// acknowledge_records는 레코드에 대한 확인을 처리합니다.
pub fn (mut c ShareGroupCoordinator) acknowledge_records(group_id string, member_id string, batch domain.AcknowledgementBatch) domain.ShareAcknowledgeResult {
	c.lock.rlock()
	group := c.groups[group_id] or {
		c.lock.runlock()
		return domain.ShareAcknowledgeResult{
			topic_name:    batch.topic_name
			partition:     batch.partition
			error_code:    69
			error_message: 'share group not found'
		}
	}
	delivery_attempt_limit := group.delivery_attempt_limit
	c.lock.runlock()

	return c.partition_manager.acknowledge_records(group_id, member_id, batch, delivery_attempt_limit)
}

/// release_expired_locks는 획득 잠금이 만료된 레코드를 해제합니다.
pub fn (mut c ShareGroupCoordinator) release_expired_locks() {
	c.lock.rlock()
	// 각 그룹의 배달 시도 제한 추출
	mut group_limits := map[string]i32{}
	for k, g in c.groups {
		group_limits[k] = g.delivery_attempt_limit
	}
	c.lock.runlock()

	c.partition_manager.release_expired_locks_with_limits(group_limits)
}

// 세션 관리 (위임)

/// get_or_create_session은 share 세션을 가져오거나 생성합니다.
pub fn (mut c ShareGroupCoordinator) get_or_create_session(group_id string, member_id string) &domain.ShareSession {
	return c.session_manager.get_or_create_session(group_id, member_id)
}

/// update_session은 share 세션을 업데이트합니다.
pub fn (mut c ShareGroupCoordinator) update_session(group_id string, member_id string, epoch i32, partitions_to_add []domain.ShareSessionPartition, partitions_to_remove []domain.ShareSessionPartition) !&domain.ShareSession {
	return c.session_manager.update_session(group_id, member_id, epoch, partitions_to_add,
		partitions_to_remove)
}

/// close_session은 share 세션을 종료합니다.
pub fn (mut c ShareGroupCoordinator) close_session(group_id string, member_id string) {
	// 획득한 잠금 해제
	c.partition_manager.release_member_records(group_id, member_id)

	c.session_manager.close_session(group_id, member_id)
}

// 통계

/// ShareGroupStats는 share 그룹의 통계를 담습니다.
pub struct ShareGroupStats {
pub:
	group_id           string
	member_count       int
	partition_count    int
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// get_stats는 share 그룹의 통계를 반환합니다.
pub fn (mut c ShareGroupCoordinator) get_stats(group_id string) ShareGroupStats {
	c.lock.rlock()
	group := c.groups[group_id] or {
		c.lock.runlock()
		return ShareGroupStats{
			group_id: group_id
		}
	}
	member_count := group.members.len
	c.lock.runlock()

	partition_count, total_acquired, total_acknowledged, total_released, total_rejected := c.partition_manager.get_partition_stats(group_id)

	return ShareGroupStats{
		group_id:           group_id
		member_count:       member_count
		partition_count:    partition_count
		total_acquired:     total_acquired
		total_acknowledged: total_acknowledged
		total_released:     total_released
		total_rejected:     total_rejected
	}
}

/// generate_member_id는 UUID 형식의 멤버 ID를 생성합니다.
fn generate_member_id() string {
	mut bytes := []u8{len: 16}
	for i in 0 .. 16 {
		bytes[i] = u8(rand.intn(256) or { 0 })
	}
	// 버전 4 및 variant 비트 설정
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80

	return '${bytes[0..4].hex()}-${bytes[4..6].hex()}-${bytes[6..8].hex()}-${bytes[8..10].hex()}-${bytes[10..16].hex()}'
}

/// arrays_equal은 두 문자열 배열이 같은지 비교합니다.
fn arrays_equal(a []string, b []string) bool {
	if a.len != b.len {
		return false
	}
	for i, v in a {
		if v != b[i] {
			return false
		}
	}
	return true
}
