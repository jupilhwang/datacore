// KIP-848 새 컨슈머 프로토콜 - 그룹 코디네이터
// 점진적 리밸런싱을 지원하는 서버 측 파티션 할당
//

// DataCore 무상태 아키텍처 참고

// DataCore는 모든 브로커가 공유 스토리지(S3, PostgreSQL 등)에 접근하는
// 무상태 브로커 아키텍처를 사용합니다. 이는 파티션 할당을 단순화합니다:
//
// - 브로커-파티션 친화성 없음: 모든 브로커가 모든 파티션을 서비스 가능
// - 리밸런싱 비용 없음: 할당 변경이 데이터 이동을 유발하지 않음
// - 단순 할당자: Sticky/Cooperative 알고리즘이 이점을 제공하지 않음
//
// 타입 정의는 kip848_types.v, 할당자 구현은 kip848_assignors.v를 참조하세요.

module group

import service.port
import time
import rand

// KIP-848 그룹 코디네이터

/// KIP848GroupCoordinator는 새 프로토콜을 사용하는 컨슈머 그룹을 관리합니다.
/// 서버 측 파티션 할당과 점진적 리밸런싱을 지원합니다.
pub struct KIP848GroupCoordinator {
	assignors        map[string]ServerAssignor
	default_assignor string
mut:
	storage               port.StoragePort
	groups                map[string]&KIP848ConsumerGroup
	heartbeat_interval_ms i32
	session_timeout_ms    i32
	rebalance_timeout_ms  i32
}

/// new_kip848_coordinator는 새로운 KIP-848 그룹 코디네이터를 생성합니다.
pub fn new_kip848_coordinator(storage port.StoragePort) &KIP848GroupCoordinator {
	mut assignors := map[string]ServerAssignor{}
	assignors['range'] = new_range_assignor()
	assignors['roundrobin'] = new_round_robin_assignor()
	assignors['sticky'] = new_sticky_assignor()
	assignors['cooperative-sticky'] = new_cooperative_sticky_assignor()
	assignors['uniform'] = new_uniform_assignor()

	return &KIP848GroupCoordinator{
		storage:               storage
		assignors:             assignors
		default_assignor:      'sticky'
		groups:                map[string]&KIP848ConsumerGroup{}
		heartbeat_interval_ms: 3000
		session_timeout_ms:    45000
		rebalance_timeout_ms:  300000
	}
}

/// HeartbeatResult는 하트비트 작업의 결과를 나타냅니다.
pub struct HeartbeatResult {
pub:
	error_code            i16
	error_message         ?string
	member_id             string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?[]TopicPartition
}

/// process_heartbeat는 ConsumerGroupHeartbeat 요청을 처리합니다.
/// 새 멤버 참가, 탈퇴, 일반 하트비트를 처리합니다.
pub fn (mut c KIP848GroupCoordinator) process_heartbeat(group_id string,
	member_id string,
	member_epoch i32,
	instance_id ?string,
	rack_id ?string,
	rebalance_timeout_ms i32,
	subscribed_topic_names []string,
	server_assignor ?string) !HeartbeatResult {
	now := time.now().unix_milli()

	// group_id 유효성 검사
	if group_id == '' {
		return HeartbeatResult{
			error_code:    24
			error_message: 'Group ID cannot be empty'
			member_epoch:  -1
		}
	}

	// 그룹 가져오기 또는 생성
	mut group := c.get_or_create_group(group_id)

	// member_epoch에 따라 처리
	if member_epoch == 0 && member_id == '' {
		// 새 멤버 참가
		return c.handle_join(mut group, instance_id, rack_id, rebalance_timeout_ms, subscribed_topic_names,
			server_assignor, now)
	} else if member_epoch == -1 {
		// 멤버 탈퇴
		return c.handle_leave(mut group, member_id, now)
	} else if member_epoch > 0 {
		// 일반 하트비트
		return c.handle_heartbeat(mut group, member_id, member_epoch, subscribed_topic_names,
			now)
	} else {
		// 유효하지 않은 에포크
		return HeartbeatResult{
			error_code:    25
			error_message: 'Invalid member epoch'
			member_epoch:  -1
		}
	}
}

/// get_or_create_group은 기존 그룹을 반환하거나 새로 생성합니다.
fn (mut c KIP848GroupCoordinator) get_or_create_group(group_id string) &KIP848ConsumerGroup {
	// 그룹이 존재하면 반환
	if group := c.groups[group_id] {
		return group
	}

	now := time.now().unix_milli()
	group := &KIP848ConsumerGroup{
		group_id:           group_id
		group_epoch:        0
		assignment_epoch:   0
		state:              .empty
		protocol_type:      'consumer'
		server_assignor:    c.default_assignor
		members:            map[string]&KIP848Member{}
		target_assignment:  map[string][]TopicPartition{}
		current_assignment: map[string][]TopicPartition{}
		subscribed_topics:  map[string]bool{}
		created_at:         now
		updated_at:         now
	}
	c.groups[group_id] = group
	return group
}

/// handle_join은 새 멤버 참가를 처리합니다.
fn (mut c KIP848GroupCoordinator) handle_join(mut group KIP848ConsumerGroup,
	instance_id ?string,
	rack_id ?string,
	rebalance_timeout_ms i32,
	subscribed_topic_names []string,
	server_assignor ?string,
	now i64) !HeartbeatResult {
	// 새 member_id 생성
	member_id := 'consumer-${group.group_id}-${rand.i64()}'

	// 정적 멤버 (instance_id) 확인
	if inst_id := instance_id {
		// 정적 멤버 - 동일한 instance_id를 가진 기존 멤버 확인
		for member_key, _ in group.members {
			if mut existing_member := group.members[member_key] {
				if m_inst := existing_member.instance_id {
					if m_inst == inst_id {
						// 이전 멤버 펜스 및 재사용
						existing_member.state = .fenced
					}
				}
			}
		}
	}

	// 새 멤버 생성
	member := &KIP848Member{
		member_id:              member_id
		instance_id:            instance_id
		rack_id:                rack_id
		subscribed_topic_names: subscribed_topic_names
		server_assignor:        server_assignor
		member_epoch:           1
		previous_member_epoch:  0
		state:                  .subscribing
		assigned_partitions:    []TopicPartition{}
		pending_partitions:     []TopicPartition{}
		revoking_partitions:    []TopicPartition{}
		rebalance_timeout_ms:   rebalance_timeout_ms
		session_timeout_ms:     c.session_timeout_ms
		last_heartbeat:         now
		joined_at:              now
	}

	group.members[member_id] = member

	// 구독 토픽 업데이트
	for topic in subscribed_topic_names {
		group.subscribed_topics[topic] = true
	}

	// 지정된 경우 할당자 설정
	if assignor := server_assignor {
		if assignor in c.assignors {
			group.server_assignor = assignor
		}
	}

	// 리밸런싱 트리거
	group.group_epoch++
	group.state = .assigning
	group.updated_at = now

	// 새 할당 계산
	c.compute_assignment(mut group) or {
		return HeartbeatResult{
			error_code:    -1
			error_message: 'Failed to compute assignment: ${err.msg()}'
			member_id:     member_id
			member_epoch:  -1
		}
	}

	// 이 멤버의 할당 가져오기
	assignment := group.target_assignment[member_id] or { []TopicPartition{} }

	return HeartbeatResult{
		error_code:            0
		member_id:             member_id
		member_epoch:          1
		heartbeat_interval_ms: c.heartbeat_interval_ms
		assignment:            assignment
	}
}

/// handle_leave는 멤버 탈퇴를 처리합니다.
fn (mut c KIP848GroupCoordinator) handle_leave(mut group KIP848ConsumerGroup,
	member_id string,
	now i64) !HeartbeatResult {
	// 멤버 제거
	if member_id in group.members {
		group.members.delete(member_id)
		group.target_assignment.delete(member_id)
		group.current_assignment.delete(member_id)
	}

	// 그룹 상태 업데이트
	if group.members.len == 0 {
		group.state = .empty
	} else {
		// 리밸런싱 트리거
		group.group_epoch++
		group.state = .assigning
		c.compute_assignment(mut group)!
	}

	group.updated_at = now

	return HeartbeatResult{
		error_code:            0
		member_id:             member_id
		member_epoch:          -1
		heartbeat_interval_ms: 0
	}
}

/// handle_heartbeat는 일반 하트비트를 처리합니다.
fn (mut c KIP848GroupCoordinator) handle_heartbeat(mut group KIP848ConsumerGroup,
	member_id string,
	member_epoch i32,
	subscribed_topic_names []string,
	now i64) !HeartbeatResult {
	// 멤버 찾기
	mut member := group.members[member_id] or {
		return HeartbeatResult{
			error_code:    25
			error_message: 'Unknown member ID'
			member_epoch:  -1
		}
	}

	// 에포크 확인
	if member_epoch < member.member_epoch {
		return HeartbeatResult{
			error_code:    82
			error_message: 'Member epoch is stale'
			member_id:     member_id
			member_epoch:  member.member_epoch
		}
	}

	// 마지막 하트비트 업데이트
	member.last_heartbeat = now

	// 구독 변경 확인
	mut subscription_changed := false
	if subscribed_topic_names.len != member.subscribed_topic_names.len {
		subscription_changed = true
	} else {
		for topic in subscribed_topic_names {
			if topic !in member.subscribed_topic_names {
				subscription_changed = true
				break
			}
		}
	}

	if subscription_changed {
		member.subscribed_topic_names = subscribed_topic_names.clone()
		member.member_epoch++

		// 그룹 구독 토픽 업데이트
		group.subscribed_topics.clear()
		for _, m in group.members {
			for topic in m.subscribed_topic_names {
				group.subscribed_topics[topic] = true
			}
		}

		// 리밸런싱 트리거
		group.group_epoch++
		group.state = .assigning
		c.compute_assignment(mut group)!
	}

	// 현재 할당 가져오기
	assignment := group.target_assignment[member_id] or { []TopicPartition{} }

	// 할당 변경 확인
	if assignment.len != member.assigned_partitions.len {
		member.state = .reconciling
	} else {
		member.state = .stable
	}

	return HeartbeatResult{
		error_code:            0
		member_id:             member_id
		member_epoch:          member.member_epoch
		heartbeat_interval_ms: c.heartbeat_interval_ms
		assignment:            assignment
	}
}

/// compute_assignment는 모든 멤버에 대한 새 파티션 할당을 계산합니다.
fn (mut c KIP848GroupCoordinator) compute_assignment(mut group KIP848ConsumerGroup) ! {
	if group.members.len == 0 {
		group.target_assignment.clear()
		group.assignment_epoch = group.group_epoch
		group.state = .empty
		return
	}

	// 멤버 구독 구성
	mut subscriptions := []MemberSubscription{}
	for member_id, member in group.members {
		subscriptions << MemberSubscription{
			member_id:        member_id
			instance_id:      member.instance_id
			rack_id:          member.rack_id
			topics:           member.subscribed_topic_names
			owned_partitions: member.assigned_partitions
		}
	}

	// 토픽 메타데이터 구성
	mut topics := map[string]TopicMetadata{}
	for topic_name, _ in group.subscribed_topics {
		topic_meta := c.storage.get_topic(topic_name) or { continue }
		topics[topic_name] = TopicMetadata{
			topic_id:        topic_meta.topic_id
			topic_name:      topic_name
			partition_count: topic_meta.partition_count
		}
	}

	// 할당자 가져오기
	assignor := c.assignors[group.server_assignor] or {
		c.assignors[c.default_assignor] or { return error('No assignor available') }
	}

	// 할당 계산
	assignment := assignor.assign(subscriptions, topics)!

	// 대상 할당 업데이트
	group.target_assignment = assignment.clone()
	group.assignment_epoch = group.group_epoch

	// 멤버 할당 업데이트
	for member_id, partitions in assignment {
		if mut member := group.members[member_id] {
			member.pending_partitions = partitions
		}
	}

	// 모든 멤버가 안정 상태인지 확인
	mut all_stable := true
	for _, member in group.members {
		if member.state != .stable {
			all_stable = false
			break
		}
	}

	if all_stable {
		group.state = .stable
		group.current_assignment = group.target_assignment.clone()
	} else {
		group.state = .reconciling
	}
}

/// get_group은 ID로 그룹을 반환합니다.
pub fn (c &KIP848GroupCoordinator) get_group(group_id string) ?&KIP848ConsumerGroup {
	return c.groups[group_id] or { return none }
}

/// list_groups는 모든 그룹을 반환합니다.
pub fn (c &KIP848GroupCoordinator) list_groups() []&KIP848ConsumerGroup {
	mut result := []&KIP848ConsumerGroup{}
	for _, g in c.groups {
		result << g
	}
	return result
}

/// expire_members는 하트비트를 보내지 않은 멤버를 제거합니다.
pub fn (mut c KIP848GroupCoordinator) expire_members() {
	now := time.now().unix_milli()

	for group_id, mut group in c.groups {
		mut expired := []string{}

		for member_id, member in group.members {
			if now - member.last_heartbeat > member.session_timeout_ms {
				expired << member_id
			}
		}

		for member_id in expired {
			group.members.delete(member_id)
			group.target_assignment.delete(member_id)
			group.current_assignment.delete(member_id)
		}

		if expired.len > 0 {
			if group.members.len == 0 {
				group.state = .empty
			} else {
				group.group_epoch++
				group.state = .assigning
				c.compute_assignment(mut group) or {}
			}
		}

		// 타임아웃 후 빈 그룹 제거
		if group.state == .empty && now - group.updated_at > 300000 {
			c.groups.delete(group_id)
		}
	}
}
