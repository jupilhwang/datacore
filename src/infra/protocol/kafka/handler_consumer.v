// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat 요청 처리
//
// 이 모듈은 Kafka Consumer Group 관련 API들을 구현합니다.
// 컨슈머 그룹의 조인, 동기화, 하트비트, 탈퇴 등의 기능을 제공하며,
// KIP-848 기반의 새로운 ConsumerGroupHeartbeat API도 지원합니다.
module kafka

import domain
import infra.observability
import rand
import time

// 핸들러 함수 - 바이트 배열 기반 요청 처리 (레거시)

/// JoinGroup 핸들러 - 컨슈머 그룹 조인 요청을 처리합니다.
/// 새로운 멤버가 그룹에 참여하거나 기존 멤버가 재조인할 때 사용됩니다.
fn (mut h Handler) handle_join_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_join_group_request(mut reader, version, is_flexible_version(.join_group,
		version))!
	resp := h.process_join_group(req, version)!
	return resp.encode(version)
}

/// SyncGroup 핸들러 - 그룹 동기화 요청을 처리합니다.
/// 리더가 파티션 할당을 배포하고 멤버들이 할당을 받을 때 사용됩니다.
fn (mut h Handler) handle_sync_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_sync_group_request(mut reader, version, is_flexible_version(.sync_group,
		version))!
	resp := h.process_sync_group(req, version)!
	return resp.encode(version)
}

/// Heartbeat 핸들러 - 컨슈머 하트비트 요청을 처리합니다.
/// 멤버가 그룹에 여전히 활성 상태임을 알리고 리밸런스 상태를 확인합니다.
fn (mut h Handler) handle_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_heartbeat_request(mut reader, version, is_flexible_version(.heartbeat,
		version))!
	resp := h.process_heartbeat(req, version)!
	return resp.encode(version)
}

/// LeaveGroup 핸들러 - 그룹 탈퇴 요청을 처리합니다.
/// 멤버가 그룹을 떠날 때 사용되며, 리밸런스를 트리거합니다.
fn (mut h Handler) handle_leave_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_leave_group_request(mut reader, version, is_flexible_version(.leave_group,
		version))!
	resp := h.process_leave_group(req, version)!
	return resp.encode(version)
}

/// ConsumerGroupHeartbeat 핸들러 (KIP-848) - 새로운 컨슈머 그룹 프로토콜의 하트비트를 처리합니다.
/// 기존 JoinGroup/SyncGroup/Heartbeat를 단일 API로 통합한 새로운 프로토콜입니다.
fn (mut h Handler) handle_consumer_group_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_consumer_group_heartbeat_request(mut reader, version, true)!

	resp := h.process_consumer_group_heartbeat(req, version)!
	return resp.encode(version)
}

/// ConsumerGroupDescribe 핸들러 (KIP-848) - 컨슈머 그룹 상세 정보를 조회합니다.
fn (mut h Handler) handle_consumer_group_describe(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_consumer_group_describe_request(mut reader, version, true)!

	resp := h.process_consumer_group_describe(req, version)!
	return resp.encode(version)
}

// 프로세스 함수 - 비즈니스 로직 처리

/// ConsumerGroupHeartbeat 요청을 처리합니다 (KIP-848).
/// 새로운 멤버 등록, 기존 멤버 하트비트, 멤버 탈퇴를 모두 처리합니다.
fn (mut h Handler) process_consumer_group_heartbeat(req ConsumerGroupHeartbeatRequest, version i16) !ConsumerGroupHeartbeatResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing consumer group heartbeat', observability.field_string('group_id',
		req.group_id), observability.field_string('member_id', req.member_id), observability.field_int('member_epoch',
		req.member_epoch))

	mut error_code := i16(0)
	mut error_message := ?string(none)
	mut member_id := req.member_id
	mut member_epoch := req.member_epoch
	mut heartbeat_interval_ms := i32(3000)
	mut assignment := ?ConsumerGroupHeartbeatAssignment(none)

	// 그룹 ID 유효성 검사
	if req.group_id.len == 0 {
		h.logger.warn('Consumer group heartbeat failed: empty group ID')
		return ConsumerGroupHeartbeatResponse{
			throttle_time_ms:      0
			error_code:            i16(ErrorCode.invalid_group_id)
			error_message:         'Group ID cannot be empty'
			member_id:             none
			member_epoch:          -1
			heartbeat_interval_ms: 0
			assignment:            none
		}
	}

	// 새로운 멤버 등록 (epoch=0, member_id 없음)
	if req.member_epoch == 0 && req.member_id.len == 0 {
		// 새로운 멤버 ID 생성
		member_id = 'member-${h.broker_id}-${rand.i64()}'
		member_epoch = 1

		h.logger.info('New consumer group member joined', observability.field_string('group_id',
			req.group_id), observability.field_string('member_id', member_id))

		// 구독 토픽에 대한 파티션 할당 생성
		mut topic_partitions := []ConsumerGroupHeartbeatResponseTopicPartition{}

		for topic_name in req.subscribed_topic_names {
			topic_meta := h.storage.get_topic(topic_name) or { continue }
			mut partitions := []i32{}
			for p in 0 .. topic_meta.partition_count {
				partitions << i32(p)
			}

			topic_partitions << ConsumerGroupHeartbeatResponseTopicPartition{
				topic_id:   topic_meta.topic_id.clone()
				partitions: partitions
			}
		}

		if topic_partitions.len > 0 {
			assignment = ConsumerGroupHeartbeatAssignment{
				topic_partitions: topic_partitions
			}
		}
	} else if req.member_epoch == -1 {
		// 멤버 탈퇴 요청 (epoch=-1)
		h.logger.info('Consumer leaving group', observability.field_string('group_id',
			req.group_id), observability.field_string('member_id', req.member_id))
		member_epoch = -1
		assignment = none
	} else if req.member_epoch > 0 {
		// 일반 하트비트 - 상태 변경 없음
		h.logger.trace('Consumer heartbeat', observability.field_string('group_id', req.group_id),
			observability.field_string('member_id', req.member_id), observability.field_int('epoch',
			req.member_epoch))
	} else {
		// 잘못된 epoch 값
		h.logger.warn('Invalid consumer heartbeat', observability.field_string('group_id',
			req.group_id), observability.field_string('member_id', req.member_id), observability.field_int('epoch',
			req.member_epoch))
		error_code = i16(ErrorCode.unknown_member_id)
		error_message = 'Invalid member epoch'
		member_epoch = -1
	}

	elapsed := time.since(start_time)
	h.logger.debug('Consumer group heartbeat completed', observability.field_string('group_id',
		req.group_id), observability.field_int('error_code', error_code), observability.field_duration('latency',
		elapsed))

	return ConsumerGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            error_code
		error_message:         error_message
		member_id:             if member_id.len > 0 { member_id } else { none }
		member_epoch:          member_epoch
		heartbeat_interval_ms: heartbeat_interval_ms
		assignment:            assignment
	}
}

/// JoinGroup 요청을 처리합니다.
/// 컨슈머가 그룹에 조인하고 리밸런스에 참여합니다.
/// 첫 번째 조인 시 member_id가 생성되어 반환됩니다.
fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing join group request', observability.field_string('group_id',
		req.group_id), observability.field_string('member_id', req.member_id), observability.field_string('protocol_type',
		req.protocol_type), observability.field_int('protocols', req.protocols.len))

	// 첫 번째 조인 시 member_id 생성 필요 (v4+에서 MEMBER_ID_REQUIRED 에러 반환)
	if req.member_id.len == 0 && req.group_instance_id == none {
		new_member_id := 'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
		h.logger.info('Member ID required, generated new ID', observability.field_string('group_id',
			req.group_id), observability.field_string('new_member_id', new_member_id))
		return JoinGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.member_id_required)
			generation_id:    -1
			protocol_type:    req.protocol_type
			protocol_name:    ''
			leader:           ''
			skip_assignment:  false
			member_id:        new_member_id
			members:          []
		}
	}

	// 멤버 ID 결정 (기존 또는 새로 생성)
	member_id := if req.member_id.len > 0 {
		req.member_id
	} else {
		'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
	}

	// 선택된 프로토콜 이름 (첫 번째 프로토콜 사용)
	protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }

	// 그룹 로드 또는 새로 생성
	mut group := h.storage.load_group(req.group_id) or {
		domain.ConsumerGroup{
			group_id:      req.group_id
			generation_id: 0
			protocol_type: req.protocol_type
			protocol:      protocol_name
			state:         .preparing_rebalance
			members:       []
			leader:        ''
		}
	}

	// 멤버 정보 생성/업데이트
	member := domain.GroupMember{
		member_id:         member_id
		group_instance_id: req.group_instance_id or { '' }
		client_id:         ''
		client_host:       ''
		metadata:          if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
		assignment:        []u8{}
	}

	// 멤버 목록 업데이트 (기존 멤버 갱신 또는 새 멤버 추가)
	mut found := false
	mut new_members := []domain.GroupMember{}
	for m in group.members {
		if m.member_id == member_id {
			new_members << member
			found = true
		} else {
			new_members << m
		}
	}
	if !found {
		new_members << member
	}

	// 새로운 세대(generation) 번호와 리더 결정
	new_gen := group.generation_id + 1
	leader := if new_members.len > 0 { new_members[0].member_id } else { member_id }

	// 업데이트된 그룹 정보 생성
	new_group := domain.ConsumerGroup{
		group_id:      req.group_id
		generation_id: new_gen
		protocol_type: req.protocol_type
		protocol:      protocol_name
		state:         .stable
		members:       new_members
		leader:        leader
	}

	// 그룹 상태 저장
	h.storage.save_group(new_group) or {
		h.logger.error('Failed to save group', observability.field_string('group_id',
			req.group_id), observability.field_string('error', err.str()))
		return error('failed to save group: ${err}')
	}

	elapsed := time.since(start_time)
	h.logger.info('Member joined group', observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', member_id), observability.field_int('generation',
		new_gen), observability.field_string('leader', leader), observability.field_int('members',
		new_members.len), observability.field_duration('latency', elapsed))

	// 리더에게만 멤버 목록 반환 (파티션 할당을 위해)
	response_members := if member_id == leader {
		new_members.map(fn (m domain.GroupMember) JoinGroupResponseMember {
			return JoinGroupResponseMember{
				member_id:         m.member_id
				group_instance_id: if m.group_instance_id.len > 0 {
					m.group_instance_id
				} else {
					none
				}
				metadata:          m.metadata
			}
		})
	} else {
		[]JoinGroupResponseMember{}
	}

	return JoinGroupResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		generation_id:    new_gen
		protocol_type:    req.protocol_type
		protocol_name:    protocol_name
		leader:           leader
		skip_assignment:  false
		member_id:        member_id
		members:          response_members
	}
}

/// SyncGroup 요청을 처리합니다.
/// 리더가 파티션 할당을 제출하고, 모든 멤버가 자신의 할당을 받습니다.
fn (mut h Handler) process_sync_group(req SyncGroupRequest, version i16) !SyncGroupResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing sync group request', observability.field_string('group_id',
		req.group_id), observability.field_string('member_id', req.member_id), observability.field_int('generation',
		req.generation_id), observability.field_int('assignments', req.assignments.len))

	// 그룹 로드
	mut group := h.storage.load_group(req.group_id) or {
		h.logger.warn('Sync group failed: group not found', observability.field_string('group_id',
			req.group_id))
		return SyncGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.group_id_not_found)
			protocol_type:    ''
			protocol_name:    ''
			assignment:       []u8{}
		}
	}

	// 세대 번호 검증
	if group.generation_id != req.generation_id {
		h.logger.warn('Sync group failed: illegal generation', observability.field_string('group_id',
			req.group_id), observability.field_int('expected', group.generation_id), observability.field_int('received',
			req.generation_id))
		return SyncGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.illegal_generation)
			protocol_type:    group.protocol_type
			protocol_name:    group.protocol
			assignment:       []u8{}
		}
	}

	// 리더가 할당을 제출한 경우 그룹 멤버 업데이트
	if req.assignments.len > 0 {
		mut updated_members := []domain.GroupMember{}
		for m in group.members {
			mut member := m
			for a in req.assignments {
				if a.member_id == m.member_id {
					member = domain.GroupMember{
						member_id:         m.member_id
						group_instance_id: m.group_instance_id
						client_id:         m.client_id
						client_host:       m.client_host
						metadata:          m.metadata
						assignment:        a.assignment.clone()
					}
					break
				}
			}
			updated_members << member
		}
		group = domain.ConsumerGroup{
			group_id:      group.group_id
			generation_id: group.generation_id
			protocol_type: group.protocol_type
			protocol:      group.protocol
			state:         .stable
			members:       updated_members
			leader:        group.leader
		}
		h.storage.save_group(group) or {
			h.logger.error('Failed to save group assignments', observability.field_string('group_id',
				req.group_id), observability.field_string('error', err.str()))
			return error('failed to save group: ${err}')
		}
	}

	// 이 멤버의 할당 찾기
	mut assignment := []u8{}
	for m in group.members {
		if m.member_id == req.member_id {
			assignment = m.assignment.clone()
			break
		}
	}

	// 할당이 없으면 빈 컨슈머 할당 사용
	if assignment.len == 0 {
		assignment = empty_consumer_assignment()
	}

	elapsed := time.since(start_time)
	h.logger.debug('Sync group completed', observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id), observability.field_bytes('assignment_size',
		assignment.len), observability.field_duration('latency', elapsed))

	return SyncGroupResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		protocol_type:    group.protocol_type
		protocol_name:    group.protocol
		assignment:       assignment
	}
}

/// Heartbeat 요청을 처리합니다.
/// 멤버가 그룹에 활성 상태임을 알리고 리밸런스 상태를 확인합니다.
fn (mut h Handler) process_heartbeat(req HeartbeatRequest, version i16) !HeartbeatResponse {
	_ = version
	h.logger.trace('Processing heartbeat', observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id), observability.field_int('generation',
		req.generation_id))

	// 그룹 존재 여부 및 세대 번호 검증
	group := h.storage.load_group(req.group_id) or {
		h.logger.debug('Heartbeat failed: group not found', observability.field_string('group_id',
			req.group_id))
		return HeartbeatResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.group_id_not_found)
		}
	}

	// 세대 번호 검증
	if group.generation_id != req.generation_id {
		h.logger.debug('Heartbeat failed: illegal generation', observability.field_string('group_id',
			req.group_id), observability.field_int('expected', group.generation_id), observability.field_int('received',
			req.generation_id))
		return HeartbeatResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.illegal_generation)
		}
	}

	// 멤버가 그룹에 존재하는지 확인
	mut member_found := false
	for m in group.members {
		if m.member_id == req.member_id {
			member_found = true
			break
		}
	}

	if !member_found {
		h.logger.debug('Heartbeat failed: unknown member', observability.field_string('group_id',
			req.group_id), observability.field_string('member_id', req.member_id))
		return HeartbeatResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.unknown_member_id)
		}
	}

	return HeartbeatResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
	}
}

/// LeaveGroup 요청을 처리합니다.
/// 멤버가 그룹을 떠나면 리밸런스가 트리거됩니다.
fn (mut h Handler) process_leave_group(req LeaveGroupRequest, version i16) !LeaveGroupResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing leave group request', observability.field_string('group_id',
		req.group_id), observability.field_string('member_id', req.member_id), observability.field_int('members',
		req.members.len))

	// 그룹 로드
	mut group := h.storage.load_group(req.group_id) or {
		h.logger.warn('Leave group failed: group not found', observability.field_string('group_id',
			req.group_id))
		return LeaveGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.group_id_not_found)
			members:          []LeaveGroupResponseMember{}
		}
	}

	// 그룹에서 멤버 제거
	mut removed_members := []LeaveGroupResponseMember{}
	mut remaining_members := []domain.GroupMember{}

	for m in group.members {
		mut should_remove := false

		// 제거할 멤버인지 확인 (v3+에서는 members 배열 사용)
		if req.members.len > 0 {
			for leave_member in req.members {
				if m.member_id == leave_member.member_id {
					should_remove = true
					removed_members << LeaveGroupResponseMember{
						member_id:         m.member_id
						group_instance_id: if m.group_instance_id.len > 0 {
							?string(m.group_instance_id)
						} else {
							none
						}
						error_code:        0
					}
					break
				}
			}
		} else if m.member_id == req.member_id {
			// v0-v2: 단일 member_id 필드 사용
			should_remove = true
			removed_members << LeaveGroupResponseMember{
				member_id:         m.member_id
				group_instance_id: none
				error_code:        0
			}
		}

		if !should_remove {
			remaining_members << m
		}
	}

	// 제거된 멤버가 없으면 unknown_member_id 에러 반환
	if removed_members.len == 0 {
		h.logger.warn('Leave group failed: no members removed', observability.field_string('group_id',
			req.group_id))
		return LeaveGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.unknown_member_id)
			members:          []LeaveGroupResponseMember{}
		}
	}

	// 그룹 상태 업데이트 (멤버가 없으면 empty, 있으면 리밸런스 준비)
	new_state := if remaining_members.len == 0 {
		domain.GroupState.empty
	} else {
		domain.GroupState.preparing_rebalance
	}

	// 새로운 리더 결정 (남은 멤버 중 첫 번째)
	new_leader := if remaining_members.len > 0 {
		remaining_members[0].member_id
	} else {
		''
	}

	// 업데이트된 그룹 정보 생성 및 저장
	new_group := domain.ConsumerGroup{
		group_id:      group.group_id
		generation_id: group.generation_id + 1
		protocol_type: group.protocol_type
		protocol:      group.protocol
		state:         new_state
		members:       remaining_members
		leader:        new_leader
	}

	h.storage.save_group(new_group) or {
		h.logger.error('Leave group failed: failed to save group', observability.field_string('group_id',
			req.group_id), observability.field_string('error', err.str()))
		return error('failed to save group: ${err}')
	}

	elapsed := time.since(start_time)
	h.logger.info('Members left group', observability.field_string('group_id', req.group_id),
		observability.field_int('removed', removed_members.len), observability.field_int('remaining',
		remaining_members.len), observability.field_duration('latency', elapsed))

	return LeaveGroupResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		members:          removed_members
	}
}

/// ConsumerGroupDescribe 요청을 처리합니다 (KIP-848).
/// 컨슈머 그룹의 상세 정보를 조회합니다.
fn (mut h Handler) process_consumer_group_describe(req ConsumerGroupDescribeRequest, version i16) !ConsumerGroupDescribeResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing consumer group describe', observability.field_int('group_ids',
		req.group_ids.len))

	mut groups := []ConsumerGroupDescribeResponseGroup{}

	for group_id in req.group_ids {
		// 그룹 로드 시도
		group := h.storage.load_group(group_id) or {
			// 그룹을 찾을 수 없음
			h.logger.trace('Group not found', observability.field_string('group_id', group_id))
			groups << ConsumerGroupDescribeResponseGroup{
				error_code:            i16(ErrorCode.group_id_not_found)
				error_message:         'Group not found: ${group_id}'
				group_id:              group_id
				group_state:           ''
				group_epoch:           0
				assignment_epoch:      0
				assignor_name:         ''
				members:               []
				authorized_operations: 0
			}
			continue
		}

		// 그룹 상태 문자열 변환
		state_str := match group.state {
			.empty { 'Empty' }
			.stable { 'Stable' }
			.preparing_rebalance { 'PreparingRebalance' }
			.completing_rebalance { 'CompletingRebalance' }
			.dead { 'Dead' }
		}

		// 멤버 목록 생성
		mut response_members := []ConsumerGroupDescribeResponseMember{}
		for m in group.members {
			response_members << ConsumerGroupDescribeResponseMember{
				member_id:            m.member_id
				instance_id:          if m.group_instance_id.len > 0 {
					m.group_instance_id
				} else {
					none
				}
				rack_id:              none
				member_epoch:         group.generation_id
				client_id:            m.client_id
				client_host:          m.client_host
				subscribed_topic_ids: []
				assignment:           none
			}
		}

		h.logger.trace('Describing consumer group', observability.field_string('group_id',
			group_id), observability.field_string('state', state_str), observability.field_int('members',
			response_members.len))

		groups << ConsumerGroupDescribeResponseGroup{
			error_code:            0
			error_message:         none
			group_id:              group_id
			group_state:           state_str
			group_epoch:           group.generation_id
			assignment_epoch:      group.generation_id
			assignor_name:         group.protocol
			members:               response_members
			authorized_operations: if req.include_authorized_operations { -2147483648 } else { 0 }
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Consumer group describe completed', observability.field_int('groups',
		groups.len), observability.field_duration('latency', elapsed))

	return ConsumerGroupDescribeResponse{
		throttle_time_ms: default_throttle_time_ms
		groups:           groups
	}
}
