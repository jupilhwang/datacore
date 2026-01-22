// 서비스 레이어 - 컨슈머 그룹 코디네이터 유스케이스
// 컨슈머 그룹 조정 비즈니스 로직을 처리합니다.
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup 등의 프로토콜을 구현합니다.
module group

import domain
import rand
import service.port

/// GroupCoordinator는 컨슈머 그룹 조정을 처리합니다.
/// 그룹 멤버십 관리, 리밸런싱, 오프셋 관리를 담당합니다.
pub struct GroupCoordinator {
mut:
	storage port.StoragePort
}

/// new_group_coordinator는 새로운 GroupCoordinator를 생성합니다.
pub fn new_group_coordinator(storage port.StoragePort) &GroupCoordinator {
	return &GroupCoordinator{
		storage: storage
	}
}

/// JoinGroupRequest는 그룹 참가 요청을 나타냅니다.
pub struct JoinGroupRequest {
pub:
	group_id             string     // 그룹 ID
	session_timeout_ms   i32        // 세션 타임아웃 (ms)
	rebalance_timeout_ms i32        // 리밸런싱 타임아웃 (ms)
	member_id            string     // 멤버 ID (빈 문자열이면 새로 생성)
	group_instance_id    string     // 정적 멤버십을 위한 인스턴스 ID
	protocol_type        string     // 프로토콜 타입 (예: "consumer")
	protocols            []Protocol // 지원하는 프로토콜 목록
}

/// Protocol은 그룹 프로토콜을 나타냅니다.
pub struct Protocol {
pub:
	name     string // 프로토콜 이름 (예: "range", "roundrobin")
	metadata []u8   // 프로토콜 메타데이터 (구독 정보 등)
}

/// JoinGroupResponse는 그룹 참가 응답을 나타냅니다.
pub struct JoinGroupResponse {
pub:
	error_code    i16                  // 오류 코드 (0이면 성공)
	generation_id int                  // 그룹 세대 ID
	protocol_type string               // 선택된 프로토콜 타입
	protocol_name string               // 선택된 프로토콜 이름
	leader        string               // 리더 멤버 ID
	member_id     string               // 할당된 멤버 ID
	members       []domain.GroupMember // 그룹 멤버 목록 (리더에게만 전달)
}

/// join_group은 그룹 참가 요청을 처리합니다.
/// 새 멤버를 그룹에 추가하고 리밸런싱을 트리거합니다.
pub fn (mut c GroupCoordinator) join_group(req JoinGroupRequest) JoinGroupResponse {
	// 그룹 ID 유효성 검사
	if req.group_id.len == 0 {
		return JoinGroupResponse{
			error_code: i16(domain.ErrorCode.invalid_group_id)
		}
	}

	// 멤버 ID가 없으면 새로 생성
	member_id := if req.member_id.len > 0 {
		req.member_id
	} else {
		'member-${req.group_id}-${generate_id()}'
	}

	// 기존 그룹 로드 또는 새 그룹 생성
	mut group := c.storage.load_group(req.group_id) or {
		// 새 그룹 생성
		domain.ConsumerGroup{
			group_id:      req.group_id
			generation_id: 0
			protocol_type: req.protocol_type
			state:         .preparing_rebalance
			members:       []
		}
	}

	// 멤버 정보 생성
	mut member := domain.GroupMember{
		member_id:         member_id
		group_instance_id: req.group_instance_id
		client_id:         ''
		client_host:       ''
		metadata:          if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
	}

	// 기존 멤버 인덱스 찾기 (가능하면 clone 방지)
	mut member_idx := -1
	for i, m in group.members {
		if m.member_id == member_id {
			member_idx = i
			break
		}
	}

	// 멤버 목록 효율적으로 업데이트
	mut updated_members := if member_idx >= 0 {
		// 기존 멤버 업데이트
		mut members := group.members.clone()
		members[member_idx] = member
		members
	} else {
		// 새 멤버 추가 (단일 할당)
		mut members := []domain.GroupMember{cap: group.members.len + 1}
		members << group.members
		members << member
		members
	}

	group = domain.ConsumerGroup{
		...group
		members: updated_members
	}

	// 세대 증가 및 리더 설정
	new_gen := group.generation_id + 1
	leader := if group.members.len > 0 { group.members[0].member_id } else { member_id }
	protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }

	new_group := domain.ConsumerGroup{
		...group
		generation_id: new_gen
		protocol:      protocol_name
		leader:        leader
		state:         .stable
	}

	// 그룹 저장
	c.storage.save_group(new_group) or {
		return JoinGroupResponse{
			error_code: i16(domain.ErrorCode.unknown_server_error)
		}
	}

	return JoinGroupResponse{
		error_code:    0
		generation_id: new_gen
		protocol_type: req.protocol_type
		protocol_name: protocol_name
		leader:        leader
		member_id:     member_id
		members:       if member_id == leader { new_group.members } else { [] }
	}
}

/// SyncGroupRequest는 그룹 동기화 요청을 나타냅니다.
pub struct SyncGroupRequest {
pub:
	group_id      string             // 그룹 ID
	generation_id int                // 그룹 세대 ID
	member_id     string             // 멤버 ID
	assignments   []MemberAssignment // 멤버별 파티션 할당 (리더가 제공)
}

/// MemberAssignment는 멤버 할당을 나타냅니다.
pub struct MemberAssignment {
pub:
	member_id  string // 멤버 ID
	assignment []u8   // 할당 데이터 (파티션 목록)
}

/// SyncGroupResponse는 그룹 동기화 응답을 나타냅니다.
pub struct SyncGroupResponse {
pub:
	error_code i16  // 오류 코드 (0이면 성공)
	assignment []u8 // 이 멤버에 대한 할당 데이터
}

/// sync_group은 그룹 동기화 요청을 처리합니다.
/// 리더가 제공한 할당을 각 멤버에게 배포합니다.
pub fn (mut c GroupCoordinator) sync_group(req SyncGroupRequest) SyncGroupResponse {
	group := c.storage.load_group(req.group_id) or {
		return SyncGroupResponse{
			error_code: i16(domain.ErrorCode.group_id_not_found)
		}
	}

	// 세대 확인
	if group.generation_id != req.generation_id {
		return SyncGroupResponse{
			error_code: i16(domain.ErrorCode.illegal_generation)
		}
	}

	// 이 멤버의 할당 찾기
	for a in req.assignments {
		if a.member_id == req.member_id {
			return SyncGroupResponse{
				error_code: 0
				assignment: a.assignment
			}
		}
	}

	// 리더가 아닌 경우, 저장된 상태에서 할당 찾기
	for m in group.members {
		if m.member_id == req.member_id {
			return SyncGroupResponse{
				error_code: 0
				assignment: m.assignment
			}
		}
	}

	return SyncGroupResponse{
		error_code: i16(domain.ErrorCode.unknown_member_id)
	}
}

/// HeartbeatRequest는 하트비트 요청을 나타냅니다.
pub struct HeartbeatRequest {
pub:
	group_id      string // 그룹 ID
	generation_id int    // 그룹 세대 ID
	member_id     string // 멤버 ID
}

/// HeartbeatResponse는 하트비트 응답을 나타냅니다.
pub struct HeartbeatResponse {
pub:
	error_code i16 // 오류 코드 (0이면 성공, REBALANCE_IN_PROGRESS면 리밸런싱 필요)
}

/// heartbeat는 하트비트 요청을 처리합니다.
/// 멤버의 활성 상태를 확인하고 리밸런싱 필요 여부를 알립니다.
pub fn (mut c GroupCoordinator) heartbeat(req HeartbeatRequest) HeartbeatResponse {
	group := c.storage.load_group(req.group_id) or {
		return HeartbeatResponse{
			error_code: i16(domain.ErrorCode.group_id_not_found)
		}
	}

	// 세대 확인
	if group.generation_id != req.generation_id {
		return HeartbeatResponse{
			error_code: i16(domain.ErrorCode.illegal_generation)
		}
	}

	// 멤버 존재 확인
	for m in group.members {
		if m.member_id == req.member_id {
			return HeartbeatResponse{
				error_code: 0
			}
		}
	}

	return HeartbeatResponse{
		error_code: i16(domain.ErrorCode.unknown_member_id)
	}
}

/// leave_group은 그룹 탈퇴 요청을 처리합니다.
/// 멤버를 그룹에서 제거하고 리밸런싱을 트리거합니다.
pub fn (mut c GroupCoordinator) leave_group(group_id string, member_id string) i16 {
	group := c.storage.load_group(group_id) or { return i16(domain.ErrorCode.group_id_not_found) }

	// 멤버 제거
	mut new_members := []domain.GroupMember{}
	for m in group.members {
		if m.member_id != member_id {
			new_members << m
		}
	}

	new_group := domain.ConsumerGroup{
		...group
		members: new_members
		state:   if new_members.len == 0 {
			domain.GroupState.empty
		} else {
			domain.GroupState.preparing_rebalance
		}
	}

	c.storage.save_group(new_group) or { return i16(domain.ErrorCode.unknown_server_error) }

	return 0
}

/// list_groups는 모든 컨슈머 그룹 목록을 반환합니다.
pub fn (mut c GroupCoordinator) list_groups() ![]domain.GroupInfo {
	return c.storage.list_groups()
}

/// describe_group은 컨슈머 그룹 상세 정보를 반환합니다.
pub fn (mut c GroupCoordinator) describe_group(group_id string) !domain.ConsumerGroup {
	return c.storage.load_group(group_id)
}

/// generate_id는 고유 ID를 생성합니다.
fn generate_id() string {
	return 'member-${rand.i64()}'
}
