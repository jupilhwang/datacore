// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat의
// 요청/응답 구조체 정의
//
// 컨슈머 그룹 조인, 동기화, 하트비트, 탈퇴 등의 작업에 사용됩니다.
module kafka

// JoinGroup 요청/응답 타입 (API Key 11) - 그룹 조인

/// JoinGroup 요청 - 컨슈머가 그룹에 참여하기 위한 요청
///
/// 컨슈머가 그룹에 처음 참여하거나 리밸런싱 시 사용됩니다.
/// 리더로 선출되면 파티션 할당을 담당합니다.
pub struct JoinGroupRequest {
pub:
	group_id             string
	session_timeout_ms   i32
	rebalance_timeout_ms i32
	member_id            string
	group_instance_id    ?string
	protocol_type        string
	protocols            []JoinGroupRequestProtocol
}

/// JoinGroup 요청 프로토콜 - 컨슈머가 지원하는 할당 프로토콜
pub struct JoinGroupRequestProtocol {
pub:
	name     string
	metadata []u8
}

/// JoinGroup 응답 - 그룹 조인 결과
pub struct JoinGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	generation_id    i32
	protocol_type    ?string
	protocol_name    ?string
	leader           string
	skip_assignment  bool
	member_id        string
	members          []JoinGroupResponseMember
}

/// JoinGroup 응답 멤버 - 그룹 멤버 정보
pub struct JoinGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	metadata          []u8
}

// SyncGroup 요청/응답 타입 (API Key 14) - 그룹 동기화

/// SyncGroup 요청 - 파티션 할당 동기화 요청
///
/// 리더가 파티션 할당을 완료한 후 모든 멤버에게 할당을 전파합니다.
/// 리더만 assignments를 포함하고, 팔로워는 빈 배열을 전송합니다.
pub struct SyncGroupRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
	protocol_type     ?string
	protocol_name     ?string
	assignments       []SyncGroupRequestAssignment
}

/// SyncGroup 요청 할당 - 멤버별 파티션 할당
pub struct SyncGroupRequestAssignment {
pub:
	member_id  string
	assignment []u8
}

/// SyncGroup 응답 - 파티션 할당 결과
pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	protocol_type    ?string
	protocol_name    ?string
	assignment       []u8
}

// Heartbeat 요청/응답 타입 (API Key 12) - 하트비트

/// Heartbeat 요청 - 그룹 멤버십 유지를 위한 하트비트
///
/// 컨슈머가 주기적으로 전송하여 그룹 멤버십을 유지합니다.
/// 세션 타임아웃 내에 하트비트가 없으면 멤버가 제거됩니다.
pub struct HeartbeatRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
}

/// Heartbeat 응답 - 하트비트 결과
pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

// LeaveGroup 요청/응답 타입 (API Key 13) - 그룹 탈퇴

/// LeaveGroup 요청 - 그룹에서 탈퇴하기 위한 요청
///
/// 컨슈머가 정상적으로 종료될 때 그룹에서 탈퇴합니다.
/// v3+에서는 여러 멤버를 한 번에 탈퇴시킬 수 있습니다.
pub struct LeaveGroupRequest {
pub:
	group_id  string
	member_id string
	members   []LeaveGroupMember
}

/// LeaveGroup 멤버 - v3+ 배치 탈퇴용 멤버 정보
pub struct LeaveGroupMember {
pub:
	member_id         string
	group_instance_id ?string
	reason            ?string
}

/// LeaveGroup 응답 - 그룹 탈퇴 결과
pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	members          []LeaveGroupResponseMember
}

/// LeaveGroup 응답 멤버 - 멤버별 탈퇴 결과
pub struct LeaveGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	error_code        i16
}

// ConsumerGroupHeartbeat 요청/응답 타입 (API Key 68) - KIP-848

// 새로운 컨슈머 리밸런스 프로토콜에서 사용됩니다.

/// ConsumerGroupHeartbeat 요청 - 새로운 컨슈머 프로토콜 하트비트 (KIP-848)
///
/// 기존 JoinGroup/SyncGroup/Heartbeat를 대체하는 단일 API입니다.
/// 서버 측 할당(server-side assignment)을 지원합니다.
pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	instance_id            ?string
	rack_id                ?string
	rebalance_timeout_ms   i32
	subscribed_topic_names []string
	server_assignor        ?string
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition
}

/// ConsumerGroupHeartbeat 토픽 파티션 - 현재 할당된 파티션
pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8 // 토픽 UUID (16바이트)
	partitions []i32
}

/// ConsumerGroupHeartbeat 응답 - 새로운 컨슈머 프로토콜 하트비트 응답
pub struct ConsumerGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         ?string
	member_id             ?string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ConsumerGroupHeartbeatAssignment
}

/// ConsumerGroupHeartbeat 할당 - 파티션 할당 정보
pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition
}

/// ConsumerGroupHeartbeat 응답 토픽 파티션 - 할당된 파티션
pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8 // 토픽 UUID (16바이트)
	partitions []i32
}

// ConsumerGroupDescribe 요청/응답 타입 (API Key 69) - KIP-848

// 컨슈머 그룹의 상세 정보를 조회합니다.

/// ConsumerGroupDescribe 요청 - 컨슈머 그룹 상세 정보 조회 요청 (KIP-848)
pub struct ConsumerGroupDescribeRequest {
pub:
	group_ids                     []string
	include_authorized_operations bool
}

/// ConsumerGroupDescribe 응답 - 컨슈머 그룹 상세 정보
pub struct ConsumerGroupDescribeResponse {
pub:
	throttle_time_ms i32
	groups           []ConsumerGroupDescribeResponseGroup
}

/// ConsumerGroupDescribe 응답 그룹 - 개별 그룹 정보
pub struct ConsumerGroupDescribeResponseGroup {
pub:
	error_code            i16
	error_message         ?string
	group_id              string
	group_state           string
	group_epoch           i32
	assignment_epoch      i32
	assignor_name         string
	members               []ConsumerGroupDescribeResponseMember
	authorized_operations i32
}

/// ConsumerGroupDescribe 응답 멤버 - 멤버 정보
pub struct ConsumerGroupDescribeResponseMember {
pub:
	member_id            string
	instance_id          ?string
	rack_id              ?string
	member_epoch         i32
	client_id            string
	client_host          string
	subscribed_topic_ids [][]u8 // 구독 토픽 ID 목록 (UUID)
	assignment           ?ConsumerGroupDescribeResponseMemberAssignment
}

/// ConsumerGroupDescribe 응답 멤버 할당 - 멤버의 파티션 할당
pub struct ConsumerGroupDescribeResponseMemberAssignment {
pub:
	topic_partitions []ConsumerGroupDescribeTopicPartition
}

/// ConsumerGroupDescribe 토픽 파티션 - 할당된 파티션
pub struct ConsumerGroupDescribeTopicPartition {
pub:
	topic_id   []u8 // 토픽 UUID (16바이트)
	partitions []i32
}
