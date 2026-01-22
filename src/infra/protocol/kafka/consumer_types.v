// 인프라 레이어 - Kafka 컨슈머 그룹 타입 정의
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat의
// 요청/응답 구조체 정의
//
// 이 모듈은 Kafka 컨슈머 그룹 프로토콜의 핵심 타입들을 정의합니다.
// 컨슈머 그룹 조인, 동기화, 하트비트, 탈퇴 등의 작업에 사용됩니다.
module kafka

// ============================================================================
// JoinGroup 요청/응답 타입 (API Key 11) - 그룹 조인
// ============================================================================

/// JoinGroup 요청 - 컨슈머가 그룹에 참여하기 위한 요청
///
/// 컨슈머가 그룹에 처음 참여하거나 리밸런싱 시 사용됩니다.
/// 리더로 선출되면 파티션 할당을 담당합니다.
pub struct JoinGroupRequest {
pub:
	group_id             string                     // 그룹 ID
	session_timeout_ms   i32                        // 세션 타임아웃 (밀리초)
	rebalance_timeout_ms i32                        // 리밸런싱 타임아웃 (밀리초)
	member_id            string                     // 멤버 ID (첫 조인 시 빈 문자열)
	group_instance_id    ?string                    // 정적 멤버십용 인스턴스 ID
	protocol_type        string                     // 프로토콜 타입 (예: "consumer")
	protocols            []JoinGroupRequestProtocol // 지원하는 프로토콜 목록
}

/// JoinGroup 요청 프로토콜 - 컨슈머가 지원하는 할당 프로토콜
pub struct JoinGroupRequestProtocol {
pub:
	name     string // 프로토콜 이름 (예: "range", "roundrobin")
	metadata []u8   // 프로토콜 메타데이터 (구독 토픽 등)
}

/// JoinGroup 응답 - 그룹 조인 결과
pub struct JoinGroupResponse {
pub:
	throttle_time_ms i32                       // 스로틀링 시간 (밀리초)
	error_code       i16                       // 에러 코드
	generation_id    i32                       // 그룹 세대 ID
	protocol_type    ?string                   // 선택된 프로토콜 타입
	protocol_name    ?string                   // 선택된 프로토콜 이름
	leader           string                    // 리더 멤버 ID
	skip_assignment  bool                      // 할당 건너뛰기 여부
	member_id        string                    // 할당된 멤버 ID
	members          []JoinGroupResponseMember // 그룹 멤버 목록 (리더에게만 전달)
}

/// JoinGroup 응답 멤버 - 그룹 멤버 정보
pub struct JoinGroupResponseMember {
pub:
	member_id         string  // 멤버 ID
	group_instance_id ?string // 정적 멤버십 인스턴스 ID (v5+)
	metadata          []u8    // 멤버 메타데이터
}

// ============================================================================
// SyncGroup 요청/응답 타입 (API Key 14) - 그룹 동기화
// ============================================================================

/// SyncGroup 요청 - 파티션 할당 동기화 요청
///
/// 리더가 파티션 할당을 완료한 후 모든 멤버에게 할당을 전파합니다.
/// 리더만 assignments를 포함하고, 팔로워는 빈 배열을 전송합니다.
pub struct SyncGroupRequest {
pub:
	group_id          string                       // 그룹 ID
	generation_id     i32                          // 그룹 세대 ID
	member_id         string                       // 멤버 ID
	group_instance_id ?string                      // 정적 멤버십 인스턴스 ID
	protocol_type     ?string                      // 프로토콜 타입
	protocol_name     ?string                      // 프로토콜 이름
	assignments       []SyncGroupRequestAssignment // 파티션 할당 (리더만)
}

/// SyncGroup 요청 할당 - 멤버별 파티션 할당
pub struct SyncGroupRequestAssignment {
pub:
	member_id  string // 멤버 ID
	assignment []u8   // 할당 데이터 (ConsumerProtocol 형식)
}

/// SyncGroup 응답 - 파티션 할당 결과
pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32     // 스로틀링 시간 (밀리초)
	error_code       i16     // 에러 코드
	protocol_type    ?string // 프로토콜 타입
	protocol_name    ?string // 프로토콜 이름
	assignment       []u8    // 이 멤버에게 할당된 파티션
}

// ============================================================================
// Heartbeat 요청/응답 타입 (API Key 12) - 하트비트
// ============================================================================

/// Heartbeat 요청 - 그룹 멤버십 유지를 위한 하트비트
///
/// 컨슈머가 주기적으로 전송하여 그룹 멤버십을 유지합니다.
/// 세션 타임아웃 내에 하트비트가 없으면 멤버가 제거됩니다.
pub struct HeartbeatRequest {
pub:
	group_id          string  // 그룹 ID
	generation_id     i32     // 그룹 세대 ID
	member_id         string  // 멤버 ID
	group_instance_id ?string // 정적 멤버십 인스턴스 ID
}

/// Heartbeat 응답 - 하트비트 결과
pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32 // 스로틀링 시간 (밀리초)
	error_code       i16 // 에러 코드 (REBALANCE_IN_PROGRESS 등)
}

// ============================================================================
// LeaveGroup 요청/응답 타입 (API Key 13) - 그룹 탈퇴
// ============================================================================

/// LeaveGroup 요청 - 그룹에서 탈퇴하기 위한 요청
///
/// 컨슈머가 정상적으로 종료될 때 그룹에서 탈퇴합니다.
/// v3+에서는 여러 멤버를 한 번에 탈퇴시킬 수 있습니다.
pub struct LeaveGroupRequest {
pub:
	group_id  string             // 그룹 ID
	member_id string             // 멤버 ID (v0-v2: 단일 멤버)
	members   []LeaveGroupMember // 멤버 목록 (v3+: 배치 탈퇴)
}

/// LeaveGroup 멤버 - v3+ 배치 탈퇴용 멤버 정보
pub struct LeaveGroupMember {
pub:
	member_id         string  // 멤버 ID
	group_instance_id ?string // 정적 멤버십 인스턴스 ID
	reason            ?string // 탈퇴 사유 (v5+)
}

/// LeaveGroup 응답 - 그룹 탈퇴 결과
pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32                        // 스로틀링 시간 (밀리초)
	error_code       i16                        // 에러 코드
	members          []LeaveGroupResponseMember // 멤버별 결과
}

/// LeaveGroup 응답 멤버 - 멤버별 탈퇴 결과
pub struct LeaveGroupResponseMember {
pub:
	member_id         string  // 멤버 ID
	group_instance_id ?string // 정적 멤버십 인스턴스 ID
	error_code        i16     // 에러 코드
}

// ============================================================================
// ConsumerGroupHeartbeat 요청/응답 타입 (API Key 68) - KIP-848
// ============================================================================
// 새로운 컨슈머 리밸런스 프로토콜에서 사용됩니다.

/// ConsumerGroupHeartbeat 요청 - 새로운 컨슈머 프로토콜 하트비트 (KIP-848)
///
/// 기존 JoinGroup/SyncGroup/Heartbeat를 대체하는 단일 API입니다.
/// 서버 측 할당(server-side assignment)을 지원합니다.
pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string   // 그룹 ID
	member_id              string   // 멤버 ID
	member_epoch           i32      // 멤버 에포크
	instance_id            ?string  // 정적 멤버십 (group.instance.id)
	rack_id                ?string  // 랙 ID
	rebalance_timeout_ms   i32      // 리밸런싱 타임아웃
	subscribed_topic_names []string // 구독 토픽 이름 목록
	server_assignor        ?string  // 서버 할당자 이름
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition // 현재 할당
}

/// ConsumerGroupHeartbeat 토픽 파티션 - 현재 할당된 파티션
pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8  // 토픽 UUID (16바이트)
	partitions []i32 // 파티션 인덱스 목록
}

/// ConsumerGroupHeartbeat 응답 - 새로운 컨슈머 프로토콜 하트비트 응답
pub struct ConsumerGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32     // 스로틀링 시간 (밀리초)
	error_code            i16     // 에러 코드
	error_message         ?string // 에러 메시지
	member_id             ?string // 멤버 ID
	member_epoch          i32     // 멤버 에포크
	heartbeat_interval_ms i32     // 하트비트 간격 (밀리초)
	assignment            ?ConsumerGroupHeartbeatAssignment // 새로운 할당
}

/// ConsumerGroupHeartbeat 할당 - 파티션 할당 정보
pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition // 토픽별 파티션
}

/// ConsumerGroupHeartbeat 응답 토픽 파티션 - 할당된 파티션
pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8  // 토픽 UUID (16바이트)
	partitions []i32 // 파티션 인덱스 목록
}

// ============================================================================
// ConsumerGroupDescribe 요청/응답 타입 (API Key 69) - KIP-848
// ============================================================================
// 컨슈머 그룹의 상세 정보를 조회합니다.

/// ConsumerGroupDescribe 요청 - 컨슈머 그룹 상세 정보 조회 요청 (KIP-848)
pub struct ConsumerGroupDescribeRequest {
pub:
	group_ids                     []string // 조회할 그룹 ID 목록
	include_authorized_operations bool     // 권한 있는 작업 포함 여부
}

/// ConsumerGroupDescribe 응답 - 컨슈머 그룹 상세 정보
pub struct ConsumerGroupDescribeResponse {
pub:
	throttle_time_ms i32 // 스로틀링 시간 (밀리초)
	groups           []ConsumerGroupDescribeResponseGroup // 그룹 정보 목록
}

/// ConsumerGroupDescribe 응답 그룹 - 개별 그룹 정보
pub struct ConsumerGroupDescribeResponseGroup {
pub:
	error_code            i16     // 에러 코드
	error_message         ?string // 에러 메시지
	group_id              string  // 그룹 ID
	group_state           string  // 그룹 상태
	group_epoch           i32     // 그룹 에포크
	assignment_epoch      i32     // 할당 에포크
	assignor_name         string  // 할당자 이름
	members               []ConsumerGroupDescribeResponseMember // 멤버 목록
	authorized_operations i32 // 권한 있는 작업
}

/// ConsumerGroupDescribe 응답 멤버 - 멤버 정보
pub struct ConsumerGroupDescribeResponseMember {
pub:
	member_id            string  // 멤버 ID
	instance_id          ?string // 인스턴스 ID
	rack_id              ?string // 랙 ID
	member_epoch         i32     // 멤버 에포크
	client_id            string  // 클라이언트 ID
	client_host          string  // 클라이언트 호스트
	subscribed_topic_ids [][]u8  // 구독 토픽 ID 목록 (UUID)
	assignment           ?ConsumerGroupDescribeResponseMemberAssignment // 현재 할당
}

/// ConsumerGroupDescribe 응답 멤버 할당 - 멤버의 파티션 할당
pub struct ConsumerGroupDescribeResponseMemberAssignment {
pub:
	topic_partitions []ConsumerGroupDescribeTopicPartition // 토픽별 파티션 목록
}

/// ConsumerGroupDescribe 토픽 파티션 - 할당된 파티션
pub struct ConsumerGroupDescribeTopicPartition {
pub:
	topic_id   []u8  // 토픽 UUID (16바이트)
	partitions []i32 // 파티션 인덱스 목록
}
