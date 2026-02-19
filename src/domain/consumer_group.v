// Kafka 컨슈머 그룹 관련 데이터 구조를 정의합니다.
module domain

/// ConsumerGroup은 컨슈머 그룹을 나타냅니다.
/// group_id: 그룹 식별자
/// generation_id: 세대 ID (리밸런싱마다 증가)
/// protocol_type: 프로토콜 유형 ('consumer')
/// protocol: 할당 프로토콜 ('range', 'roundrobin' 등)
/// leader: 리더 멤버 ID
/// state: 그룹 상태
/// members: 그룹 멤버 목록
pub struct ConsumerGroup {
pub:
	group_id      string
	generation_id int
	protocol_type string
	protocol      string
	leader        string
	state         GroupState
	members       []GroupMember
}

/// GroupState는 컨슈머 그룹의 상태를 나타냅니다.
/// empty: 멤버 없음
/// preparing_rebalance: 리밸런싱 준비 중
/// completing_rebalance: 리밸런싱 완료 중
/// stable: 안정 상태
/// dead: 그룹 삭제됨
pub enum GroupState {
	empty
	preparing_rebalance
	completing_rebalance
	stable
	dead
}

/// GroupMember는 컨슈머 그룹의 멤버를 나타냅니다.
/// member_id: 멤버 고유 식별자
/// group_instance_id: 정적 멤버십을 위한 인스턴스 ID
/// client_id: 클라이언트 ID
/// client_host: 클라이언트 호스트
/// metadata: 멤버 메타데이터 (구독 토픽 등)
/// assignment: 파티션 할당 정보
pub struct GroupMember {
pub:
	member_id         string
	group_instance_id string
	client_id         string
	client_host       string
	metadata          []u8
	assignment        []u8
}

/// GroupInfo는 컨슈머 그룹의 간략한 정보입니다.
/// 그룹 목록 조회 시 사용됩니다.
pub struct GroupInfo {
pub:
	group_id      string
	protocol_type string
	state         string
}
