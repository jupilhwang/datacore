// KIP-848 타입 정의
// KIP-848 새 컨슈머 프로토콜 구현을 위한 공유 타입입니다.
// 서버 측 파티션 할당과 점진적 리밸런싱을 지원합니다.
module group

// KIP-848 멤버 상태 머신

/// MemberState는 새 프로토콜에서 멤버의 상태를 나타냅니다.
pub enum MemberState {
	unsubscribed  // 구독 없음
	subscribing   // 토픽 구독 중
	stable        // 안정적인 할당 보유
	reconciling   // 할당 변경 조정 중
	assigning     // 서버가 새 할당 계산 중
	unsubscribing // 그룹 탈퇴 중
	fenced        // 펜스됨 (에포크 불일치)
}

/// KIP848Member는 KIP-848 프로토콜을 사용하는 컨슈머 그룹 멤버를 나타냅니다.
pub struct KIP848Member {
pub mut:
	member_id              string           // 멤버 ID
	instance_id            ?string          // 정적 멤버십을 위한 인스턴스 ID
	rack_id                ?string          // 랙 ID
	client_id              string           // 클라이언트 ID
	client_host            string           // 클라이언트 호스트
	subscribed_topic_names []string         // 구독 토픽 이름 목록
	subscribed_topic_regex ?string          // 구독 토픽 정규식
	server_assignor        ?string          // 서버 할당자
	member_epoch           i32              // 멤버 에포크
	previous_member_epoch  i32              // 이전 멤버 에포크
	state                  MemberState      // 멤버 상태
	assigned_partitions    []TopicPartition // 할당된 파티션
	pending_partitions     []TopicPartition // 할당 예정 파티션
	revoking_partitions    []TopicPartition // 회수 예정 파티션
	rebalance_timeout_ms   i32              // 리밸런싱 타임아웃 (ms)
	session_timeout_ms     i32              // 세션 타임아웃 (ms)
	last_heartbeat         i64              // 마지막 하트비트 (Unix 타임스탬프 ms)
	joined_at              i64              // 참가 시간
}

/// TopicPartition은 토픽-파티션 쌍을 나타냅니다.
pub struct TopicPartition {
pub:
	topic_id   []u8   // UUID (16바이트)
	topic_name string // 토픽 이름
	partition  i32    // 파티션 번호
}

// KIP-848 컨슈머 그룹

/// KIP848ConsumerGroup은 새 프로토콜을 사용하는 컨슈머 그룹을 나타냅니다.
pub struct KIP848ConsumerGroup {
pub mut:
	group_id           string                      // 그룹 ID
	group_epoch        i32                         // 그룹 에포크
	assignment_epoch   i32                         // 할당 에포크
	state              KIP848GroupState            // 그룹 상태
	protocol_type      string                      // 프로토콜 타입
	server_assignor    string                      // 서버 할당자
	members            map[string]&KIP848Member    // 멤버 맵 (member_id -> 멤버)
	target_assignment  map[string][]TopicPartition // 대상 할당 (member_id -> 파티션)
	current_assignment map[string][]TopicPartition // 현재 안정 할당
	subscribed_topics  map[string]bool             // 모든 구독 토픽
	created_at         i64                         // 생성 시간
	updated_at         i64                         // 업데이트 시간
}

/// KIP848GroupState는 KIP-848 그룹의 상태를 나타냅니다.
pub enum KIP848GroupState {
	empty       // 멤버 없음
	assigning   // 새 할당 계산 중
	reconciling // 멤버들이 조정 중
	stable      // 모든 멤버가 안정적인 할당 보유
	dead        // 그룹 삭제 중
}

// 서버 측 할당자 인터페이스 및 타입

/// ServerAssignor는 서버 측에서 파티션 할당을 계산합니다.
pub interface ServerAssignor {
	/// 할당자 이름을 반환합니다.
	name() string

	/// 파티션 할당을 계산합니다.
	assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition
}

/// MemberSubscription은 할당을 위한 멤버의 구독 정보를 담습니다.
pub struct MemberSubscription {
pub:
	member_id        string           // 멤버 ID
	instance_id      ?string          // 인스턴스 ID
	rack_id          ?string          // 랙 ID
	topics           []string         // 구독 토픽 목록
	owned_partitions []TopicPartition // 현재 소유 파티션 (sticky 할당용)
}

/// TopicMetadata는 할당을 위한 토픽 정보를 담습니다.
pub struct TopicMetadata {
pub:
	topic_id        []u8   // 토픽 ID
	topic_name      string // 토픽 이름
	partition_count int    // 파티션 수
}
