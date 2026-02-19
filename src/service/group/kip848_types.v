// KIP-848 타입 정의
// KIP-848 새 컨슈머 프로토콜 구현을 위한 공유 타입입니다.
// 서버 측 파티션 할당과 점진적 리밸런싱을 지원합니다.
module group

// KIP-848 멤버 상태 머신

/// MemberState는 새 프로토콜에서 멤버의 상태를 나타냅니다.
pub enum MemberState {
	unsubscribed
	subscribing
	stable
	reconciling
	assigning
	unsubscribing
	fenced
}

/// KIP848Member는 KIP-848 프로토콜을 사용하는 컨슈머 그룹 멤버를 나타냅니다.
pub struct KIP848Member {
pub mut:
	member_id              string
	instance_id            ?string
	rack_id                ?string
	client_id              string
	client_host            string
	subscribed_topic_names []string
	subscribed_topic_regex ?string
	server_assignor        ?string
	member_epoch           i32
	previous_member_epoch  i32
	state                  MemberState
	assigned_partitions    []TopicPartition
	pending_partitions     []TopicPartition
	revoking_partitions    []TopicPartition
	rebalance_timeout_ms   i32
	session_timeout_ms     i32
	last_heartbeat         i64
	joined_at              i64
}

/// TopicPartition은 토픽-파티션 쌍을 나타냅니다.
pub struct TopicPartition {
pub:
	topic_id   []u8 // UUID (16바이트)
	topic_name string
	partition  i32
}

// KIP-848 컨슈머 그룹

/// KIP848ConsumerGroup은 새 프로토콜을 사용하는 컨슈머 그룹을 나타냅니다.
pub struct KIP848ConsumerGroup {
pub mut:
	group_id           string
	group_epoch        i32
	assignment_epoch   i32
	state              KIP848GroupState
	protocol_type      string
	server_assignor    string
	members            map[string]&KIP848Member
	target_assignment  map[string][]TopicPartition
	current_assignment map[string][]TopicPartition
	subscribed_topics  map[string]bool
	created_at         i64
	updated_at         i64
}

/// KIP848GroupState는 KIP-848 그룹의 상태를 나타냅니다.
pub enum KIP848GroupState {
	empty
	assigning
	reconciling
	stable
	dead
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
	member_id        string
	instance_id      ?string
	rack_id          ?string
	topics           []string
	owned_partitions []TopicPartition
}

/// TopicMetadata는 할당을 위한 토픽 정보를 담습니다.
pub struct TopicMetadata {
pub:
	topic_id        []u8
	topic_name      string
	partition_count int
}
