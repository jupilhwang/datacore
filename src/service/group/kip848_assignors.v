// KIP-848 서버 측 파티션 할당자
// KIP-848 프로토콜을 위한 다양한 파티션 할당 전략을 구현합니다.
//
// DataCore Stateless Architecture Note:
// All brokers access shared storage (S3, PostgreSQL), eliminating
// broker-partition affinity. Complex sticky algorithms provide no benefit.
// Below assignors maintain Kafka protocol compatibility but use simple
// round-robin distribution for simplicity in stateless architecture.
module group

// Range Assignor

/// RangeAssignor는 범위 기반 파티션 할당을 구현합니다.
/// 각 토픽에 대해 파티션을 멤버 수로 나누어 연속된 범위를 할당합니다.
pub struct RangeAssignor {}

/// new_range_assignor는 새로운 Range 할당자를 생성합니다.
pub fn new_range_assignor() &RangeAssignor {
	return &RangeAssignor{}
}

pub fn (a &RangeAssignor) name() string {
	return 'range'
}

/// assign은 범위 기반 파티션 할당을 수행합니다.
pub fn (a &RangeAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	mut assignments := map[string][]TopicPartition{}

	// 모든 멤버에 대해 빈 할당 초기화
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// 각 토픽에 대해 구독한 멤버에게 파티션 할당
	for topic_name, topic_meta in topics {
		// 이 토픽을 구독한 멤버 가져오기
		mut subscribed_members := []string{}
		for m in members {
			if topic_name in m.topics {
				subscribed_members << m.member_id
			}
		}

		if subscribed_members.len == 0 {
			continue
		}

		// 일관된 할당을 위해 멤버 정렬
		subscribed_members.sort()

		// 범위 할당: 파티션을 균등하게 분배
		num_partitions := topic_meta.partition_count
		num_members := subscribed_members.len
		partitions_per_member := num_partitions / num_members
		extra_partitions := num_partitions % num_members

		mut partition_idx := 0
		for i, member_id in subscribed_members {
			// 인덱스가 낮은 멤버가 추가 파티션 1개를 받음
			count := partitions_per_member + if i < extra_partitions { 1 } else { 0 }

			for _ in 0 .. count {
				assignments[member_id] << TopicPartition{
					topic_id:   topic_meta.topic_id
					topic_name: topic_name
					partition:  partition_idx
				}
				partition_idx++
			}
		}
	}

	return assignments
}

// Round Robin Assignor

/// RoundRobinAssignor는 라운드 로빈 파티션 할당을 구현합니다.
/// 모든 파티션을 순환하며 멤버에게 할당합니다.
pub struct RoundRobinAssignor {}

/// new_round_robin_assignor는 새로운 Round Robin 할당자를 생성합니다.
pub fn new_round_robin_assignor() &RoundRobinAssignor {
	return &RoundRobinAssignor{}
}

pub fn (a &RoundRobinAssignor) name() string {
	return 'roundrobin'
}

/// assign은 라운드 로빈 파티션 할당을 수행합니다.
pub fn (a &RoundRobinAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	mut assignments := map[string][]TopicPartition{}

	// 빈 할당 초기화
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	if members.len == 0 {
		return assignments
	}

	// 모든 토픽의 모든 파티션 수집
	mut all_partitions := []TopicPartition{}
	for topic_name, topic_meta in topics {
		for p in 0 .. topic_meta.partition_count {
			all_partitions << TopicPartition{
				topic_id:   topic_meta.topic_id
				topic_name: topic_name
				partition:  p
			}
		}
	}

	// 일관된 순서를 위해 파티션 정렬
	all_partitions.sort(a.topic_name < b.topic_name)

	// 정렬된 멤버 목록 가져오기
	mut member_ids := []string{}
	for m in members {
		member_ids << m.member_id
	}
	member_ids.sort()

	// 라운드 로빈 할당
	for i, tp in all_partitions {
		// 이 토픽을 구독한 멤버가 있는지 확인
		mut assigned := false
		for j in 0 .. member_ids.len {
			member_idx := (i + j) % member_ids.len
			member_id := member_ids[member_idx]

			// 멤버 구독 확인
			for m in members {
				if m.member_id == member_id && tp.topic_name in m.topics {
					assignments[member_id] << tp
					assigned = true
					break
				}
			}
			if assigned {
				break
			}
		}
	}

	return assignments
}

// Sticky Assignor (simplified for stateless architecture)
// Note: All brokers access shared storage, eliminating broker-partition affinity.
// This is a simple round-robin implementation with 'sticky' alias for Kafka compatibility.

/// StickyAssignor는 sticky 파티션 할당을 구현합니다.
/// DataCore의 무상태 아키텍처에서는 단순 라운드 로빈을 사용합니다.
pub struct StickyAssignor {}

/// new_sticky_assignor는 새로운 Sticky 할당자를 생성합니다.
pub fn new_sticky_assignor() &StickyAssignor {
	return &StickyAssignor{}
}

pub fn (a &StickyAssignor) name() string {
	return 'sticky'
}

/// assign은 단순 라운드 로빈 분배를 사용합니다.
/// DataCore의 무상태 아키텍처에서 sticky 할당은 이점을 제공하지 않습니다.
pub fn (a &StickyAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	if members.len == 0 {
		return map[string][]TopicPartition{}
	}

	mut assignments := map[string][]TopicPartition{}
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// 모든 파티션 수집
	mut all_partitions := []TopicPartition{}
	for topic_name, meta in topics {
		for p in 0 .. meta.partition_count {
			all_partitions << TopicPartition{
				topic_id:   meta.topic_id
				topic_name: topic_name
				partition:  p
			}
		}
	}

	// 결정적 할당을 위해 정렬
	all_partitions.sort(a.partition < b.partition)

	// 정렬된 멤버 목록 가져오기
	mut member_list := []string{}
	for m in members {
		member_list << m.member_id
	}
	member_list.sort()

	// 단순 라운드 로빈 할당
	for i, tp in all_partitions {
		// 라운드 로빈으로 구독한 멤버 찾기
		for j in 0 .. member_list.len {
			member_idx := (i + j) % member_list.len
			member_id := member_list[member_idx]

			// 구독 확인
			for m in members {
				if m.member_id == member_id && tp.topic_name in m.topics {
					assignments[member_id] << tp
					break
				}
			}
			if assignments[member_id].len > i / member_list.len {
				break
			}
		}
	}

	return assignments
}

// Cooperative Sticky Assignor (compatibility alias)

/// CooperativeStickyAssignor는 cooperative sticky 할당을 구현합니다.
/// 내부적으로 StickyAssignor를 사용합니다.
pub struct CooperativeStickyAssignor {
	inner &StickyAssignor
}

/// new_cooperative_sticky_assignor는 새로운 Cooperative Sticky 할당자를 생성합니다.
pub fn new_cooperative_sticky_assignor() &CooperativeStickyAssignor {
	return &CooperativeStickyAssignor{
		inner: new_sticky_assignor()
	}
}

pub fn (a &CooperativeStickyAssignor) name() string {
	return 'cooperative-sticky'
}

pub fn (a &CooperativeStickyAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	return a.inner.assign(members, topics)
}

// Uniform Assignor (KIP-848) - round-robin alias

/// UniformAssignor는 KIP-848의 uniform 할당을 구현합니다.
/// 모든 파티션을 멤버에게 균등하게 분배합니다.
pub struct UniformAssignor {}

/// new_uniform_assignor는 새로운 Uniform 할당자를 생성합니다.
pub fn new_uniform_assignor() &UniformAssignor {
	return &UniformAssignor{}
}

pub fn (a &UniformAssignor) name() string {
	return 'uniform'
}

/// assign은 균등 라운드 로빈 할당을 수행합니다.
pub fn (a &UniformAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	if members.len == 0 {
		return map[string][]TopicPartition{}
	}

	mut assignments := map[string][]TopicPartition{}
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// 모든 파티션 수집
	mut all_partitions := []TopicPartition{}
	for topic_name, meta in topics {
		for p in 0 .. meta.partition_count {
			all_partitions << TopicPartition{
				topic_id:   meta.topic_id
				topic_name: topic_name
				partition:  p
			}
		}
	}

	// 결정적 할당을 위해 정렬
	all_partitions.sort(a.partition < b.partition)

	// 정렬된 멤버 목록 가져오기
	mut member_list := []string{}
	for m in members {
		member_list << m.member_id
	}
	member_list.sort()

	// 균등 라운드 로빈 할당
	for i, tp in all_partitions {
		member_idx := i % member_list.len
		member_id := member_list[member_idx]

		// 구독 확인
		for m in members {
			if m.member_id == member_id && tp.topic_name in m.topics {
				assignments[member_id] << tp
				break
			}
		}
	}

	return assignments
}
