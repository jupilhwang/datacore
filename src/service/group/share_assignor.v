// 서비스 레이어 - Share 그룹 할당자 (KIP-932)
// Share 그룹을 위한 파티션 할당 전략을 구현합니다.
// 전통적인 컨슈머 그룹과 달리 동일한 파티션을 여러 멤버에게 할당할 수 있습니다.
module group

import domain

// ============================================================================
// SimpleAssignor (KIP-932)
// ============================================================================

/// ShareMemberSubscription은 멤버의 구독 정보를 담습니다.
pub struct ShareMemberSubscription {
pub:
	member_id string   // 멤버 ID
	rack_id   string   // 랙 ID
	topics    []string // 구독 토픽 목록
}

/// ShareTopicMetadata는 할당을 위한 토픽 정보를 담습니다.
pub struct ShareTopicMetadata {
pub:
	topic_id        []u8   // 토픽 ID (UUID)
	topic_name      string // 토픽 이름
	partition_count int    // 파티션 수
}

/// ShareGroupSimpleAssignor는 KIP-932의 SimpleAssignor를 구현합니다.
/// 각 파티션에 할당된 컨슈머 수의 균형을 맞춥니다.
pub struct ShareGroupSimpleAssignor {}

/// new_simple_assignor는 새로운 SimpleAssignor를 생성합니다.
pub fn new_simple_assignor() &ShareGroupSimpleAssignor {
	return &ShareGroupSimpleAssignor{}
}

/// name은 할당자 이름을 반환합니다.
pub fn (a &ShareGroupSimpleAssignor) name() string {
	return 'simple'
}

/// assign은 share 그룹 멤버에 대한 파티션 할당을 계산합니다.
/// 컨슈머 그룹과 달리 share 그룹은 동일한 파티션을 여러 멤버에게 할당할 수 있습니다.
pub fn (a &ShareGroupSimpleAssignor) assign(members []ShareMemberSubscription, topics map[string]ShareTopicMetadata) map[string][]domain.SharePartitionAssignment {
	mut assignments := map[string][]domain.SharePartitionAssignment{}

	// 빈 할당 초기화
	for m in members {
		assignments[m.member_id] = []domain.SharePartitionAssignment{}
	}

	if members.len == 0 {
		return assignments
	}

	// 구독 토픽별로 멤버 그룹화
	mut topic_assignments := map[string][]string{} // topic -> member_ids

	for m in members {
		for topic in m.topics {
			if topic !in topic_assignments {
				topic_assignments[topic] = []string{}
			}
			topic_assignments[topic] << m.member_id
		}
	}

	// 각 토픽에 대해 구독한 멤버에게 파티션 할당
	for topic_name, topic_meta in topics {
		subscribed := topic_assignments[topic_name] or { continue }
		if subscribed.len == 0 {
			continue
		}

		num_partitions := topic_meta.partition_count
		num_members := subscribed.len

		// 이 토픽의 파티션 목록 생성
		mut partitions := []i32{}
		for p in 0 .. num_partitions {
			partitions << p
		}

		// 라운드 로빈으로 파티션 할당
		mut member_idx := 0
		for partition in partitions {
			member_id := subscribed[member_idx % subscribed.len]

			// 이 멤버의 토픽 할당 찾기 또는 생성
			mut found := false
			mut member_assignments := assignments[member_id]
			for i, ta in member_assignments {
				if ta.topic_name == topic_name {
					// 파티션 추가된 새 할당 생성
					mut new_partitions := ta.partitions.clone()
					new_partitions << partition
					member_assignments[i] = domain.SharePartitionAssignment{
						topic_id:   ta.topic_id
						topic_name: ta.topic_name
						partitions: new_partitions
					}
					found = true
					break
				}
			}

			if !found {
				member_assignments << domain.SharePartitionAssignment{
					topic_id:   topic_meta.topic_id
					topic_name: topic_name
					partitions: [partition]
				}
			}
			assignments[member_id] = member_assignments

			member_idx += 1

			// Share 그룹의 경우, 멤버가 파티션보다 많으면
			// 각 파티션을 여러 멤버에게 할당
			if num_members > num_partitions && member_idx < num_members {
				// 이 파티션을 더 많은 멤버에게 계속 할당
				extra_assignments := (num_members / num_partitions) - 1
				for _ in 0 .. extra_assignments {
					extra_member_id := subscribed[member_idx % subscribed.len]
					member_idx += 1

					mut extra_found := false
					mut extra_member_assignments := assignments[extra_member_id]
					for i, ta in extra_member_assignments {
						if ta.topic_name == topic_name {
							if partition !in ta.partitions {
								mut new_partitions := ta.partitions.clone()
								new_partitions << partition
								extra_member_assignments[i] = domain.SharePartitionAssignment{
									topic_id:   ta.topic_id
									topic_name: ta.topic_name
									partitions: new_partitions
								}
							}
							extra_found = true
							break
						}
					}

					if !extra_found {
						extra_member_assignments << domain.SharePartitionAssignment{
							topic_id:   topic_meta.topic_id
							topic_name: topic_name
							partitions: [partition]
						}
					}
					assignments[extra_member_id] = extra_member_assignments
				}
			}
		}
	}

	return assignments
}
