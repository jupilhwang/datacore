// Service Layer - Share Group Assignor (KIP-932)
// Implements partition assignment strategies for share groups
module group

import domain

// ============================================================================
// SimpleAssignor (KIP-932)
// ============================================================================

// ShareMemberSubscription contains member's subscription info
pub struct ShareMemberSubscription {
pub:
	member_id string
	rack_id   string
	topics    []string
}

// ShareTopicMetadata contains topic info for assignment
pub struct ShareTopicMetadata {
pub:
	topic_id        []u8
	topic_name      string
	partition_count int
}

// ShareGroupSimpleAssignor implements the SimpleAssignor from KIP-932
// It balances the number of consumers assigned to each partition
pub struct ShareGroupSimpleAssignor {}

pub fn new_simple_assignor() &ShareGroupSimpleAssignor {
	return &ShareGroupSimpleAssignor{}
}

pub fn (a &ShareGroupSimpleAssignor) name() string {
	return 'simple'
}

// assign computes partition assignments for share group members
// Unlike consumer groups, share groups can assign the same partition to multiple members
pub fn (a &ShareGroupSimpleAssignor) assign(members []ShareMemberSubscription, topics map[string]ShareTopicMetadata) map[string][]domain.SharePartitionAssignment {
	mut assignments := map[string][]domain.SharePartitionAssignment{}

	// Initialize empty assignments
	for m in members {
		assignments[m.member_id] = []domain.SharePartitionAssignment{}
	}

	if members.len == 0 {
		return assignments
	}

	// Group members by their subscribed topics
	mut topic_assignments := map[string][]string{} // topic -> member_ids

	for m in members {
		for topic in m.topics {
			if topic !in topic_assignments {
				topic_assignments[topic] = []string{}
			}
			topic_assignments[topic] << m.member_id
		}
	}

	// For each topic, assign partitions to subscribed members
	for topic_name, topic_meta in topics {
		subscribed := topic_assignments[topic_name] or { continue }
		if subscribed.len == 0 {
			continue
		}

		num_partitions := topic_meta.partition_count
		num_members := subscribed.len

		// Create partition list for this topic
		mut partitions := []i32{}
		for p in 0 .. num_partitions {
			partitions << p
		}

		// Assign partitions round-robin
		mut member_idx := 0
		for partition in partitions {
			member_id := subscribed[member_idx % subscribed.len]

			// Find or create topic assignment for this member
			mut found := false
			mut member_assignments := assignments[member_id]
			for i, ta in member_assignments {
				if ta.topic_name == topic_name {
					// Create new assignment with added partition
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

			// For share groups, if we have more members than partitions,
			// assign each partition to multiple members
			if num_members > num_partitions && member_idx < num_members {
				// Continue assigning this partition to more members
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
