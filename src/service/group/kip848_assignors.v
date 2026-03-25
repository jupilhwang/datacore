// KIP-848 server-side partition assignors
// Implements various partition assignment strategies for the KIP-848 protocol.
//
// DataCore Stateless Architecture Note:
// All brokers access shared storage (S3, PostgreSQL), eliminating
// broker-partition affinity. Complex sticky algorithms provide no benefit.
// Below assignors maintain Kafka protocol compatibility but use simple
// round-robin distribution for simplicity in stateless architecture.
module group

// Range Assignor

/// RangeAssignor implements range-based partition assignment.
/// Divides partitions into contiguous ranges, assigning one range per member per topic.
pub struct RangeAssignor {}

/// new_range_assignor creates a new Range assignor.
fn new_range_assignor() &RangeAssignor {
	return &RangeAssignor{}
}

/// name returns the assignor name.
pub fn (a &RangeAssignor) name() string {
	return 'range'
}

/// assign performs range-based partition assignment.
pub fn (a &RangeAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	mut assignments := map[string][]TopicPartition{}

	// Initialize empty assignments for all members
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// Assign partitions to subscribed members for each topic
	for topic_name, topic_meta in topics {
		// Get members subscribed to this topic
		mut subscribed_members := []string{}
		for m in members {
			if topic_name in m.topics {
				subscribed_members << m.member_id
			}
		}

		if subscribed_members.len == 0 {
			continue
		}

		// Sort members for consistent assignment
		subscribed_members.sort()

		// Range assignment: distribute partitions evenly
		num_partitions := topic_meta.partition_count
		num_members := subscribed_members.len
		partitions_per_member := num_partitions / num_members
		extra_partitions := num_partitions % num_members

		mut partition_idx := 0
		for i, member_id in subscribed_members {
			// Members with lower index receive one extra partition
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

/// RoundRobinAssignor implements round-robin partition assignment.
/// Cycles through all partitions and assigns them to members in order.
pub struct RoundRobinAssignor {}

/// new_round_robin_assignor creates a new Round Robin assignor.
fn new_round_robin_assignor() &RoundRobinAssignor {
	return &RoundRobinAssignor{}
}

/// name returns the assignor name.
pub fn (a &RoundRobinAssignor) name() string {
	return 'roundrobin'
}

/// assign performs round-robin partition assignment.
pub fn (a &RoundRobinAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	mut assignments := map[string][]TopicPartition{}

	// Initialize empty assignments
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	if members.len == 0 {
		return assignments
	}

	// Collect all partitions from all topics
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

	// Sort partitions for consistent ordering
	all_partitions.sort(a.topic_name < b.topic_name)

	// Get sorted member list
	mut member_ids := []string{}
	for m in members {
		member_ids << m.member_id
	}
	member_ids.sort()

	// Round-robin assignment
	for i, tp in all_partitions {
		// Check if any member subscribes to this topic
		mut assigned := false
		for j in 0 .. member_ids.len {
			member_idx := (i + j) % member_ids.len
			member_id := member_ids[member_idx]

			// Verify member subscription
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

/// StickyAssignor implements sticky partition assignment.
/// Uses simple round-robin in DataCore's stateless architecture.
pub struct StickyAssignor {}

/// new_sticky_assignor creates a new Sticky assignor.
fn new_sticky_assignor() &StickyAssignor {
	return &StickyAssignor{}
}

/// name returns the assignor name.
pub fn (a &StickyAssignor) name() string {
	return 'sticky'
}

/// assign uses simple round-robin distribution.
/// Sticky assignment provides no benefit in DataCore's stateless architecture.
pub fn (a &StickyAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	if members.len == 0 {
		return map[string][]TopicPartition{}
	}

	mut assignments := map[string][]TopicPartition{}
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// Collect all partitions
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

	// Sort for deterministic assignment
	all_partitions.sort(a.partition < b.partition)

	// Get sorted member list
	mut member_list := []string{}
	for m in members {
		member_list << m.member_id
	}
	member_list.sort()

	// Simple round-robin assignment
	for i, tp in all_partitions {
		// Find a subscribed member via round-robin
		for j in 0 .. member_list.len {
			member_idx := (i + j) % member_list.len
			member_id := member_list[member_idx]

			// Verify subscription
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

/// CooperativeStickyAssignor implements cooperative sticky assignment.
/// Delegates to StickyAssignor internally.
pub struct CooperativeStickyAssignor {
	inner &StickyAssignor
}

/// new_cooperative_sticky_assignor creates a new Cooperative Sticky assignor.
fn new_cooperative_sticky_assignor() &CooperativeStickyAssignor {
	return &CooperativeStickyAssignor{
		inner: new_sticky_assignor()
	}
}

/// name returns the assignor name.
pub fn (a &CooperativeStickyAssignor) name() string {
	return 'cooperative-sticky'
}

/// assign performs assignment by delegating to the inner assignor.
pub fn (a &CooperativeStickyAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	return a.inner.assign(members, topics)
}

// Uniform Assignor (KIP-848) - round-robin alias

/// UniformAssignor implements the KIP-848 uniform assignment.
/// Distributes all partitions evenly among members.
pub struct UniformAssignor {}

/// new_uniform_assignor creates a new Uniform assignor.
fn new_uniform_assignor() &UniformAssignor {
	return &UniformAssignor{}
}

/// name returns the assignor name.
pub fn (a &UniformAssignor) name() string {
	return 'uniform'
}

/// assign performs uniform round-robin assignment.
pub fn (a &UniformAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	if members.len == 0 {
		return map[string][]TopicPartition{}
	}

	mut assignments := map[string][]TopicPartition{}
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// Collect all partitions
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

	// Sort for deterministic assignment
	all_partitions.sort(a.partition < b.partition)

	// Get sorted member list
	mut member_list := []string{}
	for m in members {
		member_list << m.member_id
	}
	member_list.sort()

	// Uniform round-robin assignment
	for i, tp in all_partitions {
		member_idx := i % member_list.len
		member_id := member_list[member_idx]

		// Verify subscription
		for m in members {
			if m.member_id == member_id && tp.topic_name in m.topics {
				assignments[member_id] << tp
				break
			}
		}
	}

	return assignments
}
