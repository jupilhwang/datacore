// KIP-848 Server-side Partition Assignors
// Implements various partition assignment strategies for KIP-848 protocol
//
// ============================================================================
// DataCore Stateless Architecture Note
// ============================================================================
// DataCore uses a stateless broker architecture where all brokers access
// shared storage (S3, PostgreSQL, etc.). This simplifies partition assignment:
//
// - No broker-partition affinity: Any broker can serve any partition
// - No rebalancing cost: Changing assignment doesn't move data
// - Simple assignors: Sticky/Cooperative algorithms provide no benefit
//
// The assignors below maintain Kafka protocol compatibility but internally
// use simple round-robin distribution. Complex sticky logic is intentionally
// omitted as it provides no benefit in stateless architecture.
// ============================================================================
module group

// ============================================================================
// Range Assignor
// ============================================================================

pub struct RangeAssignor {}

pub fn new_range_assignor() &RangeAssignor {
	return &RangeAssignor{}
}

pub fn (a &RangeAssignor) name() string {
	return 'range'
}

pub fn (a &RangeAssignor) assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition {
	mut assignments := map[string][]TopicPartition{}

	// Initialize empty assignments for all members
	for m in members {
		assignments[m.member_id] = []TopicPartition{}
	}

	// For each topic, assign partitions to subscribed members
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

		// Range assignment: divide partitions evenly
		num_partitions := topic_meta.partition_count
		num_members := subscribed_members.len
		partitions_per_member := num_partitions / num_members
		extra_partitions := num_partitions % num_members

		mut partition_idx := 0
		for i, member_id in subscribed_members {
			// Members with lower indices get one extra partition
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

// ============================================================================
// Round Robin Assignor
// ============================================================================

pub struct RoundRobinAssignor {}

pub fn new_round_robin_assignor() &RoundRobinAssignor {
	return &RoundRobinAssignor{}
}

pub fn (a &RoundRobinAssignor) name() string {
	return 'roundrobin'
}

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

	// Round-robin assign
	for i, tp in all_partitions {
		// Check if any member is subscribed to this topic
		mut assigned := false
		for j in 0 .. member_ids.len {
			member_idx := (i + j) % member_ids.len
			member_id := member_ids[member_idx]

			// Find member subscription
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

// ============================================================================
// Sticky Assignor (Simplified for Stateless Architecture)
// ============================================================================
// NOTE: DataCore uses a stateless architecture where all brokers can access
// all partitions. Complex sticky/cooperative algorithms provide no benefit
// because there's no broker-partition affinity to preserve.
// This is a simple round-robin implementation aliased as 'sticky' for
// Kafka client compatibility.
// ============================================================================

pub struct StickyAssignor {}

pub fn new_sticky_assignor() &StickyAssignor {
	return &StickyAssignor{}
}

pub fn (a &StickyAssignor) name() string {
	return 'sticky'
}

// assign uses simple round-robin distribution
// In DataCore's stateless architecture, sticky assignment provides no benefit
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
		// Find subscribed member using round-robin
		for j in 0 .. member_list.len {
			member_idx := (i + j) % member_list.len
			member_id := member_list[member_idx]

			// Check subscription
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

// ============================================================================
// Cooperative Sticky Assignor (Alias for compatibility)
// ============================================================================

pub struct CooperativeStickyAssignor {
	inner &StickyAssignor
}

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

// ============================================================================
// Uniform Assignor (KIP-848) - Alias for round-robin
// ============================================================================

pub struct UniformAssignor {}

pub fn new_uniform_assignor() &UniformAssignor {
	return &UniformAssignor{}
}

pub fn (a &UniformAssignor) name() string {
	return 'uniform'
}

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

		// Check subscription
		for m in members {
			if m.member_id == member_id && tp.topic_name in m.topics {
				assignments[member_id] << tp
				break
			}
		}
	}

	return assignments
}
