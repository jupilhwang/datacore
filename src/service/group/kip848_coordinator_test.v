// KIP-848 New Consumer Protocol Tests
module group

// ============================================================================
// Assignor Tests
// ============================================================================

fn test_range_assignor_single_member() {
	assignor := new_range_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a', 'topic-b']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 3
		}
		'topic-b': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-b'
			partition_count: 2
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed: ${err.msg()}'
		return
	}

	// Single member should get all partitions
	assert 'member-1' in assignments
	member1_partitions := assignments['member-1']
	assert member1_partitions.len == 5 // 3 + 2
}

fn test_range_assignor_multiple_members() {
	assignor := new_range_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 4
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// 4 partitions / 2 members = 2 each
	assert assignments['member-1'].len == 2
	assert assignments['member-2'].len == 2
}

fn test_range_assignor_uneven_partitions() {
	assignor := new_range_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-3'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 7
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// 7 partitions / 3 members = 2 each + 1 extra for first member
	total := assignments['member-1'].len + assignments['member-2'].len + assignments['member-3'].len
	assert total == 7
	assert assignments['member-1'].len >= 2
	assert assignments['member-2'].len >= 2
	assert assignments['member-3'].len >= 2
}

fn test_round_robin_assignor() {
	assignor := new_round_robin_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a', 'topic-b']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a', 'topic-b']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 2
		}
		'topic-b': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-b'
			partition_count: 2
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// 4 partitions total, should be evenly distributed
	assert assignments['member-1'].len == 2
	assert assignments['member-2'].len == 2
}

fn test_sticky_assignor_preserves_assignment() {
	assignor := new_sticky_assignor()

	// Member 1 already owns partition 0, member 2 owns partition 1
	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: [TopicPartition{
				topic_id:   []u8{len: 16}
				topic_name: 'topic-a'
				partition:  0
			}]
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: [TopicPartition{
				topic_id:   []u8{len: 16}
				topic_name: 'topic-a'
				partition:  1
			}]
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 2
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// Should preserve existing assignments
	member1_has_p0 := assignments['member-1'].any(it.partition == 0)
	member2_has_p1 := assignments['member-2'].any(it.partition == 1)

	assert member1_has_p0, 'Member 1 should keep partition 0'
	assert member2_has_p1, 'Member 2 should keep partition 1'
}

fn test_sticky_assignor_with_new_partitions() {
	assignor := new_sticky_assignor()

	// Member 1 owns partition 0
	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: [TopicPartition{
				topic_id:   []u8{len: 16}
				topic_name: 'topic-a'
				partition:  0
			}]
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	// Topic now has 4 partitions (was 1)
	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 4
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// All 4 partitions should be assigned
	total := assignments['member-1'].len + assignments['member-2'].len
	assert total == 4

	// Member 1 should still have partition 0
	assert assignments['member-1'].any(it.partition == 0)
}

// ============================================================================
// Member State Tests
// ============================================================================

fn test_member_state_values() {
	assert int(MemberState.unsubscribed) == 0
	assert int(MemberState.subscribing) == 1
	assert int(MemberState.stable) == 2
	assert int(MemberState.reconciling) == 3
	assert int(MemberState.assigning) == 4
	assert int(MemberState.unsubscribing) == 5
	assert int(MemberState.fenced) == 6
}

fn test_group_state_values() {
	assert int(KIP848GroupState.empty) == 0
	assert int(KIP848GroupState.assigning) == 1
	assert int(KIP848GroupState.reconciling) == 2
	assert int(KIP848GroupState.stable) == 3
	assert int(KIP848GroupState.dead) == 4
}

// ============================================================================
// TopicPartition Tests
// ============================================================================

fn test_topic_partition_creation() {
	tp := TopicPartition{
		topic_id:   []u8{len: 16, init: u8(index)}
		topic_name: 'test-topic'
		partition:  5
	}

	assert tp.topic_name == 'test-topic'
	assert tp.partition == 5
	assert tp.topic_id.len == 16
}

// ============================================================================
// KIP848Member Tests
// ============================================================================

fn test_member_creation() {
	member := KIP848Member{
		member_id:              'member-123'
		instance_id:            'instance-456'
		rack_id:                'rack-a'
		subscribed_topic_names: ['topic-1', 'topic-2']
		member_epoch:           1
		state:                  .subscribing
		last_heartbeat:         1000
		joined_at:              1000
	}

	assert member.member_id == 'member-123'
	assert member.instance_id? == 'instance-456'
	assert member.rack_id? == 'rack-a'
	assert member.subscribed_topic_names.len == 2
	assert member.member_epoch == 1
	assert member.state == .subscribing
}

fn test_member_without_optional_fields() {
	member := KIP848Member{
		member_id:              'member-123'
		subscribed_topic_names: ['topic-1']
		member_epoch:           1
		state:                  .stable
		last_heartbeat:         1000
		joined_at:              1000
	}

	assert member.member_id == 'member-123'
	assert member.instance_id == none
	assert member.rack_id == none
}

// ============================================================================
// KIP848ConsumerGroup Tests
// ============================================================================

fn test_group_creation() {
	group := KIP848ConsumerGroup{
		group_id:         'test-group'
		group_epoch:      1
		assignment_epoch: 1
		state:            .empty
		protocol_type:    'consumer'
		server_assignor:  'roundrobin'
		members:          map[string]&KIP848Member{}
		created_at:       1000
		updated_at:       1000
	}

	assert group.group_id == 'test-group'
	assert group.group_epoch == 1
	assert group.state == .empty
	assert group.members.len == 0
}

// ============================================================================
// HeartbeatResult Tests
// ============================================================================

fn test_heartbeat_result_success() {
	result := HeartbeatResult{
		error_code:            0
		member_id:             'member-1'
		member_epoch:          5
		heartbeat_interval_ms: 3000
		assignment:            [
			TopicPartition{
				topic_name: 'topic-a'
				partition:  0
			},
		]
	}

	assert result.error_code == 0
	assert result.member_id == 'member-1'
	assert result.member_epoch == 5
	assert result.heartbeat_interval_ms == 3000
	assert result.assignment?.len == 1
}

fn test_heartbeat_result_error() {
	result := HeartbeatResult{
		error_code:    24 // INVALID_GROUP_ID
		error_message: 'Group ID cannot be empty'
		member_epoch:  -1
	}

	assert result.error_code == 24
	assert result.error_message? == 'Group ID cannot be empty'
	assert result.member_epoch == -1
	assert result.assignment == none
}

// ============================================================================
// MemberSubscription Tests
// ============================================================================

fn test_member_subscription() {
	sub := MemberSubscription{
		member_id:        'member-1'
		instance_id:      'instance-1'
		rack_id:          'rack-a'
		topics:           ['topic-1', 'topic-2', 'topic-3']
		owned_partitions: [
			TopicPartition{
				topic_name: 'topic-1'
				partition:  0
			},
			TopicPartition{
				topic_name: 'topic-1'
				partition:  1
			},
		]
	}

	assert sub.member_id == 'member-1'
	assert sub.topics.len == 3
	assert sub.owned_partitions.len == 2
}

// ============================================================================
// TopicMetadata Tests
// ============================================================================

fn test_topic_metadata() {
	meta := TopicMetadata{
		topic_id:        []u8{len: 16, init: u8(0x12)}
		topic_name:      'my-topic'
		partition_count: 10
	}

	assert meta.topic_name == 'my-topic'
	assert meta.partition_count == 10
	assert meta.topic_id.len == 16
}

// ============================================================================
// Assignor Name Tests
// ============================================================================

fn test_assignor_names() {
	range_assignor := new_range_assignor()
	rr_assignor := new_round_robin_assignor()
	sticky_assignor := new_sticky_assignor()

	assert range_assignor.name() == 'range'
	assert rr_assignor.name() == 'roundrobin'
	assert sticky_assignor.name() == 'sticky'
}

// ============================================================================
// Empty Assignment Tests
// ============================================================================

fn test_range_assignor_empty_members() {
	assignor := new_range_assignor()

	members := []MemberSubscription{}
	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 3
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	assert assignments.len == 0
}

fn test_range_assignor_empty_topics() {
	assignor := new_range_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := map[string]TopicMetadata{}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// Member should have empty assignment (no matching topics)
	assert assignments['member-1'].len == 0
}

fn test_round_robin_empty_members() {
	assignor := new_round_robin_assignor()

	assignments := assignor.assign([]MemberSubscription{}, map[string]TopicMetadata{}) or {
		assert false, 'Assignment failed'
		return
	}

	assert assignments.len == 0
}

// ============================================================================
// Partial Subscription Tests
// ============================================================================

fn test_range_assignor_partial_subscription() {
	assignor := new_range_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a'] // Only subscribed to topic-a
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-b'] // Only subscribed to topic-b
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 2
		}
		'topic-b': TopicMetadata{
			topic_name:      'topic-b'
			partition_count: 2
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// member-1 should only have topic-a partitions
	for tp in assignments['member-1'] {
		assert tp.topic_name == 'topic-a'
	}

	// member-2 should only have topic-b partitions
	for tp in assignments['member-2'] {
		assert tp.topic_name == 'topic-b'
	}
}

// ============================================================================
// Enhanced Sticky Assignor Tests
// ============================================================================

fn test_sticky_assignor_balanced_distribution() {
	assignor := new_sticky_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-3'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_id:        []u8{len: 16}
			topic_name:      'topic-a'
			partition_count: 10
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed: ${err}'
		return
	}

	// 10 partitions / 3 members = 3 or 4 each (balanced)
	for member_id, partitions in assignments {
		assert partitions.len >= 3 && partitions.len <= 4, 'Member ${member_id} has unbalanced assignment: ${partitions.len}'
	}
}

fn test_sticky_assignor_member_join() {
	assignor := new_sticky_assignor()

	// 3 members subscribing to topic-a
	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-3'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 10
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// All partitions should be assigned
	total := assignments['member-1'].len + assignments['member-2'].len + assignments['member-3'].len
	assert total == 10

	// All members should have some partitions
	assert assignments['member-1'].len > 0
	assert assignments['member-2'].len > 0
	assert assignments['member-3'].len > 0
}

fn test_sticky_assignor_member_leave() {
	assignor := new_sticky_assignor()

	// Only 2 members now
	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 10
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// All 10 partitions should be assigned to 2 members
	total := assignments['member-1'].len + assignments['member-2'].len
	assert total == 10

	// Each should have 5 partitions (balanced)
	assert assignments['member-1'].len == 5
	assert assignments['member-2'].len == 5
}

// ============================================================================
// Cooperative Sticky Assignor Tests
// ============================================================================

fn test_cooperative_sticky_assignor() {
	assignor := new_cooperative_sticky_assignor()

	assert assignor.name() == 'cooperative-sticky'

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 4
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	total := assignments['member-1'].len + assignments['member-2'].len
	assert total == 4
}

// ============================================================================
// Uniform Assignor Tests (KIP-848)
// ============================================================================

fn test_uniform_assignor() {
	assignor := new_uniform_assignor()

	assert assignor.name() == 'uniform'

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 6
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// 6 partitions / 2 members = 3 each
	assert assignments['member-1'].len == 3
	assert assignments['member-2'].len == 3
}

fn test_uniform_assignor_with_rack() {
	assignor := new_uniform_assignor()

	members := [
		MemberSubscription{
			member_id:        'member-1'
			topics:           ['topic-a']
			rack_id:          'rack-1'
			owned_partitions: []
		},
		MemberSubscription{
			member_id:        'member-2'
			topics:           ['topic-a']
			rack_id:          'rack-2'
			owned_partitions: []
		},
	]

	topics := {
		'topic-a': TopicMetadata{
			topic_name:      'topic-a'
			partition_count: 4
		}
	}

	assignments := assignor.assign(members, topics) or {
		assert false, 'Assignment failed'
		return
	}

	// All partitions should be assigned
	total := assignments['member-1'].len + assignments['member-2'].len
	assert total == 4
}
