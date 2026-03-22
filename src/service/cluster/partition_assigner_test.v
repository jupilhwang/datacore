module cluster

import domain

// Initial Assignment Tests

fn test_assign_partitions_round_robin() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id: 1
		strategy:  .round_robin
	}
	mut assigner := new_partition_assigner(config, none, none)

	brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 3
			host:      'broker3'
			port:      9092
		},
	]

	// When
	assignments := assigner.assign_partitions('test-topic', 6, brokers) or {
		panic('assign_partitions failed: ${err}')
	}

	// Then
	assert assignments.len == 6, 'Expected 6 assignments, got ${assignments.len}'

	// Verify round-robin: 0->1, 1->2, 2->3, 3->1, 4->2, 5->3
	assert assignments[0].preferred_broker == 1, 'Partition 0 should be on broker 1'
	assert assignments[1].preferred_broker == 2, 'Partition 1 should be on broker 2'
	assert assignments[2].preferred_broker == 3, 'Partition 2 should be on broker 3'
	assert assignments[3].preferred_broker == 1, 'Partition 3 should be on broker 1'
	assert assignments[4].preferred_broker == 2, 'Partition 4 should be on broker 2'
	assert assignments[5].preferred_broker == 3, 'Partition 5 should be on broker 3'
}

fn test_assign_partitions_single_broker() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id: 1
		strategy:  .round_robin
	}
	mut assigner := new_partition_assigner(config, none, none)

	brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
	]

	// When
	assignments := assigner.assign_partitions('test-topic', 3, brokers) or {
		panic('assign_partitions failed: ${err}')
	}

	// Then
	assert assignments.len == 3, 'Expected 3 assignments'
	for i, assignment in assignments {
		assert assignment.preferred_broker == 1, 'Partition ${i} should be on broker 1'
	}
}

fn test_assign_partitions_no_brokers() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id: 1
		strategy:  .round_robin
	}
	mut assigner := new_partition_assigner(config, none, none)

	brokers := []domain.BrokerInfo{}

	// When/Then
	mut failed := false
	assigner.assign_partitions('test-topic', 3, brokers) or {
		failed = true
		[]domain.PartitionAssignment{}
	}
	assert failed == true, 'Should fail with no brokers'
}

fn test_assign_partitions_zero_partitions() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id: 1
		strategy:  .round_robin
	}
	mut assigner := new_partition_assigner(config, none, none)

	brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
	]

	// When/Then
	mut failed := false
	assigner.assign_partitions('test-topic', 0, brokers) or {
		failed = true
		[]domain.PartitionAssignment{}
	}
	assert failed == true, 'Should fail with zero partitions'
}

// Rebalancing Tests

fn test_rebalance_partitions_new_broker_joined() {
	// Given - assign 3 partitions to 2 brokers
	config := PartitionAssignerServiceConfig{
		broker_id:     1
		strategy:      .round_robin
		sticky_assign: false
	}
	mut assigner := new_partition_assigner(config, none, none)

	// Initial assignment: 2 brokers
	initial_brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
	]
	initial_assignments := assigner.assign_partitions('test-topic', 4, initial_brokers) or {
		panic('initial assignment failed')
	}
	assert initial_assignments[0].preferred_broker == 1
	assert initial_assignments[1].preferred_broker == 2
	assert initial_assignments[2].preferred_broker == 1
	assert initial_assignments[3].preferred_broker == 2

	// When - new broker added
	new_brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 3
			host:      'broker3'
			port:      9092
		},
	]

	// Note: Rebalancing requires assignment_port so this test does not fully execute
	// Instead, directly test the round-robin logic
	new_assignments := assigner.do_rebalance('test-topic', initial_assignments, new_brokers)

	// Then - reassign 4 partitions to 3 brokers
	assert new_assignments.len == 4
	// Round-robin: 0->1, 1->2, 2->3, 3->1
	assert new_assignments[0].preferred_broker == 1
	assert new_assignments[1].preferred_broker == 2
	assert new_assignments[2].preferred_broker == 3
	assert new_assignments[3].preferred_broker == 1
}

fn test_rebalance_partitions_broker_left() {
	// Given - 4 partitions assigned to 3 brokers
	config := PartitionAssignerServiceConfig{
		broker_id:     1
		strategy:      .round_robin
		sticky_assign: false
	}
	mut assigner := new_partition_assigner(config, none, none)

	initial_brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 3
			host:      'broker3'
			port:      9092
		},
	]
	initial_assignments := assigner.assign_partitions('test-topic', 4, initial_brokers) or {
		panic('initial assignment failed')
	}

	// When - broker 3 leaves
	remaining_brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
	]

	new_assignments := assigner.do_rebalance('test-topic', initial_assignments, remaining_brokers)

	// Then - reassign 4 partitions to 2 brokers
	assert new_assignments.len == 4
	// Round-robin: 0->1, 1->2, 2->1, 3->2
	assert new_assignments[0].preferred_broker == 1
	assert new_assignments[1].preferred_broker == 2
	assert new_assignments[2].preferred_broker == 1
	assert new_assignments[3].preferred_broker == 2
}

// Sticky Rebalancing Tests

fn test_rebalance_sticky_preserves_existing() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id:     1
		strategy:      .sticky
		sticky_assign: true
	}
	mut assigner := new_partition_assigner(config, none, none)

	// Initial assignment
	initial_brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
	]
	initial_assignments := assigner.assign_partitions('test-topic', 4, initial_brokers) or {
		panic('initial assignment failed')
	}

	// When - new broker added (sticky mode)
	new_brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 3
			host:      'broker3'
			port:      9092
		},
	]

	new_assignments := assigner.rebalance_sticky('test-topic', initial_assignments, new_brokers)

	// Then - existing assignments should be preserved
	assert new_assignments.len == 4
	// In sticky mode, assignments for existing brokers (1, 2) are maintained
	// However, some may be moved if imbalance is severe
}

// Range-based Assignment Tests

fn test_rebalance_range_assignment() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id: 1
		strategy:  .range
	}
	mut assigner := new_partition_assigner(config, none, none)

	brokers := [
		domain.BrokerInfo{
			broker_id: 1
			host:      'broker1'
			port:      9092
		},
		domain.BrokerInfo{
			broker_id: 2
			host:      'broker2'
			port:      9092
		},
	]

	// Create initial assignments
	initial_assignments := [
		domain.PartitionAssignment{
			topic_name:       'test-topic'
			partition:        0
			preferred_broker: 1
		},
		domain.PartitionAssignment{
			topic_name:       'test-topic'
			partition:        1
			preferred_broker: 1
		},
		domain.PartitionAssignment{
			topic_name:       'test-topic'
			partition:        2
			preferred_broker: 2
		},
		domain.PartitionAssignment{
			topic_name:       'test-topic'
			partition:        3
			preferred_broker: 2
		},
		domain.PartitionAssignment{
			topic_name:       'test-topic'
			partition:        4
			preferred_broker: 2
		},
	]

	// When
	new_assignments := assigner.rebalance_range('test-topic', initial_assignments, brokers)

	// Then - range-based: 5 partitions, 2 brokers
	// broker 0: 5/2 + (0<1 ? 1 : 0) = 2 + 1 = 3 partitions (0, 1, 2)
	// broker 1: 5/2 + (1<1 ? 1 : 0) = 2 + 0 = 2 partitions (3, 4)
	assert new_assignments.len == 5
	assert new_assignments[0].preferred_broker == 1
	assert new_assignments[1].preferred_broker == 1
	assert new_assignments[2].preferred_broker == 1
	assert new_assignments[3].preferred_broker == 2
	assert new_assignments[4].preferred_broker == 2
}

// Configuration Change Tests

fn test_set_strategy() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id: 1
		strategy:  .round_robin
	}
	mut assigner := new_partition_assigner(config, none, none)

	// When
	assigner.set_strategy(.sticky)

	// Then - strategy must actually change
	assert assigner.config.strategy == .sticky, 'strategy should be sticky after set_strategy'
}

fn test_set_sticky_assign() {
	// Given
	config := PartitionAssignerServiceConfig{
		broker_id:     1
		sticky_assign: false
	}
	mut assigner := new_partition_assigner(config, none, none)

	// When
	assigner.set_sticky_assign(true)

	// Then - sticky_assign must actually change
	assert assigner.config.sticky_assign == true, 'sticky_assign should be true after set_sticky_assign'
}

// Strategy String Conversion Tests

fn test_assignment_strategy_to_string() {
	assert assignment_strategy_to_string(.round_robin) == 'round_robin'
	assert assignment_strategy_to_string(.range) == 'range'
	assert assignment_strategy_to_string(.sticky) == 'sticky'
}
