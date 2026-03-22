module cluster

import time

// -- Helper --

fn make_test_isr_manager() &IsrManager {
	config := IsrConfig{
		replica_lag_time_max_ms:  30000
		replica_lag_max_messages: 4000
		isr_check_interval_ms:    5000
		min_insync_replicas:      2
	}
	return new_isr_manager(config)
}

fn make_partition_key(topic string, partition int) string {
	return '${topic}-${partition}'
}

// -- Test: ISR initialization --

fn test_init_partition_isr_sets_all_replicas_in_sync() {
	mut mgr := make_test_isr_manager()
	replicas := [1, 2, 3]

	mgr.init_partition_isr('test-topic', 0, 1, replicas)

	isr := mgr.get_isr('test-topic', 0)
	assert isr.len == 3, 'Expected all 3 replicas in ISR, got ${isr.len}'
	assert 1 in isr
	assert 2 in isr
	assert 3 in isr
}

fn test_init_partition_isr_sets_leader_and_replicas() {
	mut mgr := make_test_isr_manager()

	mgr.init_partition_isr('topic-a', 1, 2, [2, 3, 4])

	state := mgr.get_isr_state('topic-a', 1) or {
		assert false, 'Expected ISR state to exist'
		return
	}
	assert state.leader_id == 2
	assert state.all_replicas == [2, 3, 4]
}

// -- Test: Replica offset update --

fn test_update_replica_offset_tracks_offset() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	mgr.update_replica_offset('test-topic', 0, 2, 100)

	state := mgr.get_isr_state('test-topic', 0) or {
		assert false, 'Expected ISR state'
		return
	}
	assert state.last_offset[2] == 100
}

fn test_update_replica_offset_updates_caught_up_time() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2])

	before := time.now().unix_milli()
	mgr.update_replica_offset('test-topic', 0, 2, 50)
	after := time.now().unix_milli()

	state := mgr.get_isr_state('test-topic', 0) or {
		assert false, 'Expected ISR state'
		return
	}
	caught_up := state.last_caught_up[2]
	assert caught_up >= before, 'last_caught_up should be >= before time'
	assert caught_up <= after, 'last_caught_up should be <= after time'
}

// -- Test: ISR shrink --

fn test_check_isr_shrinks_lagging_replica() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	// Simulate: broker 3 has old caught-up time (beyond lag threshold)
	mgr.set_last_caught_up_for_test('test-topic', 0, 3, time.now().unix_milli() - 60000)
	// Update leader offset to create lag
	mgr.update_replica_offset('test-topic', 0, 1, 1000)
	mgr.update_replica_offset('test-topic', 0, 2, 1000)

	mgr.check_isr()

	isr := mgr.get_isr('test-topic', 0)
	assert 3 !in isr, 'Broker 3 should be removed from ISR due to lag'
	assert 1 in isr
	assert 2 in isr
}

// -- Test: ISR expand --

fn test_check_isr_expands_caught_up_replica() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	// First, shrink broker 3 out of ISR
	mgr.set_last_caught_up_for_test('test-topic', 0, 3, time.now().unix_milli() - 60000)
	mgr.update_replica_offset('test-topic', 0, 1, 1000)
	mgr.update_replica_offset('test-topic', 0, 2, 1000)
	mgr.check_isr()

	// Verify broker 3 is out
	isr_before := mgr.get_isr('test-topic', 0)
	assert 3 !in isr_before, 'Broker 3 should be out of ISR before expand'

	// Now broker 3 catches up
	mgr.update_replica_offset('test-topic', 0, 3, 1000)
	mgr.check_isr()

	isr_after := mgr.get_isr('test-topic', 0)
	assert 3 in isr_after, 'Broker 3 should be back in ISR after catching up'
}

// -- Test: min.insync.replicas check --

fn test_is_isr_sufficient_returns_true_when_enough() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	result := mgr.is_isr_sufficient('test-topic', 0)
	assert result == true, 'ISR with 3 replicas should be sufficient (min=2)'
}

fn test_is_isr_sufficient_returns_false_when_not_enough() {
	mut mgr := make_test_isr_manager()
	// min_insync_replicas = 2, but only 1 replica
	mgr.init_partition_isr('test-topic', 0, 1, [1])

	result := mgr.is_isr_sufficient('test-topic', 0)
	assert result == false, 'ISR with 1 replica should be insufficient (min=2)'
}

fn test_is_isr_sufficient_returns_false_for_unknown_partition() {
	mut mgr := make_test_isr_manager()

	result := mgr.is_isr_sufficient('unknown-topic', 99)
	assert result == false, 'Unknown partition should return false'
}

// -- Test: High watermark --

fn test_get_high_watermark_returns_min_of_isr_offsets() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	mgr.update_replica_offset('test-topic', 0, 1, 100)
	mgr.update_replica_offset('test-topic', 0, 2, 80)
	mgr.update_replica_offset('test-topic', 0, 3, 90)

	hw := mgr.get_high_watermark('test-topic', 0)
	assert hw == 80, 'High watermark should be min offset (80), got ${hw}'
}

fn test_get_high_watermark_returns_zero_for_unknown_partition() {
	mut mgr := make_test_isr_manager()

	hw := mgr.get_high_watermark('unknown', 0)
	assert hw == 0, 'High watermark for unknown partition should be 0'
}

fn test_get_high_watermark_ignores_non_isr_replicas() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	// Set broker 3 offset first, then override its caught_up to be stale
	mgr.update_replica_offset('test-topic', 0, 3, 10)
	mgr.set_last_caught_up_for_test('test-topic', 0, 3, time.now().unix_milli() - 60000)
	mgr.update_replica_offset('test-topic', 0, 1, 100)
	mgr.update_replica_offset('test-topic', 0, 2, 90)
	mgr.check_isr()

	hw := mgr.get_high_watermark('test-topic', 0)
	// Only ISR members (1, 2) considered; min is 90
	assert hw == 90, 'High watermark should ignore non-ISR replica, expected 90, got ${hw}'
}

// -- Test: Broker removal from all ISRs --

fn test_remove_broker_from_all_isr() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('topic-a', 0, 1, [1, 2, 3])
	mgr.init_partition_isr('topic-b', 0, 1, [1, 2, 4])

	mgr.remove_broker_from_all_isr(2)

	isr_a := mgr.get_isr('topic-a', 0)
	assert 2 !in isr_a, 'Broker 2 should be removed from topic-a ISR'
	assert 1 in isr_a
	assert 3 in isr_a

	isr_b := mgr.get_isr('topic-b', 0)
	assert 2 !in isr_b, 'Broker 2 should be removed from topic-b ISR'
	assert 1 in isr_b
	assert 4 in isr_b
}

fn test_remove_broker_not_in_isr_is_noop() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2])

	mgr.remove_broker_from_all_isr(99)

	isr := mgr.get_isr('test-topic', 0)
	assert isr.len == 2, 'ISR should remain unchanged'
}

// -- Test: Edge cases --

fn test_get_isr_returns_empty_for_unknown_partition() {
	mut mgr := make_test_isr_manager()

	isr := mgr.get_isr('no-such-topic', 0)
	assert isr.len == 0, 'Unknown partition should return empty ISR'
}

fn test_single_replica_partition() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1])

	isr := mgr.get_isr('test-topic', 0)
	assert isr.len == 1
	assert isr[0] == 1

	hw := mgr.get_high_watermark('test-topic', 0)
	assert hw == 0, 'Initial high watermark for single replica should be 0'

	mgr.update_replica_offset('test-topic', 0, 1, 500)
	hw2 := mgr.get_high_watermark('test-topic', 0)
	assert hw2 == 500, 'High watermark should reflect single ISR member offset'
}

fn test_reinit_partition_overwrites_previous_state() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2])
	mgr.update_replica_offset('test-topic', 0, 1, 100)

	// Re-init with different replicas
	mgr.init_partition_isr('test-topic', 0, 3, [3, 4, 5])

	isr := mgr.get_isr('test-topic', 0)
	assert isr.len == 3
	assert 3 in isr
	assert 4 in isr
	assert 5 in isr
	assert 1 !in isr

	state := mgr.get_isr_state('test-topic', 0) or {
		assert false, 'Expected ISR state'
		return
	}
	assert state.leader_id == 3
}

// -- Test: Concurrent access safety --

fn test_concurrent_isr_access() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('test-topic', 0, 1, [1, 2, 3])

	// Use separate spawn calls with shared mutable state
	// to validate thread-safety without hitting V thread join limits
	t1 := spawn fn [mut mgr] () {
		for j in 0 .. 50 {
			mgr.update_replica_offset('test-topic', 0, 1, i64(j))
			_ = mgr.get_isr('test-topic', 0)
		}
	}()
	t2 := spawn fn [mut mgr] () {
		for j in 0 .. 50 {
			mgr.update_replica_offset('test-topic', 0, 2, i64(j))
			_ = mgr.get_high_watermark('test-topic', 0)
		}
	}()
	t3 := spawn fn [mut mgr] () {
		for j in 0 .. 50 {
			mgr.update_replica_offset('test-topic', 0, 3, i64(j))
			_ = mgr.is_isr_sufficient('test-topic', 0)
		}
	}()

	t1.wait()
	t2.wait()
	t3.wait()

	// Verify state is consistent (no crash, valid ISR)
	isr := mgr.get_isr('test-topic', 0)
	assert isr.len > 0, 'ISR should not be empty after concurrent access'
	assert isr.len <= 3, 'ISR should not exceed total replicas'
}

// -- Test: Multiple partitions tracked independently --

fn test_multiple_partitions_independent() {
	mut mgr := make_test_isr_manager()
	mgr.init_partition_isr('topic-a', 0, 1, [1, 2])
	mgr.init_partition_isr('topic-a', 1, 2, [2, 3])

	mgr.update_replica_offset('topic-a', 0, 1, 100)
	mgr.update_replica_offset('topic-a', 0, 2, 50)
	mgr.update_replica_offset('topic-a', 1, 2, 200)
	mgr.update_replica_offset('topic-a', 1, 3, 200)

	hw0 := mgr.get_high_watermark('topic-a', 0)
	hw1 := mgr.get_high_watermark('topic-a', 1)

	assert hw0 == 50, 'Partition 0 HW should be 50, got ${hw0}'
	assert hw1 == 200, 'Partition 1 HW should be 200, got ${hw1}'
}
