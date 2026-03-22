/// Tests for partition-level leader election.
/// Covers: registration, election from ISR, unclean election,
/// broker failure failover, epoch tracking, preferred leader, and concurrency.
module cluster

// -- Helpers --

fn create_isr_mgr_for_leader_test() &IsrManager {
	config := IsrConfig{
		replica_lag_time_max_ms:  30000
		replica_lag_max_messages: 4000
		isr_check_interval_ms:    5000
		min_insync_replicas:      1
	}
	return new_isr_manager(config)
}

fn make_test_elector_with_isr(mut isr_mgr IsrManager) &PartitionLeaderElector {
	config := LeaderElectionConfig{
		preferred_leader_check_interval_ms: 300000
	}
	return new_partition_leader_elector(mut isr_mgr, config)
}

fn make_test_elector_unclean(mut isr_mgr IsrManager) &PartitionLeaderElector {
	config := LeaderElectionConfig{
		preferred_leader_check_interval_ms: 300000
		unclean_leader_election_enable:     true
	}
	return new_partition_leader_elector(mut isr_mgr, config)
}

fn setup_isr_with_partitions(mut isr_mgr IsrManager) {
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])
	isr_mgr.init_partition_isr('orders', 1, 2, [2, 3, 1])
	isr_mgr.init_partition_isr('events', 0, 3, [3, 1, 2])
}

// -- Test: Register partition and verify initial leader --

fn test_register_partition_sets_initial_leader() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or {
		assert false, 'register_partition should not fail: ${err}'
		return
	}

	leadership := elector.get_leader('orders', 0) or {
		assert false, 'get_leader should return leadership after register'
		return
	}
	assert leadership.leader_id == 1, 'Initial leader should be first replica (1)'
	assert leadership.leader_epoch == 1, 'Initial epoch should be 1'
	assert leadership.preferred_leader == 1, 'Preferred leader should be first replica'
	assert leadership.replicas == [1, 2, 3]
	assert leadership.topic == 'orders'
	assert leadership.partition == 0
}

// -- Test: Leader election picks first ISR member --

fn test_elect_leader_picks_first_isr_member() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or {
		assert false, 'register failed: ${err}'
		return
	}

	// Simulate broker 1 leaving ISR
	isr_mgr.remove_broker_from_all_isr(1)

	// Elect new leader
	leadership := elector.elect_leader('orders', 0) or {
		assert false, 'elect_leader should succeed: ${err}'
		return
	}

	// Should pick the first available ISR member in replica order
	assert leadership.leader_id == 2, 'New leader should be broker 2 (first in ISR by replica order), got ${leadership.leader_id}'
	assert leadership.leader_epoch == 2, 'Epoch should increment to 2'
}

// -- Test: Leader election with empty ISR (unclean disabled) --

fn test_elect_leader_empty_isr_unclean_disabled_returns_error() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or {
		assert false, 'register failed: ${err}'
		return
	}

	// Remove all brokers from ISR
	isr_mgr.remove_broker_from_all_isr(1)
	isr_mgr.remove_broker_from_all_isr(2)
	isr_mgr.remove_broker_from_all_isr(3)

	// Should fail because ISR is empty and unclean election is disabled
	elector.elect_leader('orders', 0) or {
		assert err.msg().contains('no eligible'), 'Error should mention no eligible replicas: ${err}'
		return
	}
	assert false, 'elect_leader should return error when ISR empty and unclean disabled'
}

// -- Test: Leader election with empty ISR (unclean enabled) --

fn test_elect_leader_empty_isr_unclean_enabled_picks_from_replicas() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_unclean(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or {
		assert false, 'register failed: ${err}'
		return
	}

	// Remove all brokers from ISR
	isr_mgr.remove_broker_from_all_isr(1)
	isr_mgr.remove_broker_from_all_isr(2)
	isr_mgr.remove_broker_from_all_isr(3)

	// Unclean election: should pick from replicas
	leadership := elector.elect_leader('orders', 0) or {
		assert false, 'elect_leader should succeed with unclean election: ${err}'
		return
	}
	assert leadership.leader_id == 1, 'Unclean election should pick first replica (1), got ${leadership.leader_id}'
	assert leadership.leader_epoch == 2, 'Epoch should increment to 2'
}

// -- Test: on_broker_failed triggers re-election for affected partitions --

fn test_on_broker_failed_triggers_reelection() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	setup_isr_with_partitions(mut isr_mgr)

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }
	elector.register_partition('orders', 1, [2, 3, 1]) or { return }
	elector.register_partition('events', 0, [3, 1, 2]) or { return }

	// Broker 1 is leader of orders-0, remove from ISR first
	isr_mgr.remove_broker_from_all_isr(1)

	// Trigger failure
	results := elector.on_broker_failed(1)

	// Only orders-0 had broker 1 as leader
	assert results.len == 1, 'Expected 1 re-election (orders-0), got ${results.len}'
	assert results[0].topic == 'orders'
	assert results[0].partition == 0
	assert results[0].leader_id != 1, 'New leader should not be failed broker'
}

// -- Test: on_broker_failed with no affected partitions --

fn test_on_broker_failed_no_affected_partitions() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }

	// Broker 99 is not leader of any partition
	results := elector.on_broker_failed(99)
	assert results.len == 0, 'No partitions should be affected'
}

// -- Test: Leader epoch increments on each election --

fn test_leader_epoch_increments_on_each_election() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }

	// Initial epoch = 1
	l1 := elector.get_leader('orders', 0) or { return }
	assert l1.leader_epoch == 1

	// Remove broker 1 from ISR and elect
	isr_mgr.remove_broker_from_all_isr(1)
	l2 := elector.elect_leader('orders', 0) or { return }
	assert l2.leader_epoch == 2, 'Second election epoch should be 2'

	// Re-add broker 1, remove broker 2, elect again
	isr_mgr.init_partition_isr('orders', 0, 2, [1, 2, 3])
	isr_mgr.remove_broker_from_all_isr(2)
	l3 := elector.elect_leader('orders', 0) or { return }
	assert l3.leader_epoch == 3, 'Third election epoch should be 3'
}

// -- Test: Preferred leader election --

fn test_preferred_leader_election_switches_to_preferred() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }

	// Simulate failover: remove broker 1, elect broker 2
	isr_mgr.remove_broker_from_all_isr(1)
	elector.elect_leader('orders', 0) or { return }

	// Broker 1 comes back - re-add to ISR
	isr_mgr.init_partition_isr('orders', 0, 2, [1, 2, 3])

	// Trigger preferred leader election
	leadership := elector.trigger_preferred_leader_election('orders', 0) or {
		assert false, 'Preferred leader election should succeed: ${err}'
		return
	}
	assert leadership.leader_id == 1, 'Should switch back to preferred leader (1), got ${leadership.leader_id}'
	assert leadership.leader_epoch == 3, 'Epoch should be 3 after two elections'
}

// -- Test: Preferred leader not in ISR --

fn test_preferred_leader_not_in_isr_no_switch() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }

	// Simulate failover: broker 1 fails
	isr_mgr.remove_broker_from_all_isr(1)
	elector.elect_leader('orders', 0) or { return }

	// Preferred leader (1) is still out of ISR
	// trigger_preferred_leader_election should return error
	elector.trigger_preferred_leader_election('orders', 0) or {
		assert err.msg().contains('not in ISR') || err.msg().contains('already the leader'), 'Error should explain why: ${err}'
		return
	}
	assert false, 'Should return error when preferred leader not in ISR'
}

// -- Test: Unknown partition handling --

fn test_get_leader_unknown_partition_returns_none() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	mut elector := make_test_elector_with_isr(mut isr_mgr)

	result := elector.get_leader('nonexistent', 0)
	assert result == none, 'get_leader for unknown partition should return none'
}

fn test_elect_leader_unknown_partition_returns_error() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	mut elector := make_test_elector_with_isr(mut isr_mgr)

	elector.elect_leader('nonexistent', 0) or {
		assert err.msg().contains('not registered'), 'Error should mention partition not registered: ${err}'
		return
	}
	assert false, 'elect_leader for unknown partition should return error'
}

// -- Test: Multiple partitions per topic --

fn test_multiple_partitions_per_topic() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])
	isr_mgr.init_partition_isr('orders', 1, 2, [2, 3, 1])
	isr_mgr.init_partition_isr('orders', 2, 3, [3, 1, 2])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }
	elector.register_partition('orders', 1, [2, 3, 1]) or { return }
	elector.register_partition('orders', 2, [3, 1, 2]) or { return }

	l0 := elector.get_leader('orders', 0) or { return }
	l1 := elector.get_leader('orders', 1) or { return }
	l2 := elector.get_leader('orders', 2) or { return }

	assert l0.leader_id == 1
	assert l1.leader_id == 2
	assert l2.leader_id == 3

	// Each has independent epoch
	assert l0.leader_epoch == 1
	assert l1.leader_epoch == 1
	assert l2.leader_epoch == 1
}

// -- Test: get_all_leaders returns all registered partitions --

fn test_get_all_leaders_returns_all_registered() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])
	isr_mgr.init_partition_isr('events', 0, 2, [2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }
	elector.register_partition('events', 0, [2, 3]) or { return }

	all := elector.get_all_leaders()
	assert all.len == 2, 'Expected 2 leaders, got ${all.len}'
	assert 'orders-0' in all
	assert 'events-0' in all
}

// -- Test: Concurrent access safety --

fn test_concurrent_leader_election_access() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])
	isr_mgr.init_partition_isr('orders', 1, 2, [2, 3, 1])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }
	elector.register_partition('orders', 1, [2, 3, 1]) or { return }

	t1 := spawn fn [mut elector] () {
		for _ in 0 .. 30 {
			_ = elector.get_leader('orders', 0)
			_ = elector.get_all_leaders()
		}
	}()
	t2 := spawn fn [mut elector] () {
		for _ in 0 .. 30 {
			_ = elector.get_leader('orders', 1)
			_ = elector.get_all_leaders()
		}
	}()
	t3 := spawn fn [mut elector, mut isr_mgr] () {
		for _ in 0 .. 10 {
			elector.elect_leader('orders', 0) or {}
			elector.elect_leader('orders', 1) or {}
		}
	}()

	t1.wait()
	t2.wait()
	t3.wait()

	// Verify no crash and state is valid
	l0 := elector.get_leader('orders', 0) or {
		assert false, 'orders-0 should still have a leader after concurrent access'
		return
	}
	assert l0.leader_id > 0, 'Leader ID should be positive'
	assert l0.leader_epoch >= 1, 'Epoch should be at least 1'
}

// -- Test: Register partition twice overwrites --

fn test_register_partition_twice_resets_state() {
	mut isr_mgr := create_isr_mgr_for_leader_test()
	isr_mgr.init_partition_isr('orders', 0, 1, [1, 2, 3])

	mut elector := make_test_elector_with_isr(mut isr_mgr)
	elector.register_partition('orders', 0, [1, 2, 3]) or { return }

	// Re-register with different replicas
	isr_mgr.init_partition_isr('orders', 0, 5, [5, 6])
	elector.register_partition('orders', 0, [5, 6]) or { return }

	l := elector.get_leader('orders', 0) or { return }
	assert l.leader_id == 5, 'Re-register should set new leader (5), got ${l.leader_id}'
	assert l.leader_epoch == 1, 'Re-register should reset epoch to 1'
	assert l.replicas == [5, 6]
}
