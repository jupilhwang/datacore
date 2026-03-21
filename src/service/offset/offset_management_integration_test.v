// Offset Management Integration Tests
// Verifies offset commit/fetch behavior, consumer group rebalance scenarios,
// offset reset policies, and idempotent commit behavior.
module offset

import domain
import infra.observability
import infra.storage.plugins.memory
import service.port
import service.group

// --- Test Helpers ---

fn setup_manager_with_topic(topic string, partitions int) !(&OffsetManager, port.StoragePort) {
	mut storage := memory.new_memory_adapter()
	storage.create_topic(topic, partitions, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.integration.test')
	return new_offset_manager(storage, storage, logger), storage
}

fn commit_single_offset(mut manager OffsetManager, group_id string, topic string, partition int, offset_val i64) ! {
	manager.commit_offsets(OffsetCommitRequest{
		group_id: group_id
		offsets:  [
			domain.PartitionOffset{
				topic:     topic
				partition: partition
				offset:    offset_val
			},
		]
	})!
}

fn fetch_single_offset(mut manager OffsetManager, group_id string, topic string, partition int) !i64 {
	resp := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   group_id
		partitions: [
			domain.TopicPartition{
				topic:     topic
				partition: partition
			},
		]
	})!
	if resp.results.len == 0 {
		return error('no result returned')
	}
	return resp.results[0].committed_offset
}

// --- 1. Offset Commit Verification ---

fn test_offset_commit_and_fetch() ! {
	mut manager, _ := setup_manager_with_topic('orders', 3)!

	commit_single_offset(mut manager, 'grp-a', 'orders', 0, 42)!
	commit_single_offset(mut manager, 'grp-a', 'orders', 1, 99)!
	commit_single_offset(mut manager, 'grp-a', 'orders', 2, 7)!

	assert fetch_single_offset(mut manager, 'grp-a', 'orders', 0)! == 42
	assert fetch_single_offset(mut manager, 'grp-a', 'orders', 1)! == 99
	assert fetch_single_offset(mut manager, 'grp-a', 'orders', 2)! == 7
}

fn test_offset_commit_idempotent() ! {
	mut manager, _ := setup_manager_with_topic('events', 1)!

	// Commit the same offset twice - should succeed without error
	commit_single_offset(mut manager, 'grp-idem', 'events', 0, 100)!
	commit_single_offset(mut manager, 'grp-idem', 'events', 0, 100)!

	result := fetch_single_offset(mut manager, 'grp-idem', 'events', 0)!
	assert result == 100
}

fn test_offset_commit_overwrites_previous() ! {
	mut manager, _ := setup_manager_with_topic('logs', 1)!

	commit_single_offset(mut manager, 'grp-overwrite', 'logs', 0, 50)!
	commit_single_offset(mut manager, 'grp-overwrite', 'logs', 0, 150)!

	result := fetch_single_offset(mut manager, 'grp-overwrite', 'logs', 0)!
	// The latest committed offset must win
	assert result == 150
}

fn test_offset_commit_negative_offset_stored() ! {
	// The service layer does not reject negative offsets; storage accepts them.
	// This test documents the current behavior (negative values are stored as-is).
	mut manager, _ := setup_manager_with_topic('stream', 1)!

	commit_single_offset(mut manager, 'grp-neg', 'stream', 0, -2)!

	result := fetch_single_offset(mut manager, 'grp-neg', 'stream', 0)!
	// Negative offset is persisted without error
	assert result == -2
}

fn test_offset_commit_empty_group_id_returns_error() ! {
	mut manager, _ := setup_manager_with_topic('topic-x', 1)!

	manager.commit_offsets(OffsetCommitRequest{
		group_id: ''
		offsets:  [
			domain.PartitionOffset{
				topic:     'topic-x'
				partition: 0
				offset:    1
			},
		]
	}) or {
		assert err.msg().contains('invalid group_id')
		return
	}
	assert false, 'Expected an error for empty group_id'
}

fn test_offset_commit_multiple_topics_in_one_request() ! {
	mut manager, _ := setup_manager_with_topic('alpha', 2)!
	mut storage := memory.new_memory_adapter()
	storage.create_topic('alpha', 2, domain.TopicConfig{})!
	storage.create_topic('beta', 2, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.multi.test')
	mut mgr := new_offset_manager(storage, storage, logger)

	mgr.commit_offsets(OffsetCommitRequest{
		group_id: 'grp-multi'
		offsets:  [
			domain.PartitionOffset{
				topic:     'alpha'
				partition: 0
				offset:    10
			},
			domain.PartitionOffset{
				topic:     'alpha'
				partition: 1
				offset:    20
			},
			domain.PartitionOffset{
				topic:     'beta'
				partition: 0
				offset:    30
			},
			domain.PartitionOffset{
				topic:     'beta'
				partition: 1
				offset:    40
			},
		]
	})!

	assert fetch_single_offset(mut mgr, 'grp-multi', 'alpha', 0)! == 10
	assert fetch_single_offset(mut mgr, 'grp-multi', 'alpha', 1)! == 20
	assert fetch_single_offset(mut mgr, 'grp-multi', 'beta', 0)! == 30
	assert fetch_single_offset(mut mgr, 'grp-multi', 'beta', 1)! == 40
}

// --- 2. Offset Fetch Verification ---

fn test_offset_fetch_nonexistent_group_returns_minus_one() ! {
	mut manager, _ := setup_manager_with_topic('topic-y', 1)!

	result := fetch_single_offset(mut manager, 'grp-ghost', 'topic-y', 0)!
	// An uncommitted offset for an unknown group must return -1
	assert result == -1
}

fn test_offset_fetch_nonexistent_partition_returns_minus_one() ! {
	mut manager, _ := setup_manager_with_topic('topic-z', 2)!

	commit_single_offset(mut manager, 'grp-b', 'topic-z', 0, 5)!

	// Fetch a partition that was never committed
	result := fetch_single_offset(mut manager, 'grp-b', 'topic-z', 1)!
	assert result == -1
}

fn test_offset_fetch_empty_group_id_returns_error_code() ! {
	logger := observability.get_named_logger('offset.fetch.err.test')
	storage := memory.new_memory_adapter()
	mut manager := new_offset_manager(storage, storage, logger)

	resp := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   ''
		partitions: [domain.TopicPartition{
			topic:     't'
			partition: 0
		}]
	})!

	assert resp.error_code == i16(domain.ErrorCode.invalid_group_id)
}

fn test_offset_fetch_empty_partition_list_returns_empty() ! {
	logger := observability.get_named_logger('offset.empty.test')
	storage := memory.new_memory_adapter()
	mut manager := new_offset_manager(storage, storage, logger)

	resp := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   'grp-empty'
		partitions: []
	})!

	assert resp.error_code == 0
	assert resp.results.len == 0
}

fn test_offset_fetch_multiple_groups_independent() ! {
	mut storage := memory.new_memory_adapter()
	storage.create_topic('shared', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.multi.grp.test')
	mut manager := new_offset_manager(storage, storage, logger)

	commit_single_offset(mut manager, 'grp-1', 'shared', 0, 100)!
	commit_single_offset(mut manager, 'grp-2', 'shared', 0, 200)!
	commit_single_offset(mut manager, 'grp-3', 'shared', 0, 300)!

	assert fetch_single_offset(mut manager, 'grp-1', 'shared', 0)! == 100
	assert fetch_single_offset(mut manager, 'grp-2', 'shared', 0)! == 200
	assert fetch_single_offset(mut manager, 'grp-3', 'shared', 0)! == 300
}

// --- 3. Consumer Group Rebalance Offset Preservation ---

fn test_consumer_group_rebalance_offset_preserved_after_member_join() ! {
	mut storage := memory.new_memory_adapter()
	storage.create_topic('rebalance-topic', 2, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.rebalance.test')
	mut manager := new_offset_manager(storage, storage, logger)
	mut coordinator := group.new_group_coordinator(storage)

	// Member A joins group and commits offsets
	join_resp := coordinator.join_group(group.JoinGroupRequest{
		group_id:      'rebalance-grp'
		member_id:     'member-a'
		protocol_type: 'consumer'
		protocols:     [group.Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	})
	assert join_resp.error_code == 0

	commit_single_offset(mut manager, 'rebalance-grp', 'rebalance-topic', 0, 50)!
	commit_single_offset(mut manager, 'rebalance-grp', 'rebalance-topic', 1, 75)!

	// Member B joins group - triggers rebalance
	join_resp2 := coordinator.join_group(group.JoinGroupRequest{
		group_id:      'rebalance-grp'
		member_id:     'member-b'
		protocol_type: 'consumer'
		protocols:     [group.Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	})
	assert join_resp2.error_code == 0

	// Committed offsets must survive the rebalance
	assert fetch_single_offset(mut manager, 'rebalance-grp', 'rebalance-topic', 0)! == 50
	assert fetch_single_offset(mut manager, 'rebalance-grp', 'rebalance-topic', 1)! == 75
}

fn test_consumer_group_rebalance_offset_preserved_after_member_leave() ! {
	mut storage := memory.new_memory_adapter()
	storage.create_topic('leave-topic', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.leave.test')
	mut manager := new_offset_manager(storage, storage, logger)
	mut coordinator := group.new_group_coordinator(storage)

	coordinator.join_group(group.JoinGroupRequest{
		group_id:      'leave-grp'
		member_id:     'member-x'
		protocol_type: 'consumer'
		protocols:     [group.Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	})

	coordinator.join_group(group.JoinGroupRequest{
		group_id:      'leave-grp'
		member_id:     'member-y'
		protocol_type: 'consumer'
		protocols:     [group.Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	})

	commit_single_offset(mut manager, 'leave-grp', 'leave-topic', 0, 120)!

	// Member Y leaves
	err_code := coordinator.leave_group('leave-grp', 'member-y')
	assert err_code == 0

	// Remaining member X should still see the committed offset
	assert fetch_single_offset(mut manager, 'leave-grp', 'leave-topic', 0)! == 120
}

fn test_consumer_group_all_members_removed_then_rejoin_resumes_offset() ! {
	mut storage := memory.new_memory_adapter()
	storage.create_topic('resume-topic', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.resume.test')
	mut manager := new_offset_manager(storage, storage, logger)
	mut coordinator := group.new_group_coordinator(storage)

	// First session: member joins, commits, and leaves
	coordinator.join_group(group.JoinGroupRequest{
		group_id:      'resume-grp'
		member_id:     'member-1'
		protocol_type: 'consumer'
		protocols:     [group.Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	})
	commit_single_offset(mut manager, 'resume-grp', 'resume-topic', 0, 88)!
	coordinator.leave_group('resume-grp', 'member-1')

	// Verify group is now empty
	grp := coordinator.describe_group('resume-grp')!
	assert grp.members.len == 0

	// Second session: new member joins same group - offset must be preserved
	coordinator.join_group(group.JoinGroupRequest{
		group_id:      'resume-grp'
		member_id:     'member-2'
		protocol_type: 'consumer'
		protocols:     [group.Protocol{
			name:     'range'
			metadata: []u8{}
		}]
	})

	assert fetch_single_offset(mut manager, 'resume-grp', 'resume-topic', 0)! == 88
}

// --- 4. Offset Reset Scenarios ---

fn test_offset_reset_earliest() ! {
	// 'earliest' policy: start from the partition's earliest available offset (0).
	// The broker side resets to the log's earliest offset when no committed offset
	// is found. Here we verify the storage fetch returns -1 (no commit) and that
	// fetching after a produce returns offset 0 as the earliest available.
	mut storage := memory.new_memory_adapter()
	storage.create_topic('earliest-topic', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.earliest.test')
	mut manager := new_offset_manager(storage, storage, logger)

	// No committed offset yet
	result := fetch_single_offset(mut manager, 'grp-earliest', 'earliest-topic', 0)!
	assert result == -1

	// After committing offset 0 (earliest), fetch should return 0
	commit_single_offset(mut manager, 'grp-earliest', 'earliest-topic', 0, 0)!
	assert fetch_single_offset(mut manager, 'grp-earliest', 'earliest-topic', 0)! == 0
}

fn test_offset_reset_latest() ! {
	// 'latest' policy: start from the partition's high watermark.
	// We simulate by appending records, checking the high watermark, then committing
	// that offset to represent a consumer that starts from the latest position.
	mut storage := memory.new_memory_adapter()
	storage.create_topic('latest-topic', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.latest.test')
	mut manager := new_offset_manager(storage, storage, logger)

	// Append some records to advance the log
	storage.append('latest-topic', 0, [
		domain.Record{ key: 'k1'.bytes(), value: 'v1'.bytes() },
		domain.Record{
			key:   'k2'.bytes()
			value: 'v2'.bytes()
		},
		domain.Record{
			key:   'k3'.bytes()
			value: 'v3'.bytes()
		},
	], i16(1))!

	// Retrieve the current high watermark (latest offset)
	info := storage.get_partition_info('latest-topic', 0)!
	high_watermark := info.high_watermark

	// Simulate 'latest' reset: consumer commits the high watermark as its start position
	commit_single_offset(mut manager, 'grp-latest', 'latest-topic', 0, high_watermark)!
	result := fetch_single_offset(mut manager, 'grp-latest', 'latest-topic', 0)!
	assert result == high_watermark
}

fn test_offset_reset_none_no_committed_offset() ! {
	// 'none' policy: raise an error when no committed offset exists.
	// Verified by checking storage returns -1 (sentinel for "no commit").
	mut storage := memory.new_memory_adapter()
	storage.create_topic('none-topic', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.none.test')
	mut manager := new_offset_manager(storage, storage, logger)

	result := fetch_single_offset(mut manager, 'grp-none', 'none-topic', 0)!
	// -1 signals "no committed offset"; clients applying the 'none' policy
	// should treat this as an error and raise an OffsetResetException.
	assert result == -1
}

// --- 5. Persistence / Restart Simulation ---

fn test_offset_persistence_across_adapter_instances() ! {
	// Simulate broker restart by replacing the adapter but reusing the same
	// underlying storage state via a shared groups/offsets map.
	// In the memory adapter there is no disk persistence, so we verify that
	// offsets committed in one manager are visible from another manager that
	// wraps the same adapter reference.
	mut storage := memory.new_memory_adapter()
	storage.create_topic('persist-topic', 1, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.persist.test')

	// First manager: commit an offset
	mut manager1 := new_offset_manager(storage, storage, logger)
	commit_single_offset(mut manager1, 'grp-persist', 'persist-topic', 0, 77)!

	// Second manager wrapping the same storage (simulating post-restart load)
	mut manager2 := new_offset_manager(storage, storage, logger)
	result := fetch_single_offset(mut manager2, 'grp-persist', 'persist-topic', 0)!
	assert result == 77
}

fn test_offset_commit_batch_then_fetch_all() ! {
	// Commit offsets for several partitions in a single request and verify each.
	mut storage := memory.new_memory_adapter()
	storage.create_topic('batch-topic', 5, domain.TopicConfig{})!
	logger := observability.get_named_logger('offset.batch.test')
	mut manager := new_offset_manager(storage, storage, logger)

	offsets := [
		domain.PartitionOffset{
			topic:     'batch-topic'
			partition: 0
			offset:    10
		},
		domain.PartitionOffset{
			topic:     'batch-topic'
			partition: 1
			offset:    20
		},
		domain.PartitionOffset{
			topic:     'batch-topic'
			partition: 2
			offset:    30
		},
		domain.PartitionOffset{
			topic:     'batch-topic'
			partition: 3
			offset:    40
		},
		domain.PartitionOffset{
			topic:     'batch-topic'
			partition: 4
			offset:    50
		},
	]

	manager.commit_offsets(OffsetCommitRequest{
		group_id: 'grp-batch'
		offsets:  offsets
	})!

	resp := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   'grp-batch'
		partitions: [
			domain.TopicPartition{
				topic:     'batch-topic'
				partition: 0
			},
			domain.TopicPartition{
				topic:     'batch-topic'
				partition: 1
			},
			domain.TopicPartition{
				topic:     'batch-topic'
				partition: 2
			},
			domain.TopicPartition{
				topic:     'batch-topic'
				partition: 3
			},
			domain.TopicPartition{
				topic:     'batch-topic'
				partition: 4
			},
		]
	})!

	assert resp.error_code == 0
	assert resp.results.len == 5

	// Verify each partition offset
	mut by_partition := map[int]i64{}
	for r in resp.results {
		by_partition[r.partition] = r.committed_offset
	}

	assert by_partition[0] == 10
	assert by_partition[1] == 20
	assert by_partition[2] == 30
	assert by_partition[3] == 40
	assert by_partition[4] == 50
}

fn test_offset_commit_zero_offset() ! {
	// Edge case: offset 0 is a valid committed position (start of log).
	mut manager, _ := setup_manager_with_topic('zero-topic', 1)!

	commit_single_offset(mut manager, 'grp-zero', 'zero-topic', 0, 0)!
	result := fetch_single_offset(mut manager, 'grp-zero', 'zero-topic', 0)!
	assert result == 0
}

fn test_offset_commit_large_offset() ! {
	// Verify that large offsets (near i64 max) are stored correctly.
	mut manager, _ := setup_manager_with_topic('large-topic', 1)!
	large_offset := i64(9_223_372_036_854_775_800) // near i64 max

	commit_single_offset(mut manager, 'grp-large', 'large-topic', 0, large_offset)!
	result := fetch_single_offset(mut manager, 'grp-large', 'large-topic', 0)!
	assert result == large_offset
}

fn test_offset_different_groups_same_topic_same_partition() ! {
	// Two consumer groups consuming the same partition must have independent offsets.
	mut manager, _ := setup_manager_with_topic('shared-topic', 1)!

	commit_single_offset(mut manager, 'grp-consumer-1', 'shared-topic', 0, 111)!
	commit_single_offset(mut manager, 'grp-consumer-2', 'shared-topic', 0, 222)!

	assert fetch_single_offset(mut manager, 'grp-consumer-1', 'shared-topic', 0)! == 111
	assert fetch_single_offset(mut manager, 'grp-consumer-2', 'shared-topic', 0)! == 222
}
