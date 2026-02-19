// Unit Tests - Service Layer: Offset Manager
module offset

import domain
import infra.observability
import infra.storage.plugins.memory
import service.port

// Helper: Create test storage
fn create_test_storage() port.StoragePort {
	return memory.new_memory_adapter()
}

// Helper: Create test logger
fn create_test_logger() &observability.Logger {
	return observability.get_named_logger('offset.test')
}

// Helper: Create test OffsetManager with shared storage
fn create_test_manager_with_storage(storage port.StoragePort) &OffsetManager {
	logger := create_test_logger()
	return new_offset_manager(storage, logger)
}

// Helper: Create test OffsetManager
fn create_test_manager() &OffsetManager {
	storage := create_test_storage()
	return create_test_manager_with_storage(storage)
}

// OffsetManager.commit_offsets() Tests

fn test_commit_offsets_success() {
	mut manager := create_test_manager()

	// Prepare test data
	offsets := [
		domain.PartitionOffset{
			topic:        'test-topic'
			partition:    0
			offset:       100
			leader_epoch: -1
			metadata:     'test-metadata'
		},
		domain.PartitionOffset{
			topic:        'test-topic'
			partition:    1
			offset:       200
			leader_epoch: -1
			metadata:     ''
		},
	]

	req := OffsetCommitRequest{
		group_id: 'test-group'
		offsets:  offsets
	}

	// Execute
	resp := manager.commit_offsets(req) or { panic('commit_offsets failed: ${err}') }

	// Verify
	assert resp.results.len == 2
	assert resp.results[0].error_code == 0
	assert resp.results[0].topic == 'test-topic'
	assert resp.results[0].partition == 0
	assert resp.results[1].error_code == 0
	assert resp.results[1].partition == 1
}

fn test_commit_offsets_empty_group_id() {
	mut manager := create_test_manager()

	req := OffsetCommitRequest{
		group_id: ''
		offsets:  []
	}

	// Execute - should fail
	manager.commit_offsets(req) or {
		assert err.msg().contains('invalid group_id')
		return
	}

	assert false, 'should have failed with invalid group_id'
}

fn test_commit_offsets_empty_offsets() {
	mut manager := create_test_manager()

	req := OffsetCommitRequest{
		group_id: 'test-group'
		offsets:  []
	}

	// Execute
	resp := manager.commit_offsets(req) or { panic('commit_offsets failed: ${err}') }

	// Verify - should return empty results
	assert resp.results.len == 0
}

// OffsetManager.fetch_offsets() Tests

fn test_fetch_offsets_success() {
	// Create shared storage and topic
	mut storage := create_test_storage()
	storage.create_topic('test-topic', 2, domain.TopicConfig{}) or {
		panic('failed to create topic: ${err}')
	}

	// Create manager with shared storage
	mut manager := create_test_manager_with_storage(storage)

	// First, commit some offsets
	commit_req := OffsetCommitRequest{
		group_id: 'test-group'
		offsets:  [
			domain.PartitionOffset{
				topic:     'test-topic'
				partition: 0
				offset:    100
				metadata:  'meta1'
			},
			domain.PartitionOffset{
				topic:     'test-topic'
				partition: 1
				offset:    200
				metadata:  'meta2'
			},
		]
	}
	manager.commit_offsets(commit_req) or { panic('commit failed: ${err}') }

	// Now fetch them
	fetch_req := OffsetFetchRequest{
		group_id:       'test-group'
		partitions:     [
			domain.TopicPartition{
				topic:     'test-topic'
				partition: 0
			},
			domain.TopicPartition{
				topic:     'test-topic'
				partition: 1
			},
		]
		require_stable: false
	}

	resp := manager.fetch_offsets(fetch_req) or { panic('fetch_offsets failed: ${err}') }

	// Verify
	assert resp.error_code == 0
	assert resp.results.len == 2
	assert resp.results[0].topic == 'test-topic'
	assert resp.results[0].partition == 0
	assert resp.results[0].committed_offset == 100
	// Note: MemoryStorageAdapter doesn't store metadata
	assert resp.results[0].error_code == 0

	assert resp.results[1].partition == 1
	assert resp.results[1].committed_offset == 200
}

fn test_fetch_offsets_not_committed() {
	mut manager := create_test_manager()

	// Fetch offsets that were never committed
	fetch_req := OffsetFetchRequest{
		group_id:       'test-group'
		partitions:     [
			domain.TopicPartition{
				topic:     'test-topic'
				partition: 0
			},
		]
		require_stable: false
	}

	resp := manager.fetch_offsets(fetch_req) or { panic('fetch_offsets failed: ${err}') }

	// Verify - should return -1 for uncommitted offsets
	assert resp.error_code == 0
	assert resp.results.len == 1
	assert resp.results[0].committed_offset == -1
}

fn test_fetch_offsets_empty_group_id() {
	mut manager := create_test_manager()

	req := OffsetFetchRequest{
		group_id:       ''
		partitions:     []
		require_stable: false
	}

	// Execute
	resp := manager.fetch_offsets(req) or { panic('fetch_offsets failed: ${err}') }

	// Verify - should return error code
	assert resp.error_code == i16(domain.ErrorCode.invalid_group_id)
}

fn test_fetch_offsets_empty_partitions() {
	mut manager := create_test_manager()

	req := OffsetFetchRequest{
		group_id:       'test-group'
		partitions:     []
		require_stable: false
	}

	// Execute
	resp := manager.fetch_offsets(req) or { panic('fetch_offsets failed: ${err}') }

	// Verify - should return empty results
	assert resp.error_code == 0
	assert resp.results.len == 0
}

// OffsetManager Integration Tests

fn test_commit_and_fetch_multiple_groups() {
	mut manager := create_test_manager()

	// Commit offsets for group1
	manager.commit_offsets(OffsetCommitRequest{
		group_id: 'group1'
		offsets:  [
			domain.PartitionOffset{
				topic:     'topic1'
				partition: 0
				offset:    100
			},
		]
	}) or { panic(err) }

	// Commit offsets for group2
	manager.commit_offsets(OffsetCommitRequest{
		group_id: 'group2'
		offsets:  [
			domain.PartitionOffset{
				topic:     'topic1'
				partition: 0
				offset:    200
			},
		]
	}) or { panic(err) }

	// Fetch group1 offsets
	resp1 := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   'group1'
		partitions: [domain.TopicPartition{
			topic:     'topic1'
			partition: 0
		}]
	}) or { panic(err) }

	// Fetch group2 offsets
	resp2 := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   'group2'
		partitions: [domain.TopicPartition{
			topic:     'topic1'
			partition: 0
		}]
	}) or { panic(err) }

	// Verify - groups should have independent offsets
	assert resp1.results[0].committed_offset == 100
	assert resp2.results[0].committed_offset == 200
}

fn test_commit_overwrites_previous_offset() {
	mut manager := create_test_manager()

	// First commit
	manager.commit_offsets(OffsetCommitRequest{
		group_id: 'test-group'
		offsets:  [
			domain.PartitionOffset{
				topic:     'test-topic'
				partition: 0
				offset:    100
			},
		]
	}) or { panic(err) }

	// Second commit (overwrite)
	manager.commit_offsets(OffsetCommitRequest{
		group_id: 'test-group'
		offsets:  [
			domain.PartitionOffset{
				topic:     'test-topic'
				partition: 0
				offset:    200
			},
		]
	}) or { panic(err) }

	// Fetch
	resp := manager.fetch_offsets(OffsetFetchRequest{
		group_id:   'test-group'
		partitions: [domain.TopicPartition{
			topic:     'test-topic'
			partition: 0
		}]
	}) or { panic(err) }

	// Verify - should have the latest offset
	assert resp.results[0].committed_offset == 200
}
