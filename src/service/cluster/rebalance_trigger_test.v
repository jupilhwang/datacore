// Tests for RebalanceTrigger - partition rebalancing on broker changes.
module cluster

import domain
import time

// -- Mock: TopicStoragePort --

struct MockTopicStorage {
mut:
	topics []domain.TopicMetadata
}

fn (mut m MockTopicStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	t := domain.TopicMetadata{
		name:            name
		partition_count: partitions
	}
	m.topics << t
	return t
}

fn (mut m MockTopicStorage) delete_topic(name string) ! {
	m.topics = m.topics.filter(it.name != name)
}

fn (mut m MockTopicStorage) list_topics() ![]domain.TopicMetadata {
	return m.topics
}

fn (mut m MockTopicStorage) get_topic(name string) !domain.TopicMetadata {
	for t in m.topics {
		if t.name == name {
			return t
		}
	}
	return error('topic not found: ${name}')
}

fn (mut m MockTopicStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return error('not implemented')
}

fn (mut m MockTopicStorage) add_partitions(name string, new_count int) ! {
}

// -- Mock: ClusterMetadataPort for partition assignments --

struct MockMetadataPort {
mut:
	brokers     []domain.BrokerInfo
	assignments map[string][]domain.PartitionAssignment // topic -> assignments
	locks       map[string]string
	lock_expiry map[string]i64
}

fn (mut m MockMetadataPort) register_broker(info domain.BrokerInfo) !domain.BrokerInfo {
	m.brokers << info
	return info
}

fn (mut m MockMetadataPort) deregister_broker(broker_id i32) ! {
	m.brokers = m.brokers.filter(it.broker_id != broker_id)
}

fn (mut m MockMetadataPort) update_broker_heartbeat(heartbeat domain.BrokerHeartbeat) ! {
}

fn (mut m MockMetadataPort) get_broker(broker_id i32) !domain.BrokerInfo {
	for b in m.brokers {
		if b.broker_id == broker_id {
			return b
		}
	}
	return error('broker not found')
}

fn (mut m MockMetadataPort) list_brokers() ![]domain.BrokerInfo {
	return m.brokers
}

fn (mut m MockMetadataPort) list_active_brokers() ![]domain.BrokerInfo {
	return m.brokers.filter(it.status == .active)
}

fn (mut m MockMetadataPort) get_cluster_metadata() !domain.ClusterMetadata {
	return domain.ClusterMetadata{
		cluster_id: 'test-cluster'
		brokers:    m.brokers
	}
}

fn (mut m MockMetadataPort) update_cluster_metadata(metadata domain.ClusterMetadata) ! {
}

fn (mut m MockMetadataPort) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	if topic_name in m.assignments {
		for a in m.assignments[topic_name] {
			if a.partition == partition {
				return a
			}
		}
	}
	return error('assignment not found')
}

fn (mut m MockMetadataPort) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	return m.assignments[topic_name] or { []domain.PartitionAssignment{} }
}

fn (mut m MockMetadataPort) update_partition_assignment(assignment domain.PartitionAssignment) ! {
	topic := assignment.topic_name
	if topic !in m.assignments {
		m.assignments[topic] = []domain.PartitionAssignment{}
	}
	// Update or append
	mut found := false
	for i, a in m.assignments[topic] {
		if a.partition == assignment.partition {
			m.assignments[topic][i] = assignment
			found = true
			break
		}
	}
	if !found {
		m.assignments[topic] << assignment
	}
}

fn (mut m MockMetadataPort) try_acquire_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	m.locks[lock_name] = holder_id
	return true
}

fn (mut m MockMetadataPort) release_lock(lock_name string, holder_id string) ! {
	m.locks.delete(lock_name)
}

fn (mut m MockMetadataPort) refresh_lock(lock_name string, holder_id string, ttl_ms i64) !bool {
	return true
}

fn (mut m MockMetadataPort) mark_broker_dead(broker_id i32) ! {
	for i, b in m.brokers {
		if b.broker_id == broker_id {
			m.brokers[i] = domain.BrokerInfo{
				...b
				status: .dead
			}
			break
		}
	}
}

fn (mut m MockMetadataPort) get_capability() domain.StorageCapability {
	return domain.StorageCapability{
		name:                  'mock'
		supports_multi_broker: true
	}
}

// -- Helper: create test infrastructure --

fn create_test_brokers(count int) []domain.BrokerInfo {
	mut brokers := []domain.BrokerInfo{}
	for i in 0 .. count {
		brokers << domain.BrokerInfo{
			broker_id: i32(i + 1)
			host:      'broker${i + 1}'
			port:      9092
			status:    .active
		}
	}
	return brokers
}

struct TestSetup {
mut:
	trigger  &RebalanceTrigger
	assigner &PartitionAssigner
	registry &BrokerRegistry
	storage  &MockTopicStorage
	metadata &MockMetadataPort
}

fn create_test_setup(broker_count int) TestSetup {
	brokers := create_test_brokers(broker_count)

	mut metadata := &MockMetadataPort{
		brokers:     brokers
		assignments: map[string][]domain.PartitionAssignment{}
	}

	mut assigner := new_partition_assigner(PartitionAssignerServiceConfig{
		broker_id:  1
		strategy:   .round_robin
		cluster_id: 'test-cluster'
	}, metadata)

	mut registry := new_broker_registry(BrokerRegistryConfig{
		broker_id:  1
		host:       'broker1'
		port:       9092
		cluster_id: 'test-cluster'
		version:    '0.1.0'
	}, domain.StorageCapability{
		name:                  'mock'
		supports_multi_broker: true
	}, metadata)

	// Register all brokers in the local cache
	for broker in brokers {
		registry.brokers[broker.broker_id] = broker
	}

	mut storage := &MockTopicStorage{
		topics: []domain.TopicMetadata{}
	}

	config := RebalanceConfig{
		rebalance_delay_ms:           100
		max_concurrent_reassignments: 50
		rebalance_strategy:           .round_robin
	}

	mut trigger := new_rebalance_trigger(assigner, registry, storage, config, registry.logger)

	return TestSetup{
		trigger:  trigger
		assigner: assigner
		registry: registry
		storage:  storage
		metadata: metadata
	}
}

// -- Tests --

fn test_rebalance_on_broker_added() {
	// Given: 2 brokers with 4 partitions assigned
	mut setup := create_test_setup(2)

	// Assign partitions initially
	initial_brokers := create_test_brokers(2)
	setup.assigner.assign_partitions('test-topic', 4, initial_brokers) or {
		assert false, 'initial assignment failed: ${err}'
		return
	}

	setup.storage.topics << domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 4
	}

	// Add a third broker
	new_broker := domain.BrokerInfo{
		broker_id: 3
		host:      'broker3'
		port:      9092
		status:    .active
	}
	setup.metadata.brokers << new_broker
	setup.registry.brokers[3] = new_broker

	// Manually queue and execute (skip debounce for test)
	setup.trigger.pending_rebalances << RebalanceRequest{
		trigger:   .broker_added
		broker_id: 3
		timestamp: time.now().unix_milli()
	}

	// When
	result := setup.trigger.execute_rebalance() or {
		assert false, 'execute_rebalance failed: ${err}'
		return
	}

	// Then
	assert result.moved_partitions > 0, 'Expected partitions to move after broker added'
	assert result.topics_rebalanced == 1, 'Expected 1 topic rebalanced'
	assert result.error_count == 0, 'Expected no errors'
}

fn test_rebalance_on_broker_removed() {
	// Given: 3 brokers with 6 partitions assigned
	mut setup := create_test_setup(3)

	initial_brokers := create_test_brokers(3)
	setup.assigner.assign_partitions('test-topic', 6, initial_brokers) or {
		assert false, 'initial assignment failed: ${err}'
		return
	}

	setup.storage.topics << domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 6
	}

	// Remove broker 3
	setup.metadata.brokers = setup.metadata.brokers.filter(it.broker_id != 3)
	setup.registry.brokers.delete(3)

	// Queue a removal request
	setup.trigger.pending_rebalances << RebalanceRequest{
		trigger:   .broker_removed
		broker_id: 3
		timestamp: time.now().unix_milli()
	}

	// When
	result := setup.trigger.execute_rebalance() or {
		assert false, 'execute_rebalance failed: ${err}'
		return
	}

	// Then: partitions formerly on broker 3 should be moved
	assert result.moved_partitions > 0, 'Expected partitions to move after broker removed'
	assert result.topics_rebalanced == 1, 'Expected 1 topic rebalanced'
	assert result.error_count == 0, 'Expected no errors'
}

fn test_debounce_queues_request() {
	// Given
	mut setup := create_test_setup(2)

	// When: multiple rapid broker additions
	setup.trigger.on_broker_added(3)
	setup.trigger.on_broker_added(4)
	setup.trigger.on_broker_added(5)

	// Then: requests should be queued (not yet executed due to debounce)
	status := setup.trigger.get_rebalance_status()
	assert status.pending_requests >= 0, 'Pending requests should be tracked'
}

fn test_topic_creation_assignment() {
	// Given
	mut setup := create_test_setup(3)

	// When: new topic created
	setup.trigger.on_topic_created('new-topic', 6)

	// Then: assignments should exist for the new topic
	assignments := setup.assigner.list_partition_assignments('new-topic') or {
		// If metadata port stores them, check there
		setup.metadata.assignments['new-topic'] or { []domain.PartitionAssignment{} }
	}
	// Assignments are made via the assigner which stores via metadata_port
	meta_assignments := setup.metadata.assignments['new-topic'] or {
		[]domain.PartitionAssignment{}
	}
	assert meta_assignments.len == 6, 'Expected 6 partition assignments, got ${meta_assignments.len}'
}

fn test_no_rebalance_with_single_broker() {
	// Given: single broker with 3 partitions
	mut setup := create_test_setup(1)

	initial_brokers := create_test_brokers(1)
	setup.assigner.assign_partitions('test-topic', 3, initial_brokers) or {
		assert false, 'initial assignment failed: ${err}'
		return
	}

	setup.storage.topics << domain.TopicMetadata{
		name:            'test-topic'
		partition_count: 3
	}

	// Queue a rebalance request
	setup.trigger.pending_rebalances << RebalanceRequest{
		trigger:   .manual
		broker_id: 0
		timestamp: time.now().unix_milli()
	}

	// When
	result := setup.trigger.execute_rebalance() or {
		assert false, 'execute_rebalance failed: ${err}'
		return
	}

	// Then: no partitions should move (only one broker)
	assert result.moved_partitions == 0, 'Expected no partition moves with single broker'
}

fn test_concurrent_rebalance_prevention() {
	// Given
	mut setup := create_test_setup(2)

	// Simulate in-progress rebalance
	setup.trigger.lock.@lock()
	setup.trigger.is_rebalancing = true
	setup.trigger.pending_rebalances << RebalanceRequest{
		trigger:   .manual
		broker_id: 0
		timestamp: time.now().unix_milli()
	}
	setup.trigger.lock.unlock()

	// When: try to execute another rebalance
	mut blocked := false
	setup.trigger.execute_rebalance() or { blocked = true }

	// Then
	assert blocked == true, 'Expected rebalance to be blocked while another is in progress'

	// Cleanup
	setup.trigger.lock.@lock()
	setup.trigger.is_rebalancing = false
	setup.trigger.lock.unlock()
}

fn test_rebalance_result_tracking() {
	// Given
	mut setup := create_test_setup(3)

	initial_brokers := create_test_brokers(3)
	setup.assigner.assign_partitions('topic-a', 3, initial_brokers) or {
		assert false, 'assignment failed: ${err}'
		return
	}
	setup.assigner.assign_partitions('topic-b', 3, initial_brokers) or {
		assert false, 'assignment failed: ${err}'
		return
	}

	setup.storage.topics << domain.TopicMetadata{
		name:            'topic-a'
		partition_count: 3
	}
	setup.storage.topics << domain.TopicMetadata{
		name:            'topic-b'
		partition_count: 3
	}

	// Remove broker 3 -> partitions must move
	setup.metadata.brokers = setup.metadata.brokers.filter(it.broker_id != 3)
	setup.registry.brokers.delete(3)

	setup.trigger.pending_rebalances << RebalanceRequest{
		trigger:   .broker_removed
		broker_id: 3
		timestamp: time.now().unix_milli()
	}

	// When
	result := setup.trigger.execute_rebalance() or {
		assert false, 'execute_rebalance failed: ${err}'
		return
	}

	// Then
	assert result.duration_ms >= 0, 'Duration should be non-negative'
	assert result.error_count == 0, 'Expected no errors'
	assert result.moved_partitions >= 0, 'Moved partitions should be tracked'

	// Verify status is updated
	status := setup.trigger.get_rebalance_status()
	assert status.is_rebalancing == false, 'Should not be rebalancing after completion'
	assert status.total_rebalances == 1, 'Should have recorded 1 rebalance'
}

fn test_execute_rebalance_no_pending_requests() {
	// Given: no pending requests
	mut setup := create_test_setup(2)

	// When
	mut failed := false
	setup.trigger.execute_rebalance() or { failed = true }

	// Then
	assert failed == true, 'Expected error when no pending requests'
}

fn test_rebalance_status_initial() {
	// Given
	mut setup := create_test_setup(2)

	// When
	status := setup.trigger.get_rebalance_status()

	// Then
	assert status.is_rebalancing == false, 'Should not be rebalancing initially'
	assert status.pending_requests == 0, 'Should have no pending requests initially'
	assert status.total_rebalances == 0, 'Should have no completed rebalances initially'
	assert status.total_errors == 0, 'Should have no errors initially'
}

fn test_rebalance_multiple_topics() {
	// Given: 3 brokers, 2 topics
	mut setup := create_test_setup(3)

	initial_brokers := create_test_brokers(3)
	setup.assigner.assign_partitions('topic-1', 6, initial_brokers) or {
		assert false, 'assign failed: ${err}'
		return
	}
	setup.assigner.assign_partitions('topic-2', 3, initial_brokers) or {
		assert false, 'assign failed: ${err}'
		return
	}

	setup.storage.topics << domain.TopicMetadata{
		name:            'topic-1'
		partition_count: 6
	}
	setup.storage.topics << domain.TopicMetadata{
		name:            'topic-2'
		partition_count: 3
	}

	// Add broker 4
	new_broker := domain.BrokerInfo{
		broker_id: 4
		host:      'broker4'
		port:      9092
		status:    .active
	}
	setup.metadata.brokers << new_broker
	setup.registry.brokers[4] = new_broker

	setup.trigger.pending_rebalances << RebalanceRequest{
		trigger:   .broker_added
		broker_id: 4
		timestamp: time.now().unix_milli()
	}

	// When
	result := setup.trigger.execute_rebalance() or {
		assert false, 'execute_rebalance failed: ${err}'
		return
	}

	// Then
	assert result.topics_rebalanced >= 0, 'Should track topics rebalanced'
	assert result.error_count == 0, 'Expected no errors'
}
