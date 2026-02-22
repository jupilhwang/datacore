// Failover tests for Share Group state synchronization and recovery.
// Simulates multi-broker environments, consumer crash/restart, poison messages,
// and broker restart scenarios using a persistent mock storage backend.
module group

import domain
import service.port
import time

// -- Persistent Mock Storage --
// Simulates a shared PostgreSQL backend that survives broker restarts.

struct PersistentMockStorage {
mut:
	topics         map[string]domain.TopicMetadata
	partitions     map[string]domain.PartitionInfo
	records        map[string][]domain.Record
	share_states   map[string]domain.SharePartitionState
	groups         map[string]domain.ConsumerGroup
	offsets        map[string]map[string]i64
	append_offsets map[string]i64
}

fn new_persistent_mock_storage() &PersistentMockStorage {
	return &PersistentMockStorage{
		topics:         map[string]domain.TopicMetadata{}
		partitions:     map[string]domain.PartitionInfo{}
		records:        map[string][]domain.Record{}
		share_states:   map[string]domain.SharePartitionState{}
		groups:         map[string]domain.ConsumerGroup{}
		offsets:        map[string]map[string]i64{}
		append_offsets: map[string]i64{}
	}
}

fn (mut s PersistentMockStorage) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	meta := domain.TopicMetadata{
		name:            name
		topic_id:        generate_test_topic_id(name)
		partition_count: partitions
	}
	s.topics[name] = meta

	// Initialize partition info and record stores
	for i in 0 .. partitions {
		key := '${name}:${i}'
		s.partitions[key] = domain.PartitionInfo{
			topic:           name
			partition:       i
			earliest_offset: 0
			latest_offset:   0
			high_watermark:  0
		}
		s.records[key] = []domain.Record{}
		s.append_offsets[key] = 0
	}
	return meta
}

fn (s PersistentMockStorage) delete_topic(name string) ! {}

fn (s PersistentMockStorage) list_topics() ![]domain.TopicMetadata {
	mut result := []domain.TopicMetadata{}
	for _, t in s.topics {
		result << t
	}
	return result
}

fn (s PersistentMockStorage) get_topic(name string) !domain.TopicMetadata {
	return s.topics[name] or { return error('topic not found: ${name}') }
}

fn (s PersistentMockStorage) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	for _, t in s.topics {
		if t.topic_id == topic_id {
			return t
		}
	}
	return error('topic not found by id')
}

fn (s PersistentMockStorage) add_partitions(name string, new_count int) ! {}

fn (mut s PersistentMockStorage) append(topic string, partition int, records []domain.Record, required_acks i16) !domain.AppendResult {
	_ = required_acks
	key := '${topic}:${partition}'
	mut base := s.append_offsets[key] or { i64(0) }
	base_offset := base

	for r in records {
		s.records[key] << r
		base += 1
	}
	s.append_offsets[key] = base

	s.partitions[key] = domain.PartitionInfo{
		topic:           topic
		partition:       partition
		earliest_offset: 0
		latest_offset:   base - 1
		high_watermark:  base
	}

	return domain.AppendResult{
		base_offset:      base_offset
		log_append_time:  time.now().unix_milli()
		log_start_offset: 0
		record_count:     records.len
	}
}

fn (s PersistentMockStorage) fetch(topic string, partition int, offset i64, max_bytes int) !domain.FetchResult {
	key := '${topic}:${partition}'
	all_records := s.records[key] or { return domain.FetchResult{} }
	info := s.partitions[key] or { return domain.FetchResult{} }

	mut result := []domain.Record{}
	start := int(offset)
	if start < all_records.len {
		end := if start + 100 < all_records.len { start + 100 } else { all_records.len }
		result = all_records[start..end].clone()
	}

	return domain.FetchResult{
		records:        result
		first_offset:   offset
		high_watermark: info.high_watermark
	}
}

fn (s PersistentMockStorage) delete_records(topic string, partition int, before_offset i64) ! {}

fn (s PersistentMockStorage) get_partition_info(topic string, partition int) !domain.PartitionInfo {
	key := '${topic}:${partition}'
	return s.partitions[key] or {
		return domain.PartitionInfo{
			topic:           topic
			partition:       partition
			high_watermark:  0
			earliest_offset: 0
		}
	}
}

fn (s PersistentMockStorage) save_group(group domain.ConsumerGroup) ! {}

fn (s PersistentMockStorage) load_group(group_id string) !domain.ConsumerGroup {
	return error('not found')
}

fn (s PersistentMockStorage) delete_group(group_id string) ! {}

fn (s PersistentMockStorage) list_groups() ![]domain.GroupInfo {
	return []domain.GroupInfo{}
}

fn (s PersistentMockStorage) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {}

fn (s PersistentMockStorage) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return []domain.OffsetFetchResult{}
}

fn (s PersistentMockStorage) health_check() !port.HealthStatus {
	return .healthy
}

fn (s &PersistentMockStorage) get_storage_capability() domain.StorageCapability {
	return domain.memory_storage_capability
}

fn (mut s PersistentMockStorage) save_share_partition_state(state domain.SharePartitionState) ! {
	key := '${state.group_id}:${state.topic_name}:${state.partition}'
	s.share_states[key] = state
}

fn (s PersistentMockStorage) load_share_partition_state(group_id string, topic_name string, partition i32) ?domain.SharePartitionState {
	key := '${group_id}:${topic_name}:${partition}'
	return s.share_states[key] or { return none }
}

fn (mut s PersistentMockStorage) delete_share_partition_state(group_id string, topic_name string, partition i32) ! {
	key := '${group_id}:${topic_name}:${partition}'
	s.share_states.delete(key)
}

fn (s PersistentMockStorage) load_all_share_partition_states(group_id string) []domain.SharePartitionState {
	mut result := []domain.SharePartitionState{}
	prefix := '${group_id}:'
	for key, state in s.share_states {
		if key.starts_with(prefix) {
			result << state
		}
	}
	return result
}

fn (s &PersistentMockStorage) get_cluster_metadata_port() ?&port.ClusterMetadataPort {
	return none
}

// -- Helper functions --

/// generate_test_topic_id creates a deterministic 16-byte topic ID from a name.
fn generate_test_topic_id(name string) []u8 {
	name_bytes := name.bytes()
	mut id := []u8{len: 16}
	for i in 0 .. 16 {
		if i < name_bytes.len {
			id[i] = name_bytes[i]
		}
	}
	return id
}

/// setup_test_environment creates a coordinator with test topic and pre-populated records.
fn setup_test_environment(record_count int) (&ShareGroupCoordinator, &PersistentMockStorage) {
	mut storage := new_persistent_mock_storage()
	storage.create_topic('test-topic', 1, domain.TopicConfig{}) or {
		panic('topic creation failed')
	}

	// Populate records
	mut records := []domain.Record{}
	for i in 0 .. record_count {
		records << domain.Record{
			key:       'key-${i}'.bytes()
			value:     'value-${i}'.bytes()
			timestamp: time.now()
		}
	}
	storage.append('test-topic', 0, records, 1) or { panic('append failed') }

	config := domain.ShareGroupConfig{
		record_lock_duration_ms: 30000
		delivery_attempt_limit:  5
		max_partition_locks:     200
		heartbeat_interval_ms:   5000
		session_timeout_ms:      45000
	}
	coordinator := new_share_group_coordinator(storage, config)
	return coordinator, storage
}

/// join_consumer adds a consumer to the group and returns the member_id and epoch.
fn join_consumer(mut coordinator ShareGroupCoordinator, group_id string, topic string) (string, i32) {
	req := ShareGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              ''
		member_epoch:           0
		rack_id:                'rack-1'
		subscribed_topic_names: [topic]
	}
	resp := coordinator.heartbeat(req)
	if resp.error_code != 0 {
		panic('heartbeat failed: ${resp.error_message}')
	}
	return resp.member_id, resp.member_epoch
}

/// simulate_consumer_crash removes a consumer from the group, releasing its records.
fn simulate_consumer_crash(mut coordinator ShareGroupCoordinator, group_id string, member_id string) {
	coordinator.leave_group(group_id, member_id) or {}
}

/// create_coordinator_from_storage creates a new coordinator sharing the same storage.
/// Simulates a broker restart or a second broker in a multi-broker environment.
fn create_coordinator_from_storage(storage &PersistentMockStorage) &ShareGroupCoordinator {
	config := domain.ShareGroupConfig{
		record_lock_duration_ms: 30000
		delivery_attempt_limit:  5
		max_partition_locks:     200
		heartbeat_interval_ms:   5000
		session_timeout_ms:      45000
	}
	return new_share_group_coordinator(storage, config)
}

// -- Scenario 1: Consumer failover - no duplicate delivery --
// 1. Consumer A acquires messages 0, 1, 2 (acquired state)
// 2. Consumer A acks messages 0, 1, 2
// 3. Consumer A crashes (leave_group)
// 4. Consumer A rejoins with a new member_id
// 5. State is loaded from persistence; acked messages are NOT re-delivered

fn test_consumer_failover_no_duplicate() {
	mut coordinator, storage := setup_test_environment(10)
	group_id := 'failover-group-1'

	// Step 1: Consumer A joins and acquires records
	member_a, _ := join_consumer(mut coordinator, group_id, 'test-topic')

	// Initialize partition with start_offset=0 so records 0..9 become acquirable
	mut sp := coordinator.get_or_create_partition(group_id, 'test-topic', 0)
	sp.start_offset = 0
	sp.end_offset = 9
	for i in i64(0) .. 10 {
		sp.record_states[i] = .available
	}

	// Acquire records 0, 1, 2
	acquired := coordinator.acquire_records(group_id, member_a, 'test-topic', 0, 3)
	assert acquired.len == 3, 'expected 3 acquired records, got ${acquired.len}'

	// Step 2: Ack records 0, 1, 2
	batch := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      2
		acknowledge_type: .accept
		gap_offsets:      []i64{}
	}
	result := coordinator.acknowledge_records(group_id, member_a, batch)
	assert result.error_code == 0

	// Verify state was persisted
	persisted := storage.load_share_partition_state(group_id, 'test-topic', 0) or {
		assert false, 'state should be persisted'
		return
	}
	// Records 0-2 should be acknowledged (and cleaned from map after SPSO advance)
	// SPSO should have advanced past acknowledged records
	assert persisted.start_offset >= 3, 'SPSO should advance past acknowledged records, got ${persisted.start_offset}'

	// Step 3: Consumer A crashes
	simulate_consumer_crash(mut coordinator, group_id, member_a)

	// Step 4: Simulate broker restart - create a new coordinator with same storage
	mut coordinator2 := create_coordinator_from_storage(storage)

	// Load persisted state
	coordinator2.partition_manager.load_all_states(group_id)

	// Consumer A rejoins (new member_id since it is a new connection)
	member_a2, _ := join_consumer(mut coordinator2, group_id, 'test-topic')

	// Step 5: Acquire records again - should NOT get 0, 1, 2
	acquired2 := coordinator2.acquire_records(group_id, member_a2, 'test-topic', 0, 3)

	// Verify no duplicates: all acquired offsets should be >= 3
	for rec in acquired2 {
		assert rec.offset >= 3, 'duplicate delivery detected: offset ${rec.offset} was already acknowledged'
	}
}

// -- Scenario 2: Cross-consumer state sharing --
// 1. Consumer A acquires message 0
// 2. Consumer B requests messages (should get message 1, not 0)
// 3. Consumer A acks message 0
// 4. Consumer B acquires message 1
// 5. No duplicate consumption

fn test_cross_consumer_state_sharing() {
	mut coordinator, _ := setup_test_environment(10)
	group_id := 'sharing-group-1'

	// Both consumers join the same group
	member_a, _ := join_consumer(mut coordinator, group_id, 'test-topic')
	member_b, _ := join_consumer(mut coordinator, group_id, 'test-topic')

	// Initialize partition state with available records
	mut sp := coordinator.get_or_create_partition(group_id, 'test-topic', 0)
	sp.start_offset = 0
	sp.end_offset = 9
	for i in i64(0) .. 10 {
		sp.record_states[i] = .available
	}

	// Step 1: Consumer A acquires 1 record (should get offset 0)
	acquired_a := coordinator.acquire_records(group_id, member_a, 'test-topic', 0, 1)
	assert acquired_a.len == 1, 'Consumer A should acquire 1 record, got ${acquired_a.len}'
	assert acquired_a[0].offset == 0, 'Consumer A should get offset 0, got ${acquired_a[0].offset}'

	// Step 2: Consumer B acquires 1 record (should get offset 1, NOT 0)
	acquired_b := coordinator.acquire_records(group_id, member_b, 'test-topic', 0, 1)
	assert acquired_b.len == 1, 'Consumer B should acquire 1 record, got ${acquired_b.len}'
	assert acquired_b[0].offset == 1, 'Consumer B should get offset 1 (not 0), got ${acquired_b[0].offset}'

	// Step 3: Consumer A acks offset 0
	batch_a := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      0
		acknowledge_type: .accept
		gap_offsets:      []i64{}
	}
	result_a := coordinator.acknowledge_records(group_id, member_a, batch_a)
	assert result_a.error_code == 0

	// Step 4: Consumer B acks offset 1
	batch_b := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     1
		last_offset:      1
		acknowledge_type: .accept
		gap_offsets:      []i64{}
	}
	result_b := coordinator.acknowledge_records(group_id, member_b, batch_b)
	assert result_b.error_code == 0

	// Step 5: Verify sequential consumption - next acquire should start from offset 2
	acquired_c := coordinator.acquire_records(group_id, member_a, 'test-topic', 0, 1)
	assert acquired_c.len == 1, 'next acquire should return 1 record, got ${acquired_c.len}'
	assert acquired_c[0].offset == 2, 'next record should be offset 2, got ${acquired_c[0].offset}'

	// Verify statistics
	stats := coordinator.get_stats(group_id)
	assert stats.total_acknowledged == 2, 'total acked should be 2, got ${stats.total_acknowledged}'
}

// -- Scenario 3: Poison message redistribution --
// 1. Consumer repeatedly acquires and releases the same message without acking
// 2. After delivery_attempt_limit (5) is exceeded, the message is archived
// 3. Another consumer cannot re-acquire the archived message

fn test_poison_message_redistribution() {
	mut coordinator, _ := setup_test_environment(5)
	group_id := 'poison-group-1'

	// Join consumers
	member_a, _ := join_consumer(mut coordinator, group_id, 'test-topic')
	member_b, _ := join_consumer(mut coordinator, group_id, 'test-topic')

	// Initialize partition with 5 available records
	mut sp := coordinator.get_or_create_partition(group_id, 'test-topic', 0)
	sp.start_offset = 0
	sp.end_offset = 4
	for i in i64(0) .. 5 {
		sp.record_states[i] = .available
	}

	// Simulate poison message: Consumer A acquires offset 0 and releases it repeatedly
	for attempt in 0 .. 5 {
		// Acquire offset 0
		acquired := coordinator.acquire_records(group_id, member_a, 'test-topic', 0, 1)
		if acquired.len == 0 {
			// If already archived, break
			break
		}
		assert acquired[0].offset == 0, 'attempt ${attempt}: should acquire offset 0, got ${acquired[0].offset}'
		assert acquired[0].delivery_count == attempt + 1, 'attempt ${attempt}: delivery count should be ${
			attempt + 1}, got ${acquired[0].delivery_count}'

		// Release without acking (simulating processing failure)
		release_batch := domain.AcknowledgementBatch{
			topic_name:       'test-topic'
			partition:        0
			first_offset:     0
			last_offset:      0
			acknowledge_type: .release
			gap_offsets:      []i64{}
		}
		coordinator.acknowledge_records(group_id, member_a, release_batch)
	}

	// After 5 releases, the record should be archived (delivery_attempt_limit = 5)
	// The last release should have triggered archiving.
	// Verify: Consumer B tries to acquire - offset 0 should be skipped (archived)
	acquired_b := coordinator.acquire_records(group_id, member_b, 'test-topic', 0, 1)
	if acquired_b.len > 0 {
		assert acquired_b[0].offset != 0, 'poison message (offset 0) should be archived, not re-acquirable'
		assert acquired_b[0].offset >= 1, 'Consumer B should get offset >= 1, got ${acquired_b[0].offset}'
	}

	// Verify stats: total_released should account for the release cycles
	stats := coordinator.get_stats(group_id)
	assert stats.total_released >= 5, 'expected at least 5 releases, got ${stats.total_released}'
}

// -- Scenario 4: Broker restart persistence verification --
// 1. Consumer A acquires messages and acks some
// 2. State is persisted to PostgreSQL (PersistentMockStorage)
// 3. Broker restart (new coordinator, same storage)
// 4. Consumer B joins and acquires remaining messages
// 5. Acked messages are skipped

fn test_broker_restart_persistence() {
	mut coordinator, storage := setup_test_environment(10)
	group_id := 'restart-group-1'

	// Step 1: Consumer A joins and sets up partition state
	member_a, _ := join_consumer(mut coordinator, group_id, 'test-topic')

	mut sp := coordinator.get_or_create_partition(group_id, 'test-topic', 0)
	sp.start_offset = 0
	sp.end_offset = 9
	for i in i64(0) .. 10 {
		sp.record_states[i] = .available
	}

	// Consumer A acquires and acks offsets 0..4
	acquired := coordinator.acquire_records(group_id, member_a, 'test-topic', 0, 5)
	assert acquired.len == 5, 'should acquire 5 records, got ${acquired.len}'

	batch := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      4
		acknowledge_type: .accept
		gap_offsets:      []i64{}
	}
	result := coordinator.acknowledge_records(group_id, member_a, batch)
	assert result.error_code == 0

	// Step 2: Verify persistence
	persisted_state := storage.load_share_partition_state(group_id, 'test-topic', 0) or {
		assert false, 'state must be persisted after acknowledgement'
		return
	}
	assert persisted_state.start_offset >= 5, 'SPSO should be >= 5 after acking 0-4, got ${persisted_state.start_offset}'

	// Step 3: Simulate broker restart
	mut coordinator2 := create_coordinator_from_storage(storage)

	// Load persisted state into the new coordinator
	coordinator2.partition_manager.load_all_states(group_id)

	// Step 4: Consumer B joins the restarted broker
	member_b, _ := join_consumer(mut coordinator2, group_id, 'test-topic')

	// Step 5: Consumer B acquires records - should NOT get offsets 0-4
	acquired_b := coordinator2.acquire_records(group_id, member_b, 'test-topic', 0, 5)

	for rec in acquired_b {
		assert rec.offset >= 5, 'offset ${rec.offset} was already acked, should not be re-delivered'
	}

	// Verify: The partition state should reflect continuation
	sp2 := coordinator2.get_partition(group_id, 'test-topic', 0) or {
		assert false, 'partition should exist after state load'
		return
	}
	assert sp2.start_offset >= 5, 'restored SPSO should be >= 5, got ${sp2.start_offset}'
}

// -- Scenario 5: Multi-broker state synchronization --
// Two coordinators sharing the same PersistentMockStorage (simulating shared PostgreSQL)
// 1. Coordinator A (Broker 1) acquires and acks some records
// 2. Coordinator B (Broker 2) loads states from shared storage
// 3. Coordinator B should see the acked state and skip those records

fn test_multi_broker_state_sync() {
	mut storage := new_persistent_mock_storage()
	storage.create_topic('test-topic', 1, domain.TopicConfig{}) or {
		panic('topic creation failed')
	}

	// Populate 10 records
	mut records := []domain.Record{}
	for i in 0 .. 10 {
		records << domain.Record{
			key:       'key-${i}'.bytes()
			value:     'value-${i}'.bytes()
			timestamp: time.now()
		}
	}
	storage.append('test-topic', 0, records, 1) or { panic('append failed') }

	// Create Broker 1
	mut broker1 := create_coordinator_from_storage(storage)
	member_a, _ := join_consumer(mut broker1, 'sync-group', 'test-topic')

	// Initialize partition on Broker 1
	mut sp1 := broker1.get_or_create_partition('sync-group', 'test-topic', 0)
	sp1.start_offset = 0
	sp1.end_offset = 9
	for i in i64(0) .. 10 {
		sp1.record_states[i] = .available
	}

	// Broker 1: Consumer A acquires and acks offsets 0-2
	acquired1 := broker1.acquire_records('sync-group', member_a, 'test-topic', 0, 3)
	assert acquired1.len == 3

	ack_batch := domain.AcknowledgementBatch{
		topic_name:       'test-topic'
		partition:        0
		first_offset:     0
		last_offset:      2
		acknowledge_type: .accept
		gap_offsets:      []i64{}
	}
	broker1.acknowledge_records('sync-group', member_a, ack_batch)

	// Create Broker 2 - shares the same storage backend
	mut broker2 := create_coordinator_from_storage(storage)

	// Broker 2 loads states from shared storage
	broker2.partition_manager.load_all_states('sync-group')

	// Broker 2: Consumer B joins
	member_b, _ := join_consumer(mut broker2, 'sync-group', 'test-topic')

	// Broker 2: Consumer B acquires records - should skip offsets 0-2
	acquired2 := broker2.acquire_records('sync-group', member_b, 'test-topic', 0, 3)

	for rec in acquired2 {
		assert rec.offset >= 3, 'Broker 2 should not re-deliver offset ${rec.offset} (acked by Broker 1)'
	}
}

// -- Scenario 6: Expired lock recovery --
// 1. Consumer A acquires records but does NOT ack (simulating slow processing)
// 2. Locks expire
// 3. Records become available again for Consumer B

fn test_expired_lock_recovery() {
	mut coordinator, _ := setup_test_environment(5)
	group_id := 'lock-recovery-group'

	config := domain.ShareGroupConfig{
		record_lock_duration_ms: 1 // 1ms lock duration for fast expiry in test
		delivery_attempt_limit:  5
		max_partition_locks:     200
		heartbeat_interval_ms:   5000
		session_timeout_ms:      45000
	}
	mut fast_coordinator := new_share_group_coordinator(coordinator.storage, config)

	member_a, _ := join_consumer(mut fast_coordinator, group_id, 'test-topic')
	member_b, _ := join_consumer(mut fast_coordinator, group_id, 'test-topic')

	// Initialize partition
	mut sp := fast_coordinator.get_or_create_partition(group_id, 'test-topic', 0)
	sp.start_offset = 0
	sp.end_offset = 4
	for i in i64(0) .. 5 {
		sp.record_states[i] = .available
	}

	// Consumer A acquires 3 records
	acquired_a := fast_coordinator.acquire_records(group_id, member_a, 'test-topic', 0,
		3)
	assert acquired_a.len == 3, 'Consumer A should acquire 3 records'

	// Wait for lock expiration (locks are set to 1ms)
	time.sleep(5 * time.millisecond)

	// Release expired locks
	fast_coordinator.release_expired_locks()

	// Consumer B should now be able to acquire the previously locked records
	acquired_b := fast_coordinator.acquire_records(group_id, member_b, 'test-topic', 0,
		3)
	assert acquired_b.len >= 1, 'Consumer B should acquire at least 1 record after lock expiry, got ${acquired_b.len}'

	// Verify the records were made available again
	for rec in acquired_b {
		assert rec.offset >= 0 && rec.offset <= 2, 'Consumer B should get previously locked records'
		assert rec.delivery_count == 2, 'delivery count should be 2 (re-acquired after expiry)'
	}
}
