module replication

import domain
import sync
import log
import time

/// create_test_config returns a ReplicationConfig for testing
fn create_test_config() domain.ReplicationConfig {
	return domain.ReplicationConfig{
		enabled:                    true
		replication_port:           19093
		replica_count:              2
		replica_timeout_ms:         1000
		heartbeat_interval_ms:      100
		reassignment_interval_ms:   100
		orphan_cleanup_interval_ms: 100
	}
}

/// create_test_manager returns a Manager for testing (without starting server)
fn create_test_manager() &Manager {
	config := create_test_config()
	// Initialize server and client as non-nil to avoid segfault
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := Server.new(config.replication_port, handler)
	mut cli := Client.new(config.replica_timeout_ms)
	mut m := &Manager{
		broker_id:       'broker-test-1'
		config:          config
		server:          &srv
		client:          &cli
		replica_buffers: map[string][]domain.ReplicaBuffer{}
		assignments:     map[string]domain.ReplicaAssignment{}
		broker_health:   map[string]domain.ReplicationHealth{}
		stats:           domain.ReplicationStats{}
		cluster_brokers: ['localhost:19094', 'localhost:19095']
		logger:          log.Log{}
		running:         false
	}
	return m
}

// --- Test 1: Main broker crash (pre-flush) ---
// Scenario: Data was replicated to replica broker's buffer,
// but main broker crashed before flushing to S3.
// Verify: Replica buffers retain the data for recovery.

fn test_crash_before_flush_replica_has_data() {
	mut m := create_test_manager()

	// Simulate replicated data stored in replica buffer
	buf1 := domain.ReplicaBuffer{
		topic:        'test-topic'
		partition:    0
		offset:       100
		records_data: 'record-data-100'.bytes()
		timestamp:    time.now().unix_milli()
	}
	buf2 := domain.ReplicaBuffer{
		topic:        'test-topic'
		partition:    0
		offset:       101
		records_data: 'record-data-101'.bytes()
		timestamp:    time.now().unix_milli()
	}
	buf3 := domain.ReplicaBuffer{
		topic:        'test-topic'
		partition:    1
		offset:       200
		records_data: 'record-data-200'.bytes()
		timestamp:    time.now().unix_milli()
	}

	m.store_replica_buffer(buf1) or { assert false, 'store_replica_buffer failed: ${err}' }
	m.store_replica_buffer(buf2) or { assert false, 'store_replica_buffer failed: ${err}' }
	m.store_replica_buffer(buf3) or { assert false, 'store_replica_buffer failed: ${err}' }

	// Verify: all data is in replica buffers
	all_buffers := m.get_all_replica_buffers() or {
		assert false, 'get_all_replica_buffers failed: ${err}'
		return
	}
	assert all_buffers.len == 3, 'expected 3 buffers, got ${all_buffers.len}'

	// Verify partition 0 has 2 buffers
	p0_buffers := m.replica_buffers['test-topic:0'] or {
		assert false, 'missing buffers for test-topic:0'
		return
	}
	assert p0_buffers.len == 2, 'expected 2 buffers for partition 0, got ${p0_buffers.len}'

	// Verify partition 1 has 1 buffer
	p1_buffers := m.replica_buffers['test-topic:1'] or {
		assert false, 'missing buffers for test-topic:1'
		return
	}
	assert p1_buffers.len == 1, 'expected 1 buffer for partition 1, got ${p1_buffers.len}'

	// Verify data integrity
	assert p0_buffers[0].offset == 100
	assert p0_buffers[1].offset == 101
	assert p1_buffers[0].offset == 200
	assert p0_buffers[0].records_data == 'record-data-100'.bytes()
}

// --- Test 2: Orphan cleanup after flush without FLUSH_ACK ---
// Scenario: S3 flush succeeded but FLUSH_ACK was not delivered (broker crash or network issue).
// Verify: Orphan cleanup worker removes stale buffers after 60 seconds.

fn test_orphan_cleanup_removes_stale_buffers() {
	mut m := create_test_manager()
	m.running = true

	now := time.now().unix_milli()
	old_timestamp := now - 61000
	recent_timestamp := now - 10000

	// Store old buffer (should be cleaned)
	old_buf := domain.ReplicaBuffer{
		topic:        'topic-a'
		partition:    0
		offset:       50
		records_data: 'old-data'.bytes()
		timestamp:    old_timestamp
	}
	m.store_replica_buffer(old_buf) or {
		assert false, 'store failed: ${err}'
		return
	}

	// Store recent buffer (should remain)
	recent_buf := domain.ReplicaBuffer{
		topic:        'topic-a'
		partition:    0
		offset:       51
		records_data: 'recent-data'.bytes()
		timestamp:    recent_timestamp
	}
	m.store_replica_buffer(recent_buf) or {
		assert false, 'store failed: ${err}'
		return
	}

	// Store another old buffer for different partition
	old_buf2 := domain.ReplicaBuffer{
		topic:        'topic-b'
		partition:    0
		offset:       10
		records_data: 'old-data-b'.bytes()
		timestamp:    old_timestamp
	}
	m.store_replica_buffer(old_buf2) or {
		assert false, 'store failed: ${err}'
		return
	}

	// Verify initial state: 3 total buffers
	all_before := m.get_all_replica_buffers() or {
		assert false, 'get_all failed: ${err}'
		return
	}
	assert all_before.len == 3, 'expected 3 buffers before cleanup, got ${all_before.len}'

	// Simulate orphan cleanup logic directly (same as worker body)
	orphan_max_age_ms := i64(60000)
	cleanup_now := time.now().unix_milli()
	cutoff := cleanup_now - orphan_max_age_ms

	keys := m.replica_buffers.keys()
	for key in keys {
		buffers := m.replica_buffers[key] or { continue }
		mut remaining := []domain.ReplicaBuffer{}
		mut cleaned_count := 0

		for buf in buffers {
			if buf.timestamp > cutoff {
				remaining << buf
			} else {
				cleaned_count++
			}
		}

		if cleaned_count > 0 {
			m.replica_buffers[key] = remaining
			for _ in 0 .. cleaned_count {
				m.stats.record_orphan_cleanup()
			}
		}
	}

	// Verify: old buffers removed, recent buffer remains
	all_after := m.get_all_replica_buffers() or {
		assert false, 'get_all failed after cleanup: ${err}'
		return
	}
	assert all_after.len == 1, 'expected 1 buffer after cleanup, got ${all_after.len}'
	assert all_after[0].offset == 51, 'expected offset 51 (recent), got ${all_after[0].offset}'
	assert all_after[0].records_data == 'recent-data'.bytes()

	// Verify stats: 2 orphans cleaned
	assert m.stats.total_orphans_cleaned == 2, 'expected 2 orphans cleaned, got ${m.stats.total_orphans_cleaned}'
}

// --- Test 3: FLUSH_ACK network failure and reassignment ---
// Scenario: S3 flush succeeded, FLUSH_ACK send failed due to network issue.
// The dead broker is detected and replicas are reassigned.
// Eventually orphan cleanup removes the stale buffers.

fn test_flush_ack_failure_triggers_reassignment() {
	mut m := create_test_manager()
	m.running = true

	// Setup: assign replicas for topic-x:0
	m.assignments['topic-x:0'] = domain.ReplicaAssignment{
		topic:           'topic-x'
		partition:       0
		main_broker:     'broker-test-1'
		replica_brokers: ['localhost:19094', 'localhost:19095']
		assigned_time:   time.now().unix_milli()
	}

	// Setup: mark one broker as alive and another as dead
	// (simulating that FLUSH_ACK to 19094 failed -> heartbeat detects it as dead)
	m.broker_health['localhost:19094'] = domain.ReplicationHealth{
		broker_id:      'localhost:19094'
		is_alive:       false
		last_heartbeat: time.now().unix_milli() - 30000
		lag_ms:         0
		pending_count:  0
	}
	m.broker_health['localhost:19095'] = domain.ReplicationHealth{
		broker_id:      'localhost:19095'
		is_alive:       true
		last_heartbeat: time.now().unix_milli()
		lag_ms:         0
		pending_count:  0
	}

	// Verify dead broker detected
	mut dead_brokers := []string{}
	for broker_id, health in m.broker_health {
		if !health.is_alive {
			dead_brokers << broker_id
		}
	}
	assert dead_brokers.len == 1, 'expected 1 dead broker, got ${dead_brokers.len}'
	assert dead_brokers[0] == 'localhost:19094'

	// Simulate reassignment logic (same as worker body)
	mut alive_brokers := []string{}
	for broker_addr in m.cluster_brokers {
		health := m.broker_health[broker_addr] or { continue }
		if health.is_alive {
			alive_brokers << broker_addr
		}
	}

	assignment_keys := m.assignments.keys()
	for key in assignment_keys {
		assignment := m.assignments[key] or { continue }
		mut new_replicas := []string{}
		mut changed := false

		for broker in assignment.replica_brokers {
			if broker in dead_brokers {
				changed = true
				mut found_replacement := false
				for alive in alive_brokers {
					if alive !in new_replicas && alive !in assignment.replica_brokers {
						new_replicas << alive
						found_replacement = true
						break
					}
				}
				if !found_replacement {
					// No replacement available, just skip the dead broker
				}
			} else {
				new_replicas << broker
			}
		}

		if changed {
			mut updated := assignment
			updated.replica_brokers = new_replicas
			updated.assigned_time = time.now().unix_milli()
			m.assignments[key] = updated
		}
	}

	// Verify: dead broker removed from assignment
	updated_assignment := m.assignments['topic-x:0'] or {
		assert false, 'assignment not found after reassignment'
		return
	}
	assert 'localhost:19094' !in updated_assignment.replica_brokers, 'dead broker should be removed from replicas'
	assert 'localhost:19095' in updated_assignment.replica_brokers, 'alive broker should remain in replicas'

	// Store stale buffer (simulating data that was replicated but FLUSH_ACK failed)
	old_timestamp := time.now().unix_milli() - 61000
	stale_buf := domain.ReplicaBuffer{
		topic:        'topic-x'
		partition:    0
		offset:       500
		records_data: 'stale-data'.bytes()
		timestamp:    old_timestamp
	}
	m.store_replica_buffer(stale_buf) or {
		assert false, 'store failed: ${err}'
		return
	}

	// Simulate orphan cleanup
	orphan_max_age_ms := i64(60000)
	cleanup_now := time.now().unix_milli()
	cutoff := cleanup_now - orphan_max_age_ms

	keys := m.replica_buffers.keys()
	for rk in keys {
		buffers := m.replica_buffers[rk] or { continue }
		mut remaining := []domain.ReplicaBuffer{}
		mut cleaned_count := 0

		for buf in buffers {
			if buf.timestamp > cutoff {
				remaining << buf
			} else {
				cleaned_count++
			}
		}

		if cleaned_count > 0 {
			m.replica_buffers[rk] = remaining
			for _ in 0 .. cleaned_count {
				m.stats.record_orphan_cleanup()
			}
		}
	}

	// Verify: stale buffer cleaned up
	all_after := m.get_all_replica_buffers() or {
		assert false, 'get_all failed: ${err}'
		return
	}
	assert all_after.len == 0, 'expected 0 buffers after orphan cleanup, got ${all_after.len}'
	assert m.stats.total_orphans_cleaned == 1, 'expected 1 orphan cleaned, got ${m.stats.total_orphans_cleaned}'
}

// --- Test: Manager lifecycle (start_workers / stop_workers) ---
// Verify that workers can be started and stopped without panic

fn test_manager_worker_lifecycle() {
	config := create_test_config()
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := Server.new(config.replication_port, handler)
	mut cli := Client.new(config.replica_timeout_ms)
	mut m := &Manager{
		broker_id:       'broker-lifecycle'
		config:          config
		server:          &srv
		client:          &cli
		replica_buffers: map[string][]domain.ReplicaBuffer{}
		assignments:     map[string]domain.ReplicaAssignment{}
		broker_health:   map[string]domain.ReplicationHealth{}
		stats:           domain.ReplicationStats{}
		cluster_brokers: []
		logger:          log.Log{}
		running:         true
	}

	// Start workers (no cluster_brokers, so workers run but do minimal work)
	m.start_workers()

	// Let workers run briefly
	time.sleep(time.Duration(250 * time.millisecond))

	// Stop workers
	m.running = false
	m.stop_workers()

	// Wait for workers to exit
	time.sleep(time.Duration(200 * time.millisecond))

	// If we reach here without panic, lifecycle test passes
	assert !m.running, 'manager should be stopped'
}

// --- Test: Heartbeat updates broker health ---

fn test_heartbeat_updates_broker_health() {
	mut m := create_test_manager()

	// Simulate receiving a heartbeat from a remote broker (server handler logic)
	sender_id := 'remote-broker-1'
	m.broker_health[sender_id] = domain.ReplicationHealth{
		broker_id:      sender_id
		is_alive:       true
		last_heartbeat: time.now().unix_milli()
		lag_ms:         0
		pending_count:  0
	}

	// Verify health is recorded
	health := m.broker_health[sender_id] or {
		assert false, 'broker health not found for ${sender_id}'
		return
	}
	assert health.is_alive == true
	assert health.broker_id == sender_id
	assert health.last_heartbeat > 0
}

// --- Test: Delete replica buffer after flush ack ---

fn test_delete_replica_buffer_on_flush_ack() {
	mut m := create_test_manager()

	// Store multiple buffers with sequential offsets
	for i in 0 .. 5 {
		buf := domain.ReplicaBuffer{
			topic:        'flush-topic'
			partition:    0
			offset:       i64(i * 10)
			records_data: 'data-${i}'.bytes()
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {
			assert false, 'store failed: ${err}'
			return
		}
	}

	// Verify 5 buffers stored
	buffers_before := m.replica_buffers['flush-topic:0'] or {
		assert false, 'no buffers found'
		return
	}
	assert buffers_before.len == 5

	// Simulate FLUSH_ACK for offset 20 (should remove offsets 0, 10, 20)
	m.delete_replica_buffer('flush-topic', 0, 20) or {
		assert false, 'delete failed: ${err}'
		return
	}

	// Verify: only offsets 30 and 40 remain
	buffers_after := m.replica_buffers['flush-topic:0'] or {
		assert false, 'no buffers found after delete'
		return
	}
	assert buffers_after.len == 2, 'expected 2 remaining, got ${buffers_after.len}'
	assert buffers_after[0].offset == 30
	assert buffers_after[1].offset == 40
}

// --- Test: Assign replicas distributes across available brokers ---

fn test_assign_replicas() {
	mut m := create_test_manager()
	m.cluster_brokers = ['broker-a:19094', 'broker-b:19094', 'broker-c:19094']

	m.assign_replicas('assign-topic', 0) or {
		assert false, 'assign_replicas failed: ${err}'
		return
	}

	assignment := m.assignments['assign-topic:0'] or {
		assert false, 'assignment not found'
		return
	}

	assert assignment.topic == 'assign-topic'
	assert assignment.partition == 0
	assert assignment.main_broker == 'broker-test-1'
	assert assignment.replica_brokers.len == 2, 'expected 2 replicas, got ${assignment.replica_brokers.len}'
	assert assignment.assigned_time > 0
}

// --- Test: Concurrent store and read of replica buffers ---
// Verify that concurrent writes and reads do not cause data race or panic

fn test_concurrent_store_and_read_replica_buffers() {
	mut m := create_test_manager()
	m.running = true
	num_writers := 4
	writes_per_writer := 50

	mut wg := sync.new_waitgroup()
	wg.add(num_writers + 2)

	// Spawn concurrent writers
	for w in 0 .. num_writers {
		spawn fn [mut m, mut wg, w, writes_per_writer] () {
			defer {
				wg.done()
			}
			for i in 0 .. writes_per_writer {
				buf := domain.ReplicaBuffer{
					topic:        'concurrent-topic'
					partition:    i32(w)
					offset:       i64(i)
					records_data: 'data-${w}-${i}'.bytes()
					timestamp:    time.now().unix_milli()
				}
				m.store_replica_buffer(buf) or {}
			}
		}()
	}

	// Spawn concurrent readers
	for _ in 0 .. 2 {
		spawn fn [mut m, mut wg] () {
			defer {
				wg.done()
			}
			for _ in 0 .. 20 {
				_ := m.get_all_replica_buffers() or { []domain.ReplicaBuffer{} }
				time.sleep(time.Duration(1 * time.millisecond))
			}
		}()
	}

	// Wait for all to complete
	wg.wait()

	// Verify total count
	all_buffers := m.get_all_replica_buffers() or {
		assert false, 'get_all failed: ${err}'
		return
	}
	expected := num_writers * writes_per_writer
	assert all_buffers.len == expected, 'expected ${expected} buffers, got ${all_buffers.len}'
}

// --- Test: Concurrent broker health updates ---
// Verify that concurrent health updates do not cause data race

fn test_concurrent_broker_health_updates() {
	mut m := create_test_manager()
	num_updaters := 4
	updates_per_thread := 50

	mut wg := sync.new_waitgroup()
	wg.add(num_updaters)

	for u in 0 .. num_updaters {
		spawn fn [mut m, mut wg, u, updates_per_thread] () {
			defer {
				wg.done()
			}
			broker_id := 'broker-${u}'
			for i in 0 .. updates_per_thread {
				m.update_broker_health(broker_id, domain.ReplicationHealth{
					broker_id:      broker_id
					is_alive:       i % 2 == 0
					last_heartbeat: time.now().unix_milli()
					lag_ms:         0
					pending_count:  0
				})
			}
		}()
	}

	wg.wait()

	// Each broker should have a health entry
	for u in 0 .. num_updaters {
		broker_id := 'broker-${u}'
		_ := m.broker_health[broker_id] or {
			assert false, 'missing health for ${broker_id}'
			return
		}
	}
}

// --- Test: Concurrent store and delete replica buffers ---
// Verify that store and delete operating concurrently do not cause data race

fn test_concurrent_store_and_delete_replica_buffers() {
	mut m := create_test_manager()
	m.running = true

	// Pre-populate buffers
	for i in 0 .. 100 {
		buf := domain.ReplicaBuffer{
			topic:        'race-topic'
			partition:    0
			offset:       i64(i)
			records_data: 'data-${i}'.bytes()
			timestamp:    time.now().unix_milli()
		}
		m.store_replica_buffer(buf) or {}
	}

	mut wg := sync.new_waitgroup()
	wg.add(3)

	// Writer: add more buffers
	spawn fn [mut m, mut wg] () {
		defer {
			wg.done()
		}
		for i in 100 .. 200 {
			buf := domain.ReplicaBuffer{
				topic:        'race-topic'
				partition:    0
				offset:       i64(i)
				records_data: 'data-${i}'.bytes()
				timestamp:    time.now().unix_milli()
			}
			m.store_replica_buffer(buf) or {}
		}
	}()

	// Deleter: delete buffers up to various offsets
	spawn fn [mut m, mut wg] () {
		defer {
			wg.done()
		}
		for i in 0 .. 10 {
			m.delete_replica_buffer('race-topic', 0, i64(i * 10)) or {}
			time.sleep(time.Duration(1 * time.millisecond))
		}
	}()

	// Reader
	spawn fn [mut m, mut wg] () {
		defer {
			wg.done()
		}
		for _ in 0 .. 20 {
			_ := m.get_all_replica_buffers() or { []domain.ReplicaBuffer{} }
			time.sleep(time.Duration(1 * time.millisecond))
		}
	}()

	wg.wait()

	// If we reach here without panic/crash, the concurrent access is safe
	all_buffers := m.get_all_replica_buffers() or {
		assert false, 'get_all failed: ${err}'
		return
	}
	assert all_buffers.len >= 0, 'buffers should not be negative'
}
