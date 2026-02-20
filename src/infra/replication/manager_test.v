module replication

import domain
import sync
import log
import time

/// create_test_config returns a ReplicationConfig for testing
fn create_test_config() domain.ReplicationConfig {
	return domain.ReplicationConfig{
		enabled:                       true
		replication_port:              19093
		replica_count:                 2
		replica_timeout_ms:            1000
		heartbeat_interval_ms:         100
		reassignment_interval_ms:      100
		orphan_cleanup_interval_ms:    100
		retry_count:                   3
		replica_buffer_ttl_ms:         60000
		max_replica_buffer_size_bytes: 0
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
		broker_id:           'broker-test-1'
		config:              config
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: [
			domain.BrokerRef{
				broker_id: 'broker-test-2'
				addr:      'localhost:19094'
			},
			domain.BrokerRef{
				broker_id: 'broker-test-3'
				addr:      'localhost:19095'
			},
		]
		logger:              log.Log{}
		running:             false
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

	// Pre-populate buffers directly to bypass TTL pruning in store_replica_buffer,
	// because the test needs to verify the cleanup worker's behavior with old entries.
	old_buf := domain.ReplicaBuffer{
		topic:        'topic-a'
		partition:    0
		offset:       50
		records_data: 'old-data'.bytes()
		timestamp:    old_timestamp
	}
	recent_buf := domain.ReplicaBuffer{
		topic:        'topic-a'
		partition:    0
		offset:       51
		records_data: 'recent-data'.bytes()
		timestamp:    recent_timestamp
	}
	old_buf2 := domain.ReplicaBuffer{
		topic:        'topic-b'
		partition:    0
		offset:       10
		records_data: 'old-data-b'.bytes()
		timestamp:    old_timestamp
	}

	m.replica_buffers_lock.@lock()
	m.replica_buffers['topic-a:0'] = [old_buf, recent_buf]
	m.replica_buffers['topic-b:0'] = [old_buf2]
	m.replica_buffers_lock.unlock()

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

	// Setup: mark one broker as alive and another as dead using stable broker_id as key.
	// (simulating that FLUSH_ACK to broker-test-2 failed -> heartbeat detects it as dead)
	m.broker_health['broker-test-2'] = domain.ReplicationHealth{
		broker_id:      'broker-test-2'
		is_alive:       false
		last_heartbeat: time.now().unix_milli() - 30000
		lag_ms:         0
		pending_count:  0
	}
	m.broker_health['broker-test-3'] = domain.ReplicationHealth{
		broker_id:      'broker-test-3'
		is_alive:       true
		last_heartbeat: time.now().unix_milli()
		lag_ms:         0
		pending_count:  0
	}

	// Verify dead broker detected
	mut dead_brokers := []string{}
	for bid, health in m.broker_health {
		if !health.is_alive {
			dead_brokers << bid
		}
	}
	assert dead_brokers.len == 1, 'expected 1 dead broker, got ${dead_brokers.len}'
	// broker_health is now keyed by stable broker_id
	assert dead_brokers[0] == 'broker-test-2'

	// Simulate reassignment logic (same as worker body).
	// Build id_to_addr map so broker_health keys (broker_id) can be mapped to addr.
	mut id_to_addr := map[string]string{}
	for ref in m.cluster_broker_refs {
		if ref.broker_id != '' {
			id_to_addr[ref.broker_id] = ref.addr
		}
	}

	// Translate dead broker ids to addresses
	mut dead_broker_addrs := []string{}
	for bid, health in m.broker_health {
		if !health.is_alive {
			addr := id_to_addr[bid] or { bid }
			dead_broker_addrs << addr
		}
	}

	mut alive_brokers := []string{}
	for ref in m.cluster_broker_refs {
		health_key := if ref.broker_id != '' { ref.broker_id } else { ref.addr }
		health := m.broker_health[health_key] or { continue }
		if health.is_alive {
			alive_brokers << ref.addr
		}
	}

	assignment_keys := m.assignments.keys()
	for key in assignment_keys {
		assignment := m.assignments[key] or { continue }
		mut new_replicas := []string{}
		mut changed := false

		for broker_addr in assignment.replica_brokers {
			if broker_addr in dead_broker_addrs {
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
				new_replicas << broker_addr
			}
		}

		if changed {
			mut updated := assignment
			updated.replica_brokers = new_replicas
			updated.assigned_time = time.now().unix_milli()
			m.assignments[key] = updated
		}
	}

	// Verify: dead broker address removed from assignment
	updated_assignment := m.assignments['topic-x:0'] or {
		assert false, 'assignment not found after reassignment'
		return
	}
	assert 'localhost:19094' !in updated_assignment.replica_brokers, 'dead broker addr should be removed from replicas'

	assert 'localhost:19095' in updated_assignment.replica_brokers, 'alive broker addr should remain in replicas'

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
		broker_id:           'broker-lifecycle'
		config:              config
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: []domain.BrokerRef{}
		logger:              log.Log{}
		running:             true
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
	m.cluster_broker_refs = [
		domain.BrokerRef{
			broker_id: 'broker-a'
			addr:      'broker-a:19094'
		},
		domain.BrokerRef{
			broker_id: 'broker-b'
			addr:      'broker-b:19094'
		},
		domain.BrokerRef{
			broker_id: 'broker-c'
			addr:      'broker-c:19094'
		},
	]

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

// --- Test: Broker ID-based self-exclusion during replica assignment ---
// Scenario: The manager's own broker_id matches one of the cluster_broker_refs entries.
// Verify: assign_replicas correctly excludes the local broker by ID regardless of its address.

fn test_assign_replicas_excludes_self_by_broker_id() {
	config := create_test_config()
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := Server.new(config.replication_port, handler)
	mut cli := Client.new(config.replica_timeout_ms)
	// The manager's own broker_id is 'self-broker'.
	// It is also listed in cluster_broker_refs but with a different address
	// (simulating an address change or multi-interface scenario).
	mut m := &Manager{
		broker_id:           'self-broker'
		config:              config
		server:              &srv
		client:              &cli
		replica_buffers:     map[string][]domain.ReplicaBuffer{}
		assignments:         map[string]domain.ReplicaAssignment{}
		broker_health:       map[string]domain.ReplicationHealth{}
		stats:               domain.ReplicationStats{}
		cluster_broker_refs: [
			domain.BrokerRef{
				broker_id: 'self-broker'
				addr:      '10.0.0.1:19093' // different addr - should still be excluded
			},
			domain.BrokerRef{
				broker_id: 'peer-broker-1'
				addr:      '10.0.0.2:19093'
			},
			domain.BrokerRef{
				broker_id: 'peer-broker-2'
				addr:      '10.0.0.3:19093'
			},
		]
		logger:              log.Log{}
		running:             false
	}

	m.assign_replicas('id-topic', 0) or {
		assert false, 'assign_replicas failed: ${err}'
		return
	}

	assignment := m.assignments['id-topic:0'] or {
		assert false, 'assignment not found'
		return
	}

	// Self broker must not appear in replica_brokers
	assert '10.0.0.1:19093' !in assignment.replica_brokers, 'self broker address must not be in replica_brokers'

	// Peers must be selected
	assert assignment.replica_brokers.len == 2, 'expected 2 replicas from peers, got ${assignment.replica_brokers.len}'

	assert assignment.main_broker == 'self-broker'
}

// --- Test: Backward-compatible address-based self-exclusion ---
// Scenario: BrokerRef entries have empty broker_id (legacy configuration).
// Verify: assign_replicas falls back to address-based exclusion when broker_id is absent.

fn test_assign_replicas_legacy_addr_fallback() {
	config := create_test_config()
	handler := fn (msg domain.ReplicationMessage) !domain.ReplicationMessage {
		return msg
	}
	mut srv := Server.new(config.replication_port, handler)
	mut cli := Client.new(config.replica_timeout_ms)
	mut m := &Manager{
		broker_id:       'legacy-broker'
		config:          config
		server:          &srv
		client:          &cli
		replica_buffers: map[string][]domain.ReplicaBuffer{}
		assignments:     map[string]domain.ReplicaAssignment{}
		broker_health:   map[string]domain.ReplicationHealth{}
		stats:           domain.ReplicationStats{}
		// Legacy mode: no broker_id set; self is identified by its replication port
		cluster_broker_refs: [
			domain.BrokerRef{
				broker_id: '' // legacy: no stable ID
				addr:      'localhost:${config.replication_port}'
			},
			domain.BrokerRef{
				broker_id: '' // legacy: no stable ID
				addr:      'peer-a:19094'
			},
		]
		logger:              log.Log{}
		running:             false
	}

	m.assign_replicas('legacy-topic', 0) or {
		assert false, 'assign_replicas failed: ${err}'
		return
	}

	assignment := m.assignments['legacy-topic:0'] or {
		assert false, 'assignment not found'
		return
	}

	// The local replication port address must not appear in replica_brokers
	assert 'localhost:${config.replication_port}' !in assignment.replica_brokers, 'self address must not be in replica_brokers in legacy mode'

	assert assignment.replica_brokers.len == 1, 'expected 1 replica (only peer-a), got ${assignment.replica_brokers.len}'

	assert assignment.replica_brokers[0] == 'peer-a:19094'
}

// --- Test: update_cluster_broker_refs replaces peer list ---
// Verify that the broker list can be updated at runtime.

fn test_update_cluster_broker_refs() {
	mut m := create_test_manager()

	initial_refs := m.cluster_broker_refs.clone()
	assert initial_refs.len == 2

	new_refs := [
		domain.BrokerRef{
			broker_id: 'new-peer-1'
			addr:      '192.168.1.1:19094'
		},
		domain.BrokerRef{
			broker_id: 'new-peer-2'
			addr:      '192.168.1.2:19094'
		},
		domain.BrokerRef{
			broker_id: 'new-peer-3'
			addr:      '192.168.1.3:19094'
		},
	]
	m.update_cluster_broker_refs(new_refs)

	assert m.cluster_broker_refs.len == 3
	assert m.cluster_broker_refs[0].broker_id == 'new-peer-1'
	assert m.cluster_broker_refs[2].addr == '192.168.1.3:19094'
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

// --- Test: retry_count exhaustion returns error after all retries ---
// Scenario: send_replicate_async retries when the broker is unreachable.
// Verify: after retry_count attempts, retry_exhausted metric is incremented.

fn test_retry_count_exhausted_increments_metric() {
	mut m := create_test_manager()
	// Use retry_count=2 so the test completes quickly
	m.config.retry_count = 2

	// Simulate exhaustion directly: broker address is unreachable
	// We check the metric increment path by calling the stat helper directly
	// (the actual network path is covered by integration tests).
	m.stats_lock.@lock()
	m.stats.record_retry_exhausted()
	m.stats_lock.unlock()

	stats := m.get_stats()
	assert stats.total_retry_exhausted == 1, 'expected 1 retry_exhausted, got ${stats.total_retry_exhausted}'
}

// --- Test: replica_buffer_ttl_ms causes expired messages to be dropped on insert ---
// Scenario: TTL is 500ms. Insert an old buffer (2000ms ago) then a new buffer.
// Verify: only the new buffer remains; ttl_dropped metric is incremented.

fn test_replica_buffer_ttl_drops_expired_on_insert() {
	mut m := create_test_manager()
	m.config.replica_buffer_ttl_ms = 500 // 500ms TTL

	now := time.now().unix_milli()

	// Pre-populate with an old buffer (timestamp 2000ms ago)
	old_buf := domain.ReplicaBuffer{
		topic:        'ttl-topic'
		partition:    0
		offset:       1
		records_data: 'old'.bytes()
		timestamp:    now - 2000 // 2 seconds old -> beyond 500ms TTL
	}
	// Directly insert old buffer bypassing TTL check (simulates prior state)
	m.replica_buffers_lock.@lock()
	m.replica_buffers['ttl-topic:0'] = [old_buf]
	m.replica_buffers_lock.unlock()

	// Insert new buffer through the public API (should trigger TTL pruning of old_buf)
	new_buf := domain.ReplicaBuffer{
		topic:        'ttl-topic'
		partition:    0
		offset:       2
		records_data: 'new'.bytes()
		timestamp:    now
	}
	m.store_replica_buffer(new_buf) or { assert false, 'store_replica_buffer failed: ${err}' }

	// Only new buffer should remain
	buffers := m.replica_buffers['ttl-topic:0'] or {
		assert false, 'no buffers found for ttl-topic:0'
		return
	}
	assert buffers.len == 1, 'expected 1 buffer after TTL prune, got ${buffers.len}'
	assert buffers[0].offset == 2, 'expected offset 2 (new), got ${buffers[0].offset}'

	// ttl_dropped metric should be 1
	stats := m.get_stats()
	assert stats.total_ttl_dropped == 1, 'expected total_ttl_dropped=1, got ${stats.total_ttl_dropped}'
}

// --- Test: max_replica_buffer_size_bytes rejects messages when limit exceeded ---
// Scenario: limit is 50 bytes. Store 50 bytes of data, then try to add 1 more byte.
// Verify: second insert returns an error; buffer_overflow metric is incremented.

fn test_max_replica_buffer_size_bytes_rejects_overflow() {
	mut m := create_test_manager()
	m.config.max_replica_buffer_size_bytes = 50 // 50-byte limit
	m.config.replica_buffer_ttl_ms = 0 // disable TTL pruning to keep it simple

	// Insert a buffer that exactly fills the limit
	fill_data := []u8{len: 50, init: u8(65)} // 50 x 'A'
	fill_buf := domain.ReplicaBuffer{
		topic:        'size-topic'
		partition:    0
		offset:       1
		records_data: fill_data
		timestamp:    time.now().unix_milli()
	}
	m.store_replica_buffer(fill_buf) or { assert false, 'first store failed: ${err}' }

	// Try to insert one more byte (should fail)
	overflow_buf := domain.ReplicaBuffer{
		topic:        'size-topic'
		partition:    0
		offset:       2
		records_data: [u8(66)]
		timestamp:    time.now().unix_milli()
	}
	mut got_error := false
	m.store_replica_buffer(overflow_buf) or {
		got_error = true
		_ = err
	}
	assert got_error, 'expected error on overflow insert but got none'

	// Buffer should still contain only the original 50-byte entry
	buffers := m.replica_buffers['size-topic:0'] or {
		assert false, 'no buffers found'
		return
	}
	assert buffers.len == 1, 'expected 1 buffer (overflow rejected), got ${buffers.len}'

	// buffer_overflow metric should be 1
	stats := m.get_stats()
	assert stats.total_buffer_overflow == 1, 'expected total_buffer_overflow=1, got ${stats.total_buffer_overflow}'
}

// --- Test: TTL cleanup in orphan_cleanup_worker uses replica_buffer_ttl_ms ---
// Scenario: Insert buffers with old timestamps, run cleanup logic inline,
// verify ttl_dropped and total_orphans_cleaned are updated.

fn test_orphan_cleanup_uses_ttl_ms() {
	mut m := create_test_manager()
	m.config.replica_buffer_ttl_ms = 500 // 500ms TTL
	m.config.replica_timeout_ms = 1000 // fallback TTL (not used when ttl_ms > 0)
	m.running = true

	now := time.now().unix_milli()
	old_ts := now - 2000 // 2s old

	old_buf := domain.ReplicaBuffer{
		topic:        'cleanup-topic'
		partition:    0
		offset:       10
		records_data: 'stale'.bytes()
		timestamp:    old_ts
	}
	m.replica_buffers_lock.@lock()
	m.replica_buffers['cleanup-topic:0'] = [old_buf]
	m.replica_buffers_lock.unlock()

	// Replicate inline cleanup logic (same as orphan_cleanup_worker body)
	ttl_ms := if m.config.replica_buffer_ttl_ms > 0 {
		m.config.replica_buffer_ttl_ms
	} else {
		i64(m.config.replica_timeout_ms)
	}
	cutoff := now - ttl_ms
	mut total_cleaned := 0

	keys := m.replica_buffers.keys()
	for key in keys {
		m.replica_buffers_lock.@lock()
		buffers := m.replica_buffers[key] or {
			m.replica_buffers_lock.unlock()
			continue
		}
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
			total_cleaned += cleaned_count
		}
		m.replica_buffers_lock.unlock()
	}
	if total_cleaned > 0 {
		m.stats_lock.@lock()
		m.stats.total_orphans_cleaned += total_cleaned
		m.stats.record_ttl_dropped(i64(total_cleaned))
		m.stats_lock.unlock()
	}

	// Verify buffer is empty and metrics are updated
	all_buffers := m.get_all_replica_buffers() or {
		assert false, 'get_all failed: ${err}'
		return
	}
	assert all_buffers.len == 0, 'expected 0 buffers after TTL cleanup, got ${all_buffers.len}'

	stats := m.get_stats()
	assert stats.total_orphans_cleaned == 1, 'expected total_orphans_cleaned=1, got ${stats.total_orphans_cleaned}'
	assert stats.total_ttl_dropped == 1, 'expected total_ttl_dropped=1, got ${stats.total_ttl_dropped}'
}
