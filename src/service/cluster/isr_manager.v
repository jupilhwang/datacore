/// ISR (In-Sync Replicas) tracking and management.
/// Tracks which replicas are up-to-date for each partition,
/// manages ISR shrink/expand based on replica lag, and provides
/// high watermark calculation.
///
/// Integration points (not yet wired):
/// - infra/replication/manager.v send_replicate_async(): on ACK, call update_replica_offset()
/// - service/broker/produce.v: before ACK, check is_isr_sufficient()
/// - service/cluster/broker_registry.v: on broker death, call remove_broker_from_all_isr()
module cluster

import sync
import time
import infra.observability

/// IsrConfig holds configuration for ISR management.
pub struct IsrConfig {
pub:
	replica_lag_time_max_ms  i64 = 30000
	replica_lag_max_messages i64 = 4000
	isr_check_interval_ms    i64 = 5000
	min_insync_replicas      int = 1
}

/// IsrState tracks the ISR state for a single topic-partition.
pub struct IsrState {
pub mut:
	topic          string
	partition      int
	leader_id      int
	isr            []int
	all_replicas   []int
	last_caught_up map[int]i64
	last_offset    map[int]i64
	high_watermark i64
}

/// IsrManager manages ISR tracking across all partitions.
pub struct IsrManager {
mut:
	lock      sync.RwMutex
	isr_state map[string]IsrState
	config    IsrConfig
	logger    &observability.Logger
	running   bool
}

/// new_isr_manager creates a new IsrManager with the given configuration.
pub fn new_isr_manager(config IsrConfig) &IsrManager {
	return &IsrManager{
		config:    config
		isr_state: map[string]IsrState{}
		logger:    observability.get_named_logger('isr_manager')
		running:   false
	}
}

/// init_partition_isr initializes ISR tracking for a partition.
/// All replicas start as in-sync.
pub fn (mut m IsrManager) init_partition_isr(topic string, partition int, leader_id int, replicas []int) {
	key := make_isr_key(topic, partition)
	now := time.now().unix_milli()

	mut caught_up := map[int]i64{}
	mut offsets := map[int]i64{}
	for r in replicas {
		caught_up[r] = now
		offsets[r] = 0
	}

	state := IsrState{
		topic:          topic
		partition:      partition
		leader_id:      leader_id
		isr:            replicas.clone()
		all_replicas:   replicas.clone()
		last_caught_up: caught_up
		last_offset:    offsets
		high_watermark: 0
	}

	m.lock.@lock()
	m.isr_state[key] = state
	m.lock.unlock()

	m.logger.info('ISR initialized', observability.field_string('topic', topic), observability.field_int('partition',
		i64(partition)), observability.field_int('replicas', i64(replicas.len)))
}

/// update_replica_offset updates the last replicated offset for a replica.
/// Called when a replication ACK is received.
pub fn (mut m IsrManager) update_replica_offset(topic string, partition int, broker_id int, offset i64) {
	key := make_isr_key(topic, partition)
	now := time.now().unix_milli()

	m.lock.@lock()
	defer { m.lock.unlock() }

	if key !in m.isr_state {
		return
	}

	m.isr_state[key].last_offset[broker_id] = offset
	m.isr_state[key].last_caught_up[broker_id] = now
}

/// check_isr performs periodic ISR validation.
/// Shrinks ISR for lagging replicas, expands for caught-up ones.
pub fn (mut m IsrManager) check_isr() {
	m.lock.@lock()
	defer { m.lock.unlock() }

	now := time.now().unix_milli()
	keys := m.isr_state.keys()

	for key in keys {
		mut state := m.isr_state[key]
		leader_offset := state.last_offset[state.leader_id] or { i64(0) }

		m.shrink_isr(mut state, now, leader_offset)
		m.expand_isr(mut state, now, leader_offset)

		m.isr_state[key] = state
	}
}

/// shrink_isr removes replicas that have fallen behind from the ISR.
/// Must be called with m.lock held.
fn (mut m IsrManager) shrink_isr(mut state IsrState, now i64, leader_offset i64) {
	mut new_isr := []int{}

	for replica_id in state.isr {
		if replica_id == state.leader_id {
			new_isr << replica_id
			continue
		}
		caught_up_time := state.last_caught_up[replica_id] or { i64(0) }
		time_lag := now - caught_up_time
		if time_lag > m.config.replica_lag_time_max_ms {
			m.logger.warn('Removing replica from ISR due to lag', observability.field_string('topic',
				state.topic), observability.field_int('partition', i64(state.partition)),
				observability.field_int('broker_id', i64(replica_id)), observability.field_int('time_lag_ms',
				time_lag))
			continue
		}
		new_isr << replica_id
	}

	state.isr = new_isr
}

/// expand_isr adds back replicas that have caught up to the leader.
/// Must be called with m.lock held.
fn (mut m IsrManager) expand_isr(mut state IsrState, now i64, leader_offset i64) {
	for replica_id in state.all_replicas {
		if replica_id in state.isr {
			continue
		}
		replica_offset := state.last_offset[replica_id] or { i64(0) }
		caught_up_time := state.last_caught_up[replica_id] or { i64(0) }
		time_lag := now - caught_up_time

		offset_caught_up := replica_offset >= leader_offset
		time_ok := time_lag <= m.config.replica_lag_time_max_ms

		if offset_caught_up && time_ok {
			state.isr << replica_id
			m.logger.info('Replica rejoined ISR', observability.field_string('topic',
				state.topic), observability.field_int('partition', i64(state.partition)),
				observability.field_int('broker_id', i64(replica_id)))
		}
	}
}

/// get_isr returns the current ISR list for a partition.
pub fn (mut m IsrManager) get_isr(topic string, partition int) []int {
	key := make_isr_key(topic, partition)

	m.lock.rlock()
	defer { m.lock.runlock() }

	state := m.isr_state[key] or { return []int{} }
	return state.isr.clone()
}

/// get_isr_state returns the full ISR state for a partition (for inspection/testing).
pub fn (mut m IsrManager) get_isr_state(topic string, partition int) ?IsrState {
	key := make_isr_key(topic, partition)

	m.lock.rlock()
	defer { m.lock.runlock() }

	if key in m.isr_state {
		return m.isr_state[key]
	}
	return none
}

/// is_isr_sufficient checks if the ISR has enough replicas to satisfy min.insync.replicas.
pub fn (mut m IsrManager) is_isr_sufficient(topic string, partition int) bool {
	key := make_isr_key(topic, partition)

	m.lock.rlock()
	defer { m.lock.runlock() }

	state := m.isr_state[key] or { return false }
	return state.isr.len >= m.config.min_insync_replicas
}

/// get_high_watermark returns the high watermark for a partition.
/// The high watermark is the minimum offset among all ISR members.
pub fn (mut m IsrManager) get_high_watermark(topic string, partition int) i64 {
	key := make_isr_key(topic, partition)

	m.lock.rlock()
	defer { m.lock.runlock() }

	state := m.isr_state[key] or { return i64(0) }
	return calc_high_watermark(state)
}

/// calc_high_watermark computes the minimum offset across all ISR members.
fn calc_high_watermark(state IsrState) i64 {
	if state.isr.len == 0 {
		return 0
	}
	mut min_offset := i64(9223372036854775807) // max i64
	for replica_id in state.isr {
		offset := state.last_offset[replica_id] or { i64(0) }
		if offset < min_offset {
			min_offset = offset
		}
	}
	return min_offset
}

/// remove_broker_from_all_isr removes a broker from the ISR of all partitions.
/// Called when a broker is detected as dead.
pub fn (mut m IsrManager) remove_broker_from_all_isr(broker_id int) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	keys := m.isr_state.keys()
	for key in keys {
		mut state := m.isr_state[key]
		mut new_isr := []int{}
		for r in state.isr {
			if r != broker_id {
				new_isr << r
			}
		}
		if new_isr.len != state.isr.len {
			m.logger.warn('Removed dead broker from ISR', observability.field_string('topic',
				state.topic), observability.field_int('partition', i64(state.partition)),
				observability.field_int('broker_id', i64(broker_id)))
			state.isr = new_isr
			m.isr_state[key] = state
		}
	}
}

/// set_last_caught_up_for_test sets the last_caught_up timestamp for a replica.
/// Intended for testing ISR shrink/expand behavior.
pub fn (mut m IsrManager) set_last_caught_up_for_test(topic string, partition int, broker_id int, ts i64) {
	key := make_isr_key(topic, partition)

	m.lock.@lock()
	defer { m.lock.unlock() }

	if key in m.isr_state {
		m.isr_state[key].last_caught_up[broker_id] = ts
	}
}

/// start_isr_check_loop starts a background loop that calls check_isr() periodically.
pub fn (mut m IsrManager) start_isr_check_loop() {
	m.running = true
	spawn m.isr_check_worker()
}

/// stop stops the background ISR check loop.
pub fn (mut m IsrManager) stop() {
	m.running = false
}

/// isr_check_worker runs the periodic ISR check loop.
fn (mut m IsrManager) isr_check_worker() {
	m.logger.info('ISR check worker started', observability.field_int('interval_ms', m.config.isr_check_interval_ms))

	for m.running {
		time.sleep(time.Duration(m.config.isr_check_interval_ms * time.millisecond))
		if !m.running {
			break
		}
		m.check_isr()
	}

	m.logger.info('ISR check worker stopped')
}

/// make_isr_key creates a map key from topic and partition.
fn make_isr_key(topic string, partition int) string {
	return '${topic}-${partition}'
}
