// Manages partition rebalancing triggers when brokers join or leave the cluster.
// Provides debouncing, concurrent rebalance prevention, and per-topic rebalance orchestration.
module cluster

import domain
import service.port
import infra.observability
import sync
import time

/// RebalanceTriggerType represents the reason a rebalance was triggered.
pub enum RebalanceTriggerType {
	broker_added
	broker_removed
	broker_dead
	topic_created
	manual
}

/// RebalanceConfig holds configuration for the rebalance trigger.
pub struct RebalanceConfig {
pub:
	rebalance_delay_ms           i64                       = 10000
	max_concurrent_reassignments int                       = 50
	rebalance_strategy           domain.AssignmentStrategy = .round_robin
}

/// RebalanceRequest represents a queued rebalance request.
pub struct RebalanceRequest {
pub:
	trigger   RebalanceTriggerType
	broker_id i32
	timestamp i64
}

/// PartitionAssignmentChange tracks a single partition move during rebalance.
pub struct PartitionMovement {
pub:
	topic_name string
	partition  i32
	old_broker i32
	new_broker i32
}

/// RebalanceResult holds the outcome of a rebalance execution.
pub struct RebalanceResult {
pub:
	moved_partitions    int
	assignments_changed []PartitionMovement
	duration_ms         i64
	error_count         int
	topics_rebalanced   int
}

/// RebalanceStatus reports the current state of the rebalance trigger.
pub struct RebalanceStatus {
pub:
	is_rebalancing    bool
	pending_requests  int
	last_rebalance_at i64
	total_rebalances  int
	total_errors      int
}

/// RebalanceTrigger orchestrates partition rebalancing in response to broker changes.
/// It debounces rapid broker additions, prevents concurrent rebalances,
/// and delegates the actual assignment to PartitionAssigner.
@[heap]
pub struct RebalanceTrigger {
mut:
	lock               sync.Mutex
	assigner           &PartitionAssigner
	registry           &BrokerRegistry
	storage            port.TopicStoragePort
	logger             &observability.Logger
	config             RebalanceConfig
	pending_rebalances []RebalanceRequest
	is_rebalancing     bool
	last_rebalance_at  i64
	total_rebalances   int
	total_errors       int
	// Metrics
	rebalance_triggered &observability.Metric
	rebalance_completed &observability.Metric
	partitions_moved    &observability.Metric
}

/// new_rebalance_trigger creates a new RebalanceTrigger.
pub fn new_rebalance_trigger(assigner &PartitionAssigner, registry &BrokerRegistry, storage port.TopicStoragePort, config RebalanceConfig, logger &observability.Logger) &RebalanceTrigger {
	mut reg := observability.get_registry()

	return &RebalanceTrigger{
		assigner:            assigner
		registry:            registry
		storage:             storage
		logger:              logger
		config:              config
		pending_rebalances:  []RebalanceRequest{}
		is_rebalancing:      false
		last_rebalance_at:   0
		total_rebalances:    0
		total_errors:        0
		rebalance_triggered: reg.register('rebalance_trigger_triggered_total', 'Total rebalance triggers',
			.counter)
		rebalance_completed: reg.register('rebalance_trigger_completed_total', 'Total rebalances completed',
			.counter)
		partitions_moved:    reg.register('rebalance_trigger_partitions_moved_total',
			'Total partitions moved', .counter)
	}
}

/// on_broker_added queues a rebalance request for a newly added broker.
/// Uses debounce delay to batch rapid successive additions.
pub fn (mut t RebalanceTrigger) on_broker_added(broker_id i32) {
	t.lock.@lock()
	defer { t.lock.unlock() }

	t.rebalance_triggered.inc()
	now := time.now().unix_milli()

	t.pending_rebalances << RebalanceRequest{
		trigger:   .broker_added
		broker_id: broker_id
		timestamp: now
	}

	t.logger.info('Broker added rebalance queued (debounced)', observability.field_int('broker_id',
		i64(broker_id)), observability.field_int('pending_count', i64(t.pending_rebalances.len)))

	// Schedule debounced execution in a background thread
	spawn t.execute_after_delay()
}

/// on_broker_removed triggers an immediate rebalance for a removed broker.
/// Partitions on the removed broker must be moved away quickly.
pub fn (mut t RebalanceTrigger) on_broker_removed(broker_id i32) {
	t.lock.@lock()

	t.rebalance_triggered.inc()
	now := time.now().unix_milli()

	t.pending_rebalances << RebalanceRequest{
		trigger:   .broker_removed
		broker_id: broker_id
		timestamp: now
	}

	t.logger.info('Broker removed, triggering immediate rebalance', observability.field_int('broker_id',
		i64(broker_id)))

	t.lock.unlock()

	// Immediate execution for broker removal
	t.execute_rebalance() or {
		t.logger.warn('Rebalance after broker removal failed', observability.field_err_str(err.str()))
	}
}

/// on_topic_created assigns partitions for a newly created topic.
pub fn (mut t RebalanceTrigger) on_topic_created(topic_name string, partition_count int) {
	t.lock.@lock()
	defer { t.lock.unlock() }

	t.rebalance_triggered.inc()

	t.logger.info('Assigning partitions for new topic', observability.field_string('topic',
		topic_name), observability.field_int('partition_count', i64(partition_count)))

	active_brokers := t.get_active_brokers() or {
		t.logger.warn('No active brokers for topic creation', observability.field_string('topic',
			topic_name))
		return
	}

	mut assigner := t.assigner
	assigner.assign_partitions(topic_name, partition_count, active_brokers) or {
		t.logger.warn('Failed to assign partitions for new topic', observability.field_string('topic',
			topic_name), observability.field_err_str(err.str()))
		t.total_errors++
		return
	}

	t.logger.info('Topic partition assignment completed', observability.field_string('topic',
		topic_name))
}

/// execute_rebalance performs a rebalance across all topics.
/// Returns the rebalance result or an error if rebalance cannot proceed.
pub fn (mut t RebalanceTrigger) execute_rebalance() !RebalanceResult {
	t.lock.@lock()

	if t.is_rebalancing {
		t.lock.unlock()
		return error('rebalance already in progress')
	}

	if t.pending_rebalances.len == 0 {
		t.lock.unlock()
		return error('no pending rebalance requests')
	}

	t.is_rebalancing = true
	requests := t.pending_rebalances.clone()
	t.pending_rebalances.clear()
	t.lock.unlock()

	start_time := time.now().unix_milli()
	mut result := RebalanceResult{
		moved_partitions:    0
		assignments_changed: []PartitionMovement{}
		duration_ms:         0
		error_count:         0
		topics_rebalanced:   0
	}

	defer {
		t.lock.@lock()
		t.is_rebalancing = false
		t.last_rebalance_at = time.now().unix_milli()
		t.total_rebalances++
		t.lock.unlock()
	}

	t.logger.info('Executing rebalance', observability.field_int('pending_requests', i64(requests.len)))

	// Get active brokers
	active_brokers := t.get_active_brokers() or {
		t.lock.@lock()
		t.total_errors++
		t.lock.unlock()
		return error('no active brokers for rebalance: ${err}')
	}

	// Get all topics from storage
	mut storage := t.storage
	topics := storage.list_topics() or {
		t.lock.@lock()
		t.total_errors++
		t.lock.unlock()
		return error('failed to list topics: ${err}')
	}

	// Rebalance each topic
	for topic in topics {
		topic_result := t.rebalance_topic(topic.name, active_brokers) or {
			t.logger.warn('Failed to rebalance topic', observability.field_string('topic',
				topic.name), observability.field_err_str(err.str()))
			result = RebalanceResult{
				...result
				error_count: result.error_count + 1
			}
			continue
		}
		result = t.merge_topic_result(result, topic_result)
	}

	elapsed := time.now().unix_milli() - start_time
	result = RebalanceResult{
		...result
		duration_ms: elapsed
	}

	t.partitions_moved.inc_by(f64(result.moved_partitions))
	t.rebalance_completed.inc()

	t.logger.info('Rebalance completed', observability.field_int('moved_partitions', i64(result.moved_partitions)),
		observability.field_int('topics_rebalanced', i64(result.topics_rebalanced)), observability.field_int('duration_ms',
		elapsed))

	return result
}

/// get_rebalance_status returns the current status of the rebalance trigger.
pub fn (mut t RebalanceTrigger) get_rebalance_status() RebalanceStatus {
	t.lock.@lock()
	defer { t.lock.unlock() }

	return RebalanceStatus{
		is_rebalancing:    t.is_rebalancing
		pending_requests:  t.pending_rebalances.len
		last_rebalance_at: t.last_rebalance_at
		total_rebalances:  t.total_rebalances
		total_errors:      t.total_errors
	}
}

// -- Internal helpers --

/// execute_after_delay waits for the configured debounce delay, then executes rebalance.
fn (mut t RebalanceTrigger) execute_after_delay() {
	time.sleep(time.Duration(t.config.rebalance_delay_ms * time.millisecond))

	t.lock.@lock()
	has_pending := t.pending_rebalances.len > 0
	t.lock.unlock()

	if has_pending {
		t.execute_rebalance() or {
			t.logger.warn('Debounced rebalance failed', observability.field_err_str(err.str()))
		}
	}
}

/// get_active_brokers retrieves the list of active brokers from the registry.
fn (mut t RebalanceTrigger) get_active_brokers() ![]domain.BrokerInfo {
	mut reg := t.registry
	brokers := reg.list_active_brokers()!

	if brokers.len == 0 {
		return error('no active brokers available')
	}
	return brokers
}

/// rebalance_topic performs rebalancing for a single topic and returns changes.
fn (mut t RebalanceTrigger) rebalance_topic(topic_name string, active_brokers []domain.BrokerInfo) !RebalanceResult {
	mut assigner := t.assigner

	// Snapshot current broker assignments before rebalance.
	// Uses a map to avoid V array reference sharing with the metadata port.
	current_assignments := assigner.list_partition_assignments(topic_name) or {
		return error('failed to get assignments for ${topic_name}: ${err}')
	}

	if current_assignments.len == 0 {
		return RebalanceResult{
			topics_rebalanced: 0
		}
	}

	mut old_broker_map := map[i32]i32{}
	for a in current_assignments {
		old_broker_map[a.partition] = a.preferred_broker
	}

	// Perform rebalance
	new_assignments := assigner.rebalance_partitions(topic_name, active_brokers) or {
		return error('rebalance failed for ${topic_name}: ${err}')
	}

	// Calculate changes by comparing snapshot with rebalanced state
	changes := t.calculate_changes_from_map(topic_name, old_broker_map, new_assignments)

	return RebalanceResult{
		moved_partitions:    changes.len
		assignments_changed: changes
		topics_rebalanced:   if changes.len > 0 { 1 } else { 0 }
	}
}

/// calculate_changes_from_map compares an old partition-to-broker map with new assignments.
fn (t &RebalanceTrigger) calculate_changes_from_map(topic_name string, old_broker_map map[i32]i32, new_assignments []domain.PartitionAssignment) []PartitionMovement {
	mut changes := []PartitionMovement{}

	for new_a in new_assignments {
		old_broker := old_broker_map[new_a.partition] or { continue }
		if old_broker != new_a.preferred_broker {
			changes << PartitionMovement{
				topic_name: topic_name
				partition:  new_a.partition
				old_broker: old_broker
				new_broker: new_a.preferred_broker
			}
		}
	}

	return changes
}

/// merge_topic_result merges a per-topic result into the aggregate result.
fn (t &RebalanceTrigger) merge_topic_result(aggregate RebalanceResult, topic_result RebalanceResult) RebalanceResult {
	mut merged_changes := aggregate.assignments_changed.clone()
	merged_changes << topic_result.assignments_changed

	return RebalanceResult{
		moved_partitions:    aggregate.moved_partitions + topic_result.moved_partitions
		assignments_changed: merged_changes
		duration_ms:         aggregate.duration_ms
		error_count:         aggregate.error_count + topic_result.error_count
		topics_rebalanced:   aggregate.topics_rebalanced + topic_result.topics_rebalanced
	}
}
