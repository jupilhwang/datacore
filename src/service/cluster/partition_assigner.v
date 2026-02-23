// Manages partition-broker assignment and implements rebalancing algorithms
module cluster

import domain
import service.port
import infra.observability
import sync
import time
import rand

// Partition Assignment Service

/// PartitionAssigner is a service that manages partition-broker assignment.
/// Supports round-robin and sticky assignment strategies, and handles reassignment on broker changes.
@[heap]
pub struct PartitionAssigner {
	broker_id  i32
	cluster_id string
mut:
	config        domain.PartitionAssignerConfig
	metadata_port ?port.ClusterMetadataPort
	lock          sync.RwMutex
	logger        &observability.Logger
	// Metrics
	assignments_total        &observability.Metric
	rebalance_total          &observability.Metric
	rebalance_duration_ms    &observability.Metric
	assignment_changes_total &observability.Metric
}

/// PartitionAssignerConfig holds partition assignment service configuration.
pub struct PartitionAssignerServiceConfig {
pub:
	broker_id                 i32
	strategy                  domain.AssignmentStrategy = .round_robin
	rack_aware                bool
	sticky_assign             bool = true
	min_partitions_per_broker i32  = 1
	cluster_id                string
}

/// new_partition_assigner creates a new partition assignment service.
pub fn new_partition_assigner(config PartitionAssignerServiceConfig, metadata_port ?port.ClusterMetadataPort) &PartitionAssigner {
	logger := observability.get_named_logger('partition_assigner')
	mut reg := observability.get_registry()

	return &PartitionAssigner{
		broker_id:                config.broker_id
		cluster_id:               config.cluster_id
		config:                   domain.PartitionAssignerConfig{
			strategy:      config.strategy
			rack_aware:    config.rack_aware
			sticky_assign: config.sticky_assign
		}
		metadata_port:            metadata_port
		logger:                   logger
		assignments_total:        reg.register('partition_assigner_assignments_total',
			'Total partition assignments', .counter)
		rebalance_total:          reg.register('partition_assigner_rebalance_total', 'Total rebalances performed',
			.counter)
		rebalance_duration_ms:    reg.register('partition_assigner_rebalance_duration_ms',
			'Rebalance duration in milliseconds', .histogram)
		assignment_changes_total: reg.register('partition_assigner_assignment_changes_total',
			'Total assignment changes', .counter)
	}
}

// Initial Assignment

/// assign_partitions performs initial partition assignment for a new topic to brokers.
/// topic_name: topic name
/// partition_count: partition count
/// brokers: list of available brokers
pub fn (mut a PartitionAssigner) assign_partitions(topic_name string, partition_count int, brokers []domain.BrokerInfo) ![]domain.PartitionAssignment {
	a.lock.@lock()
	defer { a.lock.unlock() }

	if brokers.len == 0 {
		return error('no available brokers for partition assignment')
	}

	if partition_count <= 0 {
		return error('partition count must be positive')
	}

	a.logger.info('Assigning partitions for topic', observability.field_string('topic',
		topic_name), observability.field_int('partition_count', i64(partition_count)),
		observability.field_int('broker_count', i64(brokers.len)))

	mut assignments := []domain.PartitionAssignment{}

	// Round-robin assignment
	for i in 0 .. partition_count {
		broker_idx := i % brokers.len
		broker := brokers[broker_idx]

		assignment := domain.PartitionAssignment{
			topic_name:       topic_name
			partition:        i32(i)
			preferred_broker: broker.broker_id
			replica_brokers:  [broker.broker_id]
			isr_brokers:      [broker.broker_id]
			partition_epoch:  1
		}

		assignments << assignment
		a.assignments_total.inc()

		// Store to distributed storage
		if mut mp := a.metadata_port {
			mp.update_partition_assignment(assignment) or {
				a.logger.warn('Failed to store partition assignment', observability.field_string('topic',
					topic_name), observability.field_int('partition', i64(i)), observability.field_err_str(err.str()))
			}
		}
	}

	a.logger.info('Partition assignment completed', observability.field_string('topic',
		topic_name), observability.field_int('assigned_partitions', i64(assignments.len)))

	return assignments
}

// Rebalancing

/// rebalance_partitions reassigns partitions based on broker changes.
/// topic_name: topic name
/// active_brokers: list of currently active brokers
pub fn (mut a PartitionAssigner) rebalance_partitions(topic_name string, active_brokers []domain.BrokerInfo) ![]domain.PartitionAssignment {
	a.lock.@lock()
	defer { a.lock.unlock() }

	if active_brokers.len == 0 {
		return error('no active brokers available for rebalance')
	}

	mut timer := a.rebalance_duration_ms.start_timer()
	defer { timer.observe_duration() }

	a.logger.info('Starting partition rebalance', observability.field_string('topic',
		topic_name), observability.field_int('active_brokers', i64(active_brokers.len)))

	// Get current assignments
	mut current_assignments := []domain.PartitionAssignment{}
	if mut mp := a.metadata_port {
		current_assignments = mp.list_partition_assignments(topic_name) or {
			[]domain.PartitionAssignment{}
		}
	}

	if current_assignments.len == 0 {
		a.logger.warn('No existing assignments found for topic', observability.field_string('topic',
			topic_name))
		return []domain.PartitionAssignment{}
	}

	// Perform rebalancing
	mut new_assignments := a.do_rebalance(topic_name, current_assignments, active_brokers)

	// Store changed assignments
	mut changes_count := 0
	for mut assignment in new_assignments {
		// Compare with previous assignment to detect changes
		mut found := false
		for current in current_assignments {
			if current.partition == assignment.partition {
				found = true
				if current.preferred_broker != assignment.preferred_broker {
					changes_count++
					assignment.partition_epoch = current.partition_epoch + 1
					assignment.reassigned_at = time.now().unix_milli()
					a.logger.info('Partition reassigned', observability.field_string('topic',
						topic_name), observability.field_int('partition', i64(assignment.partition)),
						observability.field_int('old_broker', i64(current.preferred_broker)),
						observability.field_int('new_broker', i64(assignment.preferred_broker)))
				} else {
					// No change, keep existing epoch
					assignment.partition_epoch = current.partition_epoch
				}
				break
			}
		}
		if !found {
			// New partition
			changes_count++
			assignment.partition_epoch = 1
		}

		// Store
		if mut mp := a.metadata_port {
			mp.update_partition_assignment(assignment) or {
				a.logger.warn('Failed to update partition assignment', observability.field_string('topic',
					topic_name), observability.field_int('partition', i64(assignment.partition)),
					observability.field_err_str(err.str()))
			}
		}
	}

	a.rebalance_total.inc()
	a.assignment_changes_total.inc_by(f64(changes_count))

	a.logger.info('Partition rebalance completed', observability.field_string('topic',
		topic_name), observability.field_int('total_partitions', i64(new_assignments.len)),
		observability.field_int('changes', i64(changes_count)))

	return new_assignments
}

/// do_rebalance performs the actual rebalancing algorithm.
fn (mut a PartitionAssigner) do_rebalance(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	match a.config.strategy {
		.round_robin { return a.rebalance_round_robin(topic_name, current, brokers) }
		.range { return a.rebalance_range(topic_name, current, brokers) }
		.sticky { return a.rebalance_sticky(topic_name, current, brokers) }
	}
}

/// rebalance_round_robin reassigns partitions using round-robin strategy.
fn (mut a PartitionAssigner) rebalance_round_robin(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	mut new_assignments := []domain.PartitionAssignment{}
	broker_count := brokers.len

	// Sort partitions to ensure consistent assignment
	mut sorted_partitions := current.clone()
	sorted_partitions.sort(a.partition < b.partition)

	for i, assignment in sorted_partitions {
		broker_idx := i % broker_count
		broker := brokers[broker_idx]

		new_assignments << domain.PartitionAssignment{
			topic_name:       topic_name
			partition:        assignment.partition
			preferred_broker: broker.broker_id
			replica_brokers:  [broker.broker_id]
			isr_brokers:      [broker.broker_id]
			partition_epoch:  assignment.partition_epoch
		}
	}

	return new_assignments
}

/// rebalance_range reassigns partitions using range-based strategy.
fn (mut a PartitionAssigner) rebalance_range(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	mut new_assignments := []domain.PartitionAssignment{}
	broker_count := brokers.len
	partition_count := current.len

	// Calculate number of partitions to assign per broker
	partitions_per_broker := partition_count / broker_count
	extra_partitions := partition_count % broker_count

	// Sort partitions
	mut sorted_partitions := current.clone()
	sorted_partitions.sort(a.partition < b.partition)

	mut partition_idx := 0
	for broker_idx, broker in brokers {
		// Number of partitions to assign to this broker
		count := partitions_per_broker + if broker_idx < extra_partitions { 1 } else { 0 }

		for _ in 0 .. count {
			if partition_idx >= sorted_partitions.len {
				break
			}
			assignment := sorted_partitions[partition_idx]
			new_assignments << domain.PartitionAssignment{
				topic_name:       topic_name
				partition:        assignment.partition
				preferred_broker: broker.broker_id
				replica_brokers:  [broker.broker_id]
				isr_brokers:      [broker.broker_id]
				partition_epoch:  assignment.partition_epoch
			}
			partition_idx++
		}
	}

	return new_assignments
}

/// rebalance_sticky reassigns partitions using sticky strategy.
/// Preserves existing assignments as much as possible and only corrects imbalances.
fn (mut a PartitionAssigner) rebalance_sticky(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	mut new_assignments := []domain.PartitionAssignment{}

	// Build set of active broker IDs
	mut active_broker_ids := map[i32]bool{}
	for broker in brokers {
		active_broker_ids[broker.broker_id] = true
	}

	// Count currently assigned partitions per broker
	mut broker_partition_count := map[i32]int{}
	for broker in brokers {
		broker_partition_count[broker.broker_id] = 0
	}

	// Step 1: Keep partitions assigned to alive brokers
	for assignment in current {
		if assignment.preferred_broker in active_broker_ids {
			new_assignments << domain.PartitionAssignment{
				topic_name:       topic_name
				partition:        assignment.partition
				preferred_broker: assignment.preferred_broker
				replica_brokers:  assignment.replica_brokers.clone()
				isr_brokers:      assignment.isr_brokers.clone()
				partition_epoch:  assignment.partition_epoch
			}
			broker_partition_count[assignment.preferred_broker]++
		}
	}

	// Step 2: Reassign partitions from dead brokers
	target_partitions_per_broker := current.len / brokers.len
	mut extra_partitions := current.len % brokers.len

	for assignment in current {
		if assignment.preferred_broker !in active_broker_ids {
			// Find broker with fewest partitions
			mut min_broker_id := brokers[0].broker_id
			mut min_count := broker_partition_count[min_broker_id]

			for broker in brokers {
				count := broker_partition_count[broker.broker_id]
				if count < min_count {
					min_count = count
					min_broker_id = broker.broker_id
				}
			}

			// Assign to that broker
			new_assignments << domain.PartitionAssignment{
				topic_name:       topic_name
				partition:        assignment.partition
				preferred_broker: min_broker_id
				replica_brokers:  [min_broker_id]
				isr_brokers:      [min_broker_id]
				partition_epoch:  assignment.partition_epoch
			}
			broker_partition_count[min_broker_id]++
		}
	}

	// Step 3: Correct imbalance (optional)
	if a.config.sticky_assign {
		new_assignments = a.rebalance_even_distribution(new_assignments, brokers, target_partitions_per_broker,
			extra_partitions)
	}

	return new_assignments
}

/// rebalance_even_distribution distributes partitions evenly.
fn (mut a PartitionAssigner) rebalance_even_distribution(current []domain.PartitionAssignment, brokers []domain.BrokerInfo, target int, extra int) []domain.PartitionAssignment {
	mut result := current.clone()

	// Calculate current partition count per broker
	mut broker_partition_count := map[i32]int{}
	for broker in brokers {
		broker_partition_count[broker.broker_id] = 0
	}
	for assignment in result {
		if assignment.preferred_broker in broker_partition_count {
			broker_partition_count[assignment.preferred_broker]++
		}
	}

	mut remaining_extra := extra

	// Move partitions from overloaded brokers
	for broker in brokers {
		broker_id := broker.broker_id
		count := broker_partition_count[broker_id]
		max_allowed := target + if remaining_extra > 0 { 1 } else { 0 }

		if count > max_allowed {
			// Need to move partitions from this broker
			mut to_move := count - max_allowed

			for i := 0; i < result.len && to_move > 0; i++ {
				if result[i].preferred_broker == broker_id {
					// Find broker with fewest partitions
					mut min_broker_id := brokers[0].broker_id
					mut min_count := broker_partition_count[min_broker_id]

					for b in brokers {
						if b.broker_id == broker_id {
							continue
						}
						c := broker_partition_count[b.broker_id]
						if c < min_count {
							min_count = c
							min_broker_id = b.broker_id
						}
					}

					// Move if least-loaded broker has capacity
					if min_count < target {
						result[i].preferred_broker = min_broker_id
						result[i].replica_brokers = [min_broker_id]
						result[i].isr_brokers = [min_broker_id]
						broker_partition_count[broker_id]--
						broker_partition_count[min_broker_id]++
						to_move--
					}
				}
			}

			if remaining_extra > 0 {
				remaining_extra--
			}
		}
	}

	return result
}

// Query Methods

/// get_partition_leader returns the current leader broker ID for a specific partition.
pub fn (mut a PartitionAssigner) get_partition_leader(topic_name string, partition i32) !i32 {
	a.lock.rlock()
	defer { a.lock.runlock() }

	if mut mp := a.metadata_port {
		assignment := mp.get_partition_assignment(topic_name, partition) or {
			return error('partition assignment not found: ${topic_name}-${partition}')
		}
		return assignment.preferred_broker
	}

	return error('metadata port not available')
}

/// get_partition_assignment returns assignment information for a specific partition.
pub fn (mut a PartitionAssigner) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	a.lock.rlock()
	defer { a.lock.runlock() }

	if mut mp := a.metadata_port {
		return mp.get_partition_assignment(topic_name, partition)
	}

	return error('metadata port not available')
}

/// list_partition_assignments returns all partition assignments for a topic.
pub fn (mut a PartitionAssigner) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	a.lock.rlock()
	defer { a.lock.runlock() }

	if mut mp := a.metadata_port {
		return mp.list_partition_assignments(topic_name)
	}

	return []domain.PartitionAssignment{}
}

// Reassignment Plan

/// generate_reassignment_plan generates a reassignment plan based on broker changes.
/// changes: broker change information (added/removed brokers)
pub fn (mut a PartitionAssigner) generate_reassignment_plan(changes BrokerChanges) !domain.ReassignmentPlan {
	a.lock.rlock()
	defer { a.lock.runlock() }

	plan_id := rand.uuid_v4()
	now := time.now().unix_milli()

	mut plan := domain.ReassignmentPlan{
		plan_id:        plan_id
		cluster_id:     a.cluster_id
		trigger_reason: changes.reason
		changes:        []domain.PartitionAssignmentChange{}
		created_at:     now
		executed_at:    0
		status:         .pending
	}

	// Inspect assignments for all topics to identify partitions needing change
	// TODO: Need method to get topic list
	// Currently returning empty plan

	a.logger.info('Generated reassignment plan', observability.field_string('plan_id',
		plan_id), observability.field_string('reason', changes.reason), observability.field_int('added_brokers',
		i64(changes.added.len)), observability.field_int('removed_brokers', i64(changes.removed.len)))

	return plan
}

/// BrokerChanges holds broker change information.
pub struct BrokerChanges {
pub:
	reason  string
	added   []domain.BrokerInfo
	removed []domain.BrokerInfo
}

// Configuration Changes

/// set_strategy changes the assignment strategy.
pub fn (mut a PartitionAssigner) set_strategy(strategy domain.AssignmentStrategy) {
	a.lock.@lock()
	defer { a.lock.unlock() }

	unsafe {
		mut config := a.config
		config.strategy = strategy
	}
	a.logger.info('Assignment strategy changed', observability.field_string('strategy',
		assignment_strategy_to_string(strategy)))
}

/// set_sticky_assign sets the sticky assignment mode.
pub fn (mut a PartitionAssigner) set_sticky_assign(enabled bool) {
	a.lock.@lock()
	defer { a.lock.unlock() }

	unsafe {
		mut config := a.config
		config.sticky_assign = enabled
	}
	a.logger.info('Sticky assignment mode changed', observability.field_bool('enabled',
		enabled))
}

/// assignment_strategy_to_string converts AssignmentStrategy to a string.
fn assignment_strategy_to_string(s domain.AssignmentStrategy) string {
	return match s {
		.round_robin { 'round_robin' }
		.range { 'range' }
		.sticky { 'sticky' }
	}
}
