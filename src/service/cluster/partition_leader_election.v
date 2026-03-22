/// Partition-level leader election.
/// Manages leader assignment per partition, handles broker failure failover,
/// and supports preferred leader election for rebalancing.
///
/// Integration points:
/// - IsrManager: provides current ISR for election decisions
/// - BrokerRegistry: on broker death, call on_broker_failed()
/// - ControllerElector: only the controller should trigger elections
module cluster

import sync
import infra.observability

/// LeaderElectionConfig holds configuration for partition leader election.
pub struct LeaderElectionConfig {
pub:
	preferred_leader_check_interval_ms i64 = 300000
	unclean_leader_election_enable     bool
}

/// PartitionLeadership tracks the leadership state for a single partition.
pub struct PartitionLeadership {
pub:
	topic            string
	partition        int
	leader_id        int
	leader_epoch     int
	replicas         []int
	preferred_leader int
}

/// PartitionLeaderElector manages leader election for all partitions.
pub struct PartitionLeaderElector {
mut:
	lock              sync.RwMutex
	partition_leaders map[string]PartitionLeadership
	isr_manager       &IsrManager
	logger            &observability.Logger
	config            LeaderElectionConfig
}

/// new_partition_leader_elector creates a new PartitionLeaderElector.
pub fn new_partition_leader_elector(mut isr_manager IsrManager, config LeaderElectionConfig) &PartitionLeaderElector {
	return &PartitionLeaderElector{
		partition_leaders: map[string]PartitionLeadership{}
		isr_manager:       isr_manager
		logger:            observability.get_named_logger('partition_leader_election')
		config:            config
	}
}

/// register_partition registers a partition and assigns initial leadership.
/// The first replica is set as the preferred leader with epoch 1.
pub fn (mut e PartitionLeaderElector) register_partition(topic string, partition int, replicas []int) ! {
	if replicas.len == 0 {
		return error('cannot register partition with empty replicas')
	}

	key := make_leader_key(topic, partition)
	leadership := PartitionLeadership{
		topic:            topic
		partition:        partition
		leader_id:        replicas[0]
		leader_epoch:     1
		replicas:         replicas.clone()
		preferred_leader: replicas[0]
	}

	e.lock.@lock()
	e.partition_leaders[key] = leadership
	e.lock.unlock()

	e.logger.info('Partition registered', observability.field_string('topic', topic),
		observability.field_int('partition', i64(partition)), observability.field_int('leader',
		i64(replicas[0])))
}

/// elect_leader elects a new leader for a partition.
/// Picks the first broker in ISR by replica order.
/// Falls back to unclean election if configured.
pub fn (mut e PartitionLeaderElector) elect_leader(topic string, partition int) !PartitionLeadership {
	key := make_leader_key(topic, partition)

	e.lock.@lock()
	defer { e.lock.unlock() }

	current := e.partition_leaders[key] or {
		return error('partition ${topic}-${partition} not registered')
	}

	isr := e.isr_manager.get_isr(topic, partition)
	new_leader := pick_leader_from_isr(current.replicas, isr)

	if new_leader >= 0 {
		return e.apply_new_leader(key, current, new_leader)
	}

	return e.handle_empty_isr(key, current)
}

/// get_leader returns the current leadership for a partition.
pub fn (mut e PartitionLeaderElector) get_leader(topic string, partition int) ?PartitionLeadership {
	key := make_leader_key(topic, partition)

	e.lock.rlock()
	defer { e.lock.runlock() }

	if key in e.partition_leaders {
		return e.partition_leaders[key]
	}
	return none
}

/// on_broker_failed re-elects leaders for all partitions where the broker was leader.
/// Returns the list of new leadership assignments.
pub fn (mut e PartitionLeaderElector) on_broker_failed(broker_id int) []PartitionLeadership {
	affected_keys := e.find_partitions_led_by(broker_id)
	mut results := []PartitionLeadership{}

	for key in affected_keys {
		parts := parse_leader_key(key)
		leadership := e.elect_leader(parts.topic, parts.partition) or {
			e.logger.error('Re-election failed on broker failure', observability.field_string('key',
				key), observability.field_string('error', err.msg()))
			continue
		}
		results << leadership
	}
	return results
}

/// trigger_preferred_leader_election switches leadership to the preferred leader
/// if the preferred leader is in the ISR and is not already the leader.
pub fn (mut e PartitionLeaderElector) trigger_preferred_leader_election(topic string, partition int) !PartitionLeadership {
	key := make_leader_key(topic, partition)

	e.lock.@lock()
	defer { e.lock.unlock() }

	current := e.partition_leaders[key] or {
		return error('partition ${topic}-${partition} not registered')
	}

	if current.leader_id == current.preferred_leader {
		return error('preferred leader is already the leader')
	}

	isr := e.isr_manager.get_isr(topic, partition)
	if current.preferred_leader !in isr {
		return error('preferred leader ${current.preferred_leader} not in ISR')
	}

	return e.apply_new_leader(key, current, current.preferred_leader)
}

/// get_all_leaders returns all current partition leadership assignments.
pub fn (mut e PartitionLeaderElector) get_all_leaders() map[string]PartitionLeadership {
	e.lock.rlock()
	defer { e.lock.runlock() }

	mut result := map[string]PartitionLeadership{}
	for k, v in e.partition_leaders {
		result[k] = v
	}
	return result
}

// -- Internal helpers --

/// apply_new_leader creates a new leadership with incremented epoch.
/// Must be called while holding e.lock.
fn (mut e PartitionLeaderElector) apply_new_leader(key string, current PartitionLeadership, new_leader int) PartitionLeadership {
	new_epoch := current.leader_epoch + 1
	leadership := PartitionLeadership{
		topic:            current.topic
		partition:        current.partition
		leader_id:        new_leader
		leader_epoch:     new_epoch
		replicas:         current.replicas
		preferred_leader: current.preferred_leader
	}
	e.partition_leaders[key] = leadership

	e.logger.info('Leader elected', observability.field_string('topic', current.topic),
		observability.field_int('partition', i64(current.partition)), observability.field_int('new_leader',
		i64(new_leader)), observability.field_int('epoch', i64(new_epoch)))

	return leadership
}

/// handle_empty_isr handles election when ISR is empty.
/// Must be called while holding e.lock.
fn (mut e PartitionLeaderElector) handle_empty_isr(key string, current PartitionLeadership) !PartitionLeadership {
	if !e.config.unclean_leader_election_enable {
		return error('no eligible replicas: ISR is empty and unclean leader election is disabled')
	}

	if current.replicas.len == 0 {
		return error('no eligible replicas: both ISR and replicas are empty')
	}

	e.logger.warn('Unclean leader election', observability.field_string('topic', current.topic),
		observability.field_int('partition', i64(current.partition)), observability.field_int('new_leader',
		i64(current.replicas[0])))

	return e.apply_new_leader(key, current, current.replicas[0])
}

/// find_partitions_led_by returns keys of partitions led by the given broker.
fn (mut e PartitionLeaderElector) find_partitions_led_by(broker_id int) []string {
	e.lock.rlock()
	defer { e.lock.runlock() }

	mut keys := []string{}
	for k, v in e.partition_leaders {
		if v.leader_id == broker_id {
			keys << k
		}
	}
	return keys
}

/// pick_leader_from_isr selects the first replica that is in the ISR.
/// Returns -1 if no replica is in the ISR.
fn pick_leader_from_isr(replicas []int, isr []int) int {
	for r in replicas {
		if r in isr {
			return r
		}
	}
	return -1
}

/// LeaderKeyParts holds parsed topic and partition from a leader key.
struct LeaderKeyParts {
	topic     string
	partition int
}

/// parse_leader_key extracts topic and partition from a "topic-partition" key.
fn parse_leader_key(key string) LeaderKeyParts {
	idx := key.last_index('-') or { return LeaderKeyParts{
		topic:     key
		partition: 0
	} }
	topic := key[..idx]
	partition := key[idx + 1..].int()
	return LeaderKeyParts{
		topic:     topic
		partition: partition
	}
}

/// make_leader_key creates a map key from topic and partition.
fn make_leader_key(topic string, partition int) string {
	return '${topic}-${partition}'
}
