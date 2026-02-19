// 파티션-브로커 할당 관리 및 리밸런싱 알고리즘 구현
module cluster

import domain
import service.port
import infra.observability
import sync
import time
import rand

// 파티션 할당 서비스

/// PartitionAssigner는 파티션-브로커 할당을 관리하는 서비스입니다.
/// 라운드로빈 및 스티키 할당 전략을 지원하며, 브로커 변화 시 재할당을 처리합니다.
@[heap]
pub struct PartitionAssigner {
	broker_id  i32
	cluster_id string
mut:
	config        domain.PartitionAssignerConfig
	metadata_port ?port.ClusterMetadataPort
	lock          sync.RwMutex
	logger        &observability.Logger
	// 메트릭
	assignments_total        &observability.Metric
	rebalance_total          &observability.Metric
	rebalance_duration_ms    &observability.Metric
	assignment_changes_total &observability.Metric
}

/// PartitionAssignerConfig는 파티션 할당 서비스 설정을 담습니다.
pub struct PartitionAssignerServiceConfig {
pub:
	broker_id                 i32
	strategy                  domain.AssignmentStrategy = .round_robin
	rack_aware                bool
	sticky_assign             bool = true
	min_partitions_per_broker i32  = 1
	cluster_id                string
}

/// new_partition_assigner는 새로운 파티션 할당 서비스를 생성합니다.
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

// 초기 할당

/// assign_partitions는 새로운 토픽의 파티션을 브로커에 초기 할당합니다.
/// topic_name: 토픽 이름
/// partition_count: 파티션 수
/// brokers: 사용 가능한 브로커 목록
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

	// 라운드로빈 할당
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

		// 분산 스토리지에 저장
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

// 리밸런싱

/// rebalance_partitions는 브로커 변화에 따라 파티션을 재할당합니다.
/// topic_name: 토픽 이름
/// active_brokers: 현재 활성 브로커 목록
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

	// 현재 할당 조회
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

	// 리밸런싱 수행
	mut new_assignments := a.do_rebalance(topic_name, current_assignments, active_brokers)

	// 변경된 할당 저장
	mut changes_count := 0
	for mut assignment in new_assignments {
		// 이전 할당과 비교하여 변경 여부 확인
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
					// 변경 없음, 기존 에포크 유지
					assignment.partition_epoch = current.partition_epoch
				}
				break
			}
		}
		if !found {
			// 새로운 파티션
			changes_count++
			assignment.partition_epoch = 1
		}

		// 저장
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

/// do_rebalance는 실제 리밸런싱 알고리즘을 수행합니다.
fn (mut a PartitionAssigner) do_rebalance(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	match a.config.strategy {
		.round_robin { return a.rebalance_round_robin(topic_name, current, brokers) }
		.range { return a.rebalance_range(topic_name, current, brokers) }
		.sticky { return a.rebalance_sticky(topic_name, current, brokers) }
	}
}

/// rebalance_round_robin는 라운드로빈 방식으로 파티션을 재할당합니다.
fn (mut a PartitionAssigner) rebalance_round_robin(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	mut new_assignments := []domain.PartitionAssignment{}
	broker_count := brokers.len

	// 파티션을 정렬하여 일관된 할당 보장
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

/// rebalance_range는 범위 기반 방식으로 파티션을 재할당합니다.
fn (mut a PartitionAssigner) rebalance_range(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	mut new_assignments := []domain.PartitionAssignment{}
	broker_count := brokers.len
	partition_count := current.len

	// 각 브로커당 할당할 파티션 수 계산
	partitions_per_broker := partition_count / broker_count
	extra_partitions := partition_count % broker_count

	// 파티션을 정렬
	mut sorted_partitions := current.clone()
	sorted_partitions.sort(a.partition < b.partition)

	mut partition_idx := 0
	for broker_idx, broker in brokers {
		// 이 브로커에 할당할 파티션 수
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

/// rebalance_sticky는 스티키 방식으로 파티션을 재할당합니다.
/// 기존 할당을 최대한 유지하면서 불균형만 수정합니다.
fn (mut a PartitionAssigner) rebalance_sticky(topic_name string, current []domain.PartitionAssignment, brokers []domain.BrokerInfo) []domain.PartitionAssignment {
	mut new_assignments := []domain.PartitionAssignment{}

	// 활성 브로커 ID 집합 생성
	mut active_broker_ids := map[i32]bool{}
	for broker in brokers {
		active_broker_ids[broker.broker_id] = true
	}

	// 각 브로커당 현재 할당된 파티션 수 계산
	mut broker_partition_count := map[i32]int{}
	for broker in brokers {
		broker_partition_count[broker.broker_id] = 0
	}

	// 1단계: 살아있는 브로커에 할당된 파티션은 유지
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

	// 2단계: 죽은 브로커의 파티션을 재할당
	target_partitions_per_broker := current.len / brokers.len
	mut extra_partitions := current.len % brokers.len

	for assignment in current {
		if assignment.preferred_broker !in active_broker_ids {
			// 가장 적은 파티션을 가진 브로커 찾기
			mut min_broker_id := brokers[0].broker_id
			mut min_count := broker_partition_count[min_broker_id]

			for broker in brokers {
				count := broker_partition_count[broker.broker_id]
				if count < min_count {
					min_count = count
					min_broker_id = broker.broker_id
				}
			}

			// 해당 브로커에 할당
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

	// 3단계: 불균형 수정 (선택적)
	if a.config.sticky_assign {
		new_assignments = a.rebalance_even_distribution(new_assignments, brokers, target_partitions_per_broker,
			extra_partitions)
	}

	return new_assignments
}

/// rebalance_even_distribution은 파티션을 균등하게 분배합니다.
fn (mut a PartitionAssigner) rebalance_even_distribution(current []domain.PartitionAssignment, brokers []domain.BrokerInfo, target int, extra int) []domain.PartitionAssignment {
	mut result := current.clone()

	// 각 브로커당 현재 파티션 수 계산
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

	// 과부하된 브로커에서 파티션 이동
	for broker in brokers {
		broker_id := broker.broker_id
		count := broker_partition_count[broker_id]
		max_allowed := target + if remaining_extra > 0 { 1 } else { 0 }

		if count > max_allowed {
			// 이 브로커에서 파티션 이동 필요
			mut to_move := count - max_allowed

			for i := 0; i < result.len && to_move > 0; i++ {
				if result[i].preferred_broker == broker_id {
					// 가장 적은 파티션을 가진 브로커 찾기
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

					// 최소 부하 브로커가 여유가 있으면 이동
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

// 조회 메서드

/// get_partition_leader는 특정 파티션의 현재 리더 브로커 ID를 반환합니다.
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

/// get_partition_assignment는 특정 파티션의 할당 정보를 반환합니다.
pub fn (mut a PartitionAssigner) get_partition_assignment(topic_name string, partition i32) !domain.PartitionAssignment {
	a.lock.rlock()
	defer { a.lock.runlock() }

	if mut mp := a.metadata_port {
		return mp.get_partition_assignment(topic_name, partition)
	}

	return error('metadata port not available')
}

/// list_partition_assignments는 토픽의 모든 파티션 할당을 반환합니다.
pub fn (mut a PartitionAssigner) list_partition_assignments(topic_name string) ![]domain.PartitionAssignment {
	a.lock.rlock()
	defer { a.lock.runlock() }

	if mut mp := a.metadata_port {
		return mp.list_partition_assignments(topic_name)
	}

	return []domain.PartitionAssignment{}
}

// 재할당 계획

/// generate_reassignment_plan은 브로커 변화에 따른 재할당 계획을 생성합니다.
/// changes: 브로커 변화 정보 (추가/제거된 브로커)
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

	// 모든 토픽의 할당을 검사하여 변경 필요한 파티션 식별
	// TODO: 토픽 목록을 가져오는 메서드 필요
	// 현재는 빈 계획 반환

	a.logger.info('Generated reassignment plan', observability.field_string('plan_id',
		plan_id), observability.field_string('reason', changes.reason), observability.field_int('added_brokers',
		i64(changes.added.len)), observability.field_int('removed_brokers', i64(changes.removed.len)))

	return plan
}

/// BrokerChanges는 브로커 변화 정보를 담습니다.
pub struct BrokerChanges {
pub:
	reason  string
	added   []domain.BrokerInfo
	removed []domain.BrokerInfo
}

// 설정 변경

/// set_strategy는 할당 전략을 변경합니다.
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

/// set_sticky_assign은 스티키 할당 모드를 설정합니다.
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

/// assignment_strategy_to_string는 AssignmentStrategy를 문자열로 변환합니다.
fn assignment_strategy_to_string(s domain.AssignmentStrategy) string {
	return match s {
		.round_robin { 'round_robin' }
		.range { 'range' }
		.sticky { 'sticky' }
	}
}
