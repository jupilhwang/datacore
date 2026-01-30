// 서비스 레이어 - 브로커 레지스트리
// 클러스터 내 브로커 등록, 하트비트, 상태 모니터링을 관리합니다.
module cluster

import domain
import service.port
import infra.observability
import sync
import time

// 브로커 레지스트리

/// MetricsProvider는 외부에서 브로커 부하 메트릭을 제공하는 콜백 함수 타입입니다.
/// 연결 수, 초당 바이트 입출력 등의 메트릭을 반환합니다.
pub type MetricsProvider = fn () domain.BrokerLoad

/// BrokerRegistry는 클러스터 내 브로커 등록 및 상태를 관리합니다.
/// 단일 브로커 모드와 멀티 브로커 모드를 모두 지원합니다.
pub struct BrokerRegistry {
	config domain.ClusterConfig
mut:
	local_broker     domain.BrokerInfo         // 로컬 브로커 정보
	metadata_port    ?port.ClusterMetadataPort // 클러스터 메타데이터 포트 (분산 스토리지용)
	brokers          map[i32]domain.BrokerInfo // 브로커 인메모리 캐시 (단일 브로커 모드 또는 캐싱용)
	lock             sync.RwMutex              // 스레드 안전성
	running          bool                      // 백그라운드 워커 제어
	capability       domain.StorageCapability  // 스토리지 기능 정보
	metrics_provider ?MetricsProvider          // 메트릭 프로바이더 콜백 (v0.29.0)
	prev_bytes_in    u64                       // 이전 입력 바이트 (속도 계산용)
	prev_bytes_out   u64                       // 이전 출력 바이트 (속도 계산용)
	prev_time        i64                       // 이전 시간 (속도 계산용)
	// 파티션 할당 서비스
	partition_assigner ?&PartitionAssigner   // 파티션 할당 서비스 (v0.40.0)
	logger             &observability.Logger // 구조화된 로거
	// 브로커 변경 콜백
	on_broker_change_cb ?fn (changes BrokerChanges) // 브로커 변경 시 호출될 콜백
}

/// BrokerRegistryConfig는 레지스트리 설정을 담습니다.
pub struct BrokerRegistryConfig {
pub:
	broker_id  i32    // 브로커 ID
	host       string // 호스트명
	port       i32    // 포트
	rack       string // 랙 정보
	cluster_id string // 클러스터 ID
	version    string // 버전
	// 타이밍 설정
	heartbeat_interval_ms i32 = 3000  // 하트비트 간격 (ms)
	session_timeout_ms    i32 = 10000 // 세션 타임아웃 (ms)
}

/// new_broker_registry는 새로운 브로커 레지스트리를 생성합니다.
/// config: 브로커 설정
/// capability: 스토리지 기능 정보
/// metadata_port: 분산 스토리지용 클러스터 메타데이터 포트 (멀티 브로커 모드)
pub fn new_broker_registry(config BrokerRegistryConfig, capability domain.StorageCapability, metadata_port ?port.ClusterMetadataPort) &BrokerRegistry {
	local_broker := domain.BrokerInfo{
		broker_id:         config.broker_id
		host:              config.host
		port:              config.port
		rack:              config.rack
		security_protocol: 'PLAINTEXT'
		endpoints:         [
			domain.BrokerEndpoint{
				name:              'PLAINTEXT'
				host:              config.host
				port:              config.port
				security_protocol: 'PLAINTEXT'
			},
		]
		status:            .starting
		registered_at:     time.now().unix_milli()
		last_heartbeat:    time.now().unix_milli()
		version:           config.version
		features:          []string{}
	}

	cluster_config := domain.ClusterConfig{
		cluster_id:                   config.cluster_id
		broker_heartbeat_interval_ms: config.heartbeat_interval_ms
		broker_session_timeout_ms:    config.session_timeout_ms
		multi_broker_enabled:         capability.supports_multi_broker
	}

	return &BrokerRegistry{
		config:              cluster_config
		local_broker:        local_broker
		metadata_port:       metadata_port
		brokers:             map[i32]domain.BrokerInfo{}
		capability:          capability
		running:             false
		prev_bytes_in:       0
		prev_bytes_out:      0
		prev_time:           time.now().unix_milli()
		partition_assigner:  none
		logger:              observability.get_named_logger('broker_registry')
		on_broker_change_cb: none
	}
}

/// set_metrics_provider는 브로커 부하 메트릭을 제공하는 콜백 함수를 설정합니다.
/// 이 콜백은 heartbeat_loop에서 주기적으로 호출되어 실제 서버 메트릭을 수집합니다.
pub fn (mut r BrokerRegistry) set_metrics_provider(provider MetricsProvider) {
	r.metrics_provider = provider
}

/// set_partition_assigner는 파티션 할당 서비스를 설정합니다.
pub fn (mut r BrokerRegistry) set_partition_assigner(assigner &PartitionAssigner) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.partition_assigner = assigner
	r.logger.info('Partition assigner registered')
}

/// set_on_broker_change는 브로커 변경 시 호출될 콜백을 설정합니다.
pub fn (mut r BrokerRegistry) set_on_broker_change(callback fn (changes BrokerChanges)) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.on_broker_change_cb = callback
}

// 등록

/// register는 로컬 브로커를 클러스터에 등록합니다.
pub fn (mut r BrokerRegistry) register() !domain.BrokerInfo {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	r.local_broker.registered_at = now
	r.local_broker.last_heartbeat = now
	r.local_broker.status = .active

	// 분산 스토리지를 사용하는 멀티 브로커 모드인 경우
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			// 분산 스토리지에 등록
			registered := mp.register_broker(r.local_broker)!
			r.local_broker = registered
			r.brokers[registered.broker_id] = registered

			// 새 브로커 가입 이벤트 트리거
			changes := BrokerChanges{
				reason:  'broker_joined'
				added:   [registered]
				removed: []domain.BrokerInfo{}
			}
			r.on_broker_change(changes) or {
				r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
			}

			return registered
		}
	}

	// 단일 브로커 모드 - 로컬에만 저장
	r.brokers[r.local_broker.broker_id] = r.local_broker
	return r.local_broker
}

/// deregister는 로컬 브로커를 클러스터에서 제거합니다.
pub fn (mut r BrokerRegistry) deregister() ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.local_broker.status = .shutdown

	// 브로커 퇴장 이벤트 트리거 (삭제 전에)
	changes := BrokerChanges{
		reason:  'broker_left'
		added:   []domain.BrokerInfo{}
		removed: [r.local_broker]
	}
	r.on_broker_change(changes) or {
		r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
	}

	// 분산 스토리지를 사용하는 멀티 브로커 모드인 경우
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			mp.deregister_broker(r.local_broker.broker_id)!
		}
	}

	r.brokers.delete(r.local_broker.broker_id)
}

// 하트비트

/// send_heartbeat는 로컬 브로커의 하트비트를 전송합니다.
pub fn (mut r BrokerRegistry) send_heartbeat(load domain.BrokerLoad) ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	r.local_broker.last_heartbeat = now

	heartbeat := domain.BrokerHeartbeat{
		broker_id:      r.local_broker.broker_id
		timestamp:      now
		current_load:   load
		wants_shutdown: r.local_broker.status == .draining
	}

	// 분산 스토리지를 사용하는 멀티 브로커 모드인 경우
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			mp.update_broker_heartbeat(heartbeat)!
		}
	}

	// 로컬 캐시 업데이트
	r.brokers[r.local_broker.broker_id] = r.local_broker
}

// 조회

/// get_local_broker는 로컬 브로커 정보를 반환합니다.
pub fn (r &BrokerRegistry) get_local_broker() domain.BrokerInfo {
	return r.local_broker
}

/// get_broker는 특정 브로커의 정보를 반환합니다.
pub fn (mut r BrokerRegistry) get_broker(broker_id i32) !domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// 분산 스토리지 먼저 시도
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.get_broker(broker_id)
		}
	}

	// 로컬 캐시로 폴백
	return r.brokers[broker_id] or { return error('broker not found: ${broker_id}') }
}

/// list_brokers는 등록된 모든 브로커를 반환합니다.
pub fn (mut r BrokerRegistry) list_brokers() ![]domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// 분산 스토리지 먼저 시도
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_brokers()
		}
	}

	// 로컬 캐시로 폴백
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		result << broker
	}
	return result
}

/// list_active_brokers는 활성 브로커만 반환합니다.
pub fn (mut r BrokerRegistry) list_active_brokers() ![]domain.BrokerInfo {
	r.lock.rlock()
	defer { r.lock.runlock() }

	// 분산 스토리지 먼저 시도
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_active_brokers()
		}
	}

	// 로컬 캐시로 폴백 - 활성 브로커 필터링
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		if broker.status == .active {
			result << broker
		}
	}
	return result
}

// 상태 모니터링

/// check_expired_brokers는 하트비트를 놓친 브로커를 확인합니다.
pub fn (mut r BrokerRegistry) check_expired_brokers() ![]i32 {
	r.lock.@lock()
	defer { r.lock.unlock() }

	now := time.now().unix_milli()
	timeout := i64(r.config.broker_session_timeout_ms)
	mut expired := []i32{}
	mut expired_brokers := []domain.BrokerInfo{}

	// 멀티 브로커 모드에서는 분산 스토리지 확인
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			brokers := mp.list_brokers()!
			for broker in brokers {
				if broker.status == .active && now - broker.last_heartbeat > timeout {
					expired << broker.broker_id
					expired_brokers << broker
					mp.mark_broker_dead(broker.broker_id) or {}
				}
			}

			// 만료된 브로커가 있으면 변경 이벤트 트리거
			if expired.len > 0 {
				changes := BrokerChanges{
					reason:  'broker_failed'
					added:   []domain.BrokerInfo{}
					removed: expired_brokers
				}
				r.on_broker_change(changes) or {
					r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
				}
			}

			return expired
		}
	}

	// 단일 브로커 모드 - 로컬 캐시 확인
	for broker_id, broker in r.brokers {
		if broker.status == .active && now - broker.last_heartbeat > timeout {
			expired << broker_id
			expired_brokers << broker
			r.brokers[broker_id] = domain.BrokerInfo{
				...broker
				status: .dead
			}
		}
	}

	// 만료된 브로커가 있으면 변경 이벤트 트리거
	if expired.len > 0 {
		changes := BrokerChanges{
			reason:  'broker_failed'
			added:   []domain.BrokerInfo{}
			removed: expired_brokers
		}
		r.on_broker_change(changes) or {
			r.logger.warn('Failed to handle broker change', observability.field_err_str(err.str()))
		}
	}

	return expired
}

// 백그라운드 워커

/// start_heartbeat_worker는 백그라운드 하트비트 워커를 시작합니다.
pub fn (mut r BrokerRegistry) start_heartbeat_worker() {
	r.running = true
	spawn r.heartbeat_loop()
}

/// stop_heartbeat_worker는 백그라운드 하트비트 워커를 중지합니다.
pub fn (mut r BrokerRegistry) stop_heartbeat_worker() {
	r.running = false
}

fn (mut r BrokerRegistry) heartbeat_loop() {
	interval := time.Duration(r.config.broker_heartbeat_interval_ms * time.millisecond)

	for r.running {
		// 메트릭 수집: 외부 프로바이더가 설정된 경우 실제 메트릭 사용
		load := if provider := r.metrics_provider {
			provider()
		} else {
			// 프로바이더가 없으면 기본 빈 메트릭 반환
			domain.BrokerLoad{}
		}

		r.send_heartbeat(load) or { eprintln('[WARN] Failed to send heartbeat: ${err}') }

		// 만료된 브로커 확인
		expired := r.check_expired_brokers() or { []i32{} }
		if expired.len > 0 {
			eprintln('[INFO] Detected ${expired.len} expired brokers: ${expired}')
		}

		time.sleep(interval)
	}
}

// 클러스터 메타데이터

/// get_cluster_metadata는 현재 클러스터 메타데이터를 반환합니다.
pub fn (mut r BrokerRegistry) get_cluster_metadata() !domain.ClusterMetadata {
	// 분산 스토리지 먼저 시도
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.get_cluster_metadata()
		}
	}

	// 로컬 상태에서 구성
	brokers := r.list_brokers()!

	return domain.ClusterMetadata{
		cluster_id:       r.config.cluster_id
		controller_id:    r.local_broker.broker_id // 단일 브로커에서는 자신이 컨트롤러
		brokers:          brokers
		metadata_version: 1
		updated_at:       time.now().unix_milli()
	}
}

/// is_multi_broker_enabled는 멀티 브로커 모드가 활성화되었는지 반환합니다.
pub fn (r &BrokerRegistry) is_multi_broker_enabled() bool {
	return r.capability.supports_multi_broker
}

/// get_capability는 스토리지 기능 정보를 반환합니다.
pub fn (r &BrokerRegistry) get_capability() domain.StorageCapability {
	return r.capability
}

// 브로커 변경 처리 (파티션 리밸런싱 트리거)

/// on_broker_change는 브로커 목록 변경 시 호출됩니다.
/// 파티션 리밸런싱을 트리거하고 콜백을 호출합니다.
pub fn (mut r BrokerRegistry) on_broker_change(changes BrokerChanges) ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	r.logger.info('Broker change detected', observability.field_string('reason', changes.reason),
		observability.field_int('added', i64(changes.added.len)), observability.field_int('removed',
		i64(changes.removed.len)))

	// 콜백 호출
	if cb := r.on_broker_change_cb {
		spawn cb(changes)
	}

	// 파티션 할당 서비스가 있으면 리밸런싱 트리거
	if r.partition_assigner != none {
		// 활성 브로커 목록 조회
		active_brokers := r.list_active_brokers_internal() or { []domain.BrokerInfo{} }

		if active_brokers.len == 0 {
			r.logger.warn('No active brokers available for rebalance')
			return
		}

		// TODO: 모든 토픽에 대해 리밸런싱 수행
		// 현재는 콜백만 호출하고 실제 리밸런싱은 외부에서 처리
		r.logger.info('Triggering partition rebalance for all topics', observability.field_int('active_brokers',
			i64(active_brokers.len)))
	}

	return
}

/// list_active_brokers_internal는 락을 획득하지 않고 활성 브로커 목록을 반환합니다.
/// 내부 사용용 (이미 락을 획득한 상태에서 호출)
fn (mut r BrokerRegistry) list_active_brokers_internal() ![]domain.BrokerInfo {
	// 분산 스토리지 먼저 시도
	if r.capability.supports_multi_broker {
		if mut mp := r.metadata_port {
			return mp.list_active_brokers()
		}
	}

	// 로컬 캐시로 폴백 - 활성 브로커 필터링
	mut result := []domain.BrokerInfo{}
	for _, broker in r.brokers {
		if broker.status == .active {
			result << broker
		}
	}
	return result
}

/// trigger_rebalance_for_topic는 특정 토픽에 대해 리밸런싱을 수행합니다.
pub fn (mut r BrokerRegistry) trigger_rebalance_for_topic(topic_name string) ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if mut assigner := r.partition_assigner {
		active_brokers := r.list_active_brokers_internal() or { []domain.BrokerInfo{} }

		if active_brokers.len == 0 {
			return error('no active brokers available for rebalance')
		}

		assigner.rebalance_partitions(topic_name, active_brokers) or {
			return error('rebalance failed: ${err}')
		}

		r.logger.info('Rebalance completed for topic', observability.field_string('topic',
			topic_name))
	} else {
		return error('partition assigner not configured')
	}
}
