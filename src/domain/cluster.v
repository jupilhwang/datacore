module domain

import time

/// PartitionAssignment은 파티션의 브로커 할당 정보를 나타냅니다.
/// 토픽의 각 파티션이 어떤 브로커에 할당되었는지 추적합니다.
/// Stateless 모드에서는 파티션에 고정된 리더가 없습니다.
/// 데이터가 공유 스토리지에 있으므로 모든 브로커가 모든 파티션을 서비스할 수 있습니다.
/// 그러나 로드 밸런싱을 위해 "선호" 브로커를 추적합니다.
pub struct PartitionAssignment {
pub mut:
	topic_name       string
	topic_id         []u8
	partition        i32
	preferred_broker i32
	replica_brokers  []i32
	isr_brokers      []i32
	partition_epoch  i32
	assigned_at      i64
	reassigned_at    i64
}

/// PartitionAssignmentSnapshot은 특정 시점의 모든 파티션 할당을 나타냅니다.
pub struct PartitionAssignmentSnapshot {
pub mut:
	cluster_id  string
	version     i64
	assignments []PartitionAssignment
	created_at  i64
	created_by  i32
}

/// ReassignmentPlan은 파티션 재할당 계획을 나타냅니다.
pub struct ReassignmentPlan {
pub mut:
	plan_id        string
	cluster_id     string
	trigger_reason string
	changes        []PartitionAssignmentChange
	created_at     i64
	executed_at    i64
	status         ReassignmentStatus
}

/// PartitionAssignmentChange는 단일 파티션의 할당 변경을 나타냅니다.
pub struct PartitionAssignmentChange {
pub mut:
	topic_name   string
	partition_id int
	old_leader   i32
	new_leader   i32
	reason       string
}

/// ReassignmentStatus는 재할당 계획의 상태를 나타냅니다.
pub enum ReassignmentStatus {
	pending
	executing
	completed
	failed
	cancelled
}

/// PartitionAssignerConfig는 파티션 할당 설정을 나타냅니다.
pub struct PartitionAssignerConfig {
pub mut:
	strategy      AssignmentStrategy
	rack_aware    bool
	sticky_assign bool
}

/// AssignmentStrategy는 파티션 할당 전략을 나타냅니다.
pub enum AssignmentStrategy {
	round_robin
	range
	sticky
}

/// BrokerInfo는 클러스터 내의 브로커를 나타냅니다.
/// broker_id: 브로커 고유 ID
/// host: 호스트 주소
/// port: 포트 번호
/// rack: 랙 정보 (데이터 지역성용)
/// security_protocol: 보안 프로토콜 (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
/// endpoints: 리스너 엔드포인트 목록
/// status: 브로커 상태
pub struct BrokerInfo {
pub mut:
	broker_id         i32
	host              string
	port              i32
	rack              string
	security_protocol string
	endpoints         []BrokerEndpoint
	status            BrokerStatus
	registered_at     i64
	last_heartbeat    i64
	version           string
	features          []string
}

/// BrokerEndpoint는 리스너 엔드포인트를 나타냅니다.
/// name: 엔드포인트 이름 (예: "PLAINTEXT", "SSL", "SASL_SSL")
/// host: 호스트 주소
/// port: 포트 번호
/// security_protocol: 보안 프로토콜
pub struct BrokerEndpoint {
pub:
	name              string
	host              string
	port              i32
	security_protocol string
}

/// BrokerStatus는 브로커의 상태를 나타냅니다.
/// starting: 시작 중
/// active: 활성 (요청 수락 중)
/// draining: 드레이닝 (새 연결 거부)
/// shutdown: 종료 중
/// dead: 죽음 (하트비트 누락)
pub enum BrokerStatus {
	starting
	active
	draining
	shutdown
	dead
}

/// BrokerHeartbeat는 브로커로부터의 하트비트를 나타냅니다.
/// broker_id: 브로커 ID
/// timestamp: 타임스탬프
/// current_load: 현재 부하 정보
/// wants_shutdown: 종료 요청 여부
pub struct BrokerHeartbeat {
pub:
	broker_id      i32
	timestamp      i64
	current_load   BrokerLoad
	wants_shutdown bool
}

/// BrokerLoad는 브로커의 현재 부하를 나타냅니다.
/// connections: 현재 연결 수
/// requests_per_sec: 초당 요청 수
/// bytes_in_per_sec: 초당 수신 바이트
/// bytes_out_per_sec: 초당 송신 바이트
/// cpu_percent: CPU 사용률
/// memory_percent: 메모리 사용률
pub struct BrokerLoad {
pub:
	connections       int
	requests_per_sec  f64
	bytes_in_per_sec  f64
	bytes_out_per_sec f64
	cpu_percent       f64
	memory_percent    f64
}

/// ClusterMetadata는 클러스터 상태를 나타냅니다.
/// cluster_id: 클러스터 ID
/// controller_id: 컨트롤러 브로커 ID (stateless 모드에서는 -1)
/// brokers: 브로커 목록
/// metadata_version: 메타데이터 버전 (낙관적 동시성용)
/// updated_at: 마지막 업데이트 시간
pub struct ClusterMetadata {
pub mut:
	cluster_id       string
	controller_id    i32
	brokers          []BrokerInfo
	metadata_version i64
	updated_at       i64
}

/// new_cluster_metadata는 새로운 클러스터 메타데이터 인스턴스를 생성합니다.
pub fn new_cluster_metadata(cluster_id string) ClusterMetadata {
	return ClusterMetadata{
		cluster_id:       cluster_id
		controller_id:    -1 // Stateless 모드에서는 컨트롤러 없음
		brokers:          []BrokerInfo{}
		metadata_version: 0
		updated_at:       time.now().unix_milli()
	}
}

/// StorageCapability는 스토리지 엔진이 지원하는 기능을 나타냅니다.
/// name: 스토리지 이름
/// supports_multi_broker: 멀티 브로커 지원 여부
/// supports_transactions: 트랜잭션 지원 여부
/// supports_compaction: 컴팩션 지원 여부
/// is_persistent: 영속성 여부
/// is_distributed: 분산 여부
pub struct StorageCapability {
pub:
	name                  string
	supports_multi_broker bool
	supports_transactions bool
	supports_compaction   bool
	is_persistent         bool
	is_distributed        bool
}

pub const memory_storage_capability = StorageCapability{
	name:                  'memory'
	supports_multi_broker: false
	supports_transactions: true
	supports_compaction:   false
	is_persistent:         false
	is_distributed:        false
}

pub const s3_storage_capability = StorageCapability{
	name:                  's3'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

pub const postgresql_storage_capability = StorageCapability{
	name:                  'postgresql'
	supports_multi_broker: true
	supports_transactions: true
	supports_compaction:   true
	is_persistent:         true
	is_distributed:        true
}

/// ClusterConfig는 클러스터 전체 설정을 보관합니다.
/// cluster_id: 클러스터 ID
/// broker_heartbeat_interval_ms: 하트비트 전송 간격 (밀리초)
/// broker_session_timeout_ms: 브로커 세션 타임아웃 (밀리초)
/// metadata_refresh_interval_ms: 메타데이터 갱신 간격 (밀리초)
/// multi_broker_enabled: 멀티 브로커 모드 활성화 여부
pub struct ClusterConfig {
pub:
	cluster_id                   string
	broker_heartbeat_interval_ms i32 = 3000
	broker_session_timeout_ms    i32 = 10000
	metadata_refresh_interval_ms i32 = 30000
	multi_broker_enabled         bool
}

/// ClusterEvent는 클러스터에서 발생하는 이벤트를 나타냅니다.
/// event_type: 이벤트 유형
/// broker_id: 관련 브로커 ID
/// timestamp: 발생 시간
/// details: 상세 정보
pub struct ClusterEvent {
pub:
	event_type ClusterEventType
	broker_id  i32
	timestamp  i64
	details    string
}

/// ClusterEventType은 클러스터 이벤트 유형을 나타냅니다.
/// broker_joined: 브로커 가입
/// broker_left: 브로커 탈퇴
/// broker_failed: 브로커 실패
/// metadata_updated: 메타데이터 업데이트
/// partition_reassigned: 파티션 재할당
pub enum ClusterEventType {
	broker_joined
	broker_left
	broker_failed
	metadata_updated
	partition_reassigned
}
