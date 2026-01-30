// 도메인 레이어 - 클러스터 및 멀티 브로커 모델
// 멀티 브로커 클러스터 조정을 위한 핵심 타입을 정의합니다.
module domain

import time

// 파티션 할당 (Partition Assignment)

/// PartitionAssignment은 파티션의 브로커 할당 정보를 나타냅니다.
/// 토픽의 각 파티션이 어떤 브로커에 할당되었는지 추적합니다.
/// Stateless 모드에서는 파티션에 고정된 리더가 없습니다.
/// 데이터가 공유 스토리지에 있으므로 모든 브로커가 모든 파티션을 서비스할 수 있습니다.
/// 그러나 로드 밸런싱을 위해 "선호" 브로커를 추적합니다.
pub struct PartitionAssignment {
pub mut:
	topic_name       string // 토픽 이름
	topic_id         []u8   // 토픽 UUID
	partition        i32    // 파티션 번호
	preferred_broker i32    // 선호 브로커 (로드 밸런싱 힌트)
	replica_brokers  []i32  // 레플리카 브로커 목록 (Kafka 프로토콜 호환성용)
	isr_brokers      []i32  // ISR 브로커 목록 (Stateless 모드에서는 모든 활성 브로커)
	partition_epoch  i32    // 파티션 에포크
	assigned_at      i64    // 할당 시간
	reassigned_at    i64    // 재할당 시간 (있는 경우)
}

/// PartitionAssignmentSnapshot은 특정 시점의 모든 파티션 할당을 나타냅니다.
pub struct PartitionAssignmentSnapshot {
pub mut:
	cluster_id  string                // 클러스터 ID
	version     i64                   // 스냅샷 버전
	assignments []PartitionAssignment // 모든 파티션 할당 목록
	created_at  i64                   // 생성 시간
	created_by  i32                   // 생성한 브로커 ID
}

/// ReassignmentPlan은 파티션 재할당 계획을 나타냅니다.
pub struct ReassignmentPlan {
pub mut:
	plan_id        string                      // 계획 ID
	cluster_id     string                      // 클러스터 ID
	trigger_reason string                      // 트리거 원인 ("broker_joined", "broker_left", "manual")
	changes        []PartitionAssignmentChange // 변경 사항 목록
	created_at     i64                         // 생성 시간
	executed_at    i64                         // 실행 시간 (0이면 미실행)
	status         ReassignmentStatus          // 계획 상태
}

/// PartitionAssignmentChange는 단일 파티션의 할당 변경을 나타냅니다.
pub struct PartitionAssignmentChange {
pub mut:
	topic_name   string // 토픽 이름
	partition_id int    // 파티션 ID
	old_leader   i32    // 이전 리더 브로커 ID
	new_leader   i32    // 새 리더 브로커 ID
	reason       string // 변경 사유
}

/// ReassignmentStatus는 재할당 계획의 상태를 나타냅니다.
pub enum ReassignmentStatus {
	pending   // 대기 중
	executing // 실행 중
	completed // 완료
	failed    // 실패
	cancelled // 취소됨
}

/// PartitionAssignerConfig는 파티션 할당 설정을 나타냅니다.
pub struct PartitionAssignerConfig {
pub mut:
	strategy      AssignmentStrategy // 할당 전략
	rack_aware    bool               // 랙 인식 여부
	sticky_assign bool               // 스티키 할당 여부 (재할당 시 기존 할당 유지)
}

/// AssignmentStrategy는 파티션 할당 전략을 나타냅니다.
pub enum AssignmentStrategy {
	round_robin // 라운드로빈
	range       // 범위 기반
	sticky      // 스티키 (최소 이동)
}

// 브로커 등록

// 브로커 등록

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
	broker_id i32
	host      string
	port      i32
	rack      string
	// 보안
	security_protocol string // PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	// 다른 리스너를 위한 엔드포인트
	endpoints []BrokerEndpoint
	// 상태
	status BrokerStatus
	// 타임스탬프
	registered_at  i64
	last_heartbeat i64
	// 메타데이터
	version  string
	features []string
}

/// BrokerEndpoint는 리스너 엔드포인트를 나타냅니다.
/// name: 엔드포인트 이름 (예: "PLAINTEXT", "SSL", "SASL_SSL")
/// host: 호스트 주소
/// port: 포트 번호
/// security_protocol: 보안 프로토콜
pub struct BrokerEndpoint {
pub:
	name              string // 예: "PLAINTEXT", "SSL", "SASL_SSL"
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
	starting // 브로커가 시작 중
	active   // 브로커가 활성 상태이며 요청을 수락 중
	draining // 브로커가 드레이닝 중 (새 연결을 수락하지 않음)
	shutdown // 브로커가 종료 중
	dead     // 브로커가 죽은 것으로 간주됨 (하트비트 누락)
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

// 클러스터 메타데이터

/// ClusterMetadata는 클러스터 상태를 나타냅니다.
/// cluster_id: 클러스터 ID
/// controller_id: 컨트롤러 브로커 ID (stateless 모드에서는 -1)
/// brokers: 브로커 목록
/// metadata_version: 메타데이터 버전 (낙관적 동시성용)
/// updated_at: 마지막 업데이트 시간
pub struct ClusterMetadata {
pub mut:
	cluster_id    string
	controller_id i32 // stateless 모드에서는 -1
	brokers       []BrokerInfo
	// 낙관적 동시성을 위한 에포크
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

// 스토리지 기능

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

// 사전 정의된 스토리지 기능
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

// 클러스터 설정

/// ClusterConfig는 클러스터 전체 설정을 보관합니다.
/// cluster_id: 클러스터 ID
/// broker_heartbeat_interval_ms: 하트비트 전송 간격 (밀리초)
/// broker_session_timeout_ms: 브로커 세션 타임아웃 (밀리초)
/// metadata_refresh_interval_ms: 메타데이터 갱신 간격 (밀리초)
/// multi_broker_enabled: 멀티 브로커 모드 활성화 여부
pub struct ClusterConfig {
pub:
	cluster_id string
	// 브로커 등록
	broker_heartbeat_interval_ms i32 = 3000  // 브로커가 하트비트를 보내는 빈도
	broker_session_timeout_ms    i32 = 10000 // 브로커를 죽은 것으로 간주하는 시간
	// 메타데이터
	metadata_refresh_interval_ms i32 = 30000 // 클러스터 메타데이터를 갱신하는 빈도
	// 멀티 브로커 모드
	multi_broker_enabled bool
}

// 클러스터 이벤트

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
	broker_joined        // 브로커 가입
	broker_left          // 브로커 탈퇴
	broker_failed        // 브로커 실패
	metadata_updated     // 메타데이터 업데이트
	partition_reassigned // 파티션 재할당
}
