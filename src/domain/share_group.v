// 도메인 레이어 - Share Group 도메인 모델 (KIP-932)
// Share Group은 컨슈머들이 레코드 수준의 확인 및 자동 재전송을 통해
// 협력적으로 레코드를 소비할 수 있게 합니다.
module domain

import time

// ============================================================================
// Share Group 타입
// ============================================================================

/// ShareGroup은 Share Group (KIP-932)을 나타냅니다.
/// Share Group은 Consumer Group과 다음과 같은 점에서 다릅니다:
/// - 파티션이 여러 컨슈머에게 할당될 수 있음
/// - 레코드가 개별적으로 확인됨
/// - 독 메시지 처리를 위해 전송 시도 횟수가 추적됨
pub struct ShareGroup {
pub mut:
	group_id          string
	group_epoch       i32
	assignment_epoch  i32
	state             ShareGroupState
	members           map[string]&ShareMember
	target_assignment map[string][]SharePartitionAssignment // member_id -> 할당 목록
	subscribed_topics map[string]bool                       // 모든 구독된 토픽
	// 설정
	record_lock_duration_ms i32 = 30000 // 기본 30초
	delivery_attempt_limit  i32 = 5     // 최대 전송 시도 횟수
	max_partition_locks     i32 = 200   // 파티션당 최대 진행 중 레코드 수
	heartbeat_interval_ms   i32 = 5000  // Heartbeat 간격
	session_timeout_ms      i32 = 45000 // 세션 타임아웃
	// 타임스탬프
	created_at i64
	updated_at i64
}

/// ShareGroupState는 Share Group의 상태를 나타냅니다.
pub enum ShareGroupState {
	empty  // 그룹에 멤버 없음
	stable // 그룹에 활성 멤버 있음
	dead   // 그룹이 삭제되는 중
}

/// ShareMember는 Share Group의 멤버를 나타냅니다.
pub struct ShareMember {
pub mut:
	member_id              string
	rack_id                string
	client_id              string
	client_host            string
	subscribed_topic_names []string
	member_epoch           i32
	state                  ShareMemberState
	assigned_partitions    []SharePartitionAssignment
	last_heartbeat         i64 // Unix 타임스탬프 (밀리초)
	joined_at              i64
}

/// ShareMemberState는 Share Group 멤버의 상태를 나타냅니다.
pub enum ShareMemberState {
	joining // 멤버가 그룹에 참여 중
	stable  // 멤버가 안정적인 할당을 가짐
	leaving // 멤버가 그룹을 떠나는 중
	fenced  // 멤버가 펜싱됨
}

/// SharePartitionAssignment는 Share Group을 위한 파티션 할당을 나타냅니다.
pub struct SharePartitionAssignment {
pub:
	topic_id   []u8 // UUID (16바이트)
	topic_name string
	partitions []i32
}

// ============================================================================
// Share Partition 상태
// ============================================================================

/// SharePartition은 토픽-파티션에 대한 Share Group의 뷰를 나타냅니다.
/// SPSO와 SPEO 사이의 진행 중 레코드를 관리합니다.
pub struct SharePartition {
pub mut:
	topic_name string
	partition  i32
	group_id   string
	// 오프셋
	start_offset i64 // Share Partition Start Offset (SPSO)
	end_offset   i64 // Share Partition End Offset (SPEO)
	// 진행 중 레코드 상태
	record_states map[i64]RecordState // offset -> 상태
	// 잠금 관리
	acquired_records map[i64]AcquiredRecord // offset -> 획득 정보
	// 통계
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// RecordState는 Share Partition 내 레코드의 상태를 나타냅니다.
pub enum RecordState {
	available    // 레코드가 전송 가능
	acquired     // 레코드가 컨슈머에 의해 획득됨
	acknowledged // 레코드가 성공적으로 처리됨
	archived     // 레코드가 더 이상 사용 불가 (거부됨 또는 최대 시도 초과)
}

/// AcquiredRecord는 레코드의 획득 정보를 추적합니다.
pub struct AcquiredRecord {
pub mut:
	offset          i64
	member_id       string
	delivery_count  i32
	acquired_at     i64 // Unix 타임스탬프 (밀리초)
	lock_expires_at i64 // 획득 잠금 만료 시간
}

// ============================================================================
// Share 세션
// ============================================================================

/// ShareSession은 컨슈머의 Share 세션을 나타냅니다.
/// 세션은 fetch 컨텍스트와 획득된 레코드를 추적합니다.
pub struct ShareSession {
pub mut:
	group_id       string
	member_id      string
	session_epoch  i32
	partitions     []ShareSessionPartition
	acquired_locks map[string][]i64 // topic-partition -> 획득된 오프셋 목록
	created_at     i64
	last_used      i64
}

/// ShareSessionPartition은 Share 세션 내의 파티션을 나타냅니다.
pub struct ShareSessionPartition {
pub:
	topic_id   []u8
	topic_name string
	partition  i32
}

// ============================================================================
// 확인 타입
// ============================================================================

/// AcknowledgeType은 레코드를 어떻게 확인할지 나타냅니다.
pub enum AcknowledgeType {
	accept  // 레코드가 성공적으로 처리됨
	release // 재전송을 위해 해제
	reject  // 처리 불가로 거부 (독 메시지)
}

/// AcknowledgementBatch는 확인 배치를 나타냅니다.
pub struct AcknowledgementBatch {
pub:
	topic_name       string
	partition        i32
	first_offset     i64
	last_offset      i64
	acknowledge_type AcknowledgeType
	gap_offsets      []i64 // 레코드에 해당하지 않는 오프셋 (갭)
}

// ============================================================================
// Share Fetch/Acknowledge 결과
// ============================================================================

/// ShareFetchResult는 Share Fetch의 결과를 나타냅니다.
pub struct ShareFetchResult {
pub:
	topic_name          string
	partition           i32
	records             []Record
	acquired_records    []AcquiredRecordInfo
	error_code          i16
	error_message       string
	acquired_offset     i64 // 이번 fetch에서 획득된 첫 번째 오프셋
	last_fetched_offset i64 // 마지막으로 fetch된 오프셋
}

/// AcquiredRecordInfo는 획득된 레코드에 대한 정보를 포함합니다.
pub struct AcquiredRecordInfo {
pub:
	offset         i64
	delivery_count i32
	timestamp      i64
}

/// ShareAcknowledgeResult는 확인의 결과를 나타냅니다.
pub struct ShareAcknowledgeResult {
pub:
	topic_name    string
	partition     i32
	error_code    i16
	error_message string
}

// ============================================================================
// 설정
// ============================================================================

/// ShareGroupConfig는 Share Group 설정을 보관합니다.
pub struct ShareGroupConfig {
pub:
	// 레코드 잠금 지속 시간 (밀리초, 기본 30000)
	record_lock_duration_ms i32 = 30000
	// 아카이브 전 최대 전송 시도 횟수 (기본 5)
	delivery_attempt_limit i32 = 5
	// 파티션당 최대 진행 중 레코드 수 (기본 200)
	max_partition_locks i32 = 200
	// Heartbeat 간격 (밀리초, 기본 5000)
	heartbeat_interval_ms i32 = 5000
	// 세션 타임아웃 (밀리초, 기본 45000)
	session_timeout_ms i32 = 45000
	// 브로커당 최대 Share 세션 수 (기본 1000)
	max_share_sessions i32 = 1000
}

// ============================================================================
// 헬퍼 함수
// ============================================================================

/// new_share_group은 새로운 Share Group을 생성합니다.
pub fn new_share_group(group_id string, config ShareGroupConfig) ShareGroup {
	now := time.now().unix_milli()
	return ShareGroup{
		group_id:                group_id
		group_epoch:             0
		assignment_epoch:        0
		state:                   .empty
		members:                 map[string]&ShareMember{}
		target_assignment:       map[string][]SharePartitionAssignment{}
		subscribed_topics:       map[string]bool{}
		record_lock_duration_ms: config.record_lock_duration_ms
		delivery_attempt_limit:  config.delivery_attempt_limit
		max_partition_locks:     config.max_partition_locks
		heartbeat_interval_ms:   config.heartbeat_interval_ms
		session_timeout_ms:      config.session_timeout_ms
		created_at:              now
		updated_at:              now
	}
}

/// new_share_partition은 새로운 Share Partition을 생성합니다.
pub fn new_share_partition(topic_name string, partition i32, group_id string, start_offset i64) SharePartition {
	return SharePartition{
		topic_name:       topic_name
		partition:        partition
		group_id:         group_id
		start_offset:     start_offset
		end_offset:       start_offset
		record_states:    map[i64]RecordState{}
		acquired_records: map[i64]AcquiredRecord{}
	}
}

/// str은 ShareGroupState를 문자열로 변환합니다.
pub fn (s ShareGroupState) str() string {
	return match s {
		.empty { 'EMPTY' }
		.stable { 'STABLE' }
		.dead { 'DEAD' }
	}
}

/// str은 ShareMemberState를 문자열로 변환합니다.
pub fn (s ShareMemberState) str() string {
	return match s {
		.joining { 'JOINING' }
		.stable { 'STABLE' }
		.leaving { 'LEAVING' }
		.fenced { 'FENCED' }
	}
}

/// str은 RecordState를 문자열로 변환합니다.
pub fn (s RecordState) str() string {
	return match s {
		.available { 'AVAILABLE' }
		.acquired { 'ACQUIRED' }
		.acknowledged { 'ACKNOWLEDGED' }
		.archived { 'ARCHIVED' }
	}
}

/// str은 AcknowledgeType을 문자열로 변환합니다.
pub fn (t AcknowledgeType) str() string {
	return match t {
		.accept { 'ACCEPT' }
		.release { 'RELEASE' }
		.reject { 'REJECT' }
	}
}

/// acknowledge_type_from_value는 API 값을 AcknowledgeType으로 변환합니다.
pub fn acknowledge_type_from_value(value u8) !AcknowledgeType {
	return match value {
		1 { .accept }
		2 { .release }
		3 { .reject }
		else { error('unknown acknowledge type: ${value}') }
	}
}

/// is_share_group_type은 그룹 타입 문자열이 Share Group을 나타내는지 확인합니다.
pub fn is_share_group_type(group_type string) bool {
	return group_type == 'share'
}
