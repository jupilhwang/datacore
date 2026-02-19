// Share Group은 컨슈머들이 레코드 수준의 확인 및 자동 재전송을 통해
// 협력적으로 레코드를 소비할 수 있게 합니다.
module domain

import time

/// ShareGroup은 Share Group (KIP-932)을 나타냅니다.
/// Share Group은 Consumer Group과 다음과 같은 점에서 다릅니다:
/// - 파티션이 여러 컨슈머에게 할당될 수 있음
/// - 레코드가 개별적으로 확인됨
/// - 독 메시지 처리를 위해 전송 시도 횟수가 추적됨
pub struct ShareGroup {
pub mut:
	group_id                string
	group_epoch             i32
	assignment_epoch        i32
	state                   ShareGroupState
	members                 map[string]&ShareMember
	target_assignment       map[string][]SharePartitionAssignment
	subscribed_topics       map[string]bool
	record_lock_duration_ms i32 = 30000
	delivery_attempt_limit  i32 = 5
	max_partition_locks     i32 = 200
	heartbeat_interval_ms   i32 = 5000
	session_timeout_ms      i32 = 45000
	created_at              i64
	updated_at              i64
}

/// ShareGroupState는 Share Group의 상태를 나타냅니다.
pub enum ShareGroupState {
	empty
	stable
	dead
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
	last_heartbeat         i64
	joined_at              i64
}

/// ShareMemberState는 Share Group 멤버의 상태를 나타냅니다.
pub enum ShareMemberState {
	joining
	stable
	leaving
	fenced
}

/// SharePartitionAssignment는 Share Group을 위한 파티션 할당을 나타냅니다.
pub struct SharePartitionAssignment {
pub:
	topic_id   []u8 // UUID (16바이트)
	topic_name string
	partitions []i32
}

/// SharePartition은 토픽-파티션에 대한 Share Group의 뷰를 나타냅니다.
/// SPSO와 SPEO 사이의 진행 중 레코드를 관리합니다.
pub struct SharePartition {
pub mut:
	topic_name         string
	partition          i32
	group_id           string
	start_offset       i64
	end_offset         i64
	record_states      map[i64]RecordState
	acquired_records   map[i64]AcquiredRecord
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// RecordState는 Share Partition 내 레코드의 상태를 나타냅니다.
pub enum RecordState {
	available
	acquired
	acknowledged
	archived
}

/// AcquiredRecord는 레코드의 획득 정보를 추적합니다.
pub struct AcquiredRecord {
pub mut:
	offset          i64
	member_id       string
	delivery_count  i32
	acquired_at     i64
	lock_expires_at i64
}

/// ShareSession은 컨슈머의 Share 세션을 나타냅니다.
/// 세션은 fetch 컨텍스트와 획득된 레코드를 추적합니다.
pub struct ShareSession {
pub mut:
	group_id       string
	member_id      string
	session_epoch  i32
	partitions     []ShareSessionPartition
	acquired_locks map[string][]i64
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

/// AcknowledgeType은 레코드를 어떻게 확인할지 나타냅니다.
pub enum AcknowledgeType {
	accept
	release
	reject
}

/// AcknowledgementBatch는 확인 배치를 나타냅니다.
pub struct AcknowledgementBatch {
pub:
	topic_name       string
	partition        i32
	first_offset     i64
	last_offset      i64
	acknowledge_type AcknowledgeType
	gap_offsets      []i64
}

/// ShareFetchResult는 Share Fetch의 결과를 나타냅니다.
pub struct ShareFetchResult {
pub:
	topic_name          string
	partition           i32
	records             []Record
	acquired_records    []AcquiredRecordInfo
	error_code          i16
	error_message       string
	acquired_offset     i64
	last_fetched_offset i64
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

/// ShareGroupConfig는 Share Group 설정을 보관합니다.
pub struct ShareGroupConfig {
pub:
	record_lock_duration_ms i32 = 30000
	delivery_attempt_limit  i32 = 5
	max_partition_locks     i32 = 200
	heartbeat_interval_ms   i32 = 5000
	session_timeout_ms      i32 = 45000
	max_share_sessions      i32 = 1000
}

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
