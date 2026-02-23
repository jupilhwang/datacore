// Share Group allows consumers to cooperatively consume records
// with record-level acknowledgement and automatic redelivery.
module domain

import time

/// ShareGroup represents a Share Group (KIP-932).
/// Share Group differs from Consumer Group in the following ways:
/// - partitions can be assigned to multiple consumers
/// - records are acknowledged individually
/// - delivery attempt count is tracked for poison message handling
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

/// ShareGroupState represents the state of a Share Group.
pub enum ShareGroupState {
	empty
	stable
	dead
}

/// ShareMember represents a member of a Share Group.
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

/// ShareMemberState represents the state of a Share Group member.
pub enum ShareMemberState {
	joining
	stable
	leaving
	fenced
}

/// SharePartitionAssignment represents a partition assignment for a Share Group.
pub struct SharePartitionAssignment {
pub:
	topic_id   []u8 // UUID (16 bytes)
	topic_name string
	partitions []i32
}

/// SharePartition represents a Share Group's view of a topic-partition.
/// Manages in-flight records between SPSO and SPEO.
pub struct SharePartition {
pub mut:
	topic_name         string
	partition          i32
	group_id           string
	start_offset       i64
	end_offset         i64
	record_states      map[i64]RecordState
	acquired_records   map[i64]AcquiredRecord
	delivery_counts    map[i64]i32
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// RecordState represents the state of a record within a Share Partition.
pub enum RecordState {
	available
	acquired
	acknowledged
	archived
}

/// AcquiredRecord tracks acquisition information for a record.
pub struct AcquiredRecord {
pub mut:
	offset          i64
	member_id       string
	delivery_count  i32
	acquired_at     i64
	lock_expires_at i64
}

/// ShareSession represents a consumer's Share session.
/// A session tracks the fetch context and acquired records.
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

/// ShareSessionPartition represents a partition within a Share session.
pub struct ShareSessionPartition {
pub:
	topic_id   []u8
	topic_name string
	partition  i32
}

/// AcknowledgeType indicates how a record should be acknowledged.
pub enum AcknowledgeType {
	accept
	release
	reject
}

/// AcknowledgementBatch represents an acknowledgement batch.
pub struct AcknowledgementBatch {
pub:
	topic_name       string
	partition        i32
	first_offset     i64
	last_offset      i64
	acknowledge_type AcknowledgeType
	gap_offsets      []i64
}

/// ShareFetchResult represents the result of a Share Fetch.
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

/// AcquiredRecordInfo contains information about an acquired record.
pub struct AcquiredRecordInfo {
pub:
	offset         i64
	delivery_count i32
	timestamp      i64
}

/// ShareAcknowledgeResult represents the result of an acknowledgement.
pub struct ShareAcknowledgeResult {
pub:
	topic_name    string
	partition     i32
	error_code    i16
	error_message string
}

/// ShareGroupConfig holds Share Group configuration.
pub struct ShareGroupConfig {
pub:
	record_lock_duration_ms i32 = 30000
	delivery_attempt_limit  i32 = 5
	max_partition_locks     i32 = 200
	heartbeat_interval_ms   i32 = 5000
	session_timeout_ms      i32 = 45000
	max_share_sessions      i32 = 1000
}

/// new_share_group creates a new Share Group.
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

/// new_share_partition creates a new Share Partition.
pub fn new_share_partition(topic_name string, partition i32, group_id string, start_offset i64) SharePartition {
	return SharePartition{
		topic_name:       topic_name
		partition:        partition
		group_id:         group_id
		start_offset:     start_offset
		end_offset:       start_offset
		record_states:    map[i64]RecordState{}
		acquired_records: map[i64]AcquiredRecord{}
		delivery_counts:  map[i64]i32{}
	}
}

/// str converts ShareGroupState to a string.
pub fn (s ShareGroupState) str() string {
	return match s {
		.empty { 'EMPTY' }
		.stable { 'STABLE' }
		.dead { 'DEAD' }
	}
}

/// str converts ShareMemberState to a string.
pub fn (s ShareMemberState) str() string {
	return match s {
		.joining { 'JOINING' }
		.stable { 'STABLE' }
		.leaving { 'LEAVING' }
		.fenced { 'FENCED' }
	}
}

/// str converts RecordState to a string.
pub fn (s RecordState) str() string {
	return match s {
		.available { 'AVAILABLE' }
		.acquired { 'ACQUIRED' }
		.acknowledged { 'ACKNOWLEDGED' }
		.archived { 'ARCHIVED' }
	}
}

/// str converts AcknowledgeType to a string.
pub fn (t AcknowledgeType) str() string {
	return match t {
		.accept { 'ACCEPT' }
		.release { 'RELEASE' }
		.reject { 'REJECT' }
	}
}

/// acknowledge_type_from_value converts an API value to an AcknowledgeType.
pub fn acknowledge_type_from_value(value u8) !AcknowledgeType {
	return match value {
		1 { .accept }
		2 { .release }
		3 { .reject }
		else { error('unknown acknowledge type: ${value}') }
	}
}

/// is_share_group_type checks whether a group type string represents a Share Group.
pub fn is_share_group_type(group_type string) bool {
	return group_type == 'share'
}

/// record_state_to_u8 converts a RecordState enum to its u8 representation.
pub fn record_state_to_u8(s RecordState) u8 {
	return match s {
		.available { 0 }
		.acquired { 1 }
		.acknowledged { 2 }
		.archived { 3 }
	}
}

/// record_state_from_u8 converts a u8 value to a RecordState enum.
pub fn record_state_from_u8(v u8) RecordState {
	return match v {
		0 { .available }
		1 { .acquired }
		2 { .acknowledged }
		3 { .archived }
		else { .available }
	}
}

/// SharePartitionState is the persistable state of a SharePartition.
/// Used for serialization and storage to survive broker restarts.
pub struct SharePartitionState {
pub mut:
	group_id           string
	topic_name         string
	partition          i32
	start_offset       i64
	end_offset         i64
	record_states      map[i64]u8
	acquired_records   map[i64]AcquiredRecordState
	delivery_counts    map[i64]i32
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

/// AcquiredRecordState is the persistable state of an acquired record.
pub struct AcquiredRecordState {
pub mut:
	offset          i64
	member_id       string
	delivery_count  i32
	acquired_at     i64
	lock_expires_at i64
}

/// to_state converts a SharePartition to a SharePartitionState for persistence.
pub fn (sp SharePartition) to_state() SharePartitionState {
	mut rs := map[i64]u8{}
	for offset, state in sp.record_states {
		rs[offset] = record_state_to_u8(state)
	}

	mut ar := map[i64]AcquiredRecordState{}
	for offset, acquired in sp.acquired_records {
		ar[offset] = AcquiredRecordState{
			offset:          acquired.offset
			member_id:       acquired.member_id
			delivery_count:  acquired.delivery_count
			acquired_at:     acquired.acquired_at
			lock_expires_at: acquired.lock_expires_at
		}
	}

	mut dc := map[i64]i32{}
	for offset, count in sp.delivery_counts {
		dc[offset] = count
	}

	return SharePartitionState{
		group_id:           sp.group_id
		topic_name:         sp.topic_name
		partition:          sp.partition
		start_offset:       sp.start_offset
		end_offset:         sp.end_offset
		record_states:      rs
		acquired_records:   ar
		delivery_counts:    dc
		total_acquired:     sp.total_acquired
		total_acknowledged: sp.total_acknowledged
		total_released:     sp.total_released
		total_rejected:     sp.total_rejected
	}
}

/// to_partition creates a SharePartition from a SharePartitionState.
pub fn (sps SharePartitionState) to_partition() SharePartition {
	mut rs := map[i64]RecordState{}
	for offset, state_val in sps.record_states {
		rs[offset] = record_state_from_u8(state_val)
	}

	mut ar := map[i64]AcquiredRecord{}
	for offset, acquired in sps.acquired_records {
		ar[offset] = AcquiredRecord{
			offset:          acquired.offset
			member_id:       acquired.member_id
			delivery_count:  acquired.delivery_count
			acquired_at:     acquired.acquired_at
			lock_expires_at: acquired.lock_expires_at
		}
	}

	mut dc := map[i64]i32{}
	for offset, count in sps.delivery_counts {
		dc[offset] = count
	}

	return SharePartition{
		group_id:           sps.group_id
		topic_name:         sps.topic_name
		partition:          sps.partition
		start_offset:       sps.start_offset
		end_offset:         sps.end_offset
		record_states:      rs
		acquired_records:   ar
		delivery_counts:    dc
		total_acquired:     sps.total_acquired
		total_acknowledged: sps.total_acknowledged
		total_released:     sps.total_released
		total_rejected:     sps.total_rejected
	}
}
