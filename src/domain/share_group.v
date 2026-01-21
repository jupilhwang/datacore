// Domain Layer - Share Group Domain Model (KIP-932)
// Share groups allow consumers to cooperatively consume records with
// record-level acknowledgement and automatic redelivery
module domain

import time

// ============================================================================
// Share Group Types
// ============================================================================

// ShareGroup represents a share group (KIP-932)
// Share groups differ from consumer groups in that:
// - Partitions can be assigned to multiple consumers
// - Records are acknowledged individually
// - Delivery attempts are tracked for poison message handling
pub struct ShareGroup {
pub mut:
	group_id          string
	group_epoch       i32
	assignment_epoch  i32
	state             ShareGroupState
	members           map[string]&ShareMember
	target_assignment map[string][]SharePartitionAssignment // member_id -> assignments
	subscribed_topics map[string]bool                       // All subscribed topics
	// Configuration
	record_lock_duration_ms i32 = 30000 // Default 30s
	delivery_attempt_limit  i32 = 5     // Max delivery attempts
	max_partition_locks     i32 = 200   // Max in-flight records per partition
	heartbeat_interval_ms   i32 = 5000  // Heartbeat interval
	session_timeout_ms      i32 = 45000 // Session timeout
	// Timestamps
	created_at i64
	updated_at i64
}

// ShareGroupState represents the state of a share group
pub enum ShareGroupState {
	empty  // No members in the group
	stable // Group has active members
	dead   // Group is being deleted
}

// ShareMember represents a member of a share group
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
	last_heartbeat         i64 // Unix timestamp ms
	joined_at              i64
}

// ShareMemberState represents the state of a share group member
pub enum ShareMemberState {
	joining // Member is joining the group
	stable  // Member has stable assignment
	leaving // Member is leaving the group
	fenced  // Member has been fenced
}

// SharePartitionAssignment represents a partition assignment for share groups
pub struct SharePartitionAssignment {
pub:
	topic_id   []u8 // UUID (16 bytes)
	topic_name string
	partitions []i32
}

// ============================================================================
// Share Partition State
// ============================================================================

// SharePartition represents the share group's view of a topic-partition
// It manages in-flight records between SPSO and SPEO
pub struct SharePartition {
pub mut:
	topic_name string
	partition  i32
	group_id   string
	// Offsets
	start_offset i64 // Share Partition Start Offset (SPSO)
	end_offset   i64 // Share Partition End Offset (SPEO)
	// In-flight record states
	record_states map[i64]RecordState // offset -> state
	// Lock management
	acquired_records map[i64]AcquiredRecord // offset -> acquisition info
	// Statistics
	total_acquired     i64
	total_acknowledged i64
	total_released     i64
	total_rejected     i64
}

// RecordState represents the state of a record in a share partition
pub enum RecordState {
	available    // Record is available for delivery
	acquired     // Record has been acquired by a consumer
	acknowledged // Record has been successfully processed
	archived     // Record is no longer available (rejected or max attempts)
}

// AcquiredRecord tracks acquisition info for a record
pub struct AcquiredRecord {
pub mut:
	offset          i64
	member_id       string
	delivery_count  i32
	acquired_at     i64 // Unix timestamp ms
	lock_expires_at i64 // When the acquisition lock expires
}

// ============================================================================
// Share Session
// ============================================================================

// ShareSession represents a share session for a consumer
// Sessions track fetch context and acquired records
pub struct ShareSession {
pub mut:
	group_id       string
	member_id      string
	session_epoch  i32
	partitions     []ShareSessionPartition
	acquired_locks map[string][]i64 // topic-partition -> acquired offsets
	created_at     i64
	last_used      i64
}

// ShareSessionPartition represents a partition in a share session
pub struct ShareSessionPartition {
pub:
	topic_id   []u8
	topic_name string
	partition  i32
}

// ============================================================================
// Acknowledgement Types
// ============================================================================

// AcknowledgeType represents how a record should be acknowledged
pub enum AcknowledgeType {
	accept  // Record processed successfully
	release // Release for redelivery
	reject  // Reject as unprocessable (poison message)
}

// AcknowledgementBatch represents a batch of acknowledgements
pub struct AcknowledgementBatch {
pub:
	topic_name       string
	partition        i32
	first_offset     i64
	last_offset      i64
	acknowledge_type AcknowledgeType
	gap_offsets      []i64 // Offsets that don't correspond to records (gaps)
}

// ============================================================================
// Share Fetch/Acknowledge Results
// ============================================================================

// ShareFetchResult represents the result of a share fetch
pub struct ShareFetchResult {
pub:
	topic_name          string
	partition           i32
	records             []Record
	acquired_records    []AcquiredRecordInfo
	error_code          i16
	error_message       string
	acquired_offset     i64 // First offset acquired in this fetch
	last_fetched_offset i64 // Last offset fetched
}

// AcquiredRecordInfo contains info about an acquired record
pub struct AcquiredRecordInfo {
pub:
	offset         i64
	delivery_count i32
	timestamp      i64
}

// ShareAcknowledgeResult represents the result of acknowledgements
pub struct ShareAcknowledgeResult {
pub:
	topic_name    string
	partition     i32
	error_code    i16
	error_message string
}

// ============================================================================
// Configuration
// ============================================================================

// ShareGroupConfig holds share group configuration
pub struct ShareGroupConfig {
pub:
	// Record lock duration in milliseconds (default 30000)
	record_lock_duration_ms i32 = 30000
	// Maximum delivery attempts before archiving (default 5)
	delivery_attempt_limit i32 = 5
	// Maximum in-flight records per partition (default 200)
	max_partition_locks i32 = 200
	// Heartbeat interval in milliseconds (default 5000)
	heartbeat_interval_ms i32 = 5000
	// Session timeout in milliseconds (default 45000)
	session_timeout_ms i32 = 45000
	// Maximum share sessions per broker (default 1000)
	max_share_sessions i32 = 1000
}

// ============================================================================
// Helper Functions
// ============================================================================

// new_share_group creates a new share group
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

// new_share_partition creates a new share partition
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

// ShareGroupState string conversion
pub fn (s ShareGroupState) str() string {
	return match s {
		.empty { 'EMPTY' }
		.stable { 'STABLE' }
		.dead { 'DEAD' }
	}
}

// ShareMemberState string conversion
pub fn (s ShareMemberState) str() string {
	return match s {
		.joining { 'JOINING' }
		.stable { 'STABLE' }
		.leaving { 'LEAVING' }
		.fenced { 'FENCED' }
	}
}

// RecordState string conversion
pub fn (s RecordState) str() string {
	return match s {
		.available { 'AVAILABLE' }
		.acquired { 'ACQUIRED' }
		.acknowledged { 'ACKNOWLEDGED' }
		.archived { 'ARCHIVED' }
	}
}

// AcknowledgeType string conversion
pub fn (t AcknowledgeType) str() string {
	return match t {
		.accept { 'ACCEPT' }
		.release { 'RELEASE' }
		.reject { 'REJECT' }
	}
}

// acknowledge_type_from_value converts API value to AcknowledgeType
pub fn acknowledge_type_from_value(value u8) !AcknowledgeType {
	return match value {
		1 { .accept }
		2 { .release }
		3 { .reject }
		else { error('unknown acknowledge type: ${value}') }
	}
}

// is_share_group_type checks if a group type string indicates a share group
pub fn is_share_group_type(group_type string) bool {
	return group_type == 'share'
}
