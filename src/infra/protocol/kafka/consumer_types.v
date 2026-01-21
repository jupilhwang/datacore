// Kafka Protocol - Consumer Group Types
// Request/Response struct definitions for JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

// ============================================================================
// JoinGroup Request/Response Types (API Key 11)
// ============================================================================

pub struct JoinGroupRequest {
pub:
	group_id             string
	session_timeout_ms   i32
	rebalance_timeout_ms i32
	member_id            string
	group_instance_id    ?string
	protocol_type        string
	protocols            []JoinGroupRequestProtocol
}

pub struct JoinGroupRequestProtocol {
pub:
	name     string
	metadata []u8
}

pub struct JoinGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	generation_id    i32
	protocol_type    ?string
	protocol_name    ?string
	leader           string
	skip_assignment  bool
	member_id        string
	members          []JoinGroupResponseMember
}

pub struct JoinGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string // v5+
	metadata          []u8
}

// ============================================================================
// SyncGroup Request/Response Types (API Key 14)
// ============================================================================

pub struct SyncGroupRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
	protocol_type     ?string
	protocol_name     ?string
	assignments       []SyncGroupRequestAssignment
}

pub struct SyncGroupRequestAssignment {
pub:
	member_id  string
	assignment []u8
}

pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	protocol_type    ?string
	protocol_name    ?string
	assignment       []u8
}

// ============================================================================
// Heartbeat Request/Response Types (API Key 12)
// ============================================================================

pub struct HeartbeatRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
}

pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

// ============================================================================
// LeaveGroup Request/Response Types (API Key 13)
// ============================================================================

pub struct LeaveGroupRequest {
pub:
	group_id  string
	member_id string             // v0-v2: single member_id
	members   []LeaveGroupMember // v3+: batch member identities
}

// LeaveGroupMember - v3+ member identity for batch leaving
pub struct LeaveGroupMember {
pub:
	member_id         string
	group_instance_id ?string
	reason            ?string // v5+
}

pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	members          []LeaveGroupResponseMember
}

pub struct LeaveGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	error_code        i16
}

// ============================================================================
// ConsumerGroupHeartbeat Request/Response Types (API Key 68) - KIP-848
// ============================================================================
// Used for the new Consumer Rebalance Protocol

pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	instance_id            ?string // Static membership (group.instance.id)
	rack_id                ?string
	rebalance_timeout_ms   i32
	subscribed_topic_names []string
	server_assignor        ?string
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition // Current assignment
}

pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

pub struct ConsumerGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         ?string
	member_id             ?string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ConsumerGroupHeartbeatAssignment
}

pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition
}

pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}
