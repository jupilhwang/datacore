// Request/response struct definitions for
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
//
// Used for consumer group join, sync, heartbeat, leave, and related operations.
module kafka

// JoinGroup request/response types (API Key 11) - group join

/// JoinGroup request - request for a consumer to join a group
///
/// Used when a consumer first joins a group or rejoins during rebalancing.
/// The leader is responsible for partition assignment.
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

/// JoinGroup request protocol - assignment protocols supported by the consumer
pub struct JoinGroupRequestProtocol {
pub:
	name     string
	metadata []u8
}

/// JoinGroup response - result of joining a group
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

/// JoinGroup response member - group member information
pub struct JoinGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	metadata          []u8
}

// SyncGroup request/response types (API Key 14) - group sync

/// SyncGroup request - partition assignment sync request
///
/// After the leader completes partition assignment, propagates assignments to all members.
/// Only the leader includes assignments; followers send an empty array.
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

/// SyncGroup request assignment - partition assignment per member
pub struct SyncGroupRequestAssignment {
pub:
	member_id  string
	assignment []u8
}

/// SyncGroup response - partition assignment result
pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	protocol_type    ?string
	protocol_name    ?string
	assignment       []u8
}

// Heartbeat request/response types (API Key 12) - heartbeat

/// Heartbeat request - heartbeat to maintain group membership
///
/// Sent periodically by consumers to maintain group membership.
/// If no heartbeat is received within the session timeout, the member is removed.
pub struct HeartbeatRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
}

/// Heartbeat response - heartbeat result
pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

// LeaveGroup request/response types (API Key 13) - group leave

/// LeaveGroup request - request to leave a group
///
/// Used when a consumer shuts down gracefully to leave the group.
/// v3+ supports batch leaving of multiple members at once.
pub struct LeaveGroupRequest {
pub:
	group_id  string
	member_id string
	members   []LeaveGroupMember
}

/// LeaveGroup member - member info for v3+ batch leave
pub struct LeaveGroupMember {
pub:
	member_id         string
	group_instance_id ?string
	reason            ?string
}

/// LeaveGroup response - group leave result
pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	members          []LeaveGroupResponseMember
}

/// LeaveGroup response member - leave result per member
pub struct LeaveGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	error_code        i16
}

// ConsumerGroupHeartbeat request/response types (API Key 68) - KIP-848

// Used in the new consumer rebalance protocol.

/// ConsumerGroupHeartbeat request - new consumer protocol heartbeat (KIP-848)
///
/// A single API that replaces JoinGroup/SyncGroup/Heartbeat.
/// Supports server-side assignment.
pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	instance_id            ?string
	rack_id                ?string
	rebalance_timeout_ms   i32
	subscribed_topic_names []string
	server_assignor        ?string
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition
}

/// ConsumerGroupHeartbeat topic partition - currently assigned partitions
pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8 // topic UUID (16 bytes)
	partitions []i32
}

/// ConsumerGroupHeartbeat response - new consumer protocol heartbeat response
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

/// ConsumerGroupHeartbeat assignment - partition assignment information
pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition
}

/// ConsumerGroupHeartbeat response topic partition - assigned partitions
pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8 // topic UUID (16 bytes)
	partitions []i32
}

// ConsumerGroupDescribe request/response types (API Key 69) - KIP-848

// Retrieves detailed information about consumer groups.

/// ConsumerGroupDescribe request - request to describe consumer groups (KIP-848)
pub struct ConsumerGroupDescribeRequest {
pub:
	group_ids                     []string
	include_authorized_operations bool
}

/// ConsumerGroupDescribe response - consumer group detailed information
pub struct ConsumerGroupDescribeResponse {
pub:
	throttle_time_ms i32
	groups           []ConsumerGroupDescribeResponseGroup
}

/// ConsumerGroupDescribe response group - individual group information
pub struct ConsumerGroupDescribeResponseGroup {
pub:
	error_code            i16
	error_message         ?string
	group_id              string
	group_state           string
	group_epoch           i32
	assignment_epoch      i32
	assignor_name         string
	members               []ConsumerGroupDescribeResponseMember
	authorized_operations i32
}

/// ConsumerGroupDescribe response member - member information
pub struct ConsumerGroupDescribeResponseMember {
pub:
	member_id            string
	instance_id          ?string
	rack_id              ?string
	member_epoch         i32
	client_id            string
	client_host          string
	subscribed_topic_ids [][]u8 // subscribed topic ID list (UUID)
	assignment           ?ConsumerGroupDescribeResponseMemberAssignment
}

/// ConsumerGroupDescribe response member assignment - member's partition assignment
pub struct ConsumerGroupDescribeResponseMemberAssignment {
pub:
	topic_partitions []ConsumerGroupDescribeTopicPartition
}

/// ConsumerGroupDescribe topic partition - assigned partitions
pub struct ConsumerGroupDescribeTopicPartition {
pub:
	topic_id   []u8 // topic UUID (16 bytes)
	partitions []i32
}
