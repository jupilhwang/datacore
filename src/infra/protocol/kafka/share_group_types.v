// Kafka Protocol - Share Group Types (KIP-932)
// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// Request/Response struct definitions
module kafka

// ============================================================================
// ShareGroupHeartbeat (API Key 76) Types
// ============================================================================

// ShareGroupHeartbeatRequest represents a share group heartbeat request
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

// ShareGroupHeartbeatResponse represents a share group heartbeat response
pub struct ShareGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         string
	member_id             string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ShareGroupAssignment
}

// ShareGroupAssignment represents the partition assignment in heartbeat response
pub struct ShareGroupAssignment {
pub:
	topic_partitions []ShareGroupTopicPartitions
}

// ShareGroupTopicPartitions represents topic partition assignments
pub struct ShareGroupTopicPartitions {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

// ============================================================================
// ShareFetch (API Key 78) Types
// ============================================================================

// ShareFetchRequest represents a share fetch request
pub struct ShareFetchRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	max_wait_ms         i32
	min_bytes           i32
	max_bytes           i32
	max_records         i32
	batch_size          i32
	topics              []ShareFetchTopic
	forgotten_topics    []ShareForgottenTopic
}

// ShareFetchTopic represents topics to fetch in a share fetch request
pub struct ShareFetchTopic {
pub:
	topic_id   []u8 // UUID
	partitions []ShareFetchPartition
}

// ShareFetchPartition represents partitions to fetch
pub struct ShareFetchPartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

// ShareAcknowledgementBatch represents a batch of acknowledgements
pub struct ShareAcknowledgementBatch {
pub:
	first_offset      i64
	last_offset       i64
	acknowledge_types []u8 // 0:Gap, 1:Accept, 2:Release, 3:Reject
}

// ShareForgottenTopic represents topics to remove from session
pub struct ShareForgottenTopic {
pub:
	topic_id   []u8
	partitions []i32
}

// ShareFetchResponse represents a share fetch response
pub struct ShareFetchResponse {
pub:
	throttle_time_ms            i32
	error_code                  i16
	error_message               string
	acquisition_lock_timeout_ms i32
	responses                   []ShareFetchTopicResponse
	node_endpoints              []ShareNodeEndpoint
}

// ShareFetchTopicResponse represents topic responses
pub struct ShareFetchTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareFetchPartitionResponse
}

// ShareFetchPartitionResponse represents partition responses
pub struct ShareFetchPartitionResponse {
pub:
	partition_index           i32
	error_code                i16
	error_message             string
	acknowledge_error_code    i16
	acknowledge_error_message string
	current_leader            ShareLeaderIdAndEpoch
	records                   []u8 // Record batch bytes
	acquired_records          []ShareAcquiredRecords
}

// ShareLeaderIdAndEpoch represents leader info
pub struct ShareLeaderIdAndEpoch {
pub:
	leader_id    i32
	leader_epoch i32
}

// ShareAcquiredRecords represents acquired record ranges
pub struct ShareAcquiredRecords {
pub:
	first_offset   i64
	last_offset    i64
	delivery_count i16
}

// ShareNodeEndpoint represents broker endpoint
pub struct ShareNodeEndpoint {
pub:
	node_id i32
	host    string
	port    i32
	rack    string
}

// ============================================================================
// ShareAcknowledge (API Key 79) Types
// ============================================================================

// ShareAcknowledgeRequest represents a share acknowledge request
pub struct ShareAcknowledgeRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	topics              []ShareAcknowledgeTopic
}

// ShareAcknowledgeTopic represents topics with acknowledgements
pub struct ShareAcknowledgeTopic {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartition
}

// ShareAcknowledgePartition represents partitions with acknowledgements
pub struct ShareAcknowledgePartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

// ShareAcknowledgeResponse represents a share acknowledge response
pub struct ShareAcknowledgeResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    string
	responses        []ShareAcknowledgeTopicResponse
	node_endpoints   []ShareNodeEndpoint
}

// ShareAcknowledgeTopicResponse represents topic responses
pub struct ShareAcknowledgeTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartitionResponse
}

// ShareAcknowledgePartitionResponse represents partition responses
pub struct ShareAcknowledgePartitionResponse {
pub:
	partition_index i32
	error_code      i16
	error_message   string
	current_leader  ShareLeaderIdAndEpoch
}
