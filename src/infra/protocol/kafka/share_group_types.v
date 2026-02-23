// ShareGroupHeartbeat (API Key 76), ShareFetch (API Key 78), ShareAcknowledge (API Key 79)
// Request/response struct definitions
//
// Share Group supports queue-based message consumption patterns,
// and guarantees at-least-once delivery via the acknowledge mechanism.
module kafka

// ShareGroupHeartbeat (API Key 76) type - Share Group heartbeat

/// ShareGroupHeartbeat request - heartbeat for maintaining Share Group membership
///
/// Share Group members send this periodically to maintain group membership.
/// Also used to notify the broker of subscribed topic changes.
pub struct ShareGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	rack_id                string
	subscribed_topic_names []string
}

/// ShareGroupHeartbeat response - Share Group heartbeat response
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

/// ShareGroup assignment - partition assignment in heartbeat response
pub struct ShareGroupAssignment {
pub:
	topic_partitions []ShareGroupTopicPartitions
}

/// ShareGroup topic partitions - per-topic partition assignment
pub struct ShareGroupTopicPartitions {
pub:
	topic_id   []u8 // topic UUID (16 bytes)
	partitions []i32
}

// ShareFetch (API Key 78) type - Share Group message fetch

/// ShareFetch request - fetches messages from a Share Group
///
/// Unlike regular Fetch, messages are "acquired",
/// and acquired messages are not delivered to other consumers until acknowledged.
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

/// ShareFetch topic - topic in a ShareFetch request
pub struct ShareFetchTopic {
pub:
	topic_id   []u8
	partitions []ShareFetchPartition
}

/// ShareFetch partition - partition to fetch from
pub struct ShareFetchPartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

/// Share acknowledgement batch - batch of message acknowledgements
///
/// Specifies the acknowledgement type for a range of messages.
/// 0: Gap, 1: Accept, 2: Release, 3: Reject
pub struct ShareAcknowledgementBatch {
pub:
	first_offset      i64
	last_offset       i64
	acknowledge_types []u8
}

/// Share forgotten topic - topic to remove from the session
pub struct ShareForgottenTopic {
pub:
	topic_id   []u8
	partitions []i32
}

/// ShareFetch response - response to a ShareFetch request
pub struct ShareFetchResponse {
pub:
	throttle_time_ms            i32
	error_code                  i16
	error_message               string
	acquisition_lock_timeout_ms i32
	responses                   []ShareFetchTopicResponse
	node_endpoints              []ShareNodeEndpoint
}

/// ShareFetch topic response - per-topic response
pub struct ShareFetchTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareFetchPartitionResponse
}

/// ShareFetch partition response - per-partition response
pub struct ShareFetchPartitionResponse {
pub:
	partition_index           i32
	error_code                i16
	error_message             string
	acknowledge_error_code    i16
	acknowledge_error_message string
	current_leader            ShareLeaderIdAndEpoch
	records                   []u8
	acquired_records          []ShareAcquiredRecords
}

/// Share leader ID and epoch - leader information
pub struct ShareLeaderIdAndEpoch {
pub:
	leader_id    i32
	leader_epoch i32
}

/// Share acquired records - range of acquired records
pub struct ShareAcquiredRecords {
pub:
	first_offset   i64
	last_offset    i64
	delivery_count i16
}

/// Share node endpoint - broker endpoint
pub struct ShareNodeEndpoint {
pub:
	node_id i32
	host    string
	port    i32
	rack    string
}

// ShareAcknowledge (API Key 79) type - Share Group message acknowledgement

/// ShareAcknowledge request - request to acknowledge acquired messages
///
/// Consumers send this after finishing processing messages.
/// Supports acknowledgement types: Accept, Release, Reject.
pub struct ShareAcknowledgeRequest {
pub:
	group_id            string
	member_id           string
	share_session_epoch i32
	topics              []ShareAcknowledgeTopic
}

/// ShareAcknowledge topic - topic to acknowledge
pub struct ShareAcknowledgeTopic {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartition
}

/// ShareAcknowledge partition - partition to acknowledge
pub struct ShareAcknowledgePartition {
pub:
	partition_index         i32
	acknowledgement_batches []ShareAcknowledgementBatch
}

/// ShareAcknowledge response - result of message acknowledgement
pub struct ShareAcknowledgeResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    string
	responses        []ShareAcknowledgeTopicResponse
	node_endpoints   []ShareNodeEndpoint
}

/// ShareAcknowledge topic response - per-topic acknowledgement result
pub struct ShareAcknowledgeTopicResponse {
pub:
	topic_id   []u8
	partitions []ShareAcknowledgePartitionResponse
}

/// ShareAcknowledge partition response - per-partition acknowledgement result
pub struct ShareAcknowledgePartitionResponse {
pub:
	partition_index i32
	error_code      i16
	error_message   string
	current_leader  ShareLeaderIdAndEpoch
}
