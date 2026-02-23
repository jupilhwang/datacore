// KIP-848 type definitions
// Shared types for implementing the KIP-848 new consumer protocol.
// Supports server-side partition assignment and incremental rebalancing.
module group

// KIP-848 member state machine

/// MemberState represents the state of a member in the new protocol.
pub enum MemberState {
	unsubscribed
	subscribing
	stable
	reconciling
	assigning
	unsubscribing
	fenced
}

/// KIP848Member represents a consumer group member using the KIP-848 protocol.
pub struct KIP848Member {
pub mut:
	member_id              string
	instance_id            ?string
	rack_id                ?string
	client_id              string
	client_host            string
	subscribed_topic_names []string
	subscribed_topic_regex ?string
	server_assignor        ?string
	member_epoch           i32
	previous_member_epoch  i32
	state                  MemberState
	assigned_partitions    []TopicPartition
	pending_partitions     []TopicPartition
	revoking_partitions    []TopicPartition
	rebalance_timeout_ms   i32
	session_timeout_ms     i32
	last_heartbeat         i64
	joined_at              i64
}

/// TopicPartition represents a topic-partition pair.
pub struct TopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	topic_name string
	partition  i32
}

// KIP-848 consumer group

/// KIP848ConsumerGroup represents a consumer group using the new protocol.
pub struct KIP848ConsumerGroup {
pub mut:
	group_id           string
	group_epoch        i32
	assignment_epoch   i32
	state              KIP848GroupState
	protocol_type      string
	server_assignor    string
	members            map[string]&KIP848Member
	target_assignment  map[string][]TopicPartition
	current_assignment map[string][]TopicPartition
	subscribed_topics  map[string]bool
	created_at         i64
	updated_at         i64
}

/// KIP848GroupState represents the state of a KIP-848 group.
pub enum KIP848GroupState {
	empty
	assigning
	reconciling
	stable
	dead
}

// Server-side assignor interface and types

/// ServerAssignor computes partition assignments on the server side.
pub interface ServerAssignor {
	/// Returns the assignor name.
	name() string

	/// Computes partition assignments.
	assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition
}

/// MemberSubscription holds a member's subscription information for assignment.
pub struct MemberSubscription {
pub:
	member_id        string
	instance_id      ?string
	rack_id          ?string
	topics           []string
	owned_partitions []TopicPartition
}

/// TopicMetadata holds topic information for assignment.
pub struct TopicMetadata {
pub:
	topic_id        []u8
	topic_name      string
	partition_count int
}
