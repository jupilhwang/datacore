// KIP-848 Type Definitions
// Shared types for KIP-848 New Consumer Protocol implementation
module group

// ============================================================================
// KIP-848 Member State Machine
// ============================================================================

// MemberState represents the state of a member in the new protocol
pub enum MemberState {
	unsubscribed  // Member has no subscriptions
	subscribing   // Member is subscribing to topics
	stable        // Member has a stable assignment
	reconciling   // Member is reconciling assignment changes
	assigning     // Server is computing new assignment
	unsubscribing // Member is leaving the group
	fenced        // Member has been fenced (epoch mismatch)
}

// KIP848Member represents a consumer group member using KIP-848 protocol
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
	pending_partitions     []TopicPartition // Partitions to be assigned
	revoking_partitions    []TopicPartition // Partitions to be revoked
	rebalance_timeout_ms   i32
	session_timeout_ms     i32
	last_heartbeat         i64 // Unix timestamp ms
	joined_at              i64
}

// TopicPartition represents a topic-partition pair
pub struct TopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	topic_name string
	partition  i32
}

// ============================================================================
// KIP-848 Consumer Group
// ============================================================================

// KIP848ConsumerGroup represents a consumer group using the new protocol
pub struct KIP848ConsumerGroup {
pub mut:
	group_id           string
	group_epoch        i32
	assignment_epoch   i32
	state              KIP848GroupState
	protocol_type      string
	server_assignor    string
	members            map[string]&KIP848Member
	target_assignment  map[string][]TopicPartition // member_id -> assigned partitions
	current_assignment map[string][]TopicPartition // Current stable assignment
	subscribed_topics  map[string]bool             // All subscribed topics
	created_at         i64
	updated_at         i64
}

// KIP848GroupState represents the state of a KIP-848 group
pub enum KIP848GroupState {
	empty       // No members
	assigning   // Computing new assignment
	reconciling // Members are reconciling
	stable      // All members have stable assignment
	dead        // Group is being deleted
}

// ============================================================================
// Server-side Assignor Interface and Types
// ============================================================================

// ServerAssignor computes partition assignments on the server side
pub interface ServerAssignor {
	name() string
	assign(members []MemberSubscription, topics map[string]TopicMetadata) !map[string][]TopicPartition
}

// MemberSubscription contains member's subscription info for assignment
pub struct MemberSubscription {
pub:
	member_id        string
	instance_id      ?string
	rack_id          ?string
	topics           []string
	owned_partitions []TopicPartition // Currently owned for sticky assignment
}

// TopicMetadata contains topic info for assignment
pub struct TopicMetadata {
pub:
	topic_id        []u8
	topic_name      string
	partition_count int
}
