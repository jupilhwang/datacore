module domain

/// ConsumerGroup represents a consumer group.
/// group_id: group identifier
/// generation_id: generation ID (incremented on each rebalance)
/// protocol_type: protocol type ('consumer')
/// protocol: assignment protocol ('range', 'roundrobin', etc.)
/// leader: leader member ID
/// state: group state
/// members: list of group members
pub struct ConsumerGroup {
pub:
	group_id      string
	generation_id int
	protocol_type string
	protocol      string
	leader        string
	state         GroupState
	members       []GroupMember
}

/// GroupState represents the state of a consumer group.
/// empty: no members
/// preparing_rebalance: preparing for rebalance
/// completing_rebalance: completing rebalance
/// stable: stable state
/// dead: group deleted
pub enum GroupState {
	empty
	preparing_rebalance
	completing_rebalance
	stable
	dead
}

/// GroupMember represents a member of a consumer group.
/// member_id: unique member identifier
/// group_instance_id: instance ID for static membership
/// client_id: client ID
/// client_host: client host
/// metadata: member metadata (subscribed topics, etc.)
/// assignment: partition assignment information
pub struct GroupMember {
pub:
	member_id         string
	group_instance_id string
	client_id         string
	client_host       string
	metadata          []u8
	assignment        []u8
}

/// GroupInfo is a summary of consumer group information.
/// Used when listing groups.
pub struct GroupInfo {
pub:
	group_id      string
	protocol_type string
	state         string
}
