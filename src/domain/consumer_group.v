// Entity Layer - Consumer Group Domain Model
module domain

// ConsumerGroup represents a consumer group
pub struct ConsumerGroup {
pub:
    group_id            string
    generation_id       int
    protocol_type       string
    protocol            string
    leader              string
    state               GroupState
    members             []GroupMember
}

// GroupState represents the state of a consumer group
pub enum GroupState {
    empty
    preparing_rebalance
    completing_rebalance
    stable
    dead
}

// GroupMember represents a member of a consumer group
pub struct GroupMember {
pub:
    member_id           string
    group_instance_id   string
    client_id           string
    client_host         string
    metadata            []u8
    assignment          []u8
}

// GroupInfo is a simplified view of a consumer group
pub struct GroupInfo {
pub:
    group_id            string
    protocol_type       string
    state               string
}
