// UseCase Layer - Consumer Group Coordinator UseCase
module group

import domain
import rand
import service.port

// GroupCoordinator handles consumer group coordination
pub struct GroupCoordinator {
mut:
    storage port.StoragePort
}

pub fn new_group_coordinator(storage port.StoragePort) &GroupCoordinator {
    return &GroupCoordinator{
        storage: storage
    }
}

// JoinGroupRequest represents a join group request
pub struct JoinGroupRequest {
pub:
    group_id            string
    session_timeout_ms  i32
    rebalance_timeout_ms i32
    member_id           string
    group_instance_id   string
    protocol_type       string
    protocols           []Protocol
}

// Protocol represents a group protocol
pub struct Protocol {
pub:
    name        string
    metadata    []u8
}

// JoinGroupResponse represents a join group response
pub struct JoinGroupResponse {
pub:
    error_code      i16
    generation_id   int
    protocol_type   string
    protocol_name   string
    leader          string
    member_id       string
    members         []domain.GroupMember
}

// JoinGroup processes a join group request
pub fn (mut c GroupCoordinator) join_group(req JoinGroupRequest) JoinGroupResponse {
    // Validate group ID
    if req.group_id.len == 0 {
        return JoinGroupResponse{
            error_code: i16(domain.ErrorCode.invalid_group_id)
        }
    }
    
    // Generate member ID if not provided
    member_id := if req.member_id.len > 0 {
        req.member_id
    } else {
        'member-${req.group_id}-${generate_id()}'
    }
    
    // Try to load existing group
    mut group := c.storage.load_group(req.group_id) or {
        // Create new group
        domain.ConsumerGroup{
            group_id: req.group_id
            generation_id: 0
            protocol_type: req.protocol_type
            state: .preparing_rebalance
            members: []
        }
    }
    
    // Add or update member
    mut member := domain.GroupMember{
        member_id: member_id
        group_instance_id: req.group_instance_id
        client_id: ''
        client_host: ''
        metadata: if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
    }
    
    mut member_exists := false
    mut updated_members := group.members.clone()
    for i, m in updated_members {
        if m.member_id == member_id {
            updated_members[i] = member
            member_exists = true
            break
        }
    }
    
    if !member_exists {
        updated_members << member
    }
    
    group = domain.ConsumerGroup{
        ...group
        members: updated_members
    }
    
    // Increment generation and set leader
    new_gen := group.generation_id + 1
    leader := if group.members.len > 0 { group.members[0].member_id } else { member_id }
    protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }
    
    new_group := domain.ConsumerGroup{
        ...group
        generation_id: new_gen
        protocol: protocol_name
        leader: leader
        state: .stable
    }
    
    // Save group
    c.storage.save_group(new_group) or {
        return JoinGroupResponse{
            error_code: i16(domain.ErrorCode.unknown_server_error)
        }
    }
    
    return JoinGroupResponse{
        error_code: 0
        generation_id: new_gen
        protocol_type: req.protocol_type
        protocol_name: protocol_name
        leader: leader
        member_id: member_id
        members: if member_id == leader { new_group.members } else { [] }
    }
}

// SyncGroupRequest represents a sync group request
pub struct SyncGroupRequest {
pub:
    group_id        string
    generation_id   int
    member_id       string
    assignments     []MemberAssignment
}

// MemberAssignment represents a member assignment
pub struct MemberAssignment {
pub:
    member_id   string
    assignment  []u8
}

// SyncGroupResponse represents a sync group response
pub struct SyncGroupResponse {
pub:
    error_code  i16
    assignment  []u8
}

// SyncGroup processes a sync group request
pub fn (mut c GroupCoordinator) sync_group(req SyncGroupRequest) SyncGroupResponse {
    group := c.storage.load_group(req.group_id) or {
        return SyncGroupResponse{
            error_code: i16(domain.ErrorCode.group_id_not_found)
        }
    }
    
    // Check generation
    if group.generation_id != req.generation_id {
        return SyncGroupResponse{
            error_code: i16(domain.ErrorCode.illegal_generation)
        }
    }
    
    // Find assignment for this member
    for a in req.assignments {
        if a.member_id == req.member_id {
            return SyncGroupResponse{
                error_code: 0
                assignment: a.assignment
            }
        }
    }
    
    // If this member is not the leader, find their assignment in saved state
    for m in group.members {
        if m.member_id == req.member_id {
            return SyncGroupResponse{
                error_code: 0
                assignment: m.assignment
            }
        }
    }
    
    return SyncGroupResponse{
        error_code: i16(domain.ErrorCode.unknown_member_id)
    }
}

// HeartbeatRequest represents a heartbeat request
pub struct HeartbeatRequest {
pub:
    group_id        string
    generation_id   int
    member_id       string
}

// HeartbeatResponse represents a heartbeat response
pub struct HeartbeatResponse {
pub:
    error_code  i16
}

// Heartbeat processes a heartbeat request
pub fn (mut c GroupCoordinator) heartbeat(req HeartbeatRequest) HeartbeatResponse {
    group := c.storage.load_group(req.group_id) or {
        return HeartbeatResponse{
            error_code: i16(domain.ErrorCode.group_id_not_found)
        }
    }
    
    // Check generation
    if group.generation_id != req.generation_id {
        return HeartbeatResponse{
            error_code: i16(domain.ErrorCode.illegal_generation)
        }
    }
    
    // Check member exists
    for m in group.members {
        if m.member_id == req.member_id {
            return HeartbeatResponse{
                error_code: 0
            }
        }
    }
    
    return HeartbeatResponse{
        error_code: i16(domain.ErrorCode.unknown_member_id)
    }
}

// LeaveGroup processes a leave group request
pub fn (mut c GroupCoordinator) leave_group(group_id string, member_id string) i16 {
    group := c.storage.load_group(group_id) or {
        return i16(domain.ErrorCode.group_id_not_found)
    }
    
    // Remove member
    mut new_members := []domain.GroupMember{}
    for m in group.members {
        if m.member_id != member_id {
            new_members << m
        }
    }
    
    new_group := domain.ConsumerGroup{
        ...group
        members: new_members
        state: if new_members.len == 0 { domain.GroupState.empty } else { domain.GroupState.preparing_rebalance }
    }
    
    c.storage.save_group(new_group) or {
        return i16(domain.ErrorCode.unknown_server_error)
    }
    
    return 0
}

// ListGroups lists all consumer groups
pub fn (mut c GroupCoordinator) list_groups() ![]domain.GroupInfo {
    return c.storage.list_groups()
}

// DescribeGroup describes a consumer group
pub fn (mut c GroupCoordinator) describe_group(group_id string) !domain.ConsumerGroup {
    return c.storage.load_group(group_id)
}

// Helper function to generate unique ID
fn generate_id() string {
    return 'member-${rand.i64()}'
}
