// Handles consumer group coordination business logic.
// Implements protocols such as JoinGroup, SyncGroup, Heartbeat, and LeaveGroup.
module group

import domain
import rand
import service.port

/// GroupCoordinator handles consumer group coordination.
/// Responsible for group membership management, rebalancing, and offset management.
pub struct GroupCoordinator {
mut:
	storage port.StoragePort
}

/// new_group_coordinator creates a new GroupCoordinator.
pub fn new_group_coordinator(storage port.StoragePort) &GroupCoordinator {
	return &GroupCoordinator{
		storage: storage
	}
}

/// JoinGroupRequest represents a group join request.
pub struct JoinGroupRequest {
pub:
	group_id             string
	session_timeout_ms   i32
	rebalance_timeout_ms i32
	member_id            string
	group_instance_id    string
	protocol_type        string
	protocols            []Protocol
}

/// Protocol represents a group protocol.
pub struct Protocol {
pub:
	name     string
	metadata []u8
}

/// JoinGroupResponse represents a group join response.
pub struct JoinGroupResponse {
pub:
	error_code    i16
	generation_id int
	protocol_type string
	protocol_name string
	leader        string
	member_id     string
	members       []domain.GroupMember
}

/// join_group handles a group join request.
/// Adds a new member to the group and triggers rebalancing.
pub fn (mut c GroupCoordinator) join_group(req JoinGroupRequest) JoinGroupResponse {
	// Validate group ID
	if req.group_id.len == 0 {
		return JoinGroupResponse{
			error_code: i16(domain.ErrorCode.invalid_group_id)
		}
	}

	// Generate a new member ID if not provided
	member_id := if req.member_id.len > 0 {
		req.member_id
	} else {
		'member-${req.group_id}-${generate_id()}'
	}

	// Load existing group or create a new one
	mut group := c.storage.load_group(req.group_id) or {
		// Create new group
		domain.ConsumerGroup{
			group_id:      req.group_id
			generation_id: 0
			protocol_type: req.protocol_type
			state:         .preparing_rebalance
			members:       []
		}
	}

	// Build member info
	mut member := domain.GroupMember{
		member_id:         member_id
		group_instance_id: req.group_instance_id
		client_id:         ''
		client_host:       ''
		metadata:          if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
	}

	// Find existing member index (avoid clone where possible)
	mut member_idx := -1
	for i, m in group.members {
		if m.member_id == member_id {
			member_idx = i
			break
		}
	}

	// Update member list efficiently
	mut updated_members := []domain.GroupMember{}
	if member_idx >= 0 {
		// Update existing member - pre-allocate capacity instead of clone
		updated_members = []domain.GroupMember{cap: group.members.len}
		for i, m in group.members {
			if i == member_idx {
				updated_members << member
			} else {
				updated_members << m
			}
		}
	} else {
		// Add new member (single allocation)
		updated_members = []domain.GroupMember{cap: group.members.len + 1}
		updated_members << group.members
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
		protocol:      protocol_name
		leader:        leader
		state:         .stable
	}

	// Save group
	c.storage.save_group(new_group) or {
		return JoinGroupResponse{
			error_code: i16(domain.ErrorCode.unknown_server_error)
		}
	}

	return JoinGroupResponse{
		error_code:    0
		generation_id: new_gen
		protocol_type: req.protocol_type
		protocol_name: protocol_name
		leader:        leader
		member_id:     member_id
		members:       if member_id == leader { new_group.members } else { [] }
	}
}

/// SyncGroupRequest represents a group sync request.
pub struct SyncGroupRequest {
pub:
	group_id      string
	generation_id int
	member_id     string
	assignments   []MemberAssignment
}

/// MemberAssignment represents a member assignment.
pub struct MemberAssignment {
pub:
	member_id  string
	assignment []u8
}

/// SyncGroupResponse represents a group sync response.
pub struct SyncGroupResponse {
pub:
	error_code i16
	assignment []u8
}

/// sync_group handles a group sync request.
/// Distributes the leader-provided assignment to each member.
pub fn (mut c GroupCoordinator) sync_group(req SyncGroupRequest) SyncGroupResponse {
	group := c.storage.load_group(req.group_id) or {
		return SyncGroupResponse{
			error_code: i16(domain.ErrorCode.group_id_not_found)
		}
	}

	// Verify generation
	if group.generation_id != req.generation_id {
		return SyncGroupResponse{
			error_code: i16(domain.ErrorCode.illegal_generation)
		}
	}

	// Find this member's assignment
	for a in req.assignments {
		if a.member_id == req.member_id {
			return SyncGroupResponse{
				error_code: 0
				assignment: a.assignment
			}
		}
	}

	// If not the leader, find assignment from stored state
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

/// HeartbeatRequest represents a heartbeat request.
pub struct HeartbeatRequest {
pub:
	group_id      string
	generation_id int
	member_id     string
}

/// HeartbeatResponse represents a heartbeat response.
pub struct HeartbeatResponse {
pub:
	error_code i16
}

/// heartbeat handles a heartbeat request.
/// Verifies member liveness and notifies whether rebalancing is needed.
pub fn (mut c GroupCoordinator) heartbeat(req HeartbeatRequest) HeartbeatResponse {
	group := c.storage.load_group(req.group_id) or {
		return HeartbeatResponse{
			error_code: i16(domain.ErrorCode.group_id_not_found)
		}
	}

	// Verify generation
	if group.generation_id != req.generation_id {
		return HeartbeatResponse{
			error_code: i16(domain.ErrorCode.illegal_generation)
		}
	}

	// Verify member exists
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

/// leave_group handles a group leave request.
/// Removes the member from the group and triggers rebalancing.
pub fn (mut c GroupCoordinator) leave_group(group_id string, member_id string) i16 {
	group := c.storage.load_group(group_id) or { return i16(domain.ErrorCode.group_id_not_found) }

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
		state:   if new_members.len == 0 {
			domain.GroupState.empty
		} else {
			domain.GroupState.preparing_rebalance
		}
	}

	c.storage.save_group(new_group) or { return i16(domain.ErrorCode.unknown_server_error) }

	return 0
}

/// list_groups returns a list of all consumer groups.
pub fn (mut c GroupCoordinator) list_groups() ![]domain.GroupInfo {
	return c.storage.list_groups()
}

/// describe_group returns detailed information for a consumer group.
pub fn (mut c GroupCoordinator) describe_group(group_id string) !domain.ConsumerGroup {
	return c.storage.load_group(group_id)
}

/// generate_id generates a unique ID.
fn generate_id() string {
	return 'member-${rand.i64()}'
}
