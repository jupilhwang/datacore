// Handles JoinGroup, SyncGroup, Heartbeat, LeaveGroup, and ConsumerGroupHeartbeat requests.
//
// This module implements the Kafka Consumer Group APIs.
// Provides join, sync, heartbeat, and leave operations for consumer groups,
// and also supports the new ConsumerGroupHeartbeat API based on KIP-848.
module kafka

import domain
import service.port
import rand
import time

// Handler functions - byte-array-based request processing (legacy)

/// JoinGroup handler - processes consumer group join requests.
/// Used when a new member joins a group or an existing member rejoins.
fn (mut h Handler) handle_join_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_join_group_request(mut reader, version, is_flexible_version(.join_group,
		version))!
	resp := h.process_join_group(req, version)!
	return resp.encode(version)
}

/// SyncGroup handler - processes group sync requests.
/// Used when the leader distributes partition assignments and members receive them.
fn (mut h Handler) handle_sync_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_sync_group_request(mut reader, version, is_flexible_version(.sync_group,
		version))!
	resp := h.process_sync_group(req, version)!
	return resp.encode(version)
}

/// Heartbeat handler - processes consumer heartbeat requests.
/// Members signal that they are still active and check rebalance status.
fn (mut h Handler) handle_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_heartbeat_request(mut reader, version, is_flexible_version(.heartbeat,
		version))!
	resp := h.process_heartbeat(req, version)!
	return resp.encode(version)
}

/// LeaveGroup handler - processes group leave requests.
/// Used when a member leaves a group, triggering a rebalance.
fn (mut h Handler) handle_leave_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_leave_group_request(mut reader, version, is_flexible_version(.leave_group,
		version))!
	resp := h.process_leave_group(req, version)!
	return resp.encode(version)
}

/// ConsumerGroupHeartbeat handler (KIP-848) - processes heartbeats for the new consumer group protocol.
/// A new protocol that consolidates JoinGroup/SyncGroup/Heartbeat into a single API.
fn (mut h Handler) handle_consumer_group_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_consumer_group_heartbeat_request(mut reader, version, true)!

	resp := h.process_consumer_group_heartbeat(req, version)!
	return resp.encode(version)
}

/// ConsumerGroupDescribe handler (KIP-848) - retrieves detailed consumer group information.
fn (mut h Handler) handle_consumer_group_describe(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_consumer_group_describe_request(mut reader, version, true)!

	resp := h.process_consumer_group_describe(req, version)!
	return resp.encode(version)
}

// Process functions - business logic

/// Processes ConsumerGroupHeartbeat requests (KIP-848).
/// Handles new member registration, existing member heartbeats, and member leaves.
fn (mut h Handler) process_consumer_group_heartbeat(req ConsumerGroupHeartbeatRequest, version i16) !ConsumerGroupHeartbeatResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing consumer group heartbeat', port.field_string('group_id',
		req.group_id), port.field_string('member_id', req.member_id), port.field_int('member_epoch',
		req.member_epoch))

	mut error_code := i16(0)
	mut error_message := ?string(none)
	mut member_id := req.member_id
	mut member_epoch := req.member_epoch
	mut heartbeat_interval_ms := i32(3000)
	mut assignment := ?ConsumerGroupHeartbeatAssignment(none)

	// Validate group ID
	if req.group_id.len == 0 {
		h.logger.warn('Consumer group heartbeat failed: empty group ID')
		return ConsumerGroupHeartbeatResponse{
			throttle_time_ms:      0
			error_code:            i16(ErrorCode.invalid_group_id)
			error_message:         'Group ID cannot be empty'
			member_id:             none
			member_epoch:          -1
			heartbeat_interval_ms: 0
			assignment:            none
		}
	}

	// Register new member (epoch=0, no member_id)
	if req.member_epoch == 0 && req.member_id.len == 0 {
		// Generate new member ID
		member_id = 'member-${h.broker_id}-${rand.i64()}'
		member_epoch = 1

		h.logger.info('New consumer group member joined', port.field_string('group_id',
			req.group_id), port.field_string('member_id', member_id))

		// Create partition assignment for subscribed topics
		mut topic_partitions := []ConsumerGroupHeartbeatResponseTopicPartition{}

		for topic_name in req.subscribed_topic_names {
			topic_meta := h.storage.get_topic(topic_name) or { continue }
			mut partitions := []i32{}
			for p in 0 .. topic_meta.partition_count {
				partitions << i32(p)
			}

			topic_partitions << ConsumerGroupHeartbeatResponseTopicPartition{
				topic_id:   topic_meta.topic_id.clone()
				partitions: partitions
			}
		}

		if topic_partitions.len > 0 {
			assignment = ConsumerGroupHeartbeatAssignment{
				topic_partitions: topic_partitions
			}
		}
	} else if req.member_epoch == -1 {
		// Member leave request (epoch=-1)
		h.logger.info('Consumer leaving group', port.field_string('group_id', req.group_id),
			port.field_string('member_id', req.member_id))
		member_epoch = -1
		assignment = none
	} else if req.member_epoch > 0 {
		// Regular heartbeat - no state change
		h.logger.trace('Consumer heartbeat', port.field_string('group_id', req.group_id),
			port.field_string('member_id', req.member_id), port.field_int('epoch', req.member_epoch))
	} else {
		// Invalid epoch value
		h.logger.warn('Invalid consumer heartbeat', port.field_string('group_id', req.group_id),
			port.field_string('member_id', req.member_id), port.field_int('epoch', req.member_epoch))
		error_code = i16(ErrorCode.unknown_member_id)
		error_message = 'Invalid member epoch'
		member_epoch = -1
	}

	elapsed := time.since(start_time)
	h.logger.debug('Consumer group heartbeat completed', port.field_string('group_id',
		req.group_id), port.field_int('error_code', error_code), port.field_duration('latency',
		elapsed))

	return ConsumerGroupHeartbeatResponse{
		throttle_time_ms:      0
		error_code:            error_code
		error_message:         error_message
		member_id:             if member_id.len > 0 { member_id } else { none }
		member_epoch:          member_epoch
		heartbeat_interval_ms: heartbeat_interval_ms
		assignment:            assignment
	}
}

/// Processes JoinGroup requests.
/// Consumer joins the group and participates in rebalancing.
/// On first join, a member_id is generated and returned.
fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing join group request', port.field_string('group_id', req.group_id),
		port.field_string('member_id', req.member_id), port.field_string('protocol_type',
		req.protocol_type), port.field_int('protocols', req.protocols.len))

	// Need to generate member_id on first join (return MEMBER_ID_REQUIRED error in v4+)
	if req.member_id.len == 0 && req.group_instance_id == none {
		new_member_id := 'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
		h.logger.info('Member ID required, generated new ID', port.field_string('group_id',
			req.group_id), port.field_string('new_member_id', new_member_id))
		return JoinGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.member_id_required)
			generation_id:    -1
			protocol_type:    req.protocol_type
			protocol_name:    ''
			leader:           ''
			skip_assignment:  false
			member_id:        new_member_id
			members:          []
		}
	}

	// Determine member ID (existing or newly generated)
	member_id := if req.member_id.len > 0 {
		req.member_id
	} else {
		'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
	}

	// Selected protocol name (use the first protocol)
	protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }

	// Load existing group or create a new one
	mut group := h.storage.load_group(req.group_id) or {
		domain.ConsumerGroup{
			group_id:      req.group_id
			generation_id: 0
			protocol_type: req.protocol_type
			protocol:      protocol_name
			state:         .preparing_rebalance
			members:       []
			leader:        ''
		}
	}

	// Create/update member information
	member := domain.GroupMember{
		member_id:         member_id
		group_instance_id: req.group_instance_id or { '' }
		client_id:         ''
		client_host:       ''
		metadata:          if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
		assignment:        []u8{}
	}

	// Update member list (refresh existing member or add new one)
	mut found := false
	mut new_members := []domain.GroupMember{}
	for m in group.members {
		if m.member_id == member_id {
			new_members << member
			found = true
		} else {
			new_members << m
		}
	}
	if !found {
		new_members << member
	}

	// Determine new generation number and leader
	new_gen := group.generation_id + 1
	leader := if new_members.len > 0 { new_members[0].member_id } else { member_id }

	// Create updated group information
	new_group := domain.ConsumerGroup{
		group_id:      req.group_id
		generation_id: new_gen
		protocol_type: req.protocol_type
		protocol:      protocol_name
		state:         .stable
		members:       new_members
		leader:        leader
	}

	// Save group state
	h.storage.save_group(new_group) or {
		h.logger.error('Failed to save group', port.field_string('group_id', req.group_id),
			port.field_string('error', err.str()))
		return error('failed to save group: ${err}')
	}

	elapsed := time.since(start_time)
	h.logger.info('Member joined group', port.field_string('group_id', req.group_id),
		port.field_string('member_id', member_id), port.field_int('generation', new_gen),
		port.field_string('leader', leader), port.field_int('members', new_members.len),
		port.field_duration('latency', elapsed))

	// Return member list only to the leader (for partition assignment)
	response_members := if member_id == leader {
		new_members.map(fn (m domain.GroupMember) JoinGroupResponseMember {
			return JoinGroupResponseMember{
				member_id:         m.member_id
				group_instance_id: if m.group_instance_id.len > 0 {
					m.group_instance_id
				} else {
					none
				}
				metadata:          m.metadata
			}
		})
	} else {
		[]JoinGroupResponseMember{}
	}

	return JoinGroupResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		generation_id:    new_gen
		protocol_type:    req.protocol_type
		protocol_name:    protocol_name
		leader:           leader
		skip_assignment:  false
		member_id:        member_id
		members:          response_members
	}
}

/// Processes SyncGroup requests.
/// The leader submits partition assignments and all members receive their assignments.
fn (mut h Handler) process_sync_group(req SyncGroupRequest, version i16) !SyncGroupResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing sync group request', port.field_string('group_id', req.group_id),
		port.field_string('member_id', req.member_id), port.field_int('generation', req.generation_id),
		port.field_int('assignments', req.assignments.len))

	// Load group
	mut group := h.storage.load_group(req.group_id) or {
		h.logger.warn('Sync group failed: group not found', port.field_string('group_id',
			req.group_id))
		return SyncGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.group_id_not_found)
			protocol_type:    ''
			protocol_name:    ''
			assignment:       []u8{}
		}
	}

	// Validate generation number
	if group.generation_id != req.generation_id {
		h.logger.warn('Sync group failed: illegal generation', port.field_string('group_id',
			req.group_id), port.field_int('expected', group.generation_id), port.field_int('received',
			req.generation_id))
		return SyncGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.illegal_generation)
			protocol_type:    group.protocol_type
			protocol_name:    group.protocol
			assignment:       []u8{}
		}
	}

	// Update group members if the leader submitted assignments
	if req.assignments.len > 0 {
		mut updated_members := []domain.GroupMember{}
		for m in group.members {
			mut member := m
			for a in req.assignments {
				if a.member_id == m.member_id {
					member = domain.GroupMember{
						member_id:         m.member_id
						group_instance_id: m.group_instance_id
						client_id:         m.client_id
						client_host:       m.client_host
						metadata:          m.metadata
						assignment:        a.assignment.clone()
					}
					break
				}
			}
			updated_members << member
		}
		group = domain.ConsumerGroup{
			group_id:      group.group_id
			generation_id: group.generation_id
			protocol_type: group.protocol_type
			protocol:      group.protocol
			state:         .stable
			members:       updated_members
			leader:        group.leader
		}
		h.storage.save_group(group) or {
			h.logger.error('Failed to save group assignments', port.field_string('group_id',
				req.group_id), port.field_string('error', err.str()))
			return error('failed to save group: ${err}')
		}
	}

	// Find this member's assignment
	mut assignment := []u8{}
	for m in group.members {
		if m.member_id == req.member_id {
			assignment = m.assignment.clone()
			break
		}
	}

	// Use empty consumer assignment if no assignment found
	if assignment.len == 0 {
		assignment = empty_consumer_assignment()
	}

	elapsed := time.since(start_time)
	h.logger.debug('Sync group completed', port.field_string('group_id', req.group_id),
		port.field_string('member_id', req.member_id), port.field_bytes('assignment_size',
		assignment.len), port.field_duration('latency', elapsed))

	return SyncGroupResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		protocol_type:    group.protocol_type
		protocol_name:    group.protocol
		assignment:       assignment
	}
}

/// Processes Heartbeat requests.
/// Members signal they are active in the group and check rebalance status.
fn (mut h Handler) process_heartbeat(req HeartbeatRequest, version i16) !HeartbeatResponse {
	_ = version
	h.logger.trace('Processing heartbeat', port.field_string('group_id', req.group_id),
		port.field_string('member_id', req.member_id), port.field_int('generation', req.generation_id))

	// Validate group existence and generation number
	group := h.storage.load_group(req.group_id) or {
		h.logger.debug('Heartbeat failed: group not found', port.field_string('group_id',
			req.group_id))
		return HeartbeatResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.group_id_not_found)
		}
	}

	// Validate generation number
	if group.generation_id != req.generation_id {
		h.logger.debug('Heartbeat failed: illegal generation', port.field_string('group_id',
			req.group_id), port.field_int('expected', group.generation_id), port.field_int('received',
			req.generation_id))
		return HeartbeatResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.illegal_generation)
		}
	}

	// Check if the member exists in the group
	mut member_found := false
	for m in group.members {
		if m.member_id == req.member_id {
			member_found = true
			break
		}
	}

	if !member_found {
		h.logger.debug('Heartbeat failed: unknown member', port.field_string('group_id',
			req.group_id), port.field_string('member_id', req.member_id))
		return HeartbeatResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.unknown_member_id)
		}
	}

	return HeartbeatResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
	}
}

/// Processes LeaveGroup requests.
/// When a member leaves the group, a rebalance is triggered.
fn (mut h Handler) process_leave_group(req LeaveGroupRequest, version i16) !LeaveGroupResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing leave group request', port.field_string('group_id', req.group_id),
		port.field_string('member_id', req.member_id), port.field_int('members', req.members.len))

	// Load group
	mut group := h.storage.load_group(req.group_id) or {
		h.logger.warn('Leave group failed: group not found', port.field_string('group_id',
			req.group_id))
		return LeaveGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.group_id_not_found)
			members:          []LeaveGroupResponseMember{}
		}
	}

	// Remove members from the group
	mut removed_members := []LeaveGroupResponseMember{}
	mut remaining_members := []domain.GroupMember{}

	for m in group.members {
		mut should_remove := false

		// Check if this member should be removed (v3+ uses the members array)
		if req.members.len > 0 {
			for leave_member in req.members {
				if m.member_id == leave_member.member_id {
					should_remove = true
					removed_members << LeaveGroupResponseMember{
						member_id:         m.member_id
						group_instance_id: if m.group_instance_id.len > 0 {
							?string(m.group_instance_id)
						} else {
							none
						}
						error_code:        0
					}
					break
				}
			}
		} else if m.member_id == req.member_id {
			// v0-v2: use single member_id field
			should_remove = true
			removed_members << LeaveGroupResponseMember{
				member_id:         m.member_id
				group_instance_id: none
				error_code:        0
			}
		}

		if !should_remove {
			remaining_members << m
		}
	}

	// Return unknown_member_id error if no members were removed
	if removed_members.len == 0 {
		h.logger.warn('Leave group failed: no members removed', port.field_string('group_id',
			req.group_id))
		return LeaveGroupResponse{
			throttle_time_ms: default_throttle_time_ms
			error_code:       i16(ErrorCode.unknown_member_id)
			members:          []LeaveGroupResponseMember{}
		}
	}

	// Update group state (empty if no members, otherwise preparing for rebalance)
	new_state := if remaining_members.len == 0 {
		domain.GroupState.empty
	} else {
		domain.GroupState.preparing_rebalance
	}

	// Determine new leader (first of remaining members)
	new_leader := if remaining_members.len > 0 {
		remaining_members[0].member_id
	} else {
		''
	}

	// Create and save updated group information
	new_group := domain.ConsumerGroup{
		group_id:      group.group_id
		generation_id: group.generation_id + 1
		protocol_type: group.protocol_type
		protocol:      group.protocol
		state:         new_state
		members:       remaining_members
		leader:        new_leader
	}

	h.storage.save_group(new_group) or {
		h.logger.error('Leave group failed: failed to save group', port.field_string('group_id',
			req.group_id), port.field_string('error', err.str()))
		return error('failed to save group: ${err}')
	}

	elapsed := time.since(start_time)
	h.logger.info('Members left group', port.field_string('group_id', req.group_id), port.field_int('removed',
		removed_members.len), port.field_int('remaining', remaining_members.len), port.field_duration('latency',
		elapsed))

	return LeaveGroupResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		members:          removed_members
	}
}

/// Processes ConsumerGroupDescribe requests (KIP-848).
/// Retrieves detailed consumer group information.
fn (mut h Handler) process_consumer_group_describe(req ConsumerGroupDescribeRequest, version i16) !ConsumerGroupDescribeResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing consumer group describe', port.field_int('group_ids', req.group_ids.len))

	mut groups := []ConsumerGroupDescribeResponseGroup{}

	for group_id in req.group_ids {
		// Attempt to load the group
		group := h.storage.load_group(group_id) or {
			// Group not found
			h.logger.trace('Group not found', port.field_string('group_id', group_id))
			groups << ConsumerGroupDescribeResponseGroup{
				error_code:            i16(ErrorCode.group_id_not_found)
				error_message:         'Group not found: ${group_id}'
				group_id:              group_id
				group_state:           ''
				group_epoch:           0
				assignment_epoch:      0
				assignor_name:         ''
				members:               []
				authorized_operations: 0
			}
			continue
		}

		// Convert group state to string
		state_str := match group.state {
			.empty { 'Empty' }
			.stable { 'Stable' }
			.preparing_rebalance { 'PreparingRebalance' }
			.completing_rebalance { 'CompletingRebalance' }
			.dead { 'Dead' }
		}

		// Build member list
		mut response_members := []ConsumerGroupDescribeResponseMember{}
		for m in group.members {
			response_members << ConsumerGroupDescribeResponseMember{
				member_id:            m.member_id
				instance_id:          if m.group_instance_id.len > 0 {
					m.group_instance_id
				} else {
					none
				}
				rack_id:              none
				member_epoch:         group.generation_id
				client_id:            m.client_id
				client_host:          m.client_host
				subscribed_topic_ids: []
				assignment:           none
			}
		}

		h.logger.trace('Describing consumer group', port.field_string('group_id', group_id),
			port.field_string('state', state_str), port.field_int('members', response_members.len))

		groups << ConsumerGroupDescribeResponseGroup{
			error_code:            0
			error_message:         none
			group_id:              group_id
			group_state:           state_str
			group_epoch:           group.generation_id
			assignment_epoch:      group.generation_id
			assignor_name:         group.protocol
			members:               response_members
			authorized_operations: if req.include_authorized_operations { -2147483648 } else { 0 }
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Consumer group describe completed', port.field_int('groups', groups.len),
		port.field_duration('latency', elapsed))

	return ConsumerGroupDescribeResponse{
		throttle_time_ms: default_throttle_time_ms
		groups:           groups
	}
}
