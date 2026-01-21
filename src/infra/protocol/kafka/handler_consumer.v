// Kafka Protocol - Consumer Group Handlers
// Handler and processing logic for JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

import domain
import infra.observability
import rand
import time

// ============================================================================
// Handler Functions
// ============================================================================

// JoinGroup handler - processes consumer group join requests
fn (mut h Handler) handle_join_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_join_group_request(mut reader, version, is_flexible_version(.join_group,
		version))!
	resp := h.process_join_group(req, version)!
	return resp.encode(version)
}

// SyncGroup handler
fn (mut h Handler) handle_sync_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_sync_group_request(mut reader, version, is_flexible_version(.sync_group,
		version))!
	resp := h.process_sync_group(req, version)!
	return resp.encode(version)
}

// Heartbeat handler
fn (mut h Handler) handle_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_heartbeat_request(mut reader, version, is_flexible_version(.heartbeat,
		version))!
	resp := h.process_heartbeat(req, version)!
	return resp.encode(version)
}

// LeaveGroup handler
fn (mut h Handler) handle_leave_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_leave_group_request(mut reader, version, is_flexible_version(.leave_group,
		version))!
	resp := h.process_leave_group(req, version)!
	return resp.encode(version)
}

// ConsumerGroupHeartbeat handler (KIP-848)
fn (mut h Handler) handle_consumer_group_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_consumer_group_heartbeat_request(mut reader, version, true)!

	resp := h.process_consumer_group_heartbeat(req, version)!
	return resp.encode(version)
}

// ============================================================================
// Process Functions
// ============================================================================

fn (mut h Handler) process_consumer_group_heartbeat(req ConsumerGroupHeartbeatRequest, version i16) !ConsumerGroupHeartbeatResponse {
	start_time := time.now()

	h.logger.debug('Processing consumer group heartbeat',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id),
		observability.field_int('member_epoch', req.member_epoch))

	mut error_code := i16(0)
	mut error_message := ?string(none)
	mut member_id := req.member_id
	mut member_epoch := req.member_epoch
	mut heartbeat_interval_ms := i32(3000)
	mut assignment := ?ConsumerGroupHeartbeatAssignment(none)

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

	if req.member_epoch == 0 && req.member_id.len == 0 {
		member_id = 'member-${h.broker_id}-${rand.i64()}'
		member_epoch = 1

		h.logger.info('New consumer group member joined',
			observability.field_string('group_id', req.group_id),
			observability.field_string('member_id', member_id))

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
		h.logger.info('Consumer leaving group',
			observability.field_string('group_id', req.group_id),
			observability.field_string('member_id', req.member_id))
		member_epoch = -1
		assignment = none
	} else if req.member_epoch > 0 {
		// Regular heartbeat - no change
		h.logger.trace('Consumer heartbeat',
			observability.field_string('group_id', req.group_id),
			observability.field_string('member_id', req.member_id),
			observability.field_int('epoch', req.member_epoch))
	} else {
		h.logger.warn('Invalid consumer heartbeat',
			observability.field_string('group_id', req.group_id),
			observability.field_string('member_id', req.member_id),
			observability.field_int('epoch', req.member_epoch))
		error_code = i16(ErrorCode.unknown_member_id)
		error_message = 'Invalid member epoch'
		member_epoch = -1
	}

	elapsed := time.since(start_time)
	h.logger.debug('Consumer group heartbeat completed',
		observability.field_string('group_id', req.group_id),
		observability.field_int('error_code', error_code),
		observability.field_duration('latency', elapsed))

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

fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
	start_time := time.now()

	h.logger.debug('Processing join group request',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id),
		observability.field_string('protocol_type', req.protocol_type),
		observability.field_int('protocols', req.protocols.len))

	// Member ID required for subsequent joins
	if req.member_id.len == 0 && req.group_instance_id == none {
		new_member_id := 'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
		h.logger.info('Member ID required, generated new ID',
			observability.field_string('group_id', req.group_id),
			observability.field_string('new_member_id', new_member_id))
		return JoinGroupResponse{
			throttle_time_ms: 0
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

	member_id := if req.member_id.len > 0 {
		req.member_id
	} else {
		'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
	}

	protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }

	// Load or create group
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

	// Create/update member
	member := domain.GroupMember{
		member_id:         member_id
		group_instance_id: req.group_instance_id or { '' }
		client_id:         ''
		client_host:       ''
		metadata:          if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
		assignment:        []u8{}
	}

	// Update member list
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

	new_gen := group.generation_id + 1
	leader := if new_members.len > 0 { new_members[0].member_id } else { member_id }

	new_group := domain.ConsumerGroup{
		group_id:      req.group_id
		generation_id: new_gen
		protocol_type: req.protocol_type
		protocol:      protocol_name
		state:         .stable
		members:       new_members
		leader:        leader
	}

	h.storage.save_group(new_group) or {
		h.logger.error('Failed to save group',
			observability.field_string('group_id', req.group_id),
			observability.field_string('error', err.str()))
		return error('failed to save group: ${err}')
	}

	elapsed := time.since(start_time)
	h.logger.info('Member joined group',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', member_id),
		observability.field_int('generation', new_gen),
		observability.field_string('leader', leader),
		observability.field_int('members', new_members.len),
		observability.field_duration('latency', elapsed))

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
		throttle_time_ms: 0
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

fn (mut h Handler) process_sync_group(req SyncGroupRequest, version i16) !SyncGroupResponse {
	start_time := time.now()

	h.logger.debug('Processing sync group request',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id),
		observability.field_int('generation', req.generation_id),
		observability.field_int('assignments', req.assignments.len))

	mut group := h.storage.load_group(req.group_id) or {
		h.logger.warn('Sync group failed: group not found',
			observability.field_string('group_id', req.group_id))
		return SyncGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
			protocol_type:    ''
			protocol_name:    ''
			assignment:       []u8{}
		}
	}

	if group.generation_id != req.generation_id {
		h.logger.warn('Sync group failed: illegal generation',
			observability.field_string('group_id', req.group_id),
			observability.field_int('expected', group.generation_id),
			observability.field_int('received', req.generation_id))
		return SyncGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.illegal_generation)
			protocol_type:    group.protocol_type
			protocol_name:    group.protocol
			assignment:       []u8{}
		}
	}

	// If leader is sending assignments, update group members
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
			h.logger.error('Failed to save group assignments',
				observability.field_string('group_id', req.group_id),
				observability.field_string('error', err.str()))
			return error('failed to save group: ${err}')
		}
	}

	// Find assignment for this member
	mut assignment := []u8{}
	for m in group.members {
		if m.member_id == req.member_id {
			assignment = m.assignment.clone()
			break
		}
	}

	// If no assignment found, use empty consumer assignment
	if assignment.len == 0 {
		assignment = empty_consumer_assignment()
	}

	elapsed := time.since(start_time)
	h.logger.debug('Sync group completed',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id),
		observability.field_bytes('assignment_size', assignment.len),
		observability.field_duration('latency', elapsed))

	return SyncGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		protocol_type:    group.protocol_type
		protocol_name:    group.protocol
		assignment:       assignment
	}
}

fn (mut h Handler) process_heartbeat(req HeartbeatRequest, version i16) !HeartbeatResponse {
	h.logger.trace('Processing heartbeat',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id),
		observability.field_int('generation', req.generation_id))

	// Verify group exists and generation matches
	group := h.storage.load_group(req.group_id) or {
		h.logger.debug('Heartbeat failed: group not found',
			observability.field_string('group_id', req.group_id))
		return HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
		}
	}

	if group.generation_id != req.generation_id {
		h.logger.debug('Heartbeat failed: illegal generation',
			observability.field_string('group_id', req.group_id),
			observability.field_int('expected', group.generation_id),
			observability.field_int('received', req.generation_id))
		return HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.illegal_generation)
		}
	}

	// Verify member exists in group
	mut member_found := false
	for m in group.members {
		if m.member_id == req.member_id {
			member_found = true
			break
		}
	}

	if !member_found {
		h.logger.debug('Heartbeat failed: unknown member',
			observability.field_string('group_id', req.group_id),
			observability.field_string('member_id', req.member_id))
		return HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.unknown_member_id)
		}
	}

	return HeartbeatResponse{
		throttle_time_ms: 0
		error_code:       0
	}
}

fn (mut h Handler) process_leave_group(req LeaveGroupRequest, version i16) !LeaveGroupResponse {
	start_time := time.now()

	h.logger.debug('Processing leave group request',
		observability.field_string('group_id', req.group_id),
		observability.field_string('member_id', req.member_id),
		observability.field_int('members', req.members.len))

	mut group := h.storage.load_group(req.group_id) or {
		h.logger.warn('Leave group failed: group not found',
			observability.field_string('group_id', req.group_id))
		return LeaveGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
			members:          []LeaveGroupResponseMember{}
		}
	}

	// Remove members from group
	mut removed_members := []LeaveGroupResponseMember{}
	mut remaining_members := []domain.GroupMember{}

	for m in group.members {
		mut should_remove := false

		// Check if member should be removed (v3+ has members array)
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
			// v0-v2: single member_id field
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

	// If no members were removed, return unknown_member_id error
	if removed_members.len == 0 {
		h.logger.warn('Leave group failed: no members removed',
			observability.field_string('group_id', req.group_id))
		return LeaveGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.unknown_member_id)
			members:          []LeaveGroupResponseMember{}
		}
	}

	// Update group state
	new_state := if remaining_members.len == 0 {
		domain.GroupState.empty
	} else {
		domain.GroupState.preparing_rebalance
	}

	new_leader := if remaining_members.len > 0 {
		remaining_members[0].member_id
	} else {
		''
	}

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
		h.logger.error('Leave group failed: failed to save group',
			observability.field_string('group_id', req.group_id),
			observability.field_string('error', err.str()))
		return error('failed to save group: ${err}')
	}

	elapsed := time.since(start_time)
	h.logger.info('Members left group',
		observability.field_string('group_id', req.group_id),
		observability.field_int('removed', removed_members.len),
		observability.field_int('remaining', remaining_members.len),
		observability.field_duration('latency', elapsed))

	return LeaveGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		members:          removed_members
	}
}
