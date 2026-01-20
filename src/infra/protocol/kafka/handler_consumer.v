// Consumer group handlers - JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
module kafka

import domain
import rand

// JoinGroup handler - processes consumer group join requests
fn (mut h Handler) handle_join_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_join_group_request(mut reader, version, is_flexible_version(.join_group,
		version))!

	protocol_name := if req.protocols.len > 0 { req.protocols[0].name } else { '' }

	member_id := if req.member_id.len > 0 {
		req.member_id
	} else {
		'consumer-${h.broker_id}-${rand.uuid_v4()}'
	}

	if req.member_id.len == 0 && req.group_instance_id == none {
		resp := JoinGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.member_id_required)
			generation_id:    -1
			protocol_type:    req.protocol_type
			protocol_name:    ''
			leader:           ''
			skip_assignment:  false
			member_id:        member_id
			members:          []
		}
		return resp.encode(version)
	}

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

	member := domain.GroupMember{
		member_id:         member_id
		group_instance_id: req.group_instance_id or { '' }
		client_id:         ''
		client_host:       ''
		metadata:          if req.protocols.len > 0 { req.protocols[0].metadata } else { []u8{} }
		assignment:        []u8{}
	}

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

	h.storage.save_group(new_group) or { return error('failed to save group: ${err}') }

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

	resp := JoinGroupResponse{
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

	return resp.encode(version)
}

// SyncGroup handler
fn (mut h Handler) handle_sync_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_sync_group_request(mut reader, version, is_flexible_version(.sync_group,
		version))!

	mut group := h.storage.load_group(req.group_id) or {
		resp := SyncGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
			protocol_type:    ''
			protocol_name:    ''
			assignment:       []u8{}
		}
		return resp.encode(version)
	}

	if group.generation_id != req.generation_id {
		resp := SyncGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.illegal_generation)
			protocol_type:    group.protocol_type
			protocol_name:    group.protocol
			assignment:       []u8{}
		}
		return resp.encode(version)
	}

	if req.assignments.len > 0 {
		mut updated_members := []domain.GroupMember{}
		for m in group.members {
			mut member := m
			for a in req.assignments {
				if a.member_id == m.member_id {
					member = domain.GroupMember{
						...m
						assignment: a.assignment.clone()
					}
					break
				}
			}
			updated_members << member
		}

		group = domain.ConsumerGroup{
			...group
			members: updated_members
			state:   .stable
		}

		h.storage.save_group(group) or { return error('failed to save group: ${err}') }
	}

	mut assignment := empty_consumer_assignment()
	for m in group.members {
		if m.member_id == req.member_id {
			if m.assignment.len > 0 {
				assignment = m.assignment.clone()
			}
			break
		}
	}

	resp := SyncGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		protocol_type:    group.protocol_type
		protocol_name:    group.protocol
		assignment:       assignment
	}

	return resp.encode(version)
}

// Heartbeat handler
fn (mut h Handler) handle_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_heartbeat_request(mut reader, version, is_flexible_version(.heartbeat,
		version))!

	group := h.storage.load_group(req.group_id) or {
		resp := HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
		}
		return resp.encode(version)
	}

	if group.generation_id != req.generation_id {
		resp := HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.illegal_generation)
		}
		return resp.encode(version)
	}

	mut member_found := false
	for m in group.members {
		if m.member_id == req.member_id {
			member_found = true
			break
		}
	}

	if !member_found {
		resp := HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.unknown_member_id)
		}
		return resp.encode(version)
	}

	if group.state == .preparing_rebalance {
		resp := HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.rebalance_in_progress)
		}
		return resp.encode(version)
	}

	resp := HeartbeatResponse{
		throttle_time_ms: 0
		error_code:       0
	}

	return resp.encode(version)
}

// LeaveGroup handler
fn (mut h Handler) handle_leave_group(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_leave_group_request(mut reader, version, is_flexible_version(.leave_group,
		version))!

	mut group := h.storage.load_group(req.group_id) or {
		resp := LeaveGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
			members:          []LeaveGroupResponseMember{}
		}
		return resp.encode(version)
	}

	mut new_members := []domain.GroupMember{}
	mut found := false
	for m in group.members {
		if m.member_id != req.member_id {
			new_members << m
		} else {
			found = true
		}
	}

	if !found {
		resp := LeaveGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.unknown_member_id)
			members:          []LeaveGroupResponseMember{}
		}
		return resp.encode(version)
	}

	new_state := if new_members.len == 0 {
		domain.GroupState.empty
	} else {
		domain.GroupState.preparing_rebalance
	}

	new_group := domain.ConsumerGroup{
		...group
		members: new_members
		state:   new_state
	}

	h.storage.save_group(new_group) or { return error('failed to save group: ${err}') }

	resp := LeaveGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		members:          []LeaveGroupResponseMember{}
	}

	return resp.encode(version)
}

// ConsumerGroupHeartbeat handler (KIP-848)
fn (mut h Handler) handle_consumer_group_heartbeat(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_consumer_group_heartbeat_request(mut reader, version, true)!

	resp := h.process_consumer_group_heartbeat(req, version)!
	return resp.encode(version)
}

fn (mut h Handler) process_consumer_group_heartbeat(req ConsumerGroupHeartbeatRequest, version i16) !ConsumerGroupHeartbeatResponse {
	mut error_code := i16(0)
	mut error_message := ?string(none)
	mut member_id := req.member_id
	mut member_epoch := req.member_epoch
	mut heartbeat_interval_ms := i32(3000)
	mut assignment := ?ConsumerGroupHeartbeatAssignment(none)

	if req.group_id.len == 0 {
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
		member_epoch = -1
		assignment = none
	} else if req.member_epoch > 0 {
		// Regular heartbeat - no change
	} else {
		error_code = i16(ErrorCode.unknown_member_id)
		error_message = 'Invalid member epoch'
		member_epoch = -1
	}

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

// Process functions (Frame-based stubs)
fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
	if req.member_id.len == 0 && req.group_instance_id == none {
		return JoinGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.member_id_required)
			generation_id:    -1
			protocol_type:    req.protocol_type
			protocol_name:    ''
			leader:           ''
			skip_assignment:  false
			member_id:        'member-${h.broker_id}-1'
			members:          []
		}
	}

	return JoinGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		generation_id:    1
		protocol_type:    req.protocol_type
		protocol_name:    if req.protocols.len > 0 { req.protocols[0].name } else { '' }
		leader:           req.member_id
		skip_assignment:  false
		member_id:        req.member_id
		members:          if req.member_id.len > 0 && req.protocols.len > 0 {
			[
				JoinGroupResponseMember{
					member_id:         req.member_id
					group_instance_id: req.group_instance_id
					metadata:          req.protocols[0].metadata
				},
			]
		} else {
			[]
		}
	}
}

fn (mut h Handler) process_sync_group(req SyncGroupRequest, version i16) !SyncGroupResponse {
	mut assignment := empty_consumer_assignment()
	for a in req.assignments {
		if a.member_id == req.member_id {
			assignment = a.assignment.clone()
			break
		}
	}
	return SyncGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		protocol_type:    req.protocol_type
		protocol_name:    req.protocol_name
		assignment:       assignment
	}
}

fn (mut h Handler) process_heartbeat(req HeartbeatRequest, version i16) !HeartbeatResponse {
	return HeartbeatResponse{
		throttle_time_ms: 0
		error_code:       0
	}
}

fn (mut h Handler) process_leave_group(req LeaveGroupRequest, version i16) !LeaveGroupResponse {
	return LeaveGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		members:          []LeaveGroupResponseMember{}
	}
}
