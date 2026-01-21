// Kafka Protocol - Consumer Group Operations
// JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain
import rand

// JoinGroup Request
pub struct JoinGroupRequest {
pub:
	group_id             string
	session_timeout_ms   i32
	rebalance_timeout_ms i32
	member_id            string
	group_instance_id    ?string
	protocol_type        string
	protocols            []JoinGroupRequestProtocol
}

pub struct JoinGroupRequestProtocol {
pub:
	name     string
	metadata []u8
}

fn parse_join_group_request(mut reader BinaryReader, version i16, is_flexible bool) !JoinGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!

	session_timeout_ms := reader.read_i32()!

	mut rebalance_timeout_ms := session_timeout_ms
	if version >= 1 {
		rebalance_timeout_ms = reader.read_i32()!
	}

	member_id := reader.read_flex_string(is_flexible)!

	mut group_instance_id := ?string(none)
	if version >= 5 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}

	protocol_type := reader.read_flex_string(is_flexible)!

	protocol_count := reader.read_flex_array_len(is_flexible)!

	mut protocols := []JoinGroupRequestProtocol{}
	for _ in 0 .. protocol_count {
		name := reader.read_flex_string(is_flexible)!
		metadata := if is_flexible {
			reader.read_compact_bytes()!
		} else {
			reader.read_bytes()!
		}

		protocols << JoinGroupRequestProtocol{
			name:     name
			metadata: metadata
		}

		reader.skip_flex_tagged_fields(is_flexible)!
	}

	return JoinGroupRequest{
		group_id:             group_id
		session_timeout_ms:   session_timeout_ms
		rebalance_timeout_ms: rebalance_timeout_ms
		member_id:            member_id
		group_instance_id:    group_instance_id
		protocol_type:        protocol_type
		protocols:            protocols
	}
}

// SyncGroup Request
pub struct SyncGroupRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
	protocol_type     ?string
	protocol_name     ?string
	assignments       []SyncGroupRequestAssignment
}

pub struct SyncGroupRequestAssignment {
pub:
	member_id  string
	assignment []u8
}

fn parse_sync_group_request(mut reader BinaryReader, version i16, is_flexible bool) !SyncGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!
	generation_id := reader.read_i32()!
	member_id := reader.read_flex_string(is_flexible)!

	mut group_instance_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}

	mut protocol_type := ?string(none)
	mut protocol_name := ?string(none)
	if version >= 5 {
		if is_flexible {
			pt := reader.read_compact_string()!
			pn := reader.read_compact_string()!
			protocol_type = if pt.len > 0 { pt } else { none }
			protocol_name = if pn.len > 0 { pn } else { none }
		} else {
			pt := reader.read_nullable_string()!
			pn := reader.read_nullable_string()!
			protocol_type = if pt.len > 0 { pt } else { none }
			protocol_name = if pn.len > 0 { pn } else { none }
		}
	}

	count := reader.read_flex_array_len(is_flexible)!
	mut assignments := []SyncGroupRequestAssignment{}
	for _ in 0 .. count {
		mid := reader.read_flex_string(is_flexible)!
		assign := if is_flexible { reader.read_compact_bytes()! } else { reader.read_bytes()! }
		assignments << SyncGroupRequestAssignment{
			member_id:  mid
			assignment: assign
		}
		reader.skip_flex_tagged_fields(is_flexible)!
	}

	return SyncGroupRequest{
		group_id:          group_id
		generation_id:     generation_id
		member_id:         member_id
		group_instance_id: group_instance_id
		protocol_type:     protocol_type
		protocol_name:     protocol_name
		assignments:       assignments
	}
}

// Heartbeat Request
pub struct HeartbeatRequest {
pub:
	group_id          string
	generation_id     i32
	member_id         string
	group_instance_id ?string
}

fn parse_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !HeartbeatRequest {
	group_id := reader.read_flex_string(is_flexible)!
	generation_id := reader.read_i32()!
	member_id := reader.read_flex_string(is_flexible)!
	mut group_instance_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			group_instance_id = if str.len > 0 { str } else { none }
		}
	}
	return HeartbeatRequest{
		group_id:          group_id
		generation_id:     generation_id
		member_id:         member_id
		group_instance_id: group_instance_id
	}
}

// LeaveGroup Request
pub struct LeaveGroupRequest {
pub:
	group_id  string
	member_id string             // v0-v2: single member_id
	members   []LeaveGroupMember // v3+: batch member identities
}

// LeaveGroupMember - v3+ member identity for batch leaving
pub struct LeaveGroupMember {
pub:
	member_id         string
	group_instance_id ?string
	reason            ?string // v5+
}

fn parse_leave_group_request(mut reader BinaryReader, version i16, is_flexible bool) !LeaveGroupRequest {
	group_id := reader.read_flex_string(is_flexible)!

	// v0-v2: single member_id
	mut member_id := ''
	mut members := []LeaveGroupMember{}

	if version <= 2 {
		member_id = reader.read_flex_string(is_flexible)!
	} else {
		// v3+: members array
		members_len := if is_flexible {
			int(reader.read_uvarint()! - 1)
		} else {
			int(reader.read_i32()!)
		}

		for _ in 0 .. members_len {
			m_member_id := reader.read_flex_string(is_flexible)!
			raw_group_instance_id := reader.read_flex_nullable_string(is_flexible)!
			m_group_instance_id := if raw_group_instance_id.len > 0 {
				?string(raw_group_instance_id)
			} else {
				?string(none)
			}

			// v5+: reason field
			mut m_reason := ?string(none)
			if version >= 5 {
				raw_reason := reader.read_flex_nullable_string(is_flexible)!
				if raw_reason.len > 0 {
					m_reason = raw_reason
				}
			}

			// Skip tagged fields for each member in flexible version
			reader.skip_flex_tagged_fields(is_flexible)!

			members << LeaveGroupMember{
				member_id:         m_member_id
				group_instance_id: m_group_instance_id
				reason:            m_reason
			}
		}
	}

	// Skip tagged fields for flexible version
	reader.skip_flex_tagged_fields(is_flexible)!

	return LeaveGroupRequest{
		group_id:  group_id
		member_id: member_id
		members:   members
	}
}

// ConsumerGroupHeartbeat Request (API Key 68) - KIP-848
// Used for the new Consumer Rebalance Protocol
pub struct ConsumerGroupHeartbeatRequest {
pub:
	group_id               string
	member_id              string
	member_epoch           i32
	instance_id            ?string // Static membership (group.instance.id)
	rack_id                ?string
	rebalance_timeout_ms   i32
	subscribed_topic_names []string
	server_assignor        ?string
	topic_partitions       []ConsumerGroupHeartbeatTopicPartition // Current assignment
}

pub struct ConsumerGroupHeartbeatTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

fn parse_consumer_group_heartbeat_request(mut reader BinaryReader, version i16, is_flexible bool) !ConsumerGroupHeartbeatRequest {
	// ConsumerGroupHeartbeat is always flexible (v0+)

	// group_id: COMPACT_STRING
	group_id := reader.read_compact_string()!

	// member_id: COMPACT_STRING
	member_id := reader.read_compact_string()!

	// member_epoch: INT32
	member_epoch := reader.read_i32()!

	// instance_id: COMPACT_NULLABLE_STRING
	raw_instance_id := reader.read_compact_nullable_string()!
	instance_id := if raw_instance_id.len > 0 { ?string(raw_instance_id) } else { ?string(none) }

	// rack_id: COMPACT_NULLABLE_STRING
	raw_rack_id := reader.read_compact_nullable_string()!
	rack_id := if raw_rack_id.len > 0 { ?string(raw_rack_id) } else { ?string(none) }

	// rebalance_timeout_ms: INT32
	rebalance_timeout_ms := reader.read_i32()!

	// subscribed_topic_names: COMPACT_ARRAY[COMPACT_STRING]
	topic_count := reader.read_compact_array_len()!
	mut subscribed_topic_names := []string{}
	for _ in 0 .. topic_count {
		subscribed_topic_names << reader.read_compact_string()!
	}

	// server_assignor: COMPACT_NULLABLE_STRING
	raw_server_assignor := reader.read_compact_nullable_string()!
	server_assignor := if raw_server_assignor.len > 0 {
		?string(raw_server_assignor)
	} else {
		?string(none)
	}

	// topic_partitions: COMPACT_ARRAY[TopicPartition]
	tp_count := reader.read_compact_array_len()!
	mut topic_partitions := []ConsumerGroupHeartbeatTopicPartition{}
	for _ in 0 .. tp_count {
		// topic_id: UUID (16 bytes)
		topic_id := reader.read_uuid()!

		// partitions: COMPACT_ARRAY[INT32]
		part_count := reader.read_compact_array_len()!
		mut partitions := []i32{}
		for _ in 0 .. part_count {
			partitions << reader.read_i32()!
		}

		topic_partitions << ConsumerGroupHeartbeatTopicPartition{
			topic_id:   topic_id
			partitions: partitions
		}

		// Skip tagged fields for each topic partition
		reader.skip_tagged_fields()!
	}

	return ConsumerGroupHeartbeatRequest{
		group_id:               group_id
		member_id:              member_id
		member_epoch:           member_epoch
		instance_id:            instance_id
		rack_id:                rack_id
		rebalance_timeout_ms:   rebalance_timeout_ms
		subscribed_topic_names: subscribed_topic_names
		server_assignor:        server_assignor
		topic_partitions:       topic_partitions
	}
}

// ============================================================================
// JoinGroup Response (API Key 11)
// ============================================================================

pub struct JoinGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	generation_id    i32
	protocol_type    ?string
	protocol_name    ?string
	leader           string
	skip_assignment  bool
	member_id        string
	members          []JoinGroupResponseMember
}

pub struct JoinGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string // v5+
	metadata          []u8
}

pub fn (r JoinGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)
	writer.write_i32(r.generation_id)

	if version >= 7 {
		if is_flexible {
			writer.write_compact_nullable_string(r.protocol_type)
			writer.write_compact_nullable_string(r.protocol_name)
		} else {
			writer.write_nullable_string(r.protocol_type)
			writer.write_nullable_string(r.protocol_name)
		}
	} else {
		if is_flexible {
			writer.write_compact_string(r.protocol_name or { '' })
		} else {
			writer.write_string(r.protocol_name or { '' })
		}
	}

	if is_flexible {
		writer.write_compact_string(r.leader)
		if version >= 9 {
			writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
		}
		writer.write_compact_string(r.member_id)
		writer.write_compact_array_len(r.members.len)
	} else {
		writer.write_string(r.leader)
		if version >= 9 {
			writer.write_i8(if r.skip_assignment { i8(1) } else { i8(0) })
		}
		writer.write_string(r.member_id)
		writer.write_array_len(r.members.len)
	}

	for m in r.members {
		if is_flexible {
			writer.write_compact_string(m.member_id)
			// v5+: group_instance_id
			if version >= 5 {
				writer.write_compact_nullable_string(m.group_instance_id)
			}
			writer.write_compact_bytes(m.metadata)
			writer.write_tagged_fields()
		} else {
			writer.write_string(m.member_id)
			// v5+: group_instance_id
			if version >= 5 {
				writer.write_nullable_string(m.group_instance_id)
			}
			writer.write_bytes(m.metadata)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// SyncGroup Response (API Key 14)
// ============================================================================

pub struct SyncGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	protocol_type    ?string
	protocol_name    ?string
	assignment       []u8
}

pub fn (r SyncGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if version >= 5 {
		if is_flexible {
			writer.write_compact_nullable_string(r.protocol_type)
			writer.write_compact_nullable_string(r.protocol_name)
		} else {
			writer.write_nullable_string(r.protocol_type)
			writer.write_nullable_string(r.protocol_name)
		}
	}

	if is_flexible {
		writer.write_compact_bytes(r.assignment)
		writer.write_tagged_fields()
	} else {
		writer.write_bytes(r.assignment)
	}

	return writer.bytes()
}

// ============================================================================
// Heartbeat Response (API Key 12)
// ============================================================================

pub struct HeartbeatResponse {
pub:
	throttle_time_ms i32
	error_code       i16
}

pub fn (r HeartbeatResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// LeaveGroup Response (API Key 13)
// ============================================================================

pub struct LeaveGroupResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	members          []LeaveGroupResponseMember
}

pub struct LeaveGroupResponseMember {
pub:
	member_id         string
	group_instance_id ?string
	error_code        i16
}

pub fn (r LeaveGroupResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if version >= 3 {
		if is_flexible {
			writer.write_compact_array_len(r.members.len)
		} else {
			writer.write_array_len(r.members.len)
		}
		for m in r.members {
			if is_flexible {
				writer.write_compact_string(m.member_id)
				writer.write_compact_nullable_string(m.group_instance_id)
			} else {
				writer.write_string(m.member_id)
				writer.write_nullable_string(m.group_instance_id)
			}
			writer.write_i16(m.error_code)
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// ConsumerGroupHeartbeat Response (API Key 68) - KIP-848
// ============================================================================
// Response for the new Consumer Rebalance Protocol

pub struct ConsumerGroupHeartbeatResponse {
pub:
	throttle_time_ms      i32
	error_code            i16
	error_message         ?string
	member_id             ?string
	member_epoch          i32
	heartbeat_interval_ms i32
	assignment            ?ConsumerGroupHeartbeatAssignment
}

pub struct ConsumerGroupHeartbeatAssignment {
pub:
	topic_partitions []ConsumerGroupHeartbeatResponseTopicPartition
}

pub struct ConsumerGroupHeartbeatResponseTopicPartition {
pub:
	topic_id   []u8 // UUID (16 bytes)
	partitions []i32
}

pub fn (r ConsumerGroupHeartbeatResponse) encode(version i16) []u8 {
	// ConsumerGroupHeartbeat is always flexible (v0+)
	mut writer := new_writer()

	// throttle_time_ms: INT32
	writer.write_i32(r.throttle_time_ms)

	// error_code: INT16
	writer.write_i16(r.error_code)

	// error_message: COMPACT_NULLABLE_STRING
	writer.write_compact_nullable_string(r.error_message)

	// member_id: COMPACT_NULLABLE_STRING
	writer.write_compact_nullable_string(r.member_id)

	// member_epoch: INT32
	writer.write_i32(r.member_epoch)

	// heartbeat_interval_ms: INT32
	writer.write_i32(r.heartbeat_interval_ms)

	// assignment: Assignment (nullable)
	if assignment := r.assignment {
		// Write topic_partitions array
		writer.write_compact_array_len(assignment.topic_partitions.len)

		for tp in assignment.topic_partitions {
			// topic_id: UUID (16 bytes)
			writer.write_uuid(tp.topic_id)

			// partitions: COMPACT_ARRAY[INT32]
			writer.write_compact_array_len(tp.partitions.len)
			for p in tp.partitions {
				writer.write_i32(p)
			}

			// Tagged fields for each topic partition
			writer.write_tagged_fields()
		}

		// Tagged fields for assignment
		writer.write_tagged_fields()
	} else {
		// Write -1 to indicate null assignment
		// For compact nullable structs, we use 0 to indicate null (length = 0 - 1 = -1)
		writer.write_uvarint(0)
	}

	// Tagged fields at the end
	writer.write_tagged_fields()

	return writer.bytes()
}

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

// Process functions (Frame-based)
fn (mut h Handler) process_join_group(req JoinGroupRequest, version i16) !JoinGroupResponse {
	// Member ID required for subsequent joins
	if req.member_id.len == 0 && req.group_instance_id == none {
		return JoinGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.member_id_required)
			generation_id:    -1
			protocol_type:    req.protocol_type
			protocol_name:    ''
			leader:           ''
			skip_assignment:  false
			member_id:        'member-${h.broker_id}-${rand.i64n(1000000) or { 0 }}'
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
	mut group := h.storage.load_group(req.group_id) or {
		return SyncGroupResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
			protocol_type:    ''
			protocol_name:    ''
			assignment:       []u8{}
		}
	}

	if group.generation_id != req.generation_id {
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
		h.storage.save_group(group) or { return error('failed to save group: ${err}') }
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

	return SyncGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		protocol_type:    group.protocol_type
		protocol_name:    group.protocol
		assignment:       assignment
	}
}

fn (mut h Handler) process_heartbeat(req HeartbeatRequest, version i16) !HeartbeatResponse {
	// Verify group exists and generation matches
	group := h.storage.load_group(req.group_id) or {
		return HeartbeatResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.group_id_not_found)
		}
	}

	if group.generation_id != req.generation_id {
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
	mut group := h.storage.load_group(req.group_id) or {
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
		eprintln('[ERROR] LeaveGroup: failed to save group ${req.group_id}: ${err}')
		return error('failed to save group: ${err}')
	}

	return LeaveGroupResponse{
		throttle_time_ms: 0
		error_code:       0
		members:          removed_members
	}
}
