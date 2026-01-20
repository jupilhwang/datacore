// Kafka Protocol - Group Operations
// ListGroups, DescribeGroups
// Request/Response types, parsing, encoding, and handlers
module kafka

pub struct ListGroupsRequest {
pub:
	states_filter []string
}

fn parse_list_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !ListGroupsRequest {
	mut states_filter := []string{}
	if version >= 4 {
		count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		for _ in 0 .. count {
			states_filter << if is_flexible {
				reader.read_compact_string()!
			} else {
				reader.read_string()!
			}
		}
	}
	return ListGroupsRequest{
		states_filter: states_filter
	}
}

pub struct DescribeGroupsRequest {
pub:
	groups                        []string
	include_authorized_operations bool
}

fn parse_describe_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeGroupsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut groups := []string{}
	for _ in 0 .. count {
		groups << if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}
	mut include_authorized_operations := false
	if version >= 3 {
		include_authorized_operations = reader.read_i8()! != 0
	}
	return DescribeGroupsRequest{
		groups:                        groups
		include_authorized_operations: include_authorized_operations
	}
}

// ============================================================================
// ListGroups Response (API Key 16)
// ============================================================================

pub struct ListGroupsResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	groups           []ListGroupsResponseGroup
}

pub struct ListGroupsResponseGroup {
pub:
	group_id      string
	protocol_type string
	group_state   string
}

pub fn (r ListGroupsResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	writer.write_i16(r.error_code)

	if is_flexible {
		writer.write_compact_array_len(r.groups.len)
	} else {
		writer.write_array_len(r.groups.len)
	}

	for g in r.groups {
		if is_flexible {
			writer.write_compact_string(g.group_id)
			writer.write_compact_string(g.protocol_type)
		} else {
			writer.write_string(g.group_id)
			writer.write_string(g.protocol_type)
		}
		if version >= 4 {
			if is_flexible {
				writer.write_compact_string(g.group_state)
			} else {
				writer.write_string(g.group_state)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// DescribeGroups Response (API Key 15)
// ============================================================================

pub struct DescribeGroupsResponse {
pub:
	throttle_time_ms i32
	groups           []DescribeGroupsResponseGroup
}

pub struct DescribeGroupsResponseGroup {
pub:
	error_code    i16
	group_id      string
	group_state   string
	protocol_type string
	protocol_data string
	members       []DescribeGroupsResponseMember
}

pub struct DescribeGroupsResponseMember {
pub:
	member_id         string
	client_id         string
	client_host       string
	member_metadata   []u8
	member_assignment []u8
}

pub fn (r DescribeGroupsResponse) encode(version i16) []u8 {
	is_flexible := version >= 5
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.groups.len)
	} else {
		writer.write_array_len(r.groups.len)
	}

	for g in r.groups {
		writer.write_i16(g.error_code)
		if is_flexible {
			writer.write_compact_string(g.group_id)
			writer.write_compact_string(g.group_state)
			writer.write_compact_string(g.protocol_type)
			writer.write_compact_string(g.protocol_data)
			writer.write_compact_array_len(g.members.len)
		} else {
			writer.write_string(g.group_id)
			writer.write_string(g.group_state)
			writer.write_string(g.protocol_type)
			writer.write_string(g.protocol_data)
			writer.write_array_len(g.members.len)
		}

		for m in g.members {
			if is_flexible {
				writer.write_compact_string(m.member_id)
				writer.write_compact_string(m.client_id)
				writer.write_compact_string(m.client_host)
				writer.write_compact_bytes(m.member_metadata)
				writer.write_compact_bytes(m.member_assignment)
				writer.write_tagged_fields()
			} else {
				writer.write_string(m.member_id)
				writer.write_string(m.client_id)
				writer.write_string(m.client_host)
				writer.write_bytes(m.member_metadata)
				writer.write_bytes(m.member_assignment)
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ListGroups handler - lists consumer groups
fn (mut h Handler) handle_list_groups(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	_ := parse_list_groups_request(mut reader, version, is_flexible_version(.list_groups,
		version))!

	// Get groups from storage
	groups_info := h.storage.list_groups() or {
		resp := ListGroupsResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.unknown_server_error)
			groups:           []
		}
		return resp.encode(version)
	}

	mut groups := []ListGroupsResponseGroup{}
	for g in groups_info {
		groups << ListGroupsResponseGroup{
			group_id:      g.group_id
			protocol_type: g.protocol_type
			group_state:   g.state // Already string from storage
		}
	}

	resp := ListGroupsResponse{
		throttle_time_ms: 0
		error_code:       0
		groups:           groups
	}

	return resp.encode(version)
}

// DescribeGroups handler - describes consumer groups
fn (mut h Handler) handle_describe_groups(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_describe_groups_request(mut reader, version, is_flexible_version(.describe_groups,
		version))!

	mut groups := []DescribeGroupsResponseGroup{}
	for group_id in req.groups {
		group := h.storage.load_group(group_id) or {
			// Group not found
			groups << DescribeGroupsResponseGroup{
				error_code:    i16(ErrorCode.group_id_not_found)
				group_id:      group_id
				group_state:   ''
				protocol_type: ''
				protocol_data: ''
				members:       []
			}
			continue
		}

		// Convert members to response format
		mut response_members := []DescribeGroupsResponseMember{}
		for m in group.members {
			response_members << DescribeGroupsResponseMember{
				member_id:         m.member_id
				client_id:         m.client_id
				client_host:       m.client_host
				member_metadata:   m.metadata
				member_assignment: m.assignment
			}
		}

		state_str := match group.state {
			.empty { 'Empty' }
			.stable { 'Stable' }
			.preparing_rebalance { 'PreparingRebalance' }
			.completing_rebalance { 'CompletingRebalance' }
			.dead { 'Dead' }
		}

		groups << DescribeGroupsResponseGroup{
			error_code:    0
			group_id:      group_id
			group_state:   state_str
			protocol_type: group.protocol_type
			protocol_data: group.protocol
			members:       response_members
		}
	}

	resp := DescribeGroupsResponse{
		throttle_time_ms: 0
		groups:           groups
	}

	return resp.encode(version)
}

// Process ListGroups request (Frame-based)
fn (mut h Handler) process_list_groups(req ListGroupsRequest, version i16) !ListGroupsResponse {
	groups_info := h.storage.list_groups() or {
		return ListGroupsResponse{
			throttle_time_ms: 0
			error_code:       i16(ErrorCode.unknown_server_error)
			groups:           []
		}
	}

	mut groups := []ListGroupsResponseGroup{}
	for g in groups_info {
		groups << ListGroupsResponseGroup{
			group_id:      g.group_id
			protocol_type: g.protocol_type
			group_state:   g.state
		}
	}

	return ListGroupsResponse{
		throttle_time_ms: 0
		error_code:       0
		groups:           groups
	}
}

// Process DescribeGroups request (Frame-based)
fn (mut h Handler) process_describe_groups(req DescribeGroupsRequest, version i16) !DescribeGroupsResponse {
	mut groups := []DescribeGroupsResponseGroup{}

	for group_id in req.groups {
		group := h.storage.load_group(group_id) or {
			groups << DescribeGroupsResponseGroup{
				error_code:    i16(ErrorCode.group_id_not_found)
				group_id:      group_id
				group_state:   ''
				protocol_type: ''
				protocol_data: ''
				members:       []
			}
			continue
		}

		mut response_members := []DescribeGroupsResponseMember{}
		for m in group.members {
			response_members << DescribeGroupsResponseMember{
				member_id:         m.member_id
				client_id:         m.client_id
				client_host:       m.client_host
				member_metadata:   m.metadata
				member_assignment: m.assignment
			}
		}

		state_str := match group.state {
			.empty { 'Empty' }
			.stable { 'Stable' }
			.preparing_rebalance { 'PreparingRebalance' }
			.completing_rebalance { 'CompletingRebalance' }
			.dead { 'Dead' }
		}

		groups << DescribeGroupsResponseGroup{
			error_code:    0
			group_id:      group_id
			group_state:   state_str
			protocol_type: group.protocol_type
			protocol_data: group.protocol
			members:       response_members
		}
	}

	return DescribeGroupsResponse{
		throttle_time_ms: 0
		groups:           groups
	}
}
