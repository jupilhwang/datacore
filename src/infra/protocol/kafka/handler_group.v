// Infra Layer - Kafka Protocol Handler - Group Operations
// ListGroups, DescribeGroups handlers
module kafka

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
