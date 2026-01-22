// Kafka 프로토콜 - Group 작업
// ListGroups, DescribeGroups
// 요청/응답 타입, 파싱, 인코딩 및 핸들러
module kafka

import infra.observability
import time

pub struct ListGroupsRequest {
pub:
	states_filter []string
}

fn parse_list_groups_request(mut reader BinaryReader, version i16, is_flexible bool) !ListGroupsRequest {
	mut states_filter := []string{}
	if version >= 4 {
		count := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. count {
			states_filter << reader.read_flex_string(is_flexible)!
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
	count := reader.read_flex_array_len(is_flexible)!
	mut groups := []string{}
	for _ in 0 .. count {
		groups << reader.read_flex_string(is_flexible)!
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

// ListGroups 핸들러 - 컨슈머 그룹 목록 조회
fn (mut h Handler) handle_list_groups(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_list_groups_request(mut reader, version, is_flexible_version(.list_groups,
		version))!

	h.logger.debug('Processing list groups request', observability.field_int('states_filter',
		req.states_filter.len))

	// 스토리지에서 그룹 조회
	groups_info := h.storage.list_groups() or {
		h.logger.error('List groups failed', observability.field_string('error', err.str()))
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

	elapsed := time.since(start_time)
	h.logger.debug('List groups completed', observability.field_int('groups', groups.len),
		observability.field_duration('latency', elapsed))

	resp := ListGroupsResponse{
		throttle_time_ms: 0
		error_code:       0
		groups:           groups
	}

	return resp.encode(version)
}

// DescribeGroups 핸들러 - 컨슈머 그룹 상세 조회
fn (mut h Handler) handle_describe_groups(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_describe_groups_request(mut reader, version, is_flexible_version(.describe_groups,
		version))!

	h.logger.debug('Processing describe groups request', observability.field_int('groups',
		req.groups.len))

	mut groups := []DescribeGroupsResponseGroup{}
	mut found_count := 0
	mut not_found_count := 0

	for group_id in req.groups {
		group := h.storage.load_group(group_id) or {
			// 그룹을 찾을 수 없음
			h.logger.trace('Group not found', observability.field_string('group_id', group_id))
			not_found_count += 1
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

		found_count += 1

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

		h.logger.trace('Describing group', observability.field_string('group_id', group_id),
			observability.field_string('state', state_str), observability.field_int('members',
			response_members.len))

		groups << DescribeGroupsResponseGroup{
			error_code:    0
			group_id:      group_id
			group_state:   state_str
			protocol_type: group.protocol_type
			protocol_data: group.protocol
			members:       response_members
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Describe groups completed', observability.field_int('found', found_count),
		observability.field_int('not_found', not_found_count), observability.field_duration('latency',
		elapsed))

	resp := DescribeGroupsResponse{
		throttle_time_ms: 0
		groups:           groups
	}

	return resp.encode(version)
}

// ListGroups 요청 처리 (Frame 기반)
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

// DescribeGroups 요청 처리 (Frame 기반)
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
