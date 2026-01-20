// Adapter Layer - Kafka Group Response Building
// ListGroups, DescribeGroups responses
module kafka

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
