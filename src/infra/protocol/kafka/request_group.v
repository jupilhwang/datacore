// Infra Layer - Kafka Request Parsing - Group Operations
// ListGroups, DescribeGroups request parsing
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
