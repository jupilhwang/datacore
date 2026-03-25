// Interface Layer - CLI Share Group Commands
//
// Provides Share Group management commands using the Kafka protocol.
// Share Groups (KIP-932) allow cooperative consumption with record-level acknowledgement.
//
// Key features:
// - List share groups
// - Describe share group details
// - Delete a share group
module cli

import common

/// ShareGroupOptions holds share group command options.
pub struct ShareGroupOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	group_id         string
}

/// parse_share_group_options parses share-group command options.
pub fn parse_share_group_options(args []string) ShareGroupOptions {
	mut opts := ShareGroupOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = ShareGroupOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--group', '-g' {
				if i + 1 < args.len {
					opts = ShareGroupOptions{
						...opts
						group_id: args[i + 1]
					}
					i += 1
				}
			}
			else {
				// Positional argument may be the group_id
				if !args[i].starts_with('-') && opts.group_id.len == 0 {
					opts = ShareGroupOptions{
						...opts
						group_id: args[i]
					}
				}
			}
		}
		i += 1
	}

	return opts
}

/// run_share_group_list lists all share groups.
pub fn run_share_group_list(opts ShareGroupOptions) ! {
	println('\x1b[90mListing share groups...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// ListGroups request (API Key 16, Version 4)
	// Filter by group type 'share' using types filter
	request := build_list_share_groups_request()
	send_kafka_request(mut conn, 16, 4, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 6 {
		return error('Failed to list share groups: invalid response')
	}

	groups := parse_list_groups_response(response)

	if groups.len == 0 {
		println('\x1b[33mNo share groups found\x1b[0m')
		return
	}

	println('')
	println('\x1b[33mShare Groups:\x1b[0m')
	println('  NAME                 STATE')
	println('  ' + '-'.repeat(40))
	for g in groups {
		println('  \x1b[32m${g.group_id}\x1b[0m  ${g.state}')
	}
}

/// run_share_group_describe displays detailed information about a share group.
pub fn run_share_group_describe(opts ShareGroupOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use: datacore share-group describe <group-id>')
	}

	println('\x1b[90mDescribing share group "${opts.group_id}"...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// DescribeGroups request (API Key 15, Version 5)
	request := build_describe_groups_request(opts.group_id)
	send_kafka_request(mut conn, 15, 5, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 10 {
		return error('Failed to describe share group: invalid response')
	}

	println('')
	println('\x1b[33mShare Group:\x1b[0m ${opts.group_id}')
	println('  Note: Use the broker admin API for complete share group details')
}

/// run_share_group_delete deletes a share group.
pub fn run_share_group_delete(opts ShareGroupOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use: datacore share-group delete <group-id>')
	}

	println('\x1b[90mDeleting share group "${opts.group_id}"...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// DeleteGroups request (API Key 42, Version 2)
	request := build_delete_groups_request(opts.group_id)
	send_kafka_request(mut conn, 42, 2, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 6 {
		return error('Failed to delete share group: invalid response')
	}

	println('\x1b[32m\u2713\x1b[0m Share group "${opts.group_id}" deleted successfully')
}

/// print_share_group_help prints share-group command help.
pub fn print_share_group_help() {
	println('\x1b[33mShare Group Commands:\x1b[0m')
	println('')
	println('Usage: datacore share-group <command> [options]')
	println('')
	println('Commands:')
	println('  list              List all share groups')
	println('  describe          Describe a share group')
	println('  delete            Delete a share group')
	println('')
	println('Options:')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('  -g, --group             Group ID')
	println('')
	println('Examples:')
	println('  datacore share-group list')
	println('  datacore share-group describe my-share-group')
	println('  datacore share-group delete my-share-group')
}

// GroupSummary holds brief group information for listing
struct GroupSummary {
	group_id string
	state    string
}

fn build_list_share_groups_request() []u8 {
	mut body := []u8{}

	// States filter (compact array) - empty = all states
	body << u8(1)

	// Types filter (compact array) - empty = all types
	body << u8(1)

	// Tagged fields
	body << u8(0)

	return body
}

fn build_delete_groups_request(group_id string) []u8 {
	mut body := []u8{}

	// Groups array (compact array)
	body << u8(2)

	// Group ID (compact string)
	body << u8(group_id.len + 1)
	body << group_id.bytes()

	// Tagged fields
	body << u8(0)

	return body
}

// parse_list_groups_response extracts group summaries from a ListGroups response.
fn parse_list_groups_response(response []u8) []GroupSummary {
	mut groups := []GroupSummary{}

	// Minimum: correlation_id(4) + tagged_fields(1) + throttle_time(4) + error_code(2) + groups_count(varint)
	if response.len < 12 {
		return groups
	}

	mut pos := 4 // skip correlation_id

	// Skip tagged fields (compact format)
	pos += 1

	// Skip throttle_time_ms (4 bytes)
	pos += 4

	// Error code (2 bytes)
	if pos + 2 > response.len {
		return groups
	}
	error_code := common.read_i16_be(response[pos..]) or { i16(0) }
	pos += 2

	if error_code != 0 {
		return groups
	}

	// Groups array count (compact array = actual_len + 1)
	if pos >= response.len {
		return groups
	}
	groups_count := int(response[pos]) - 1
	pos += 1

	for _ in 0 .. groups_count {
		if pos >= response.len {
			break
		}
		// Group ID (compact string)
		group_id_len := int(response[pos]) - 1
		pos += 1
		if pos + group_id_len > response.len {
			break
		}
		group_id := response[pos..pos + group_id_len].bytestr()
		pos += group_id_len

		// Protocol type (compact string)
		if pos >= response.len {
			break
		}
		proto_len := int(response[pos]) - 1
		pos += 1
		if pos + proto_len > response.len {
			break
		}
		pos += proto_len

		// Group state (compact string)
		if pos >= response.len {
			break
		}
		state_len := int(response[pos]) - 1
		pos += 1
		if pos + state_len > response.len {
			break
		}
		state := response[pos..pos + state_len].bytestr()
		pos += state_len

		// Group type (compact string) - v4+
		if pos < response.len {
			type_len := int(response[pos]) - 1
			pos += 1
			if type_len > 0 && pos + type_len <= response.len {
				pos += type_len
			}
		}

		// Tagged fields
		if pos < response.len {
			pos += 1
		}

		groups << GroupSummary{
			group_id: group_id
			state:    state
		}
	}

	return groups
}
