// Interface Layer - CLI Group Commands
//
// Provides consumer group management commands (simplified implementation).
// Supports listing groups and retrieving detailed group information.
//
// Key features:
// - List consumer groups
// - Describe consumer group details
module cli

import net as _
import time as _

/// GroupOptions is a struct holding group command options.
pub struct GroupOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	group_id         string
}

/// parse_group_options parses group command options.
pub fn parse_group_options(args []string) GroupOptions {
	mut opts := GroupOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--group', '-g' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						group_id: args[i + 1]
					}
					i += 1
				}
			}
			else {
				// Positional argument may be the group_id
				if !args[i].starts_with('-') && opts.group_id.len == 0 {
					opts = GroupOptions{
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

/// run_group_list lists all consumer groups.
pub fn run_group_list(opts GroupOptions) ! {
	println('\x1b[90m⏳ Listing consumer groups...\x1b[0m')

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build ListGroups request (API Key 16, Version 4)
	request := build_list_groups_request()

	// Send request
	send_kafka_request(mut conn, 16, 4, request)!

	// Read response
	response := read_kafka_response(mut conn)!

	// Simplified parsing - only validate response
	if response.len < 10 {
		return error('Failed to list groups: Invalid response')
	}

	println('\x1b[33mConsumer Groups:\x1b[0m')
	println('  (Note: Full parsing not yet implemented)')
	println('  Use kafka-consumer-groups.sh for complete information')
}

/// run_group_describe displays detailed information about a consumer group.
pub fn run_group_describe(opts GroupOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use --group <group-id>')
	}

	println('\x1b[90m⏳ Describing group "${opts.group_id}"...\x1b[0m')

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build DescribeGroups request (API Key 15, Version 5)
	request := build_describe_groups_request(opts.group_id)

	// Send request
	send_kafka_request(mut conn, 15, 5, request)!

	// Read response
	response := read_kafka_response(mut conn)!

	// Simplified parsing - only validate response
	if response.len < 10 {
		return error('Failed to describe group: Invalid response')
	}

	println('\x1b[33mGroup:\x1b[0m ${opts.group_id}')
	println('  (Note: Full parsing not yet implemented)')
	println('  Use kafka-consumer-groups.sh for complete information')
}

fn build_list_groups_request() []u8 {
	mut body := []u8{}

	// Groups filter (empty array = all groups)
	body << u8(1)

	// Tagged fields (empty)
	body << u8(0)

	return body
}

// build_describe_groups_request builds a DescribeGroups request.
fn build_describe_groups_request(group_id string) []u8 {
	mut body := []u8{}

	// Groups array (compact array)
	body << u8(2)

	// Group ID (compact string)
	body << u8(group_id.len + 1)
	body << group_id.bytes()

	// Include authorized operations (1 byte)
	body << u8(0)

	// Tagged fields (empty)
	body << u8(0)

	return body
}
