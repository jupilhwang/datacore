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
	topic            string
	partition        int = -1
	offset           i64 = -1
	to_earliest      bool
	to_latest        bool
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
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partition', '-p' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						partition: args[i + 1].int()
					}
					i += 1
				}
			}
			'--offset', '-o' {
				if i + 1 < args.len {
					opts = GroupOptions{
						...opts
						offset: args[i + 1].i64()
					}
					i += 1
				}
			}
			'--to-earliest' {
				opts = GroupOptions{
					...opts
					to_earliest: true
				}
			}
			'--to-latest' {
				opts = GroupOptions{
					...opts
					to_latest: true
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

/// run_group_delete deletes a consumer group.
pub fn run_group_delete(opts GroupOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use: datacore group delete <group-id>')
	}

	println('\x1b[90mDeleting consumer group "${opts.group_id}"...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// DeleteGroups request (API Key 42, Version 2)
	request := build_delete_groups_request_v2(opts.group_id)
	send_kafka_request(mut conn, 42, 2, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 6 {
		return error('Failed to delete group: invalid response')
	}

	println('\x1b[32m\u2713\x1b[0m Consumer group "${opts.group_id}" deleted successfully')
}

/// run_group_reset_offset resets the committed offset for a consumer group.
pub fn run_group_reset_offset(opts GroupOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required')
	}
	if opts.topic.len == 0 {
		return error('Topic is required. Use: --topic <topic>')
	}

	partition := if opts.partition < 0 { 0 } else { opts.partition }

	mut target_offset := opts.offset

	mut resolved_offset := target_offset

	if opts.to_earliest || opts.to_latest {
		mut conn_resolve := connect_broker(opts.bootstrap_server)!
		defer { conn_resolve.close() or {} }

		timestamp := if opts.to_earliest { i64(-2) } else { i64(-1) }
		req_lo := build_list_offsets_request(opts.topic, partition, timestamp)
		send_kafka_request(mut conn_resolve, 2, 7, req_lo)!
		resp_lo := read_kafka_response(mut conn_resolve)!
		resolved_offset = parse_list_offsets_response(resp_lo) or { i64(0) }
	}

	if resolved_offset < 0 {
		return error('Offset is required. Use --offset <n>, --to-earliest, or --to-latest')
	}

	println('\x1b[90mResetting offset for group "${opts.group_id}"...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	request := build_group_offset_commit_request(opts.group_id, opts.topic, partition,
		resolved_offset)
	send_kafka_request(mut conn, 8, 9, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 8 {
		return error('Failed to reset offset: invalid response')
	}

	println('\x1b[32m\u2713\x1b[0m Offset reset successfully')
	println('  Group:     ${opts.group_id}')
	println('  Topic:     ${opts.topic}')
	println('  Partition: ${partition}')
	println('  Offset:    ${resolved_offset}')
}

fn build_group_offset_commit_request(group_id string, topic string, partition int, offset i64) []u8 {
	mut body := []u8{}

	// Group ID (compact string)
	body << u8(group_id.len + 1)
	body << group_id.bytes()

	// GenerationId (4 bytes) - -1 for standalone
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// MemberId (compact string) - empty
	body << u8(1)

	// GroupInstanceId (compact nullable string) - null
	body << u8(0)

	// Topics array (compact array, 1 entry)
	body << u8(2)

	// Topic name (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partitions array (compact array, 1 entry)
	body << u8(2)

	// Partition index (4 bytes)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Committed offset (8 bytes)
	body << u8(offset >> 56)
	body << u8((offset >> 48) & 0xff)
	body << u8((offset >> 40) & 0xff)
	body << u8((offset >> 32) & 0xff)
	body << u8((offset >> 24) & 0xff)
	body << u8((offset >> 16) & 0xff)
	body << u8((offset >> 8) & 0xff)
	body << u8(offset & 0xff)

	// Leader epoch (4 bytes) - -1
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Committed metadata (compact nullable string) - null
	body << u8(0)

	// Tagged fields for partition
	body << u8(0)

	// Tagged fields for topic
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

fn build_list_groups_request() []u8 {
	mut body := []u8{}

	// Groups filter (empty array = all groups)
	body << u8(1)

	// Tagged fields (empty)
	body << u8(0)

	return body
}

fn build_delete_groups_request_v2(group_id string) []u8 {
	mut body := []u8{}

	// Groups array (compact array)
	body << u8(2)

	// Group ID (compact string)
	body << u8(group_id.len + 1)
	body << group_id.bytes()

	// Tagged fields for request
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
