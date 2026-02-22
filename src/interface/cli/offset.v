// Interface Layer - CLI Offset Commands
//
// Provides offset management commands using the Kafka protocol.
// Supports getting and setting consumer group offsets.
//
// Key features:
// - Get current offset for a group/topic/partition
// - Set (override) offset for a group/topic/partition
module cli

/// OffsetOptions holds offset command options.
pub struct OffsetOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	group_id         string
	topic            string
	partition        int = -1
	offset           i64 = -1
	to_earliest      bool
	to_latest        bool
}

/// parse_offset_options parses offset command options.
pub fn parse_offset_options(args []string) OffsetOptions {
	mut opts := OffsetOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = OffsetOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = OffsetOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partition', '-p' {
				if i + 1 < args.len {
					opts = OffsetOptions{
						...opts
						partition: args[i + 1].int()
					}
					i += 1
				}
			}
			'--offset', '-o' {
				if i + 1 < args.len {
					opts = OffsetOptions{
						...opts
						offset: args[i + 1].i64()
					}
					i += 1
				}
			}
			'--to-earliest' {
				opts = OffsetOptions{
					...opts
					to_earliest: true
				}
			}
			'--to-latest' {
				opts = OffsetOptions{
					...opts
					to_latest: true
				}
			}
			else {
				// Positional argument may be the group_id
				if !args[i].starts_with('-') && opts.group_id.len == 0 {
					opts = OffsetOptions{
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

/// run_offset_get gets the current committed offset for a consumer group.
pub fn run_offset_get(opts OffsetOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use: datacore offset get <group-id> --topic <topic>')
	}
	if opts.topic.len == 0 {
		return error('Topic is required. Use: --topic <topic>')
	}

	partition := if opts.partition < 0 { 0 } else { opts.partition }

	println('\x1b[90mFetching offset for group "${opts.group_id}"...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	request := build_offset_fetch_request(opts.group_id, opts.topic, partition)
	send_kafka_request(mut conn, 9, 8, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 10 {
		return error('Failed to fetch offset: invalid response')
	}

	committed_offset := parse_offset_fetch_response(response)

	println('')
	println('\x1b[33mCommitted Offset:\x1b[0m')
	println('  Group:     ${opts.group_id}')
	println('  Topic:     ${opts.topic}')
	println('  Partition: ${partition}')
	if committed_offset < 0 {
		println('  Offset:    \x1b[33mNo committed offset\x1b[0m')
	} else {
		println('  Offset:    ${committed_offset}')
	}
}

/// run_offset_set sets the committed offset for a consumer group.
pub fn run_offset_set(opts OffsetOptions) ! {
	if opts.group_id.len == 0 {
		return error('Group ID is required. Use: datacore offset set <group-id> --topic <topic> --partition <n> --offset <n>')
	}
	if opts.topic.len == 0 {
		return error('Topic is required. Use: --topic <topic>')
	}

	partition := if opts.partition < 0 { 0 } else { opts.partition }

	mut target_offset := opts.offset

	// Resolve special offset values
	if opts.to_earliest || opts.to_latest {
		mut conn_resolve := connect_broker(opts.bootstrap_server)!
		defer { conn_resolve.close() or {} }

		timestamp := if opts.to_earliest { i64(-2) } else { i64(-1) }
		req_lo := build_list_offsets_request(opts.topic, partition, timestamp)
		send_kafka_request(mut conn_resolve, 2, 7, req_lo)!
		resp_lo := read_kafka_response(mut conn_resolve)!
		target_offset = parse_list_offsets_response(resp_lo) or { i64(0) }
	}

	if target_offset < 0 {
		return error('Offset is required. Use --offset <n>, --to-earliest, or --to-latest')
	}

	println('\x1b[90mSetting offset for group "${opts.group_id}"...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	request := build_offset_commit_request(opts.group_id, opts.topic, partition, target_offset)
	send_kafka_request(mut conn, 8, 9, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 8 {
		return error('Failed to set offset: invalid response')
	}

	println('\x1b[32m\u2713\x1b[0m Offset set successfully')
	println('  Group:     ${opts.group_id}')
	println('  Topic:     ${opts.topic}')
	println('  Partition: ${partition}')
	println('  Offset:    ${target_offset}')
}

/// print_offset_help prints offset command help.
pub fn print_offset_help() {
	println('\x1b[33mOffset Commands:\x1b[0m')
	println('')
	println('Usage: datacore offset <command> [options]')
	println('')
	println('Commands:')
	println('  get     Get committed offset for a consumer group')
	println('  set     Set committed offset for a consumer group')
	println('')
	println('Options:')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('  -t, --topic             Topic name (required)')
	println('  -p, --partition         Partition number (default: 0)')
	println('  -o, --offset            Offset value (for set)')
	println('      --to-earliest       Set offset to earliest')
	println('      --to-latest         Set offset to latest')
	println('')
	println('Examples:')
	println('  datacore offset get my-group --topic my-topic --partition 0')
	println('  datacore offset set my-group --topic my-topic --partition 0 --offset 100')
	println('  datacore offset set my-group --topic my-topic --to-earliest')
	println('  datacore offset set my-group --topic my-topic --to-latest')
}

fn build_offset_fetch_request(group_id string, topic string, partition int) []u8 {
	mut body := []u8{}

	// Group ID (compact string)
	body << u8(group_id.len + 1)
	body << group_id.bytes()

	// Topics array (compact array, 1 entry)
	body << u8(2)

	// Topic name (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partition indices array (compact array, 1 entry)
	body << u8(2)
	// Partition index (4 bytes)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)
	// Tagged fields for partition
	body << u8(0)

	// Tagged fields for topic
	body << u8(0)

	// Require stable (1 byte) - v7+
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

fn build_offset_commit_request(group_id string, topic string, partition int, offset i64) []u8 {
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

// parse_offset_fetch_response extracts the committed offset from an OffsetFetch response.
fn parse_offset_fetch_response(response []u8) i64 {
	if response.len < 12 {
		return -1
	}

	mut pos := 4 // skip correlation_id

	// Tagged fields
	if pos < response.len {
		pos += 1
	}

	// Throttle time ms (4 bytes)
	if pos + 4 > response.len {
		return -1
	}
	pos += 4

	// Topics array count
	if pos >= response.len {
		return -1
	}
	topics_count := int(response[pos]) - 1
	pos += 1

	for _ in 0 .. topics_count {
		if pos >= response.len {
			break
		}
		// Topic name (compact string)
		name_len := int(response[pos]) - 1
		pos += 1
		if pos + name_len > response.len {
			break
		}
		pos += name_len

		// Partitions array count
		if pos >= response.len {
			break
		}
		partitions_count := int(response[pos]) - 1
		pos += 1

		for _ in 0 .. partitions_count {
			if pos + 4 > response.len {
				break
			}
			// Partition index (4 bytes)
			pos += 4

			// Committed offset (8 bytes)
			if pos + 8 > response.len {
				break
			}
			offset := i64(u64(response[pos]) << 56 | u64(response[pos + 1]) << 48 | u64(response[
				pos + 2]) << 40 | u64(response[pos + 3]) << 32 | u64(response[pos + 4]) << 24 | u64(response[
				pos + 5]) << 16 | u64(response[pos + 6]) << 8 | u64(response[pos + 7]))
			return offset
		}
	}

	return -1
}
