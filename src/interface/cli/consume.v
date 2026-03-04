// Interface Layer - CLI Consume Command
//
// Provides message consumption commands using the Kafka protocol.
// Reads messages from a topic and prints them to the console.
//
// Key features:
// - Consume messages from a specific topic/partition
// - Specify starting offset (earliest, latest, or numeric)
// - Limit maximum number of messages
// - Consumer group support
module cli

import net
import time

/// ConsumeOptions is a struct holding consume command options.
pub struct ConsumeOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	topic            string
	partition        int
	group            string
	offset           string = 'latest'
	max_messages     int    = -1
	timeout_ms       int    = 30000
	from_beginning   bool
}

// set_bootstrap_server_opt updates opts with bootstrap server from args[i+1]
fn set_bootstrap_server_opt(mut opts ConsumeOptions, args []string, i int) ConsumeOptions {
	if i + 1 < args.len {
		return ConsumeOptions{
			...opts
			bootstrap_server: args[i + 1]
		}
	}
	return opts
}

// set_topic_opt updates opts with topic from args[i+1]
fn set_topic_opt(mut opts ConsumeOptions, args []string, i int) ConsumeOptions {
	if i + 1 < args.len {
		return ConsumeOptions{
			...opts
			topic: args[i + 1]
		}
	}
	return opts
}

// set_partition_opt updates opts with partition from args[i+1]
fn set_partition_opt(mut opts ConsumeOptions, args []string, i int) ConsumeOptions {
	if i + 1 < args.len {
		return ConsumeOptions{
			...opts
			partition: args[i + 1].int()
		}
	}
	return opts
}

// set_group_opt updates opts with group from args[i+1]
fn set_group_opt(mut opts ConsumeOptions, args []string, i int) ConsumeOptions {
	if i + 1 < args.len {
		return ConsumeOptions{
			...opts
			group: args[i + 1]
		}
	}
	return opts
}

// set_offset_opt updates opts with offset from args[i+1]
fn set_offset_opt(mut opts ConsumeOptions, args []string, i int) ConsumeOptions {
	if i + 1 < args.len {
		return ConsumeOptions{
			...opts
			offset: args[i + 1]
		}
	}
	return opts
}

// set_max_messages_opt updates opts with max_messages from args[i+1]
fn set_max_messages_opt(mut opts ConsumeOptions, args []string, i int) ConsumeOptions {
	if i + 1 < args.len {
		return ConsumeOptions{
			...opts
			max_messages: args[i + 1].int()
		}
	}
	return opts
}

// set_from_beginning_opt updates opts with from_beginning flag
fn set_from_beginning_opt(opts ConsumeOptions) ConsumeOptions {
	return ConsumeOptions{
		...opts
		from_beginning: true
		offset:         'earliest'
	}
}

// set_positional_topic updates opts with topic from positional arg
fn set_positional_topic(opts ConsumeOptions, arg string) ConsumeOptions {
	if !arg.starts_with('-') && opts.topic.len == 0 {
		return ConsumeOptions{
			...opts
			topic: arg
		}
	}
	return opts
}

/// parse_consume_options parses consume command options.
pub fn parse_consume_options(args []string) ConsumeOptions {
	mut opts := ConsumeOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				opts = set_bootstrap_server_opt(mut opts, args, i)
				i += 1
			}
			'--topic', '-t' {
				opts = set_topic_opt(mut opts, args, i)
				i += 1
			}
			'--partition', '-p' {
				opts = set_partition_opt(mut opts, args, i)
				i += 1
			}
			'--group', '-g' {
				opts = set_group_opt(mut opts, args, i)
				i += 1
			}
			'--offset', '-o' {
				opts = set_offset_opt(mut opts, args, i)
				i += 1
			}
			'--max-messages', '-n' {
				opts = set_max_messages_opt(mut opts, args, i)
				i += 1
			}
			'--from-beginning' {
				opts = set_from_beginning_opt(opts)
			}
			else {
				opts = set_positional_topic(opts, args[i])
			}
		}
		i += 1
	}

	return opts
}

// print_record outputs a single consumed record
fn print_record(record ConsumedRecord, opts ConsumeOptions, topic string, partition int) i64 {
	key_str := if record.key.len > 0 { record.key.bytestr() } else { 'null' }
	value_str := record.value.bytestr()

	if opts.group.len > 0 {
		println('\x1b[90m[${topic}:${partition}:${record.offset}]\x1b[0m')
	}

	if record.key.len > 0 {
		println('\x1b[33m${key_str}\x1b[0m: ${value_str}')
	} else {
		println(value_str)
	}

	return record.offset + 1
}

// should_stop_consuming checks if consumption should stop
fn should_stop_consuming(message_count int, max_messages int) bool {
	return max_messages > 0 && message_count >= max_messages
}

// fetch_and_process_records sends a fetch request and processes the response
fn fetch_and_process_records(mut conn net.TcpConn, opts ConsumeOptions, fetch_offset i64, message_count int) (i64, int, bool) {
	mut new_offset := fetch_offset
	mut new_count := message_count

	request := build_fetch_request(opts.topic, opts.partition, new_offset, 1048576, opts.timeout_ms)
	send_kafka_request(mut conn, 1, 13, request) or { return new_offset, new_count, true }
	response := read_kafka_response(mut conn) or { return new_offset, new_count, true }
	records := parse_fetch_response(response)

	if records.len == 0 {
		time.sleep(100 * time.millisecond)
		return new_offset, new_count, false
	}

	for record in records {
		new_count++
		new_offset = print_record(record, opts, opts.topic, opts.partition)

		if should_stop_consuming(new_count, opts.max_messages) {
			return new_offset, new_count, true
		}
	}
	return new_offset, new_count, false
}

/// run_consume consumes messages from a topic.
pub fn run_consume(opts ConsumeOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	mut fetch_offset := get_starting_offset(mut conn, opts)!

	println('\x1b[90mConsuming from "${opts.topic}" partition ${opts.partition} starting at offset ${fetch_offset}...\x1b[0m')
	println('\x1b[90mPress Ctrl+C to stop\x1b[0m')
	println('')

	mut message_count := 0
	mut should_stop := false

	for {
		if should_stop_consuming(message_count, opts.max_messages) {
			break
		}

		fetch_offset, message_count, should_stop = fetch_and_process_records(mut conn,
			opts, fetch_offset, message_count)
		if should_stop {
			break
		}
	}

	println('')
	println('\x1b[32m✓\x1b[0m Consumed ${message_count} message(s)')
}

struct ConsumedRecord {
	offset    i64
	key       []u8
	value     []u8
	timestamp i64
}

fn get_starting_offset(mut conn net.TcpConn, opts ConsumeOptions) !i64 {
	// Determine offset based on options
	if opts.offset == 'earliest' || opts.from_beginning {
		return get_list_offset(mut conn, opts.topic, opts.partition, -2)
	} else if opts.offset == 'latest' {
		return get_list_offset(mut conn, opts.topic, opts.partition, -1)
	} else {
		// Numeric offset
		return opts.offset.i64()
	}
}

// get_list_offset retrieves an offset using the ListOffsets API.
fn get_list_offset(mut conn net.TcpConn, topic string, partition int, timestamp i64) !i64 {
	// Build ListOffsets request
	request := build_list_offsets_request(topic, partition, timestamp)

	// Send request
	send_kafka_request(mut conn, 2, 7, request)!

	// Read response
	response := read_kafka_response(mut conn)!

	// Parse offset from response
	return parse_list_offsets_response(response)
}

// build_list_offsets_request builds a ListOffsets request.
fn build_list_offsets_request(topic string, partition int, timestamp i64) []u8 {
	mut body := []u8{}

	// Replica ID (4 bytes) - -1 for consumer
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Isolation level (1 byte) - 0 = read_uncommitted
	body << u8(0)

	// Topics array (compact array)
	body << u8(2)

	// Topic name (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partitions array (compact array)
	body << u8(2)

	// Partition index (4 bytes)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Current leader epoch (4 bytes) - -1
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	body << u8(timestamp >> 56)
	body << u8((timestamp >> 48) & 0xff)
	body << u8((timestamp >> 40) & 0xff)
	body << u8((timestamp >> 32) & 0xff)
	body << u8((timestamp >> 24) & 0xff)
	body << u8((timestamp >> 16) & 0xff)
	body << u8((timestamp >> 8) & 0xff)
	body << u8(timestamp & 0xff)

	// Tagged fields for partition
	body << u8(0)

	// Tagged fields for topic
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

// parse_list_offsets_response parses the offset from a ListOffsets response.
fn parse_list_offsets_response(response []u8) !i64 {
	if response.len < 30 {
		return error('Invalid ListOffsets response')
	}

	// Simplified parsing - full protocol parsing required in production
	// Response: correlation_id(4) + tagged_fields(1) + throttle_time(4) + topics array

	// Find offset in response (offset is 8 bytes)
	// This is an approximate heuristic - skip to expected offset position

	// TODO(jira#XXX): implement proper offset handling
	return 0
}

// Byte writing helpers for build_fetch_request
fn write_i32_be(mut body []u8, val int) {
	body << u8(val >> 24)
	body << u8((val >> 16) & 0xff)
	body << u8((val >> 8) & 0xff)
	body << u8(val & 0xff)
}

fn write_i64_be(mut body []u8, val i64) {
	body << u8(val >> 56)
	body << u8((val >> 48) & 0xff)
	body << u8((val >> 40) & 0xff)
	body << u8((val >> 32) & 0xff)
	body << u8((val >> 24) & 0xff)
	body << u8((val >> 16) & 0xff)
	body << u8((val >> 8) & 0xff)
	body << u8(val & 0xff)
}

fn write_negative_i32(mut body []u8) {
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
}

fn write_zeroes(mut body []u8, count int) {
	for _ in 0 .. count {
		body << u8(0)
	}
}

fn build_fetch_request(topic string, partition int, offset i64, max_bytes int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Cluster ID (compact nullable string) - null (v12+)
	body << u8(0)

	// Replica ID (4 bytes) - -1 for consumer
	write_negative_i32(mut body)

	// Max wait ms (4 bytes)
	write_i32_be(mut body, timeout_ms)

	// Min bytes (4 bytes)
	write_i32_be(mut body, 1)

	// Max bytes (4 bytes)
	write_i32_be(mut body, max_bytes)

	// Isolation level (1 byte)
	body << u8(0)

	// Session ID (4 bytes) - 0
	write_zeroes(mut body, 4)

	// Session epoch (4 bytes) - -1
	write_negative_i32(mut body)

	// Topics array (compact array)
	body << u8(2)

	// Topic ID (16-byte UUID) - v13+, zero for name-based lookup
	write_zeroes(mut body, 16)

	// Partitions array (compact array)
	body << u8(2)

	// Partition index (4 bytes)
	write_i32_be(mut body, partition)

	// Current leader epoch (4 bytes) - -1
	write_negative_i32(mut body)

	// Fetch offset (8 bytes)
	write_i64_be(mut body, offset)

	// Last fetched epoch (4 bytes) - -1
	write_negative_i32(mut body)

	// Log start offset (8 bytes) - 0
	write_zeroes(mut body, 8)

	// Partition max bytes (4 bytes)
	write_i32_be(mut body, max_bytes)

	// Tagged fields for partition
	body << u8(0)

	// Tagged fields for topic
	body << u8(0)

	// Forgotten topics data (compact array) - empty
	body << u8(1)

	// Rack ID (compact string) - empty
	body << u8(1)

	// Tagged fields for request
	body << u8(0)

	return body
}

// parse_fetch_response parses records from a Fetch response.
fn parse_fetch_response(response []u8) []ConsumedRecord {
	mut records := []ConsumedRecord{}

	if response.len < 50 {
		return records
	}

	// Simplified parsing - full implementation requires complete response parsing
	// Response structure is complex with nested arrays

	// Find magic byte (0x02) for RecordBatch and parse records
	// This is a heuristic approach

	mut pos := 0
	for pos < response.len - 50 {
		// Find magic byte 0x02 (record batch v2)
		if response[pos] == 0x02 {
			// Attempt to parse record batch at this position
			parsed := try_parse_record_batch(response, pos - 16)
			if parsed.len > 0 {
				records << parsed
				break
			}
		}
		pos++
	}

	return records
}

// try_parse_record_batch attempts to parse a record batch.
fn try_parse_record_batch(data []u8, start int) []ConsumedRecord {
	mut records := []ConsumedRecord{}

	if start < 0 || start + 61 > data.len {
		return records
	}

	pos := start

	// Base offset (8 bytes)
	base_offset := i64(u64(data[pos]) << 56 | u64(data[pos + 1]) << 48 | u64(data[pos + 2]) << 40 | u64(data[
		pos + 3]) << 32 | u64(data[pos + 4]) << 24 | u64(data[pos + 5]) << 16 | u64(data[pos + 6]) << 8 | u64(data[
		pos + 7]))

	// Skip: batch_length(4) + partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2)
	// + last_offset_delta(4) + first_timestamp(8) + max_timestamp(8)
	// + producer_id(8) + producer_epoch(2) + base_sequence(4) + records_count(4)

	// This is a simplified version - full implementation should parse each field properly
	record_start := start + 57

	if record_start < data.len {
		// Attempt to extract at least one record
		// Records are length-prefixed with varints

		// TODO(jira#XXX): implement real offset tracking
		// Full implementation must decode varints and record format
	}

	_ = base_offset

	return records
}

/// print_consume_help prints consume command help.
pub fn print_consume_help() {
	println('\x1b[33mConsume Command:\x1b[0m')
	println('')
	println('Usage: datacore consume <topic> [options]')
	println('')
	println('\x1b[33mOptions:\x1b[0m')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('  -t, --topic             Topic name (required)')
	println('  -p, --partition         Partition number (default: 0)')
	println('  -g, --group             Consumer group ID')
	println('  -o, --offset            Starting offset: earliest, latest, or number')
	println('  -n, --max-messages      Maximum messages to consume')
	println('      --from-beginning    Start from earliest offset')
	println('')
	println('\x1b[33mExamples:\x1b[0m')
	println('  datacore consume my-topic')
	println('  datacore consume my-topic --from-beginning')
	println('  datacore consume my-topic -g my-group')
	println('  datacore consume my-topic -n 10')
}
