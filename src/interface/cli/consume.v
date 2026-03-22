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

// parse_list_offsets_response parses the offset from a ListOffsets v7 (flexible) response.
// Response layout after size prefix: correlation_id(4) + tag_buffer(varuint)
// + throttle_time_ms(4) + topics_compact_array + partitions...
fn parse_list_offsets_response(response []u8) !i64 {
	if response.len < 20 {
		return error('Invalid ListOffsets response: too short (${response.len} bytes)')
	}

	mut pos := 0

	// Response header v1 (flexible): correlation_id(4) + tagged_fields(varuint)
	pos += 4

	_, tag_n := read_uvarint_at(response, pos)
	if tag_n == 0 {
		return error('Invalid ListOffsets response: cannot read header tag buffer')
	}
	pos += tag_n

	// throttle_time_ms(4)
	if pos + 4 > response.len {
		return error('Invalid ListOffsets response: cannot read throttle time')
	}
	pos += 4

	// Topics compact array: varuint(N + 1), so value 2 means 1 topic
	topics_raw, topics_n := read_uvarint_at(response, pos)
	if topics_n == 0 {
		return error('Invalid ListOffsets response: cannot read topics array length')
	}
	pos += topics_n

	if topics_raw < 2 {
		return error('No topics in ListOffsets response')
	}

	// Topic name (compact string: varuint(len + 1) + bytes)
	name_raw, name_n := read_uvarint_at(response, pos)
	if name_n == 0 || name_raw < 1 {
		return error('Invalid ListOffsets response: cannot read topic name')
	}
	pos += name_n
	topic_name_len := int(name_raw) - 1
	if pos + topic_name_len > response.len {
		return error('Invalid ListOffsets response: topic name truncated')
	}
	pos += topic_name_len

	// Partitions compact array: varuint(N + 1)
	parts_raw, parts_n := read_uvarint_at(response, pos)
	if parts_n == 0 {
		return error('Invalid ListOffsets response: cannot read partitions array length')
	}
	pos += parts_n

	if parts_raw < 2 {
		return error('No partitions in ListOffsets response')
	}

	// First partition: partition_index(4) + error_code(2) + timestamp(8) + offset(8) + leader_epoch(4)
	if pos + 26 > response.len {
		return error('Invalid ListOffsets response: partition data truncated')
	}

	pos += 4 // partition_index

	error_code := read_i16_be(response, pos)
	pos += 2

	if error_code != 0 {
		return error('ListOffsets error code: ${error_code}')
	}

	pos += 8 // timestamp

	offset := read_i64_be(response, pos)
	return offset
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

// read_i64_be reads a big-endian i64 from data at position
fn read_i64_be(data []u8, pos int) i64 {
	return i64(u64(data[pos]) << 56 | u64(data[pos + 1]) << 48 | u64(data[pos + 2]) << 40 | u64(data[
		pos + 3]) << 32 | u64(data[pos + 4]) << 24 | u64(data[pos + 5]) << 16 | u64(data[pos + 6]) << 8 | u64(data[
		pos + 7]))
}

// read_uvarint_at reads an unsigned varint from data at pos.
// Returns (value, bytes_consumed). bytes_consumed == 0 indicates failure.
fn read_uvarint_at(data []u8, pos int) (u64, int) {
	mut result := u64(0)
	mut shift := u32(0)
	mut i := pos
	for i < data.len && shift < 64 {
		b := data[i]
		result |= u64(b & 0x7f) << shift
		i++
		if b & 0x80 == 0 {
			return result, i - pos
		}
		shift += 7
	}
	return 0, 0
}

// read_varint_at reads a signed zigzag varint from data at pos.
// Returns (value, bytes_consumed). bytes_consumed == 0 indicates failure.
fn read_varint_at(data []u8, pos int) (i64, int) {
	uval, n := read_uvarint_at(data, pos)
	if n == 0 {
		return 0, 0
	}
	return i64((uval >> 1) ^ (-(uval & 1))), n
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
			if pos < 16 {
				pos += 1
				continue
			}
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

// try_parse_record_batch attempts to parse records from a RecordBatch v2.
// RecordBatch header (61 bytes): base_offset(8) + batch_length(4) +
// partition_leader_epoch(4) + magic(1) + crc(4) + attributes(2) +
// last_offset_delta(4) + first_timestamp(8) + max_timestamp(8) +
// producer_id(8) + producer_epoch(2) + base_sequence(4) + records_count(4)
fn try_parse_record_batch(data []u8, start int) []ConsumedRecord {
	mut records := []ConsumedRecord{}

	if start < 0 || start >= data.len - 61 {
		return [] // Need at least 61 bytes for batch header
	}

	base_offset := read_i64_be(data, start)
	batch_length := i64(read_i32_be(data, start + 8))
	if batch_length <= 0 || i64(start) + 12 + batch_length > i64(data.len) {
		return [] // batch_length exceeds available data
	}
	first_timestamp := read_i64_be(data, start + 27)
	records_count := read_i32_be(data, start + 57)

	if records_count <= 0 || records_count > 10000 {
		return records
	}

	mut pos := start + 61

	mut i := 0
	for i < int(records_count) {
		if pos >= data.len {
			break
		}

		// Record: length(varint) followed by record body
		record_len, len_n := read_varint_at(data, pos)
		if len_n == 0 || record_len <= 0 {
			break
		}
		pos += len_n
		record_end := pos + int(record_len)
		if record_end > data.len {
			break
		}

		// attributes(1)
		if pos >= record_end {
			break
		}
		pos += 1

		// timestamp_delta(varint)
		ts_delta, ts_n := read_varint_at(data, pos)
		if ts_n == 0 {
			break
		}
		pos += ts_n

		// offset_delta(varint)
		off_delta, off_n := read_varint_at(data, pos)
		if off_n == 0 {
			break
		}
		pos += off_n

		// key_length(varint), -1 means null key
		key_length, kl_n := read_varint_at(data, pos)
		if kl_n == 0 {
			break
		}
		pos += kl_n

		mut key := []u8{}
		if key_length > 0 {
			key_end := pos + int(key_length)
			if key_end > data.len {
				break
			}
			key = data[pos..key_end].clone()
			pos = key_end
		}

		// value_length(varint), -1 means null value
		val_length, vl_n := read_varint_at(data, pos)
		if vl_n == 0 {
			break
		}
		pos += vl_n

		mut value := []u8{}
		if val_length > 0 {
			val_end := pos + int(val_length)
			if val_end > data.len {
				break
			}
			value = data[pos..val_end].clone()
			pos = val_end
		}

		// Skip remaining record data (headers) by jumping to record_end
		pos = record_end

		records << ConsumedRecord{
			offset:    base_offset + i64(off_delta)
			key:       key
			value:     value
			timestamp: first_timestamp + ts_delta
		}
		i++
	}

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
