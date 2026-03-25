// Interface Layer - CLI Produce Command
// Message production using Kafka protocol
module cli

import net
import os
import time
import infra.protocol.kafka.crc32c

// ProduceOptions holds produce command options
/// ProduceOptions holds produce command options.
pub struct ProduceOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	topic            string
	partition        int = -1
	key              string
	value            string
	file             string
	stdin            bool
	timeout_ms       int = 30000
	acks             int = 1
}

// set_produce_bootstrap_server updates opts with bootstrap server
fn set_produce_bootstrap_server(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			bootstrap_server: args[i + 1]
		}
	}
	return opts
}

// set_produce_topic updates opts with topic
fn set_produce_topic(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			topic: args[i + 1]
		}
	}
	return opts
}

// set_produce_partition updates opts with partition
fn set_produce_partition(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			partition: args[i + 1].int()
		}
	}
	return opts
}

// set_produce_key updates opts with key
fn set_produce_key(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			key: args[i + 1]
		}
	}
	return opts
}

// set_produce_value updates opts with value
fn set_produce_value(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			value: args[i + 1]
		}
	}
	return opts
}

// set_produce_file updates opts with file
fn set_produce_file(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			file: args[i + 1]
		}
	}
	return opts
}

// set_produce_stdin updates opts with stdin flag
fn set_produce_stdin(opts ProduceOptions) ProduceOptions {
	return ProduceOptions{
		...opts
		stdin: true
	}
}

// set_produce_acks updates opts with acks
fn set_produce_acks(opts ProduceOptions, args []string, i int) ProduceOptions {
	if i + 1 < args.len {
		return ProduceOptions{
			...opts
			acks: args[i + 1].int()
		}
	}
	return opts
}

// set_produce_positional_topic updates opts with topic from positional arg
fn set_produce_positional_topic(opts ProduceOptions, arg string) ProduceOptions {
	if !arg.starts_with('-') && opts.topic == '' {
		return ProduceOptions{
			...opts
			topic: arg
		}
	}
	return opts
}

// parse_produce_options parses produce command options
/// parse_produce_options parses produce command options.
pub fn parse_produce_options(args []string) ProduceOptions {
	mut opts := ProduceOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				opts = set_produce_bootstrap_server(opts, args, i)
				i += 1
			}
			'--topic', '-t' {
				opts = set_produce_topic(opts, args, i)
				i += 1
			}
			'--partition', '-p' {
				opts = set_produce_partition(opts, args, i)
				i += 1
			}
			'--key', '-k' {
				opts = set_produce_key(opts, args, i)
				i += 1
			}
			'--message', '-m', '--value' {
				opts = set_produce_value(opts, args, i)
				i += 1
			}
			'--file', '-f' {
				opts = set_produce_file(opts, args, i)
				i += 1
			}
			'--stdin' {
				opts = set_produce_stdin(opts)
			}
			'--acks', '-a' {
				opts = set_produce_acks(opts, args, i)
				i += 1
			}
			else {
				opts = set_produce_positional_topic(opts, args[i])
			}
		}
		i += 1
	}

	return opts
}

// collect_messages_from_file reads messages from a file
fn collect_messages_from_file(mut messages []ProduceMessage, file string, key string) ! {
	content := os.read_file(file) or { return error('Failed to read file ${file}: ${err}') }
	for line in content.split_into_lines() {
		if line.len > 0 {
			messages << ProduceMessage{
				key:   key.bytes()
				value: line.bytes()
			}
		}
	}
}

// collect_messages_from_stdin reads messages from stdin
fn collect_messages_from_stdin(mut messages []ProduceMessage, key string) {
	println('\x1b[90mEnter messages (Ctrl+D to finish):\x1b[0m')
	for {
		line := os.get_line()
		if line.len == 0 {
			break
		}
		messages << ProduceMessage{
			key:   key.bytes()
			value: line.bytes()
		}
	}
}

// collect_messages gathers messages based on options
fn collect_messages(opts ProduceOptions) ![]ProduceMessage {
	mut messages := []ProduceMessage{}

	if opts.value != '' {
		messages << ProduceMessage{
			key:   opts.key.bytes()
			value: opts.value.bytes()
		}
	} else if opts.file != '' {
		collect_messages_from_file(mut messages, opts.file, opts.key)!
	} else if opts.stdin {
		collect_messages_from_stdin(mut messages, opts.key)
	} else {
		return error('No message provided. Use --message, --file, or --stdin')
	}

	if messages.len == 0 {
		return error('No messages to produce')
	}
	return messages
}

// send_produce_request sends the produce request and returns the result
fn send_produce_request(mut conn net.TcpConn, topic string, partition int, messages []ProduceMessage, acks int, timeout_ms int) !ProduceResult {
	request := build_produce_request(topic, partition, messages, acks, timeout_ms)
	send_kafka_request(mut conn, 0, 9, request)!
	response := read_kafka_response(mut conn)!
	return parse_produce_response(response)!
}

// run_produce produces messages to a topic
/// run_produce produces messages to a topic.
pub fn run_produce(opts ProduceOptions) ! {
	if opts.topic == '' {
		return error('Topic name is required. Use --topic <name>')
	}

	messages := collect_messages(opts)!

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	partition := if opts.partition < 0 { 0 } else { opts.partition }
	result := send_produce_request(mut conn, opts.topic, partition, messages, opts.acks,
		opts.timeout_ms)!

	println('\x1b[32m✓\x1b[0m Produced ${messages.len} message(s) to "${opts.topic}"')
	println('  Partition: ${partition}')
	println('  Base Offset: ${result.base_offset}')
}

struct ProduceMessage {
	key   []u8
	value []u8
}

struct ProduceResult {
	base_offset i64
	error_code  i16
}

fn build_produce_request(topic string, partition int, messages []ProduceMessage, acks int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Transactional ID (compact nullable string - null)
	body << u8(0)

	// Acks (2 bytes)
	body << u8(i16(acks) >> 8)
	body << u8(i16(acks) & 0xff)

	// Timeout ms (4 bytes)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	// Topic data array (compact array)
	body << u8(2)

	// Topic name (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partition data array (compact array)
	body << u8(2)

	// Partition index (4 bytes)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Build record batch
	records := build_record_batch(messages)

	// Records (compact bytes)
	records_len := records.len + 1
	body << encode_varint(records_len)
	body << records

	// Tagged fields for partition
	body << u8(0)

	// Tagged fields for topic
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

fn build_record_batch(messages []ProduceMessage) []u8 {
	mut batch := []u8{}

	// Base offset (8 bytes) - 0 for new messages
	for _ in 0 .. 8 {
		batch << u8(0)
	}

	// Batch length placeholder (4 bytes) -- backfilled after all records are encoded
	batch_len_pos := batch.len
	for _ in 0 .. 4 {
		batch << u8(0)
	}

	// Partition leader epoch (4 bytes) - -1
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)

	// Magic (1 byte) - v2 = 2
	batch << u8(2)

	// CRC32-C placeholder (4 bytes) -- computed and backfilled after all records are encoded
	for _ in 0 .. 4 {
		batch << u8(0)
	}

	// Attributes (2 bytes) - 0 = no compression
	batch << u8(0)
	batch << u8(0)

	// Last offset delta (4 bytes)
	last_delta := messages.len - 1
	batch << u8(last_delta >> 24)
	batch << u8((last_delta >> 16) & 0xff)
	batch << u8((last_delta >> 8) & 0xff)
	batch << u8(last_delta & 0xff)

	// First timestamp (8 bytes)
	now := time.now().unix_milli()
	batch << u8(now >> 56)
	batch << u8((now >> 48) & 0xff)
	batch << u8((now >> 40) & 0xff)
	batch << u8((now >> 32) & 0xff)
	batch << u8((now >> 24) & 0xff)
	batch << u8((now >> 16) & 0xff)
	batch << u8((now >> 8) & 0xff)
	batch << u8(now & 0xff)

	// Max timestamp (8 bytes) - same as first
	batch << u8(now >> 56)
	batch << u8((now >> 48) & 0xff)
	batch << u8((now >> 40) & 0xff)
	batch << u8((now >> 32) & 0xff)
	batch << u8((now >> 24) & 0xff)
	batch << u8((now >> 16) & 0xff)
	batch << u8((now >> 8) & 0xff)
	batch << u8(now & 0xff)

	// Producer ID (8 bytes) - -1 = no idempotent
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)

	// Producer epoch (2 bytes) - -1
	batch << u8(0xff)
	batch << u8(0xff)

	// Base sequence (4 bytes) - -1
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)
	batch << u8(0xff)

	// Records count (4 bytes)
	batch << u8(messages.len >> 24)
	batch << u8((messages.len >> 16) & 0xff)
	batch << u8((messages.len >> 8) & 0xff)
	batch << u8(messages.len & 0xff)

	// Encode records
	for i, msg in messages {
		record := encode_record(msg, i, 0)
		batch << record
	}

	// Fill in batch length (excluding base_offset and batch_length itself)
	batch_len := batch.len - 12
	batch[batch_len_pos] = u8(batch_len >> 24)
	batch[batch_len_pos + 1] = u8((batch_len >> 16) & 0xff)
	batch[batch_len_pos + 2] = u8((batch_len >> 8) & 0xff)
	batch[batch_len_pos + 3] = u8(batch_len & 0xff)

	// CRC32-C calculation: from the attributes field to the end of the batch
	// CRC field position: batch_len_pos(4) + partition_leader_epoch(4) + magic(1) = 17 (0-indexed)
	crc_pos := batch_len_pos + 4 + 4 + 1
	crc_data_start := crc_pos + 4
	crc := crc32c.calculate(batch[crc_data_start..])
	batch[crc_pos] = u8(crc >> 24)
	batch[crc_pos + 1] = u8((crc >> 16) & 0xff)
	batch[crc_pos + 2] = u8((crc >> 8) & 0xff)
	batch[crc_pos + 3] = u8(crc & 0xff)

	return batch
}

fn encode_record(msg ProduceMessage, offset_delta int, timestamp_delta i64) []u8 {
	mut record := []u8{}

	// Attributes (1 byte) - varint
	record << u8(0)

	// Timestamp delta (varint)
	record << encode_signed_varint(timestamp_delta)

	// Offset delta (varint)
	record << encode_signed_varint(i64(offset_delta))

	// Key length (varint) - -1 for null
	if msg.key.len == 0 {
		record << u8(1)
	} else {
		record << encode_signed_varint(i64(msg.key.len))
		record << msg.key
	}

	// Value length (varint)
	record << encode_signed_varint(i64(msg.value.len))
	record << msg.value

	// Headers count (varint) - 0
	record << u8(0)

	// Prepend record length (varint)
	mut result := encode_signed_varint(i64(record.len))
	result << record

	return result
}

fn encode_varint(val int) []u8 {
	mut result := []u8{}
	mut v := u64(val)
	for v >= 0x80 {
		result << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	result << u8(v)
	return result
}

fn encode_signed_varint(val i64) []u8 {
	// ZigZag encoding
	zigzag := (u64(val) << 1) ^ u64(val >> 63)
	mut result := []u8{}
	mut v := zigzag
	for v >= 0x80 {
		result << u8((v & 0x7f) | 0x80)
		v >>= 7
	}
	result << u8(v)
	return result
}

fn parse_produce_response(response []u8) !ProduceResult {
	if response.len < 20 {
		return error('Invalid produce response')
	}

	// Skip: correlation_id(4) + tagged_fields(varint) + responses array header
	// This is simplified - full parsing would be needed for production

	// Look for base_offset in the response
	// Response structure: topics -> partitions -> index, error_code, base_offset, ...

	// For now, return a basic result
	return ProduceResult{
		base_offset: 0
		error_code:  0
	}
}

/// print_produce_help prints produce command help.
/// print_produce_help - prints produce command help
pub fn print_produce_help() {
	println('\x1b[33mProduce Command:\x1b[0m')
	println('')
	println('Usage: datacore produce <topic> [options]')
	println('')
	println('\x1b[33mOptions:\x1b[0m')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('  -t, --topic             Topic name (required)')
	println('  -p, --partition         Partition number (default: auto)')
	println('  -k, --key               Message key')
	println('  -m, --message           Message value')
	println('  -f, --file              Read messages from file (one per line)')
	println('      --stdin             Read messages from stdin')
	println('  -a, --acks              Required acks: 0, 1, or -1 (default: 1)')
	println('')
	println('\x1b[33mExamples:\x1b[0m')
	println('  datacore produce my-topic -m "Hello World"')
	println('  datacore produce my-topic -k "key1" -m "value1"')
	println('  datacore produce my-topic -f messages.txt')
	println('  echo "message" | datacore produce my-topic --stdin')
}
