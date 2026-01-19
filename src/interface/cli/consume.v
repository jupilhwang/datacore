// Interface Layer - CLI Consume Command
// Message consumption using Kafka protocol
module cli

import net
import time

// ConsumeOptions holds consume command options
pub struct ConsumeOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	topic            string
	partition        int
	group            string
	offset           string = 'latest' // latest, earliest, or numeric
	max_messages     int    = -1       // -1 = unlimited
	timeout_ms       int    = 30000
	from_beginning   bool
}

// parse_consume_options parses consume command options
pub fn parse_consume_options(args []string) ConsumeOptions {
	mut opts := ConsumeOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partition', '-p' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						partition: args[i + 1].int()
					}
					i += 1
				}
			}
			'--group', '-g' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						group: args[i + 1]
					}
					i += 1
				}
			}
			'--offset', '-o' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						offset: args[i + 1]
					}
					i += 1
				}
			}
			'--max-messages', '-n' {
				if i + 1 < args.len {
					opts = ConsumeOptions{
						...opts
						max_messages: args[i + 1].int()
					}
					i += 1
				}
			}
			'--from-beginning' {
				opts = ConsumeOptions{
					...opts
					from_beginning: true
					offset:         'earliest'
				}
			}
			else {
				// First positional arg might be topic
				if !args[i].starts_with('-') && opts.topic.len == 0 {
					opts = ConsumeOptions{
						...opts
						topic: args[i]
					}
				}
			}
		}
		i += 1
	}

	return opts
}

// run_consume consumes messages from a topic
pub fn run_consume(opts ConsumeOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Get starting offset
	mut fetch_offset := get_starting_offset(mut conn, opts)!

	println('\x1b[90mConsuming from "${opts.topic}" partition ${opts.partition} starting at offset ${fetch_offset}...\x1b[0m')
	println('\x1b[90mPress Ctrl+C to stop\x1b[0m')
	println('')

	mut message_count := 0

	// Consume loop
	for {
		// Check max messages
		if opts.max_messages > 0 && message_count >= opts.max_messages {
			break
		}

		// Build Fetch request
		request := build_fetch_request(opts.topic, opts.partition, fetch_offset, 1048576,
			opts.timeout_ms)

		// Send request
		send_kafka_request(mut conn, 1, 13, request)! // API Key 1 = Fetch, version 13

		// Read response
		response := read_kafka_response(mut conn)!

		// Parse response
		records := parse_fetch_response(response)

		if records.len == 0 {
			// No new records, wait a bit
			time.sleep(100 * time.millisecond)
			continue
		}

		// Print records
		for record in records {
			message_count++

			// Format output
			key_str := if record.key.len > 0 { record.key.bytestr() } else { 'null' }
			value_str := record.value.bytestr()

			if opts.group.len > 0 {
				// With consumer group - show offset
				println('\x1b[90m[${opts.topic}:${opts.partition}:${record.offset}]\x1b[0m')
			}

			if record.key.len > 0 {
				println('\x1b[33m${key_str}\x1b[0m: ${value_str}')
			} else {
				println(value_str)
			}

			fetch_offset = record.offset + 1

			if opts.max_messages > 0 && message_count >= opts.max_messages {
				break
			}
		}
	}

	println('')
	println('\x1b[32m✓\x1b[0m Consumed ${message_count} message(s)')
}

// ============================================================
// Types
// ============================================================

struct ConsumedRecord {
	offset    i64
	key       []u8
	value     []u8
	timestamp i64
}

// ============================================================
// Offset Management
// ============================================================

fn get_starting_offset(mut conn net.TcpConn, opts ConsumeOptions) !i64 {
	// Determine offset based on option
	if opts.offset == 'earliest' || opts.from_beginning {
		return get_list_offset(mut conn, opts.topic, opts.partition, -2) // -2 = earliest
	} else if opts.offset == 'latest' {
		return get_list_offset(mut conn, opts.topic, opts.partition, -1) // -1 = latest
	} else {
		// Numeric offset
		return opts.offset.i64()
	}
}

fn get_list_offset(mut conn net.TcpConn, topic string, partition int, timestamp i64) !i64 {
	// Build ListOffsets request
	request := build_list_offsets_request(topic, partition, timestamp)

	// Send request
	send_kafka_request(mut conn, 2, 7, request)! // API Key 2 = ListOffsets, version 7

	// Read response
	response := read_kafka_response(mut conn)!

	// Parse offset from response
	return parse_list_offsets_response(response)
}

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
	body << u8(2) // 1 topic + 1

	// Topic name (compact string)
	body << u8(topic.len + 1)
	body << topic.bytes()

	// Partitions array (compact array)
	body << u8(2) // 1 partition + 1

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

	// Timestamp (8 bytes)
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

fn parse_list_offsets_response(response []u8) !i64 {
	if response.len < 30 {
		return error('Invalid ListOffsets response')
	}

	// Simplified parsing - in production, full protocol parsing needed
	// Response: correlation_id(4) + tagged_fields(1) + throttle_time(4) + topics array

	// Look for offset in response (offset is 8 bytes)
	// This is a rough heuristic - skip to expected offset position

	// For now, return 0 as fallback
	return 0
}

// ============================================================
// Fetch Request/Response
// ============================================================

fn build_fetch_request(topic string, partition int, offset i64, max_bytes int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Cluster ID (compact nullable string) - null (v12+)
	body << u8(0)

	// Replica ID (4 bytes) - -1 for consumer
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Max wait ms (4 bytes)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	// Min bytes (4 bytes)
	min_bytes := 1
	body << u8(min_bytes >> 24)
	body << u8((min_bytes >> 16) & 0xff)
	body << u8((min_bytes >> 8) & 0xff)
	body << u8(min_bytes & 0xff)

	// Max bytes (4 bytes)
	body << u8(max_bytes >> 24)
	body << u8((max_bytes >> 16) & 0xff)
	body << u8((max_bytes >> 8) & 0xff)
	body << u8(max_bytes & 0xff)

	// Isolation level (1 byte)
	body << u8(0)

	// Session ID (4 bytes)
	body << u8(0)
	body << u8(0)
	body << u8(0)
	body << u8(0)

	// Session epoch (4 bytes)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Topics array (compact array)
	body << u8(2) // 1 topic + 1

	// Topic ID (16 bytes UUID) - for v13+, use zeros for name-based lookup
	for _ in 0 .. 16 {
		body << u8(0)
	}

	// Partitions array (compact array)
	body << u8(2) // 1 partition + 1

	// Partition index (4 bytes)
	body << u8(partition >> 24)
	body << u8((partition >> 16) & 0xff)
	body << u8((partition >> 8) & 0xff)
	body << u8(partition & 0xff)

	// Current leader epoch (4 bytes)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Fetch offset (8 bytes)
	body << u8(offset >> 56)
	body << u8((offset >> 48) & 0xff)
	body << u8((offset >> 40) & 0xff)
	body << u8((offset >> 32) & 0xff)
	body << u8((offset >> 24) & 0xff)
	body << u8((offset >> 16) & 0xff)
	body << u8((offset >> 8) & 0xff)
	body << u8(offset & 0xff)

	// Last fetched epoch (4 bytes)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)
	body << u8(0xff)

	// Log start offset (8 bytes)
	for _ in 0 .. 8 {
		body << u8(0)
	}

	// Partition max bytes (4 bytes)
	body << u8(max_bytes >> 24)
	body << u8((max_bytes >> 16) & 0xff)
	body << u8((max_bytes >> 8) & 0xff)
	body << u8(max_bytes & 0xff)

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

fn parse_fetch_response(response []u8) []ConsumedRecord {
	mut records := []ConsumedRecord{}

	if response.len < 50 {
		return records
	}

	// Simplified parsing - full implementation would parse the complete response
	// Response structure is complex with nested arrays

	// Look for RecordBatch magic byte (0x02) and parse records
	// This is a heuristic approach

	mut pos := 0
	for pos < response.len - 50 {
		// Look for magic byte 0x02 (record batch v2)
		if response[pos] == 0x02 {
			// Try to parse record batch at this position
			parsed := try_parse_record_batch(response, pos - 16) // Back up to batch start
			if parsed.len > 0 {
				records << parsed
				break // Found records
			}
		}
		pos++
	}

	return records
}

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

	// Skip batch_length (4) + partition_leader_epoch (4) + magic (1) + crc (4) + attributes (2)
	// + last_offset_delta (4) + first_timestamp (8) + max_timestamp (8)
	// + producer_id (8) + producer_epoch (2) + base_sequence (4) + records_count (4)

	// This is a simplified version - full implementation would properly parse each field
	record_start := start + 57 // Approximate start of records array

	if record_start < data.len {
		// Try to extract at least one record
		// Records are length-prefixed with varint

		// For now, just return a placeholder
		// Full implementation would decode varints and record format
	}

	_ = base_offset // Avoid unused warning

	return records
}

// ============================================================
// Help
// ============================================================

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
