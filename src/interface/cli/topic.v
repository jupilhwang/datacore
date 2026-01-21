// Interface Layer - CLI Topic Commands
// Topic management commands using Kafka protocol
module cli

import net
import time

// TopicOptions holds topic command options
pub struct TopicOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	topic            string
	partitions       int = 1
	replication      int = 1
	timeout_ms       int = 30000
}

// parse_topic_options parses topic command options
pub fn parse_topic_options(args []string) TopicOptions {
	mut opts := TopicOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--topic', '-t' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						topic: args[i + 1]
					}
					i += 1
				}
			}
			'--partitions', '-p' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						partitions: args[i + 1].int()
					}
					i += 1
				}
			}
			'--replication-factor', '-r' {
				if i + 1 < args.len {
					opts = TopicOptions{
						...opts
						replication: args[i + 1].int()
					}
					i += 1
				}
			}
			else {
				// Positional argument might be topic name
				if !args[i].starts_with('-') && opts.topic.len == 0 {
					opts = TopicOptions{
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

// run_topic_create creates a topic
pub fn run_topic_create(opts TopicOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	println('\x1b[90m⏳ Creating topic "${opts.topic}"...\x1b[0m')

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build CreateTopics request
	request := build_create_topic_request(opts.topic, opts.partitions, opts.replication,
		opts.timeout_ms)

	// Send request
	send_kafka_request(mut conn, 19, 3, request)! // API Key 19 = CreateTopics, version 3 (non-flexible)

	// Read response
	response := read_kafka_response(mut conn)!

	// Parse and check response
	check_create_topic_response(response, opts.topic)!

	println('\x1b[32m✓\x1b[0m Topic "${opts.topic}" created successfully')
	println('  Partitions: ${opts.partitions}')
	println('  Replication: ${opts.replication}')
}

// run_topic_list lists all topics
pub fn run_topic_list(opts TopicOptions) ! {
	println('\x1b[90m⏳ Listing topics...\x1b[0m')

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build Metadata request (empty topics array = all topics)
	request := build_metadata_request([])

	// Send request
	send_kafka_request(mut conn, 3, 12, request)! // API Key 3 = Metadata, version 12

	// Read response
	response := read_kafka_response(mut conn)!

	// Parse and display topics
	topics := parse_metadata_response_topics(response)

	if topics.len == 0 {
		println('\x1b[33m⚠\x1b[0m  No topics found')
		return
	}

	println('')
	println('\x1b[33mTopics:\x1b[0m')
	for topic in topics {
		internal_marker := if topic.is_internal { ' \x1b[90m(internal)\x1b[0m' } else { '' }
		println('  • ${topic.name}${internal_marker}')
		println('    Partitions: ${topic.partitions}')
	}
}

// run_topic_describe describes a topic
pub fn run_topic_describe(opts TopicOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	println('\x1b[90m⏳ Describing topic "${opts.topic}"...\x1b[0m')

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build Metadata request for specific topic
	request := build_metadata_request([opts.topic])

	// Send request
	send_kafka_request(mut conn, 3, 12, request)!

	// Read response
	response := read_kafka_response(mut conn)!

	// Parse and display topic details
	topics := parse_metadata_response_topics(response)

	if topics.len == 0 {
		return error('Topic "${opts.topic}" not found')
	}

	topic := topics[0]
	println('')
	println('\x1b[33mTopic:\x1b[0m ${topic.name}')
	println('  Internal:    ${topic.is_internal}')
	println('  Partitions:  ${topic.partitions}')
	// TODO: Add partition details (leader, replicas, ISR)
}

// run_topic_delete deletes a topic
pub fn run_topic_delete(opts TopicOptions) ! {
	if opts.topic.len == 0 {
		return error('Topic name is required. Use --topic <name>')
	}

	println('\x1b[90m⏳ Deleting topic "${opts.topic}"...\x1b[0m')

	// Connect to broker
	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	// Build DeleteTopics request
	request := build_delete_topic_request(opts.topic, opts.timeout_ms)

	// Send request
	send_kafka_request(mut conn, 20, 6, request)! // API Key 20 = DeleteTopics, version 6

	// Read response
	response := read_kafka_response(mut conn)!

	// Parse and check response
	check_delete_topic_response(response, opts.topic)!

	println('\x1b[32m✓\x1b[0m Topic "${opts.topic}" deleted successfully')
}

// ============================================================
// Topic Info
// ============================================================

struct TopicInfo {
	name        string
	partitions  int
	is_internal bool
}

// ============================================================
// Kafka Protocol Helpers
// ============================================================

fn connect_broker(addr string) !&net.TcpConn {
	parts := addr.split(':')
	host := if parts.len > 0 { parts[0] } else { 'localhost' }
	port := if parts.len > 1 { parts[1].int() } else { 9092 }

	return net.dial_tcp('${host}:${port}') or {
		return error('Failed to connect to broker at ${addr}: ${err}')
	}
}

fn send_kafka_request(mut conn net.TcpConn, api_key i16, api_version i16, body []u8) ! {
	// Build header
	mut header := []u8{}

	// API Key (2 bytes)
	header << u8(api_key >> 8)
	header << u8(api_key & 0xff)

	// API Version (2 bytes)
	header << u8(api_version >> 8)
	header << u8(api_version & 0xff)

	// Correlation ID (4 bytes)
	correlation_id := i32(1)
	header << u8(correlation_id >> 24)
	header << u8((correlation_id >> 16) & 0xff)
	header << u8((correlation_id >> 8) & 0xff)
	header << u8(correlation_id & 0xff)

	// Client ID
	client_id := 'datacore-cli'

	// Determine if we should use Flexible Header (V2) or Non-Flexible Header (V1)
	// CreateTopics V3 is NON-FLEXIBLE. Metadata V12 is FLEXIBLE.
	is_flexible_api := (api_key == 3 && api_version >= 9) || // Metadata V9+
	 (api_key == 1 && api_version >= 12) || // Fetch V12+
	 (api_key == 20 && api_version >= 6) || // DeleteTopics V6+
	 (api_key == 19 && api_version >= 5) // CreateTopics V5+ is flexible

	if is_flexible_api {
		// Flexible Header V2: Compact Client ID + Tagged Fields
		// Client ID (compact string)
		header << u8(client_id.len + 1)
		header << client_id.bytes()
		// Tagged fields (empty compact array)
		header << u8(0)
	} else {
		// Non-Flexible Header V1: Nullable String Client ID
		// Use i16 for length prefix (2 bytes)
		header << u8(client_id.len >> 8)
		header << u8(client_id.len & 0xff)
		header << client_id.bytes()
	}

	// Combine header and body
	mut message := header.clone()
	message << body

	// Write size prefix (4 bytes)
	size := i32(message.len)
	mut frame := []u8{}
	frame << u8(size >> 24)
	frame << u8((size >> 16) & 0xff)
	frame << u8((size >> 8) & 0xff)
	frame << u8(size & 0xff)
	frame << message

	conn.write(frame) or { return error('Failed to send request: ${err}') }
}

fn read_kafka_response(mut conn net.TcpConn) ![]u8 {
	conn.set_read_timeout(30 * time.second)

	// Read size (4 bytes)
	mut size_buf := []u8{len: 4}
	conn.read(mut size_buf) or { return error('Failed to read response size: ${err}') }

	size := i32(u32(size_buf[0]) << 24 | u32(size_buf[1]) << 16 | u32(size_buf[2]) << 8 | u32(size_buf[3]))

	if size <= 0 || size > 104857600 {
		return error('Invalid response size: ${size}')
	}

	// Read response body
	mut response := []u8{len: int(size)}
	mut total_read := 0
	for total_read < int(size) {
		n := conn.read(mut response[total_read..]) or {
			return error('Failed to read response body: ${err}')
		}
		if n == 0 {
			break
		}
		total_read += n
	}

	return response
}

// ============================================================
// Request Builders
// ============================================================

fn build_create_topic_request(name string, partitions int, replication int, timeout_ms int) []u8 {
	mut body := []u8{}

	// Topics array (non-flexible array)
	body << u8(0) // array length byte 1
	body << u8(0) // array length byte 2
	body << u8(0) // array length byte 3
	body << u8(1) // array length byte 4 (1 topic)

	// Topic name (string)
	body << u8(name.len >> 8)
	body << u8(name.len & 0xff)
	body << name.bytes()

	// Num partitions (4 bytes)
	body << u8(partitions >> 24)
	body << u8((partitions >> 16) & 0xff)
	body << u8((partitions >> 8) & 0xff)
	body << u8(partitions & 0xff)

	// Replication factor (2 bytes)
	body << u8(replication >> 8)
	body << u8(replication & 0xff)

	// Assignments (empty array)
	body << u8(0) // array length byte 1
	body << u8(0) // array length byte 2
	body << u8(0) // array length byte 3
	body << u8(0) // array length byte 4 (0 assignments)

	// Configs (empty array)
	body << u8(0) // array length byte 1
	body << u8(0) // array length byte 2
	body << u8(0) // array length byte 3
	body << u8(0) // array length byte 4 (0 configs)

	// Timeout ms (4 bytes)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	return body
}

fn build_metadata_request(topics []string) []u8 {
	mut body := []u8{}

	// Topics array (compact nullable array)
	if topics.len == 0 {
		body << u8(1) // null array = all topics
	} else {
		body << u8(topics.len + 1)
		for topic in topics {
			// Topic name (compact string)
			body << u8(topic.len + 1)
			body << topic.bytes()

			// Topic ID (16 bytes of zeros for v12)
			for _ in 0 .. 16 {
				body << u8(0)
			}

			// Tagged fields
			body << u8(0)
		}
	}

	// Allow auto topic creation (1 byte)
	body << u8(0)

	// Include topic authorized operations (1 byte, v8+)
	body << u8(0)

	// Tagged fields
	body << u8(0)

	return body
}

fn build_delete_topic_request(name string, timeout_ms int) []u8 {
	mut body := []u8{}

	// Topics array (compact array)
	body << u8(2) // array length + 1

	// Topic name (compact nullable string)
	body << u8(name.len + 1)
	body << name.bytes()

	// Topic ID (16 bytes of zeros for delete by name)
	for _ in 0 .. 16 {
		body << u8(0)
	}

	// Tagged fields for topic
	body << u8(0)

	// Timeout ms (4 bytes)
	body << u8(timeout_ms >> 24)
	body << u8((timeout_ms >> 16) & 0xff)
	body << u8((timeout_ms >> 8) & 0xff)
	body << u8(timeout_ms & 0xff)

	// Tagged fields for request
	body << u8(0)

	return body
}

// ============================================================
// Response Parsers
// ============================================================

fn check_create_topic_response(response []u8, expected_topic string) ! {
	if response.len < 4 {
		return error('Invalid response: too short')
	}

	// Parse CreateTopics response (version 3, non-flexible)
	// Structure:
	// throttle_time_ms (4 bytes)
	// topics (array)
	//   - name (string)
	//   - error_code (2 bytes)
	//   - error_message (nullable string)
	//   - topic_id (16 bytes, v3+)
	//   - num_partitions (4 bytes)
	//   - replication_factor (2 bytes)

	mut pos := 4 // Skip throttle_time_ms

	// Read topics array length (i32)
	if pos + 4 > response.len {
		return error('Invalid response: cannot read topics array length')
	}
	topics_len := i32(u32(response[pos]) << 24 | u32(response[pos + 1]) << 16 | u32(response[pos + 2]) << 8 | u32(response[
		pos + 3]))
	pos += 4

	if topics_len != 1 {
		return error('Expected 1 topic in response, got ${topics_len}')
	}

	// Read topic name (string: i16 length + data)
	if pos + 2 > response.len {
		return error('Invalid response: cannot read topic name length')
	}
	name_len := i16(u16(response[pos]) << 8 | u16(response[pos + 1]))
	pos += 2

	if pos + int(name_len) > response.len {
		return error('Invalid response: cannot read topic name')
	}
	topic_name := response[pos..pos + int(name_len)].bytestr()
	pos += int(name_len)

	// Read error_code (2 bytes)
	if pos + 2 > response.len {
		return error('Invalid response: cannot read error code')
	}
	error_code := i16(u16(response[pos]) << 8 | u16(response[pos + 1]))
	pos += 2

	if error_code != 0 {
		// Error occurred
		mut error_message := 'Unknown error'
		// Try to read error_message (nullable string)
		if pos + 2 <= response.len {
			msg_len := i16(u16(response[pos]) << 8 | u16(response[pos + 1]))
			pos += 2
			if msg_len > 0 && pos + int(msg_len) <= response.len {
				error_message = response[pos..pos + int(msg_len)].bytestr()
			}
		}
		return error('Failed to create topic: ${error_message} (error code: ${error_code})')
	}

	// Skip error_message, topic_id, num_partitions, replication_factor
	// Just verify we got a success response

	if topic_name != expected_topic {
		return error('Topic name mismatch: expected "${expected_topic}", got "${topic_name}"')
	}
}

fn check_delete_topic_response(response []u8, expected_topic string) ! {
	if response.len < 10 {
		return error('Invalid response')
	}
	// Simplified check
	return
}

fn parse_metadata_response_topics(response []u8) []TopicInfo {
	mut topics := []TopicInfo{}

	if response.len < 20 {
		return topics
	}

	// Skip header: correlation_id(4) + tagged_fields(varint) + throttle_time(4)
	// + brokers array + cluster_id + controller_id + topics array
	// This is a simplified parser - production code would need full protocol parsing

	mut pos := 4 // Skip correlation_id
	if pos >= response.len {
		return topics
	}

	// Skip tagged fields (varint)
	pos += 1
	if pos >= response.len {
		return topics
	}

	// Skip throttle_time (4 bytes)
	pos += 4
	if pos >= response.len {
		return topics
	}

	// Skip brokers array (varint length + data)
	if pos < response.len {
		broker_count := int(response[pos]) - 1
		pos += 1

		// Skip broker data (simplified - just skip a reasonable amount)
		for _ in 0 .. broker_count {
			pos += 50 // Rough estimate for broker entry size
			if pos >= response.len {
				break
			}
		}
	}

	// Skip cluster_id, controller_id
	pos += 40 // Rough estimate

	if pos >= response.len {
		return topics
	}

	// Parse topics array
	topic_count := int(response[pos]) - 1
	pos += 1

	for _ in 0 .. topic_count {
		if pos >= response.len {
			break
		}

		// Skip error_code (2 bytes)
		pos += 2
		if pos >= response.len {
			break
		}

		// Read topic name (compact string)
		name_len := int(response[pos]) - 1
		pos += 1
		if pos + name_len > response.len {
			break
		}

		topic_name := response[pos..pos + name_len].bytestr()
		pos += name_len

		// Skip topic_id (16 bytes)
		pos += 16
		if pos >= response.len {
			break
		}

		// Read is_internal (1 byte)
		is_internal := response[pos] != 0
		pos += 1
		if pos >= response.len {
			break
		}

		// Read partitions array length
		partition_count := int(response[pos]) - 1
		pos += 1

		topics << TopicInfo{
			name:        topic_name
			partitions:  partition_count
			is_internal: is_internal
		}

		// Skip partition details + tagged fields (rough estimate)
		pos += partition_count * 30 + 10
	}

	return topics
}
