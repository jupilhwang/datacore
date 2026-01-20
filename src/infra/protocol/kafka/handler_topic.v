// Kafka Protocol - Topic Operations
// CreateTopics, DeleteTopics
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain

pub struct CreateTopicsRequest {
pub:
	topics        []CreateTopicsRequestTopic
	timeout_ms    i32
	validate_only bool // v4+
}

pub struct CreateTopicsRequestTopic {
pub:
	name               string
	num_partitions     i32
	replication_factor i16
	configs            map[string]string
}

fn parse_create_topics_request(mut reader BinaryReader, version i16, is_flexible bool) !CreateTopicsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []CreateTopicsRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		num_partitions := reader.read_i32()!
		replication_factor := reader.read_i16()!

		// Skip replica assignments
		acount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		for _ in 0 .. acount {
			_ = reader.read_i32()! // partition
			rcount := if is_flexible {
				reader.read_compact_array_len()!
			} else {
				reader.read_array_len()!
			}
			for _ in 0 .. rcount {
				_ = reader.read_i32()!
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		// Parse configs
		ccount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut configs := map[string]string{}
		for _ in 0 .. ccount {
			cname := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
			cvalue := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
			configs[cname] = cvalue
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topics << CreateTopicsRequestTopic{
			name:               name
			num_partitions:     num_partitions
			replication_factor: replication_factor
			configs:            configs
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	timeout_ms := reader.read_i32()!
	// v4+: validate_only field
	validate_only := if version >= 4 { reader.read_i8()! != 0 } else { false }
	return CreateTopicsRequest{
		topics:        topics
		timeout_ms:    timeout_ms
		validate_only: validate_only
	}
}

pub struct DeleteTopicsRequest {
pub:
	topics     []DeleteTopicsRequestTopic
	timeout_ms i32
}

pub struct DeleteTopicsRequestTopic {
pub:
	name     string // v0-v5, or empty for v6+
	topic_id []u8   // v6+: UUID (16 bytes)
}

fn parse_delete_topics_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteTopicsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []DeleteTopicsRequestTopic{}
	for _ in 0 .. count {
		mut name := ''
		mut topic_id := []u8{}

		if version >= 6 {
			// v6+: topics array contains { name: compact_nullable_string, topic_id: uuid }
			name = reader.read_compact_nullable_string() or { '' }
			topic_id = reader.read_uuid()!
		} else {
			// v0-v5: topics is just string array
			name = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		}

		topics << DeleteTopicsRequestTopic{
			name:     name
			topic_id: topic_id
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	timeout_ms := reader.read_i32()!
	return DeleteTopicsRequest{
		topics:     topics
		timeout_ms: timeout_ms
	}
}

// ============================================================================
// CreateTopics Response (API Key 19)
// ============================================================================

pub struct CreateTopicsResponse {
pub:
	throttle_time_ms i32
	topics           []CreateTopicsResponseTopic
}

pub struct CreateTopicsResponseTopic {
pub:
	name               string
	topic_id           []u8 // UUID, 16 bytes (v7+)
	error_code         i16
	error_message      ?string
	num_partitions     i32
	replication_factor i16
}

pub fn (r CreateTopicsResponse) encode(version i16) []u8 {
	is_flexible := version >= 5
	mut writer := new_writer()

	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		// topic_id (UUID, 16 bytes) - v7+
		if version >= 7 {
			writer.write_uuid(t.topic_id)
		}

		writer.write_i16(t.error_code)
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(t.error_message)
			} else {
				writer.write_nullable_string(t.error_message)
			}
		}
		if version >= 5 {
			writer.write_i32(t.num_partitions)
			writer.write_i16(t.replication_factor)
			// configs array (empty)
			writer.write_compact_array_len(0)
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// DeleteTopics Response (API Key 20)
// ============================================================================

pub struct DeleteTopicsResponse {
pub:
	throttle_time_ms i32
	topics           []DeleteTopicsResponseTopic
}

pub struct DeleteTopicsResponseTopic {
pub:
	name       string
	topic_id   []u8 // v6+: UUID (16 bytes)
	error_code i16
}

pub fn (r DeleteTopicsResponse) encode(version i16) []u8 {
	is_flexible := version >= 4
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}
		// v6+: topic_id (UUID, 16 bytes)
		if version >= 6 {
			writer.write_uuid(t.topic_id)
		}
		writer.write_i16(t.error_code)
		// v5+: error_message (nullable string, we send null for now)
		if version >= 5 {
			if is_flexible {
				writer.write_compact_nullable_string(none)
			} else {
				writer.write_nullable_string(none)
			}
		}
		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// CreateTopics handler - creates topics in storage
fn (mut h Handler) handle_create_topics(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_create_topics_request(mut reader, version, is_flexible_version(.create_topics,
		version))!

	eprintln('[CreateTopics] Parsed request with ${req.topics.len} topic(s)')
	for t in req.topics {
		eprintln('[CreateTopics]   Topic: ${t.name}, Partitions: ${t.num_partitions}')
	}

	mut topics := []CreateTopicsResponseTopic{}
	for t in req.topics {
		// Convert config map to domain.TopicConfig
		topic_config := domain.TopicConfig{
			retention_ms:        parse_config_i64(t.configs, 'retention.ms', 604800000)
			retention_bytes:     parse_config_i64(t.configs, 'retention.bytes', -1)
			segment_bytes:       parse_config_i64(t.configs, 'segment.bytes', 1073741824)
			cleanup_policy:      t.configs['cleanup.policy'] or { 'delete' }
			min_insync_replicas: parse_config_int(t.configs, 'min.insync.replicas', 1)
			max_message_bytes:   parse_config_int(t.configs, 'max.message.bytes', 1048576)
		}

		// Try to create topic in storage
		partitions := if t.num_partitions <= 0 { 1 } else { int(t.num_partitions) }

		created_meta := h.storage.create_topic(t.name, partitions, topic_config) or {
			// Determine error code based on error message
			error_code := if err.str().contains('already exists') {
				i16(ErrorCode.topic_already_exists)
			} else if err.str().contains('invalid') {
				i16(ErrorCode.invalid_topic_exception)
			} else {
				i16(ErrorCode.unknown_server_error)
			}

			topics << CreateTopicsResponseTopic{
				name:               t.name
				topic_id:           generate_uuid()
				error_code:         error_code
				error_message:      err.str()
				num_partitions:     t.num_partitions
				replication_factor: t.replication_factor
			}
			continue
		}

		// Success - use topic_id from created metadata
		topics << CreateTopicsResponseTopic{
			name:               t.name
			topic_id:           created_meta.topic_id
			error_code:         0
			error_message:      none
			num_partitions:     t.num_partitions
			replication_factor: t.replication_factor
		}
	}

	resp := CreateTopicsResponse{
		throttle_time_ms: 0
		topics:           topics
	}

	eprintln('[CreateTopics] Encoding response with ${topics.len} topic(s)...')
	encoded := resp.encode(version)
	eprintln('[CreateTopics] Encoded ${encoded.len} bytes: ${encoded.hex()}')
	return encoded
}

// DeleteTopics handler - deletes topics from storage
fn (mut h Handler) handle_delete_topics(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_delete_topics_request(mut reader, version, is_flexible_version(.delete_topics,
		version))!

	mut topics := []DeleteTopicsResponseTopic{}
	for t in req.topics {
		// For v6+, we may need to find topic name from topic_id
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		if version >= 6 && t.name.len == 0 && t.topic_id.len == 16 {
			// Look up topic by ID
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				topics << DeleteTopicsResponseTopic{
					name:       ''
					topic_id:   t.topic_id
					error_code: i16(ErrorCode.unknown_topic_or_partition)
				}
				continue
			}
		}

		// Try to delete topic from storage
		h.storage.delete_topic(topic_name) or {
			// Determine error code based on error message
			error_code := if err.str().contains('not found') {
				i16(ErrorCode.unknown_topic_or_partition)
			} else if err.str().contains('internal') {
				i16(ErrorCode.invalid_topic_exception) // Internal topics cannot be deleted
			} else {
				i16(ErrorCode.unknown_server_error)
			}

			topics << DeleteTopicsResponseTopic{
				name:       topic_name
				topic_id:   topic_id
				error_code: error_code
			}
			continue
		}

		// Success
		topics << DeleteTopicsResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			error_code: 0
		}
	}

	resp := DeleteTopicsResponse{
		throttle_time_ms: 0
		topics:           topics
	}

	return resp.encode(version)
}

// Process CreateTopics request (Frame-based)
fn (mut h Handler) process_create_topics(req CreateTopicsRequest, version i16) !CreateTopicsResponse {
	mut topics := []CreateTopicsResponseTopic{}

	for t in req.topics {
		// Convert config map to domain.TopicConfig
		topic_config := domain.TopicConfig{
			retention_ms:        parse_config_i64(t.configs, 'retention.ms', 604800000)
			retention_bytes:     parse_config_i64(t.configs, 'retention.bytes', -1)
			segment_bytes:       parse_config_i64(t.configs, 'segment.bytes', 1073741824)
			cleanup_policy:      t.configs['cleanup.policy'] or { 'delete' }
			min_insync_replicas: parse_config_int(t.configs, 'min.insync.replicas', 1)
			max_message_bytes:   parse_config_int(t.configs, 'max.message.bytes', 1048576)
		}

		// Try to create topic in storage
		partitions := if t.num_partitions <= 0 { 1 } else { int(t.num_partitions) }

		created_meta := h.storage.create_topic(t.name, partitions, topic_config) or {
			// Determine error code based on error message
			error_code := if err.str().contains('already exists') {
				i16(ErrorCode.topic_already_exists)
			} else if err.str().contains('invalid') {
				i16(ErrorCode.invalid_topic_exception)
			} else {
				i16(ErrorCode.unknown_server_error)
			}

			topics << CreateTopicsResponseTopic{
				name:               t.name
				topic_id:           generate_uuid()
				error_code:         error_code
				error_message:      err.str()
				num_partitions:     t.num_partitions
				replication_factor: t.replication_factor
			}
			continue
		}

		topics << CreateTopicsResponseTopic{
			name:               t.name
			topic_id:           created_meta.topic_id
			error_code:         0
			error_message:      none
			num_partitions:     t.num_partitions
			replication_factor: t.replication_factor
		}
	}

	return CreateTopicsResponse{
		throttle_time_ms: 0
		topics:           topics
	}
}

// Process DeleteTopics request (Frame-based)
fn (mut h Handler) process_delete_topics(req DeleteTopicsRequest, version i16) !DeleteTopicsResponse {
	mut topics := []DeleteTopicsResponseTopic{}

	for t in req.topics {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		if version >= 6 && t.name.len == 0 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				topics << DeleteTopicsResponseTopic{
					name:       ''
					topic_id:   t.topic_id
					error_code: i16(ErrorCode.unknown_topic_or_partition)
				}
				continue
			}
		}

		h.storage.delete_topic(topic_name) or {
			error_code := if err.str().contains('not found') {
				i16(ErrorCode.unknown_topic_or_partition)
			} else if err.str().contains('internal') {
				i16(ErrorCode.invalid_topic_exception)
			} else {
				i16(ErrorCode.unknown_server_error)
			}

			topics << DeleteTopicsResponseTopic{
				name:       topic_name
				topic_id:   topic_id
				error_code: error_code
			}
			continue
		}

		topics << DeleteTopicsResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			error_code: 0
		}
	}

	return DeleteTopicsResponse{
		throttle_time_ms: 0
		topics:           topics
	}
}
