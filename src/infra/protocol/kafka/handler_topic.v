// Infra Layer - Kafka Protocol Handler - Topic Operations
// CreateTopics, DeleteTopics handlers
module kafka

import domain

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
