// Kafka 프로토콜 - Topic 작업
// CreateTopics, DeleteTopics
// 요청/응답 타입, 파싱, 인코딩 및 핸들러
module kafka

import domain
import infra.observability
import time

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
	count := reader.read_flex_array_len(is_flexible)!
	mut topics := []CreateTopicsRequestTopic{}
	for _ in 0 .. count {
		name := reader.read_flex_string(is_flexible)!
		num_partitions := reader.read_i32()!
		replication_factor := reader.read_i16()!

		// Skip replica assignments
		acount := reader.read_flex_array_len(is_flexible)!
		for _ in 0 .. acount {
			_ = reader.read_i32()! // partition
			rcount := reader.read_flex_array_len(is_flexible)!
			for _ in 0 .. rcount {
				_ = reader.read_i32()!
			}
			reader.skip_flex_tagged_fields(is_flexible)!
		}

		// Parse configs
		ccount := reader.read_flex_array_len(is_flexible)!
		mut configs := map[string]string{}
		for _ in 0 .. ccount {
			cname := reader.read_flex_string(is_flexible)!
			cvalue := reader.read_flex_string(is_flexible)!
			configs[cname] = cvalue
			reader.skip_flex_tagged_fields(is_flexible)!
		}

		topics << CreateTopicsRequestTopic{
			name:               name
			num_partitions:     num_partitions
			replication_factor: replication_factor
			configs:            configs
		}
		reader.skip_flex_tagged_fields(is_flexible)!
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
	count := reader.read_flex_array_len(is_flexible)!
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
			name = reader.read_flex_string(is_flexible)!
		}

		topics << DeleteTopicsRequestTopic{
			name:     name
			topic_id: topic_id
		}
		reader.skip_flex_tagged_fields(is_flexible)!
	}
	timeout_ms := reader.read_i32()!
	return DeleteTopicsRequest{
		topics:     topics
		timeout_ms: timeout_ms
	}
}

// CreateTopics Response (API Key 19)

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

// DeleteTopics Response (API Key 20)

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

// CreateTopics 핸들러 - 스토리지에 토픽 생성
fn (mut h Handler) handle_create_topics(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_create_topics_request(mut reader, version, is_flexible_version(.create_topics,
		version))!

	h.logger.debug('Processing create topics request', observability.field_int('topics',
		req.topics.len), observability.field_int('timeout_ms', req.timeout_ms), observability.field_bool('validate_only',
		req.validate_only))

	mut topics := []CreateTopicsResponseTopic{}
	mut created_count := 0
	mut error_count := 0

	for t in req.topics {
		h.logger.trace('Creating topic', observability.field_string('topic', t.name),
			observability.field_int('partitions', t.num_partitions), observability.field_int('replication_factor',
			t.replication_factor))
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

			h.logger.warn('Failed to create topic', observability.field_string('topic',
				t.name), observability.field_int('error_code', error_code), observability.field_string('error',
				err.str()))
			error_count += 1

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

		h.logger.info('Topic created', observability.field_string('topic', t.name), observability.field_int('partitions',
			int(t.num_partitions)))
		created_count += 1

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

	elapsed := time.since(start_time)
	h.logger.debug('Create topics completed', observability.field_int('created', created_count),
		observability.field_int('errors', error_count), observability.field_duration('latency',
		elapsed))

	resp := CreateTopicsResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}

	return resp.encode(version)
}

// DeleteTopics 핸들러 - 스토리지에서 토픽 삭제
fn (mut h Handler) handle_delete_topics(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_delete_topics_request(mut reader, version, is_flexible_version(.delete_topics,
		version))!

	h.logger.debug('Processing delete topics request', observability.field_int('topics',
		req.topics.len), observability.field_int('timeout_ms', req.timeout_ms))

	mut topics := []DeleteTopicsResponseTopic{}
	mut deleted_count := 0
	mut error_count := 0

	for t in req.topics {
		// v6+의 경우 topic_id에서 토픽 이름을 찾아야 할 수 있음
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		if version >= 6 && t.name.len == 0 && t.topic_id.len == 16 {
			// Look up topic by ID
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				h.logger.warn('Delete topic failed: topic not found by ID')
				error_count += 1
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

			h.logger.warn('Failed to delete topic', observability.field_string('topic',
				topic_name), observability.field_int('error_code', error_code), observability.field_string('error',
				err.str()))
			error_count += 1

			topics << DeleteTopicsResponseTopic{
				name:       topic_name
				topic_id:   topic_id
				error_code: error_code
			}
			continue
		}

		h.logger.info('Topic deleted', observability.field_string('topic', topic_name))
		deleted_count += 1

		// Success
		topics << DeleteTopicsResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			error_code: 0
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Delete topics completed', observability.field_int('deleted', deleted_count),
		observability.field_int('errors', error_count), observability.field_duration('latency',
		elapsed))

	resp := DeleteTopicsResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}

	return resp.encode(version)
}

// CreateTopics 요청 처리 (Frame 기반)
fn (mut h Handler) process_create_topics(req CreateTopicsRequest, version i16) !CreateTopicsResponse {
	mut topics := []CreateTopicsResponseTopic{}

	for t in req.topics {
		// config 맵을 domain.TopicConfig로 변환
		topic_config := domain.TopicConfig{
			retention_ms:        parse_config_i64(t.configs, 'retention.ms', 604800000)
			retention_bytes:     parse_config_i64(t.configs, 'retention.bytes', -1)
			segment_bytes:       parse_config_i64(t.configs, 'segment.bytes', 1073741824)
			cleanup_policy:      t.configs['cleanup.policy'] or { 'delete' }
			min_insync_replicas: parse_config_int(t.configs, 'min.insync.replicas', 1)
			max_message_bytes:   parse_config_int(t.configs, 'max.message.bytes', 1048576)
		}

		// 스토리지에 토픽 생성 시도
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

		// 파티션 할당 서비스가 있으면 파티션을 브로커에 할당
		if mut assigner := h.partition_assigner {
			// 활성 브로커 목록 조회
			mut brokers := []domain.BrokerInfo{}
			if mut registry := h.broker_registry {
				brokers = registry.list_active_brokers() or { []domain.BrokerInfo{} }
			}

			// 브로커가 없으면 자신을 브로커로 추가
			if brokers.len == 0 {
				brokers << domain.BrokerInfo{
					broker_id: h.broker_id
					host:      h.host
					port:      h.broker_port
				}
			}

			// 파티션 할당 수행
			assigner.assign_partitions(t.name, partitions, brokers) or {
				h.logger.warn('Failed to assign partitions', observability.field_string('topic',
					t.name), observability.field_int('partitions', partitions), observability.field_err_str(err.str()))
			}
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
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}
}

// DeleteTopics 요청 처리 (Frame 기반)
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
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}
}
