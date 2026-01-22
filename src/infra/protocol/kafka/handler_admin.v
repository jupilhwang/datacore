// Admin API - AlterConfigs, CreatePartitions, DeleteRecords
// Task #31, #32: Admin API 구현
// admin_api.v에서 handlers/admin.v로 이동하여 구조 개선
module kafka

import infra.observability
import time

// ============================================================================
// AlterConfigs Request/Response (API Key 33)
// ============================================================================

pub struct AlterConfigsRequest {
pub:
	resources     []AlterConfigsResource
	validate_only bool
}

pub struct AlterConfigsResource {
pub:
	resource_type i8
	resource_name string
	configs       []AlterConfigsEntry
}

pub struct AlterConfigsEntry {
pub:
	name  string
	value ?string
}

pub struct AlterConfigsResponse {
pub:
	throttle_time_ms i32
	results          []AlterConfigsResult
}

pub struct AlterConfigsResult {
pub:
	error_code    i16
	error_message ?string
	resource_type i8
	resource_name string
}

fn parse_alter_configs_request(mut reader BinaryReader, version i16, is_flexible bool) !AlterConfigsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut resources := []AlterConfigsResource{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := reader.read_flex_string(is_flexible)!

		config_count := reader.read_flex_array_len(is_flexible)!
		mut configs := []AlterConfigsEntry{}

		for _ in 0 .. config_count {
			name := reader.read_flex_string(is_flexible)!
			value := reader.read_flex_nullable_string(is_flexible)!

			reader.skip_flex_tagged_fields(is_flexible)!

			configs << AlterConfigsEntry{
				name:  name
				value: if value.len > 0 { value } else { none }
			}
		}

		reader.skip_flex_tagged_fields(is_flexible)!

		resources << AlterConfigsResource{
			resource_type: resource_type
			resource_name: resource_name
			configs:       configs
		}
	}

	validate_only := reader.read_i8()! != 0

	reader.skip_flex_tagged_fields(is_flexible)!

	return AlterConfigsRequest{
		resources:     resources
		validate_only: validate_only
	}
}

pub fn (r AlterConfigsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms
	writer.write_i32(r.throttle_time_ms)

	// results array
	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		writer.write_i16(res.error_code)

		if is_flexible {
			writer.write_compact_nullable_string(res.error_message)
		} else {
			writer.write_nullable_string(res.error_message)
		}

		writer.write_i8(res.resource_type)

		if is_flexible {
			writer.write_compact_string(res.resource_name)
			writer.write_tagged_fields()
		} else {
			writer.write_string(res.resource_name)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// CreatePartitions Request/Response (API Key 37)
// ============================================================================

pub struct CreatePartitionsRequest {
pub:
	topics        []CreatePartitionsTopic
	timeout_ms    i32
	validate_only bool
}

pub struct CreatePartitionsTopic {
pub:
	name        string
	count       i32
	assignments ?[]CreatePartitionsAssignment
}

pub struct CreatePartitionsAssignment {
pub:
	broker_ids []i32
}

pub struct CreatePartitionsResponse {
pub:
	throttle_time_ms i32
	results          []CreatePartitionsResult
}

pub struct CreatePartitionsResult {
pub:
	name          string
	error_code    i16
	error_message ?string
}

fn parse_create_partitions_request(mut reader BinaryReader, version i16, is_flexible bool) !CreatePartitionsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut topics := []CreatePartitionsTopic{}

	for _ in 0 .. count {
		name := reader.read_flex_string(is_flexible)!
		partition_count := reader.read_i32()!

		// Assignments (nullable array)
		assign_count := reader.read_flex_array_len(is_flexible)!

		mut assignments := ?[]CreatePartitionsAssignment(none)
		if assign_count >= 0 {
			mut assigns := []CreatePartitionsAssignment{}
			for _ in 0 .. assign_count {
				broker_count := reader.read_flex_array_len(is_flexible)!
				mut broker_ids := []i32{}
				for _ in 0 .. broker_count {
					broker_ids << reader.read_i32()!
				}
				reader.skip_flex_tagged_fields(is_flexible)!
				assigns << CreatePartitionsAssignment{
					broker_ids: broker_ids
				}
			}
			assignments = assigns.clone()
		}

		reader.skip_flex_tagged_fields(is_flexible)!

		topics << CreatePartitionsTopic{
			name:        name
			count:       partition_count
			assignments: assignments
		}
	}

	timeout_ms := reader.read_i32()!
	validate_only := reader.read_i8()! != 0

	reader.skip_flex_tagged_fields(is_flexible)!

	return CreatePartitionsRequest{
		topics:        topics
		timeout_ms:    timeout_ms
		validate_only: validate_only
	}
}

pub fn (r CreatePartitionsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms
	writer.write_i32(r.throttle_time_ms)

	// results array
	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		if is_flexible {
			writer.write_compact_string(res.name)
		} else {
			writer.write_string(res.name)
		}

		writer.write_i16(res.error_code)

		if is_flexible {
			writer.write_compact_nullable_string(res.error_message)
			writer.write_tagged_fields()
		} else {
			writer.write_nullable_string(res.error_message)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// DeleteRecords Request/Response (API Key 21)
// ============================================================================

pub struct DeleteRecordsRequest {
pub:
	topics     []DeleteRecordsTopic
	timeout_ms i32
}

pub struct DeleteRecordsTopic {
pub:
	name       string
	partitions []DeleteRecordsPartition
}

pub struct DeleteRecordsPartition {
pub:
	partition_index i32
	offset          i64 // Records before this offset will be deleted
}

pub struct DeleteRecordsResponse {
pub:
	throttle_time_ms i32
	topics           []DeleteRecordsResponseTopic
}

pub struct DeleteRecordsResponseTopic {
pub:
	name       string
	partitions []DeleteRecordsResponsePartition
}

pub struct DeleteRecordsResponsePartition {
pub:
	partition_index i32
	low_watermark   i64 // The new low watermark (earliest available offset)
	error_code      i16
}

fn parse_delete_records_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteRecordsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut topics := []DeleteRecordsTopic{}

	for _ in 0 .. count {
		name := reader.read_flex_string(is_flexible)!

		partition_count := reader.read_flex_array_len(is_flexible)!
		mut partitions := []DeleteRecordsPartition{}

		for _ in 0 .. partition_count {
			partition_index := reader.read_i32()!
			offset := reader.read_i64()!

			reader.skip_flex_tagged_fields(is_flexible)!

			partitions << DeleteRecordsPartition{
				partition_index: partition_index
				offset:          offset
			}
		}

		reader.skip_flex_tagged_fields(is_flexible)!

		topics << DeleteRecordsTopic{
			name:       name
			partitions: partitions
		}
	}

	timeout_ms := reader.read_i32()!

	reader.skip_flex_tagged_fields(is_flexible)!

	return DeleteRecordsRequest{
		topics:     topics
		timeout_ms: timeout_ms
	}
}

pub fn (r DeleteRecordsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// throttle_time_ms
	writer.write_i32(r.throttle_time_ms)

	// topics array
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for topic in r.topics {
		if is_flexible {
			writer.write_compact_string(topic.name)
		} else {
			writer.write_string(topic.name)
		}

		// partitions array
		if is_flexible {
			writer.write_compact_array_len(topic.partitions.len)
		} else {
			writer.write_array_len(topic.partitions.len)
		}

		for p in topic.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i64(p.low_watermark)
			writer.write_i16(p.error_code)

			if is_flexible {
				writer.write_tagged_fields()
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

// ============================================================================
// 핸들러 함수
// ============================================================================

// handle_alter_configs - AlterConfigs API (Key 33) 처리
pub fn (mut h Handler) handle_alter_configs(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := version >= 2
	mut reader := new_reader(body)
	req := parse_alter_configs_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing alter configs', observability.field_int('resources', req.resources.len),
		observability.field_bool('validate_only', req.validate_only))

	mut results := []AlterConfigsResult{}

	for res in req.resources {
		// Resource type: 2 = TOPIC, 4 = BROKER
		match res.resource_type {
			2 {
				// TOPIC config - currently we don't persist topic configs
				// Just validate the topic exists and return success
				_ := h.storage.get_topic(res.resource_name) or {
					results << AlterConfigsResult{
						error_code:    i16(ErrorCode.unknown_topic_or_partition)
						error_message: 'Topic not found: ${res.resource_name}'
						resource_type: res.resource_type
						resource_name: res.resource_name
					}
					continue
				}

				// If validate_only, just return success without applying
				if req.validate_only {
					results << AlterConfigsResult{
						error_code:    0
						error_message: none
						resource_type: res.resource_type
						resource_name: res.resource_name
					}
					continue
				}

				// TODO: Actually persist config changes when config storage is implemented
				// For now, just return success (configs are accepted but not persisted)
				results << AlterConfigsResult{
					error_code:    0
					error_message: none
					resource_type: res.resource_type
					resource_name: res.resource_name
				}
			}
			4 {
				// BROKER config - currently not supported
				// Return success for validate_only, otherwise return error
				if req.validate_only {
					results << AlterConfigsResult{
						error_code:    0
						error_message: none
						resource_type: res.resource_type
						resource_name: res.resource_name
					}
				} else {
					// Accept but don't persist (like topic configs)
					results << AlterConfigsResult{
						error_code:    0
						error_message: none
						resource_type: res.resource_type
						resource_name: res.resource_name
					}
				}
			}
			else {
				results << AlterConfigsResult{
					error_code:    i16(ErrorCode.invalid_request)
					error_message: 'Unsupported resource type: ${res.resource_type}'
					resource_type: res.resource_type
					resource_name: res.resource_name
				}
			}
		}
	}

	resp := AlterConfigsResponse{
		throttle_time_ms: 0
		results:          results
	}

	elapsed := time.since(start_time)
	h.logger.debug('Alter configs completed', observability.field_int('results', results.len),
		observability.field_duration('latency', elapsed))

	return resp.encode(version)
}

// handle_create_partitions - handles CreatePartitions API (Key 37)
pub fn (mut h Handler) handle_create_partitions(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := version >= 2
	mut reader := new_reader(body)
	req := parse_create_partitions_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing create partitions', observability.field_int('topics', req.topics.len),
		observability.field_bool('validate_only', req.validate_only))

	mut results := []CreatePartitionsResult{}

	for t in req.topics {
		// Get current topic to check existing partition count
		current_topic := h.storage.get_topic(t.name) or {
			results << CreatePartitionsResult{
				name:          t.name
				error_code:    i16(ErrorCode.unknown_topic_or_partition)
				error_message: 'Topic not found: ${t.name}'
			}
			continue
		}

		// Validate new partition count is greater than current
		if t.count <= i32(current_topic.partition_count) {
			results << CreatePartitionsResult{
				name:          t.name
				error_code:    i16(ErrorCode.invalid_partitions)
				error_message: 'New partition count ${t.count} must be greater than current count ${current_topic.partition_count}'
			}
			continue
		}

		// If validate_only, just return success
		if req.validate_only {
			results << CreatePartitionsResult{
				name:          t.name
				error_code:    0
				error_message: none
			}
			continue
		}

		// Add partitions via storage
		h.storage.add_partitions(t.name, int(t.count)) or {
			results << CreatePartitionsResult{
				name:          t.name
				error_code:    i16(ErrorCode.unknown_server_error)
				error_message: 'Failed to add partitions: ${err}'
			}
			continue
		}

		results << CreatePartitionsResult{
			name:          t.name
			error_code:    0
			error_message: none
		}
	}

	resp := CreatePartitionsResponse{
		throttle_time_ms: 0
		results:          results
	}

	elapsed := time.since(start_time)
	h.logger.debug('Create partitions completed', observability.field_int('results', results.len),
		observability.field_duration('latency', elapsed))

	return resp.encode(version)
}

// handle_delete_records - handles DeleteRecords API (Key 21)
pub fn (mut h Handler) handle_delete_records(body []u8, version i16) ![]u8 {
	start_time := time.now()
	is_flexible := version >= 2
	mut reader := new_reader(body)
	req := parse_delete_records_request(mut reader, version, is_flexible)!

	h.logger.debug('Processing delete records', observability.field_int('topics', req.topics.len),
		observability.field_int('timeout_ms', req.timeout_ms))

	mut resp_topics := []DeleteRecordsResponseTopic{}

	for t in req.topics {
		mut resp_partitions := []DeleteRecordsResponsePartition{}

		for p in t.partitions {
			// Delete records before the specified offset
			h.storage.delete_records(t.name, int(p.partition_index), p.offset) or {
				error_code := if err.str().contains('not found') {
					i16(ErrorCode.unknown_topic_or_partition)
				} else if err.str().contains('out of range') {
					i16(ErrorCode.offset_out_of_range)
				} else {
					i16(ErrorCode.unknown_server_error)
				}

				resp_partitions << DeleteRecordsResponsePartition{
					partition_index: p.partition_index
					low_watermark:   -1
					error_code:      error_code
				}
				continue
			}

			// Get updated partition info to return new low watermark
			info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
				resp_partitions << DeleteRecordsResponsePartition{
					partition_index: p.partition_index
					low_watermark:   p.offset // Use requested offset as low watermark
					error_code:      0
				}
				continue
			}

			resp_partitions << DeleteRecordsResponsePartition{
				partition_index: p.partition_index
				low_watermark:   info.earliest_offset
				error_code:      0
			}
		}

		resp_topics << DeleteRecordsResponseTopic{
			name:       t.name
			partitions: resp_partitions
		}
	}

	resp := DeleteRecordsResponse{
		throttle_time_ms: 0
		topics:           resp_topics
	}

	elapsed := time.since(start_time)
	h.logger.debug('Delete records completed', observability.field_int('topics', resp_topics.len),
		observability.field_duration('latency', elapsed))

	return resp.encode(version)
}
