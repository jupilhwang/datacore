// Kafka Protocol - Produce and ListOffsets
// Request/Response types, parsing, encoding, and handlers
module kafka

import domain

// ============================================================================
// Produce (API Key 0)
// ============================================================================

pub struct ProduceRequest {
pub:
	transactional_id ?string
	acks             i16
	timeout_ms       i32
	topic_data       []ProduceRequestTopic
}

pub struct ProduceRequestTopic {
pub:
	name           string
	topic_id       []u8 // v13+ (16 bytes UUID)
	partition_data []ProduceRequestPartition
}

pub struct ProduceRequestPartition {
pub:
	index   i32
	records []u8 // RecordBatch or MessageSet
}

pub struct ProduceResponse {
pub:
	topics           []ProduceResponseTopic
	throttle_time_ms i32
}

pub struct ProduceResponseTopic {
pub:
	name       string
	topic_id   []u8 // v13+ UUID
	partitions []ProduceResponsePartition
}

pub struct ProduceResponsePartition {
pub:
	index            i32
	error_code       i16
	base_offset      i64
	log_append_time  i64
	log_start_offset i64
}

fn parse_produce_request(mut reader BinaryReader, version i16, is_flexible bool) !ProduceRequest {
	mut transactional_id := ?string(none)
	if version >= 3 {
		if is_flexible {
			str := reader.read_compact_string()!
			transactional_id = if str.len > 0 { str } else { none }
		} else {
			str := reader.read_nullable_string()!
			transactional_id = if str.len > 0 { str } else { none }
		}
	}

	acks := reader.read_i16()!
	timeout_ms := reader.read_i32()!

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut topic_data := []ProduceRequestTopic{}
	for _ in 0 .. topic_count {
		mut name := ''
		mut topic_id := []u8{}

		if version >= 13 {
			topic_id = reader.read_uuid()!
		} else if is_flexible {
			name = reader.read_compact_string()!
		} else {
			name = reader.read_string()!
		}

		partition_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		mut partition_data := []ProduceRequestPartition{}
		for _ in 0 .. partition_count {
			index := reader.read_i32()!
			records := if is_flexible {
				reader.read_compact_bytes()!
			} else {
				reader.read_bytes()!
			}

			partition_data << ProduceRequestPartition{
				index:   index
				records: records
			}

			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topic_data << ProduceRequestTopic{
			name:           name
			topic_id:       topic_id
			partition_data: partition_data
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	return ProduceRequest{
		transactional_id: transactional_id
		acks:             acks
		timeout_ms:       timeout_ms
		topic_data:       topic_data
	}
}

pub fn (r ProduceResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.base_offset)
			if version >= 2 {
				writer.write_i64(p.log_append_time)
			}
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			if version >= 8 {
				if is_flexible {
					writer.write_compact_array_len(0)
				} else {
					writer.write_array_len(0)
				}
			}
			if version >= 8 {
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
	}

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// Process produce request (Frame-based)
fn (mut h Handler) process_produce(req ProduceRequest, version i16) !ProduceResponse {
	// Validate transactional producer if transactional_id is present
	if txn_id := req.transactional_id {
		if txn_id.len > 0 {
			if mut txn_coord := h.txn_coordinator {
				meta := txn_coord.get_transaction(txn_id) or {
					return h.build_produce_error_response_typed(req, ErrorCode.transactional_id_not_found)
				}

				if meta.state != .ongoing {
					return h.build_produce_error_response_typed(req, ErrorCode.invalid_txn_state)
				}

				for t in req.topic_data {
					topic_name := if t.name.len > 0 {
						t.name
					} else {
						if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
							topic_meta.name
						} else {
							continue
						}
					}

					for p in t.partition_data {
						mut found := false
						for tp in meta.topic_partitions {
							if tp.topic == topic_name && tp.partition == int(p.index) {
								found = true
								break
							}
						}
						if !found {
							return h.build_produce_error_response_typed(req, ErrorCode.invalid_txn_state)
						}
					}
				}
			} else {
				return h.build_produce_error_response_typed(req, ErrorCode.coordinator_not_available)
			}
		}
	}

	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				mut partitions := []ProduceResponsePartition{}
				for p in t.partition_data {
					partitions << ProduceResponsePartition{
						index:            p.index
						error_code:       i16(ErrorCode.unknown_topic_id)
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					}
				}
				topics << ProduceResponseTopic{
					name:       topic_name
					topic_id:   topic_id
					partitions: partitions
				}
				continue
			}
		}

		mut partitions := []ProduceResponsePartition{}
		for p in t.partition_data {
			parsed := parse_record_batch(p.records) or {
				partitions << ProduceResponsePartition{
					index:            p.index
					error_code:       i16(ErrorCode.corrupt_message)
					base_offset:      -1
					log_append_time:  -1
					log_start_offset: -1
				}
				continue
			}

			if parsed.records.len == 0 {
				partitions << ProduceResponsePartition{
					index:            p.index
					error_code:       0
					base_offset:      0
					log_append_time:  -1
					log_start_offset: 0
				}
				continue
			}

			result := h.storage.append(topic_name, int(p.index), parsed.records) or {
				if err.str().contains('not found') {
					num_partitions := if int(p.index) >= 1 { int(p.index) + 1 } else { 1 }
					h.storage.create_topic(topic_name, num_partitions, domain.TopicConfig{}) or {
						partitions << ProduceResponsePartition{
							index:            p.index
							error_code:       i16(ErrorCode.unknown_server_error)
							base_offset:      -1
							log_append_time:  -1
							log_start_offset: -1
						}
						continue
					}
					retry_result := h.storage.append(topic_name, int(p.index), parsed.records) or {
						partitions << ProduceResponsePartition{
							index:            p.index
							error_code:       i16(ErrorCode.unknown_server_error)
							base_offset:      -1
							log_append_time:  -1
							log_start_offset: -1
						}
						continue
					}
					retry_result
				} else {
					error_code := if err.str().contains('out of range') {
						i16(ErrorCode.unknown_topic_or_partition)
					} else {
						i16(ErrorCode.unknown_server_error)
					}
					partitions << ProduceResponsePartition{
						index:            p.index
						error_code:       error_code
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					}
					continue
				}
			}

			partitions << ProduceResponsePartition{
				index:            p.index
				error_code:       0
				base_offset:      result.base_offset
				log_append_time:  result.log_append_time
				log_start_offset: result.log_start_offset
			}
		}
		topics << ProduceResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			partitions: partitions
		}
	}

	return ProduceResponse{
		topics:           topics
		throttle_time_ms: 0
	}
}

// Legacy handler - delegates to process_produce
fn (mut h Handler) handle_produce(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_produce_request(mut reader, version, is_flexible_version(.produce, version))!
	resp := h.process_produce(req, version)!
	return resp.encode(version)
}

fn (h Handler) build_produce_error_response_typed(req ProduceRequest, error_code ErrorCode) ProduceResponse {
	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		mut partitions := []ProduceResponsePartition{}
		for p in t.partition_data {
			partitions << ProduceResponsePartition{
				index:            p.index
				error_code:       i16(error_code)
				base_offset:      -1
				log_append_time:  -1
				log_start_offset: -1
			}
		}
		topics << ProduceResponseTopic{
			name:       t.name
			topic_id:   t.topic_id.clone()
			partitions: partitions
		}
	}
	return ProduceResponse{
		topics:           topics
		throttle_time_ms: 0
	}
}

// build_produce_error_response builds a ProduceResponse with error for all partitions (legacy)
fn build_produce_error_response(req ProduceRequest, error_code i16, version i16) []u8 {
	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		mut partitions := []ProduceResponsePartition{}
		for p in t.partition_data {
			partitions << ProduceResponsePartition{
				index:            p.index
				error_code:       error_code
				base_offset:      -1
				log_append_time:  -1
				log_start_offset: -1
			}
		}
		topics << ProduceResponseTopic{
			name:       t.name
			topic_id:   t.topic_id.clone()
			partitions: partitions
		}
	}
	return ProduceResponse{
		topics:           topics
		throttle_time_ms: 0
	}.encode(version)
}

// Note: Fetch (API Key 1) has been moved to handler_fetch.v
// Note: ListOffsets (API Key 2) has been moved to handler_list_offsets.v
// Note: RecordBatch encoding and CRC32-C calculation have been moved to record_batch.v
