// Kafka Protocol - Produce, Fetch, ListOffsets
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

// ============================================================================
// Fetch (API Key 1)
// ============================================================================

pub struct FetchRequest {
pub:
	replica_id            i32
	max_wait_ms           i32
	min_bytes             i32
	max_bytes             i32
	isolation_level       i8
	topics                []FetchRequestTopic
	forgotten_topics_data []FetchRequestForgottenTopic
}

pub struct FetchRequestForgottenTopic {
pub:
	name       string
	topic_id   []u8
	partitions []i32
}

pub struct FetchRequestTopic {
pub:
	name       string
	topic_id   []u8 // UUID for v13+
	partitions []FetchRequestPartition
}

pub struct FetchRequestPartition {
pub:
	partition           i32
	fetch_offset        i64
	partition_max_bytes i32
}

pub struct FetchResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	session_id       i32
	topics           []FetchResponseTopic
}

pub struct FetchResponseTopic {
pub:
	name       string
	topic_id   []u8 // UUID for v13+
	partitions []FetchResponsePartition
}

pub struct FetchResponsePartition {
pub:
	partition_index    i32
	error_code         i16
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
	records            []u8
}

fn parse_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequest {
	mut replica_id := i32(-1)
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!

	mut max_bytes := i32(0x7fffffff)
	if version >= 3 {
		max_bytes = reader.read_i32()!
	}

	mut isolation_level := i8(0)
	if version >= 4 {
		isolation_level = reader.read_i8()!
	}

	if version >= 7 {
		_ = reader.read_i32()! // session_id
		_ = reader.read_i32()! // session_epoch
	}

	topic_count := if is_flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	mut topics := []FetchRequestTopic{}
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

		mut partitions := []FetchRequestPartition{}
		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			if version >= 9 {
				_ = reader.read_i32()! // current_leader_epoch
			}

			fetch_offset := reader.read_i64()!

			if version >= 5 {
				_ = reader.read_i64()! // log_start_offset
			}

			partition_max_bytes := reader.read_i32()!

			partitions << FetchRequestPartition{
				partition:           partition
				fetch_offset:        fetch_offset
				partition_max_bytes: partition_max_bytes
			}

			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}

		topics << FetchRequestTopic{
			name:       name
			topic_id:   topic_id
			partitions: partitions
		}

		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}

	mut forgotten_topics_data := []FetchRequestForgottenTopic{}
	if version >= 7 {
		forgotten_count := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		for _ in 0 .. forgotten_count {
			mut fname := ''
			mut ftopic_id := []u8{}
			if version >= 13 {
				ftopic_id = reader.read_uuid()!
			} else if is_flexible {
				fname = reader.read_compact_string()!
			} else {
				fname = reader.read_string()!
			}
			fpartition_count := if is_flexible {
				reader.read_compact_array_len()!
			} else {
				reader.read_array_len()!
			}
			mut fpartitions := []i32{}
			for _ in 0 .. fpartition_count {
				fpartitions << reader.read_i32()!
			}
			forgotten_topics_data << FetchRequestForgottenTopic{
				name:       fname
				topic_id:   ftopic_id
				partitions: fpartitions
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
	}

	if version >= 11 {
		_ = if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}

	return FetchRequest{
		replica_id:            replica_id
		max_wait_ms:           max_wait_ms
		min_bytes:             min_bytes
		max_bytes:             max_bytes
		isolation_level:       isolation_level
		topics:                topics
		forgotten_topics_data: forgotten_topics_data
	}
}

pub fn (r FetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 12
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	if version >= 7 {
		writer.write_i16(r.error_code)
		writer.write_i32(r.session_id)
	}

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
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.high_watermark)
			if version >= 4 {
				writer.write_i64(p.last_stable_offset)
			}
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			if version >= 4 {
				if is_flexible {
					writer.write_uvarint(1)
				} else {
					writer.write_array_len(0)
				}
			}
			if version >= 11 {
				writer.write_i32(-1)
			}
			if is_flexible {
				writer.write_compact_bytes(p.records)
			} else {
				if p.records.len == 0 {
					writer.write_i32(0)
				} else {
					writer.write_i32(i32(p.records.len))
					writer.write_raw(p.records)
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

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// Process fetch request (Frame-based)
fn (mut h Handler) process_fetch(req FetchRequest, version i16) !FetchResponse {
	eprintln('[Fetch] Request: version=${version}, topics=${req.topics.len}')

	mut topics := []FetchResponseTopic{}
	for t in req.topics {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()
		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			}
		}

		mut partitions := []FetchResponsePartition{}
		for p in t.partitions {
			result := h.storage.fetch(topic_name, int(p.partition), p.fetch_offset, p.partition_max_bytes) or {
				error_code := if err.str().contains('not found') {
					i16(ErrorCode.unknown_topic_or_partition)
				} else if err.str().contains('out of range') {
					i16(ErrorCode.offset_out_of_range)
				} else {
					i16(ErrorCode.unknown_server_error)
				}

				partitions << FetchResponsePartition{
					partition_index:    p.partition
					error_code:         error_code
					high_watermark:     0
					last_stable_offset: 0
					log_start_offset:   0
					records:            []u8{}
				}
				continue
			}

			records_data := encode_record_batch_zerocopy(result.records, p.fetch_offset)
			eprintln('[Fetch] Topic=${topic_name} partition=${p.partition} has ${result.records.len} records - returning ${records_data.len} bytes')

			partitions << FetchResponsePartition{
				partition_index:    p.partition
				error_code:         0
				high_watermark:     result.high_watermark
				last_stable_offset: result.last_stable_offset
				log_start_offset:   result.log_start_offset
				records:            records_data
			}
		}
		topics << FetchResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			partitions: partitions
		}
	}

	return FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           topics
	}
}

// Legacy handler - uses SimpleFetchRequest for compatibility with zerocopy_fetch.v
fn (mut h Handler) handle_fetch(body []u8, version i16) ![]u8 {
	flexible := is_flexible_version(.fetch, version)
	simple_req := parse_fetch_request_simple(body, version, flexible)!

	// Convert SimpleFetchRequest to FetchRequest
	req := simple_fetch_to_fetch_request(simple_req)
	resp := h.process_fetch(req, version)!

	encoded := resp.encode(version)
	eprintln('[Fetch] Response version=${version}, size=${encoded.len} bytes')
	if encoded.len > 0 && encoded.len < 200 {
		eprintln('[Fetch] First 100 bytes: ${encoded[..if encoded.len > 100 {
			100
		} else {
			encoded.len
		}].hex()}')
	}

	return encoded
}

// Convert SimpleFetchRequest to FetchRequest
fn simple_fetch_to_fetch_request(simple SimpleFetchRequest) FetchRequest {
	mut topics := []FetchRequestTopic{}
	for t in simple.topics {
		mut partitions := []FetchRequestPartition{}
		for p in t.partitions {
			partitions << FetchRequestPartition{
				partition:           p.partition
				fetch_offset:        p.fetch_offset
				partition_max_bytes: p.partition_max_bytes
			}
		}
		topics << FetchRequestTopic{
			name:       t.name
			topic_id:   t.topic_id.clone()
			partitions: partitions
		}
	}

	mut forgotten := []FetchRequestForgottenTopic{}
	for f in simple.forgotten_topics {
		forgotten << FetchRequestForgottenTopic{
			name:       f.name
			topic_id:   f.topic_id.clone()
			partitions: f.partitions.clone()
		}
	}

	return FetchRequest{
		replica_id:            simple.replica_id
		max_wait_ms:           simple.max_wait_ms
		min_bytes:             simple.min_bytes
		max_bytes:             simple.max_bytes
		isolation_level:       simple.isolation_level
		topics:                topics
		forgotten_topics_data: forgotten
	}
}

// ============================================================================
// ListOffsets (API Key 2)
// ============================================================================

pub struct ListOffsetsRequest {
pub:
	replica_id      i32
	isolation_level i8
	topics          []ListOffsetsRequestTopic
}

pub struct ListOffsetsRequestTopic {
pub:
	name       string
	partitions []ListOffsetsRequestPartition
}

pub struct ListOffsetsRequestPartition {
pub:
	partition_index i32
	timestamp       i64
}

pub struct ListOffsetsResponse {
pub:
	throttle_time_ms i32
	topics           []ListOffsetsResponseTopic
}

pub struct ListOffsetsResponseTopic {
pub:
	name       string
	partitions []ListOffsetsResponsePartition
}

pub struct ListOffsetsResponsePartition {
pub:
	partition_index i32
	error_code      i16
	timestamp       i64
	offset          i64
	leader_epoch    i32
}

fn parse_list_offsets_request(mut reader BinaryReader, version i16, is_flexible bool) !ListOffsetsRequest {
	replica_id := reader.read_i32()!
	mut isolation_level := i8(0)
	if version >= 2 {
		isolation_level = reader.read_i8()!
	}

	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut topics := []ListOffsetsRequestTopic{}
	for _ in 0 .. count {
		name := if is_flexible { reader.read_compact_string()! } else { reader.read_string()! }
		pcount := if is_flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}
		mut partitions := []ListOffsetsRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			if version >= 4 {
				_ = reader.read_i32()! // current_leader_epoch
			}
			ts := reader.read_i64()!
			partitions << ListOffsetsRequestPartition{
				partition_index: pi
				timestamp:       ts
			}
			if is_flexible {
				reader.skip_tagged_fields()!
			}
		}
		topics << ListOffsetsRequestTopic{
			name:       name
			partitions: partitions
		}
		if is_flexible {
			reader.skip_tagged_fields()!
		}
	}
	return ListOffsetsRequest{
		replica_id:      replica_id
		isolation_level: isolation_level
		topics:          topics
	}
}

pub fn (r ListOffsetsResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
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
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_string(t.name)
			writer.write_array_len(t.partitions.len)
		}
		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			if version >= 1 {
				writer.write_i64(p.timestamp)
				writer.write_i64(p.offset)
			}
			if version >= 4 {
				writer.write_i32(p.leader_epoch)
			}
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

// Process list offsets request (Frame-based)
fn (mut h Handler) process_list_offsets(req ListOffsetsRequest, version i16) !ListOffsetsResponse {
	mut topics := []ListOffsetsResponseTopic{}
	for t in req.topics {
		mut partitions := []ListOffsetsResponsePartition{}
		for p in t.partitions {
			info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
				partitions << ListOffsetsResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
					timestamp:       -1
					offset:          -1
					leader_epoch:    -1
				}
				continue
			}

			offset := match p.timestamp {
				-1 { info.latest_offset }
				-2 { info.earliest_offset }
				else { info.latest_offset }
			}

			partitions << ListOffsetsResponsePartition{
				partition_index: p.partition_index
				error_code:      0
				timestamp:       p.timestamp
				offset:          offset
				leader_epoch:    0
			}
		}
		topics << ListOffsetsResponseTopic{
			name:       t.name
			partitions: partitions
		}
	}

	return ListOffsetsResponse{
		throttle_time_ms: 0
		topics:           topics
	}
}

// Legacy handler - delegates to process_list_offsets
fn (mut h Handler) handle_list_offsets(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_list_offsets_request(mut reader, version, is_flexible_version(.list_offsets, version))!
	resp := h.process_list_offsets(req, version)!
	return resp.encode(version)
}

// ============================================================================
// SimpleFetchRequest Parser (for Fetch request parsing)
// ============================================================================

// SimpleFetchRequest: lightweight Fetch request structure
pub struct SimpleFetchRequest {
pub:
	replica_id       i32
	max_wait_ms      i32
	min_bytes        i32
	max_bytes        i32
	isolation_level  i8
	session_id       i32 // Stateless: ignored, always respond with 0
	session_epoch    i32 // Stateless: ignored
	topics           []SimpleFetchTopic
	forgotten_topics []ForgottenTopic // Stateless: ignored
	rack_id          string
}

// SimpleFetchTopic holds topic data from fetch request
pub struct SimpleFetchTopic {
pub:
	topic_id   []u8
	name       string
	partitions []SimpleFetchPartition
}

// SimpleFetchPartition holds partition data from fetch request
pub struct SimpleFetchPartition {
pub:
	partition            i32
	current_leader_epoch i32
	fetch_offset         i64
	last_fetched_epoch   i32
	log_start_offset     i64
	partition_max_bytes  i32
}

// ForgottenTopic for session tracking
pub struct ForgottenTopic {
pub:
	topic_id   []u8
	name       string
	partitions []i32
}

// parse_fetch_request_simple: lightweight Fetch request parser
pub fn parse_fetch_request_simple(data []u8, version i16, flexible bool) !SimpleFetchRequest {
	first_bytes := if data.len >= 40 { data[..40].hex() } else { data.hex() }
	eprintln('[DEBUG] parse_fetch_request_simple: version=${version} flexible=${flexible} data.len=${data.len}')
	eprintln('[DEBUG] first 40 bytes: ${first_bytes}')

	mut reader := new_reader(data)

	// In v15+, replica_id was removed from main body and replaced with replica_state tagged field
	// For v0-14: replica_id (INT32) is first field
	// For v15+: replica_id is NOT in the body (it's in tagged fields as replica_state)
	mut replica_id := i32(-1) // Default for consumers
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!

	// v3+ has max_bytes
	max_bytes := if version >= 3 { reader.read_i32()! } else { i32(0x7FFFFFFF) }

	// v4+ has isolation level
	isolation_level := if version >= 4 { reader.read_i8()! } else { i8(0) }

	// v7+ has session_id and session_epoch
	session_id := if version >= 7 { reader.read_i32()! } else { i32(0) }
	session_epoch := if version >= 7 { reader.read_i32()! } else { i32(-1) }

	eprintln('[DEBUG] Fetch v${version}: parsed fields - replica_id=${replica_id}, max_wait_ms=${max_wait_ms}, min_bytes=${min_bytes}, max_bytes=${max_bytes}')
	eprintln('[DEBUG] Fetch: isolation_level=${isolation_level}, session_id=${session_id}, session_epoch=${session_epoch}')
	eprintln('[DEBUG] Fetch: after header, pos=${reader.position()}, remaining=${reader.remaining()}')

	// Parse topics
	mut topics := []SimpleFetchTopic{}
	eprintln('[DEBUG] Fetch: before reading topic_count, pos=${reader.position()}, remaining=${reader.remaining()}')
	topic_count := if flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	eprintln('[DEBUG] Fetch: topic_count=${topic_count}, pos=${reader.position()}')

	for ti in 0 .. topic_count {
		// v13+ uses topic_id
		mut topic_id := []u8{}
		mut topic_name := ''

		if version >= 13 {
			topic_id = reader.read_uuid()!
			eprintln('[DEBUG] Fetch: topic ${ti} - read topic_id=${topic_id.hex()}')
		}
		if version < 13 || !flexible {
			topic_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
			eprintln('[DEBUG] Fetch: topic ${ti} - read topic_name=${topic_name}')
		}

		// Parse partitions
		mut partitions := []SimpleFetchPartition{}
		partition_count := if flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			// v9+ has current_leader_epoch
			current_leader_epoch := if version >= 9 { reader.read_i32()! } else { i32(-1) }

			fetch_offset := reader.read_i64()!

			// v12+ has last_fetched_epoch
			last_fetched_epoch := if version >= 12 { reader.read_i32()! } else { i32(-1) }

			// v5+ has log_start_offset
			log_start_offset := if version >= 5 { reader.read_i64()! } else { i64(-1) }

			partition_max_bytes := reader.read_i32()!

			if flexible {
				reader.skip_tagged_fields()!
			}

			partitions << SimpleFetchPartition{
				partition:            partition
				current_leader_epoch: current_leader_epoch
				fetch_offset:         fetch_offset
				last_fetched_epoch:   last_fetched_epoch
				log_start_offset:     log_start_offset
				partition_max_bytes:  partition_max_bytes
			}
		}

		if flexible {
			reader.skip_tagged_fields()!
		}

		topics << SimpleFetchTopic{
			topic_id:   topic_id
			name:       topic_name
			partitions: partitions
		}
	}

	// v7+ has forgotten topics
	mut forgotten_topics := []ForgottenTopic{}
	if version >= 7 {
		forgotten_count := if flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		for _ in 0 .. forgotten_count {
			mut f_topic_id := []u8{}
			mut f_name := ''

			if version >= 13 {
				f_topic_id = reader.read_uuid()!
			}
			if version < 13 || !flexible {
				f_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
			}

			mut f_partitions := []i32{}
			f_partition_count := if flexible {
				reader.read_compact_array_len()!
			} else {
				reader.read_array_len()!
			}

			for _ in 0 .. f_partition_count {
				f_partitions << reader.read_i32()!
			}

			if flexible {
				reader.skip_tagged_fields()!
			}

			forgotten_topics << ForgottenTopic{
				topic_id:   f_topic_id
				name:       f_name
				partitions: f_partitions
			}
		}
	}

	// v11+ has rack_id
	rack_id := if version >= 11 {
		if flexible { reader.read_compact_string()! } else { reader.read_string()! }
	} else {
		''
	}

	if flexible {
		reader.skip_tagged_fields()!
	}

	return SimpleFetchRequest{
		replica_id:       replica_id
		max_wait_ms:      max_wait_ms
		min_bytes:        min_bytes
		max_bytes:        max_bytes
		isolation_level:  isolation_level
		session_id:       session_id
		session_epoch:    session_epoch
		topics:           topics
		forgotten_topics: forgotten_topics
		rack_id:          rack_id
	}
}

// ============================================================================
// RecordBatch Encoding
// ============================================================================

// encode_record_batch_zerocopy encodes records into Kafka RecordBatch format
pub fn encode_record_batch_zerocopy(records []domain.Record, base_offset i64) []u8 {
	if records.len == 0 {
		return []u8{}
	}

	// Use optimized encoding path
	// For now, delegate to existing encoder but with pre-allocated buffer
	_ = estimate_batch_size(records)

	// RecordBatch header
	mut writer := new_writer()

	// Base offset (8 bytes)
	writer.write_i64(base_offset)

	// Placeholder for batch length (4 bytes) - will be filled later
	_ = writer.data.len
	writer.write_i32(0)

	// Partition leader epoch (4 bytes)
	writer.write_i32(-1)

	// Magic byte (1 byte) - version 2
	writer.write_i8(2)

	// CRC placeholder (4 bytes)
	crc_pos := writer.data.len
	writer.write_i32(0)

	// Attributes (2 bytes) - no compression, no timestamps
	writer.write_i16(0)

	// Last offset delta
	writer.write_i32(i32(records.len - 1))

	// First timestamp (use first record)
	first_timestamp := if records.len > 0 { records[0].timestamp.unix_milli() } else { i64(0) }
	writer.write_i64(first_timestamp)

	// Max timestamp (use last record)
	max_timestamp := if records.len > 0 {
		records[records.len - 1].timestamp.unix_milli()
	} else {
		first_timestamp
	}
	writer.write_i64(max_timestamp)

	// Producer ID (-1 for non-idempotent)
	writer.write_i64(-1)

	// Producer epoch (-1 for non-idempotent)
	writer.write_i16(-1)

	// Base sequence (-1 for non-idempotent)
	writer.write_i32(-1)

	// Record count
	writer.write_i32(i32(records.len))

	// Encode records
	for i, record in records {
		offset_delta := i32(i)
		timestamp_delta := record.timestamp.unix_milli() - first_timestamp

		// Calculate record size first
		mut record_writer := new_writer()
		record_writer.write_varint(i64(0)) // length placeholder
		record_writer.write_i8(0) // attributes
		record_writer.write_varint(timestamp_delta)
		record_writer.write_varint(i64(offset_delta))

		// Key
		if record.key.len > 0 {
			record_writer.write_varint(i64(record.key.len))
			for b in record.key {
				record_writer.write_i8(i8(b))
			}
		} else {
			record_writer.write_varint(-1)
		}

		// Value
		if record.value.len > 0 {
			record_writer.write_varint(i64(record.value.len))
			for b in record.value {
				record_writer.write_i8(i8(b))
			}
		} else {
			record_writer.write_varint(-1)
		}

		// Headers (none)
		record_writer.write_varint(0)

		// Calculate actual record size (without length field)
		record_data := record_writer.bytes()
		record_size := record_data.len - 1 // -1 for the placeholder

		// Write actual record with correct length
		writer.write_varint(i64(record_size))
		writer.write_i8(0) // attributes
		writer.write_varint(timestamp_delta)
		writer.write_varint(i64(offset_delta))

		// Key
		if record.key.len > 0 {
			writer.write_varint(i64(record.key.len))
			for b in record.key {
				writer.write_i8(i8(b))
			}
		} else {
			writer.write_varint(-1)
		}

		// Value
		if record.value.len > 0 {
			writer.write_varint(i64(record.value.len))
			for b in record.value {
				writer.write_i8(i8(b))
			}
		} else {
			writer.write_varint(-1)
		}

		// Headers (none)
		writer.write_varint(0)
	}

	// Get final data
	mut batch_data := writer.bytes()

	// Calculate and fill batch length (total - base_offset - batch_length_field)
	batch_length := batch_data.len - 12
	batch_data[8] = u8(batch_length >> 24)
	batch_data[9] = u8(batch_length >> 16)
	batch_data[10] = u8(batch_length >> 8)
	batch_data[11] = u8(batch_length)

	// Calculate CRC32c for the batch (from attributes to end)
	crc := calculate_crc32c(batch_data[crc_pos + 4..])
	batch_data[crc_pos] = u8(crc >> 24)
	batch_data[crc_pos + 1] = u8(crc >> 16)
	batch_data[crc_pos + 2] = u8(crc >> 8)
	batch_data[crc_pos + 3] = u8(crc)

	return batch_data
}

// estimate_batch_size estimates the size of encoded record batch
fn estimate_batch_size(records []domain.Record) int {
	// Base batch overhead: 61 bytes (header)
	mut size := 61

	for record in records {
		// Per-record overhead: ~20 bytes (varints, attributes)
		size += 20
		size += record.key.len
		size += record.value.len
	}

	return size
}

// calculate_crc32c calculates CRC32-C checksum (Castagnoli polynomial)
fn calculate_crc32c(data []u8) u32 {
	// CRC32-C polynomial table (Castagnoli)
	crc_table := [
		u32(0x00000000),
		0xF26B8303,
		0xE13B70F7,
		0x1350F3F4,
		0xC79A971F,
		0x35F1141C,
		0x26A1E7E8,
		0xD4CA64EB,
		0x8AD958CF,
		0x78B2DBCC,
		0x6BE22838,
		0x9989AB3B,
		0x4D43CFD0,
		0xBF284CD3,
		0xAC78BF27,
		0x5E133C24,
		0x105EC76F,
		0xE235446C,
		0xF165B798,
		0x030E349B,
		0xD7C45070,
		0x25AFD373,
		0x36FF2087,
		0xC494A384,
		0x9A879FA0,
		0x68EC1CA3,
		0x7BBCEF57,
		0x89D76C54,
		0x5D1D08BF,
		0xAF768BBC,
		0xBC267848,
		0x4E4DFB4B,
		0x20BD8EDE,
		0xD2D60DDD,
		0xC186FE29,
		0x33ED7D2A,
		0xE72719C1,
		0x154C9AC2,
		0x061C6936,
		0xF477EA35,
		0xAA64D611,
		0x580F5512,
		0x4B5FA6E6,
		0xB93425E5,
		0x6DFE410E,
		0x9F95C20D,
		0x8CC531F9,
		0x7EAEB2FA,
		0x30E349B1,
		0xC288CAB2,
		0xD1D83946,
		0x23B3BA45,
		0xF779DEAE,
		0x05125DAD,
		0x1642AE59,
		0xE4292D5A,
		0xBA3A117E,
		0x4851927D,
		0x5B016189,
		0xA96AE28A,
		0x7DA08661,
		0x8FCB0562,
		0x9C9BF696,
		0x6EF07595,
		0x417B1DBC,
		0xB3109EBF,
		0xA0406D4B,
		0x522BEE48,
		0x86E18AA3,
		0x748A09A0,
		0x67DAFA54,
		0x95B17957,
		0xCBA24573,
		0x39C9C670,
		0x2A993584,
		0xD8F2B687,
		0x0C38D26C,
		0xFE53516F,
		0xED03A29B,
		0x1F682198,
		0x5125DAD3,
		0xA34E59D0,
		0xB01EAA24,
		0x42752927,
		0x96BF4DCC,
		0x64D4CECF,
		0x77843D3B,
		0x85EFBE38,
		0xDBFC821C,
		0x2997011F,
		0x3AC7F2EB,
		0xC8AC71E8,
		0x1C661503,
		0xEE0D9600,
		0xFD5D65F4,
		0x0F36E6F7,
		0x61C69362,
		0x93AD1061,
		0x80FDE395,
		0x72966096,
		0xA65C047D,
		0x5437877E,
		0x4767748A,
		0xB50CF789,
		0xEB1FCBAD,
		0x197448AE,
		0x0A24BB5A,
		0xF84F3859,
		0x2C855CB2,
		0xDEEEDFB1,
		0xCDBE2C45,
		0x3FD5AF46,
		0x7198540D,
		0x83F3D70E,
		0x90A324FA,
		0x62C8A7F9,
		0xB602C312,
		0x44694011,
		0x5739B3E5,
		0xA55230E6,
		0xFB410CC2,
		0x092A8FC1,
		0x1A7A7C35,
		0xE811FF36,
		0x3CDB9BDD,
		0xCEB018DE,
		0xDDE0EB2A,
		0x2F8B6829,
		0x82F63B78,
		0x709DB87B,
		0x63CD4B8F,
		0x91A6C88C,
		0x456CAC67,
		0xB7072F64,
		0xA457DC90,
		0x563C5F93,
		0x082F63B7,
		0xFA44E0B4,
		0xE9141340,
		0x1B7F9043,
		0xCFB5F4A8,
		0x3DDE77AB,
		0x2E8E845F,
		0xDCE5075C,
		0x92A8FC17,
		0x60C37F14,
		0x73938CE0,
		0x81F80FE3,
		0x55326B08,
		0xA759E80B,
		0xB4091BFF,
		0x466298FC,
		0x1871A4D8,
		0xEA1A27DB,
		0xF94AD42F,
		0x0B21572C,
		0xDFEB33C7,
		0x2D80B0C4,
		0x3ED04330,
		0xCCBBC033,
		0xA24BB5A6,
		0x502036A5,
		0x4370C551,
		0xB11B4652,
		0x65D122B9,
		0x97BAA1BA,
		0x84EA524E,
		0x7681D14D,
		0x2892ED69,
		0xDAF96E6A,
		0xC9A99D9E,
		0x3BC21E9D,
		0xEF087A76,
		0x1D63F975,
		0x0E330A81,
		0xFC588982,
		0xB21572C9,
		0x407EF1CA,
		0x532E023E,
		0xA145813D,
		0x758FE5D6,
		0x87E466D5,
		0x94B49521,
		0x66DF1622,
		0x38CC2A06,
		0xCAA7A905,
		0xD9F75AF1,
		0x2B9CD9F2,
		0xFF56BD19,
		0x0D3D3E1A,
		0x1E6DCDEE,
		0xEC064EED,
		0xC38D26C4,
		0x31E6A5C7,
		0x22B65633,
		0xD0DDD530,
		0x0417B1DB,
		0xF67C32D8,
		0xE52CC12C,
		0x1747422F,
		0x49547E0B,
		0xBB3FFD08,
		0xA86F0EFC,
		0x5A048DFF,
		0x8ECEE914,
		0x7CA56A17,
		0x6FF599E3,
		0x9D9E1AE0,
		0xD3D3E1AB,
		0x21B862A8,
		0x32E8915C,
		0xC083125F,
		0x144976B4,
		0xE622F5B7,
		0xF5720643,
		0x07198540,
		0x590AB964,
		0xAB613A67,
		0xB831C993,
		0x4A5A4A90,
		0x9E902E7B,
		0x6CFBAD78,
		0x7FAB5E8C,
		0x8DC0DD8F,
		0xE330A81A,
		0x115B2B19,
		0x020BD8ED,
		0xF0605BEE,
		0x24AA3F05,
		0xD6C1BC06,
		0xC5914FF2,
		0x37FACCF1,
		0x69E9F0D5,
		0x9B8273D6,
		0x88D28022,
		0x7AB90321,
		0xAE7367CA,
		0x5C18E4C9,
		0x4F48173D,
		0xBD23943E,
		0xF36E6F75,
		0x0105EC76,
		0x12551F82,
		0xE03E9C81,
		0x34F4F86A,
		0xC69F7B69,
		0xD5CF889D,
		0x27A40B9E,
		0x79B737BA,
		0x8BDCB4B9,
		0x988C474D,
		0x6AE7C44E,
		0xBE2DA0A5,
		0x4C4623A6,
		0x5F16D052,
		0xAD7D5351,
	]

	mut crc := u32(0xFFFFFFFF)
	for b in data {
		index := (crc ^ u32(b)) & 0xFF
		crc = (crc >> 8) ^ crc_table[index]
	}
	return crc ^ 0xFFFFFFFF
}
