// Kafka Protocol - Fetch (API Key 1)
// Request/Response types, parsing, encoding, and handlers
module kafka

import infra.observability
import time

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

	topic_count := reader.read_flex_array_len(is_flexible)!

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

		partition_count := reader.read_flex_array_len(is_flexible)!

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

			reader.skip_flex_tagged_fields(is_flexible)!
		}

		topics << FetchRequestTopic{
			name:       name
			topic_id:   topic_id
			partitions: partitions
		}

		reader.skip_flex_tagged_fields(is_flexible)!
	}

	mut forgotten_topics_data := []FetchRequestForgottenTopic{}
	if version >= 7 {
		forgotten_count := reader.read_flex_array_len(is_flexible)!
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
			fpartition_count := reader.read_flex_array_len(is_flexible)!
			mut fpartitions := []i32{}
			for _ in 0 .. fpartition_count {
				fpartitions << reader.read_i32()!
			}
			forgotten_topics_data << FetchRequestForgottenTopic{
				name:       fname
				topic_id:   ftopic_id
				partitions: fpartitions
			}
			reader.skip_flex_tagged_fields(is_flexible)!
		}
	}

	if version >= 11 {
		_ = reader.read_flex_string(is_flexible)!
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
	start_time := time.now()

	h.logger.debug('Processing fetch request',
		observability.field_int('version', version),
		observability.field_int('topics', req.topics.len),
		observability.field_int('max_wait_ms', req.max_wait_ms),
		observability.field_int('max_bytes', req.max_bytes))

	mut topics := []FetchResponseTopic{}
	mut total_records := 0
	mut total_bytes := i64(0)

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

				h.logger.debug('Fetch partition error',
					observability.field_string('topic', topic_name),
					observability.field_int('partition', p.partition),
					observability.field_int('error_code', error_code))

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
			total_records += result.records.len
			total_bytes += records_data.len

			h.logger.trace('Fetch partition success',
				observability.field_string('topic', topic_name),
				observability.field_int('partition', p.partition),
				observability.field_int('records', result.records.len),
				observability.field_bytes('response_size', records_data.len))

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

	elapsed := time.since(start_time)
	h.logger.debug('Fetch request completed',
		observability.field_int('topics', topics.len),
		observability.field_int('total_records', total_records),
		observability.field_bytes('total_bytes', total_bytes),
		observability.field_duration('latency', elapsed))

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

	// Parse topics
	mut topics := []SimpleFetchTopic{}
	topic_count := if flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	for _ in 0 .. topic_count {
		// v13+ uses topic_id
		mut topic_id := []u8{}
		mut topic_name := ''

		if version >= 13 {
			topic_id = reader.read_uuid()!
		}
		if version < 13 || !flexible {
			topic_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
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
