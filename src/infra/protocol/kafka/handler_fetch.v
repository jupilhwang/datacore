// Fetch request/response types, parsing, encoding, and handler implementation
//
// This module implements the Kafka Fetch API.
// Used by consumers to pull messages from the broker.
// Supports offset-based data retrieval per topic/partition.
module kafka

import infra.compression
import infra.observability
import time
import domain

/// FetchRequest is sent by a consumer to pull messages from the broker.
///
/// Supports fetching from multiple topics and partitions simultaneously.
/// Supports long polling via max_wait_ms and min_bytes.
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

/// FetchRequestForgottenTopic represents topics no longer needed in a session-based fetch.
pub struct FetchRequestForgottenTopic {
pub:
	name       string
	topic_id   []u8 // Topic UUID (v13+)
	partitions []i32
}

/// FetchRequestTopic identifies a topic to fetch from.
pub struct FetchRequestTopic {
pub:
	name       string
	topic_id   []u8 // Topic UUID (v13+, 16 bytes)
	partitions []FetchRequestPartition
}

/// FetchRequestPartition identifies a partition to fetch from.
pub struct FetchRequestPartition {
pub:
	partition           i32
	fetch_offset        i64
	partition_max_bytes i32
}

/// FetchResponse contains the fetched message data.
pub struct FetchResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	session_id       i32
	topics           []FetchResponseTopic
	node_endpoints   []NodeEndpoint
}

/// FetchResponseTopic contains per-topic fetch results.
pub struct FetchResponseTopic {
pub:
	name       string
	topic_id   []u8 // Topic UUID (v13+)
	partitions []FetchResponsePartition
}

/// FetchResponsePartition contains per-partition fetch results.
pub struct FetchResponsePartition {
pub:
	partition_index    i32
	error_code         i16
	high_watermark     i64
	last_stable_offset i64
	log_start_offset   i64
	records            []u8
}

/// NodeEndpoint holds broker node information (v16+).
pub struct NodeEndpoint {
pub:
	node_id i32
	host    string
	port    i32
	rack    string
}

// parse_fetch_request parses a Fetch request.
// Reads different fields depending on the version to build the FetchRequest struct.
fn parse_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequest {
	mut replica_id := i32(-1)
	// v15+: replica_id removed from body and moved to a tagged field
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!

	// max_bytes field added in v3+
	mut max_bytes := i32(0x7fffffff)
	if version >= 3 {
		max_bytes = reader.read_i32()!
	}

	// isolation_level field added in v4+
	mut isolation_level := i8(0)
	if version >= 4 {
		isolation_level = reader.read_i8()!
	}

	// Session-based fetch fields added in v7+
	if version >= 7 {
		_ = reader.read_i32()!
		_ = reader.read_i32()!
	}

	// Parse topic array
	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topics := []FetchRequestTopic{}
	for _ in 0 .. topic_count {
		mut name := ''
		mut topic_id := []u8{}

		// v13+ uses topic UUID instead of topic name
		if version >= 13 {
			topic_id = reader.read_uuid()!
		} else if is_flexible {
			name = reader.read_compact_string()!
		} else {
			name = reader.read_string()!
		}

		// Parse partition array
		partition_count := reader.read_flex_array_len(is_flexible)!

		mut partitions := []FetchRequestPartition{}
		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			// current_leader_epoch field added in v9+ (ignored)
			if version >= 9 {
				_ = reader.read_i32()!
			}

			fetch_offset := reader.read_i64()!

			// log_start_offset field added in v5+ (ignored)
			if version >= 5 {
				_ = reader.read_i64()!
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

	// Parse forgotten topics data in v7+ (for session-based fetch)
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

	// rack_id field added in v11+ (ignored)
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

/// encode serializes the FetchResponse to bytes.
/// Uses flexible or non-flexible format depending on the version.
pub fn (r FetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 12
	mut writer := new_writer()

	// throttle_time_ms field added in v1+
	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	// Top-level error code and session ID added in v7+
	if version >= 7 {
		writer.write_i16(r.error_code)
		writer.write_i32(r.session_id)
	}

	// Encode topic array
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		// v13+ uses topic UUID instead of topic name
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		// Encode partition array
		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.high_watermark)
			// last_stable_offset field added in v4+
			if version >= 4 {
				writer.write_i64(p.last_stable_offset)
			}
			// log_start_offset field added in v5+
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// aborted_transactions array added in v4+ (empty array)
			if version >= 4 {
				if is_flexible {
					writer.write_uvarint(1)
				} else {
					writer.write_array_len(0)
				}
			}
			// preferred_read_replica field added in v11+
			if version >= 11 {
				writer.write_i32(-1)
			}
			// Encode record batch data
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

// process_fetch handles a Fetch request (frame-based).
// Retrieves messages from the requested topic/partition and builds the response.
// Compression support: compresses records according to topic configuration before sending.
fn (mut h Handler) process_fetch(req FetchRequest, version i16) !FetchResponse {
	start_time := time.now()

	h.logger.debug('Processing fetch request', observability.field_int('version', version),
		observability.field_int('topics', req.topics.len), observability.field_int('max_wait_ms',
		req.max_wait_ms), observability.field_int('max_bytes', req.max_bytes))

	mut topics := []FetchResponseTopic{}
	mut total_records := 0
	mut total_bytes := i64(0)
	mut total_bytes_saved := i64(0)

	for t in req.topics {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()
		// v13+: resolve topic name from UUID
		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			}
		}

		// Kafka 프로토콜 호환성: RecordBatch 헤더는 비압축 상태여야 Consumer가 파싱 가능하므로
		// Fetch 경로에서는 재압축을 수행하지 않는다.
		preferred_compression := compression.CompressionType.none

		// Fetch data from each partition
		mut partitions := []FetchResponsePartition{}
		for p in t.partitions {
			// Fetch messages from storage
			result := h.storage.fetch(topic_name, int(p.partition), p.fetch_offset, p.partition_max_bytes) or {
				// Map storage error to an appropriate error code
				error_code := if err.str().contains('not found') {
					i16(ErrorCode.unknown_topic_or_partition)
				} else if err.str().contains('out of range') {
					i16(ErrorCode.offset_out_of_range)
				} else {
					i16(ErrorCode.unknown_server_error)
				}

				h.logger.debug('Fetch partition error', observability.field_string('topic',
					topic_name), observability.field_int('partition', p.partition), observability.field_int('error_code',
					error_code))

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

			// Encode fetched records into RecordBatch format.
			// first_offset: use the actual offset of the first record returned.
			records_data := encode_record_batch_zerocopy(result.records, result.first_offset)
			total_records += result.records.len

			// Apply schema decoding if configured for this topic
			mut records_for_compression := records_data.clone()
			if schema := h.get_topic_schema(topic_name) {
				h.logger.debug('Decoding records with schema', observability.field_string('topic',
					topic_name), observability.field_string('schema_type', domain.SchemaType(schema.schema_type).str()))

				mut decoded_records := []domain.Record{}
				for record in result.records {
					decoded_value := h.decode_record_with_schema(record.value, &schema) or {
						h.logger.warn('Failed to decode record with schema, returning raw data',
							observability.field_string('topic', topic_name), observability.field_err_str(err.str()))
						// Return raw data on decode failure
						decoded_records << record
						continue
					}
					decoded_records << domain.Record{
						key:       record.key
						value:     decoded_value
						timestamp: record.timestamp
						headers:   record.headers
					}
				}
				// Re-encode decoded records as RecordBatch
				records_for_compression = encode_record_batch_zerocopy(decoded_records,
					result.first_offset)
			}

			// Apply compression
			compressed_result := h.compress_records_for_fetch(records_for_compression,
				preferred_compression, topic_name, p.partition)
			final_records_data := compressed_result.data
			total_bytes_saved += compressed_result.bytes_saved

			total_bytes += final_records_data.len

			h.logger.trace('Fetch partition success', observability.field_string('topic',
				topic_name), observability.field_int('partition', p.partition), observability.field_int('records',
				result.records.len), observability.field_bytes('response_size', final_records_data.len))

			partitions << FetchResponsePartition{
				partition_index:    p.partition
				error_code:         0
				high_watermark:     result.high_watermark
				last_stable_offset: result.last_stable_offset
				log_start_offset:   result.log_start_offset
				records:            final_records_data
			}
		}
		topics << FetchResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			partitions: partitions
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Fetch request completed', observability.field_int('topics', topics.len),
		observability.field_int('total_records', total_records), observability.field_bytes('total_bytes',
		total_bytes), observability.field_bytes('bytes_saved', total_bytes_saved), observability.field_duration('latency',
		elapsed))

	return FetchResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       0
		session_id:       0
		topics:           topics
		node_endpoints:   []NodeEndpoint{}
	}
}

// get_topic_compression_type retrieves the compression type for a topic.
// Reads compression.type from topic configuration and converts it to CompressionType.
fn (mut h Handler) get_topic_compression_type(topic_name string) compression.CompressionType {
	// Default is no compression
	mut compression_type := compression.CompressionType.none

	// Look up configuration from topic metadata (TopicMetadata.config is map[string]string)
	if topic_meta := h.storage.get_topic(topic_name) {
		// Standard Kafka configuration key: compression.type
		compression_str := topic_meta.config['compression.type']
		if compression_str.len > 0 && compression_str != 'none' {
			if ct := compression.compression_type_from_string(compression_str) {
				compression_type = ct
			}
		}
	}

	return compression_type
}

// CompressionResult holds the result of a compression operation.
struct CompressionResult {
pub:
	data        []u8
	bytes_saved i64
}

// compress_records_for_fetch compresses records for a Fetch response.
// Returns the original data if compression would increase the size.
fn (mut h Handler) compress_records_for_fetch(records_data []u8, compression_type compression.CompressionType, topic_name string, partition i32) CompressionResult {
	// No compression needed
	if compression_type == .none || records_data.len == 0 {
		return CompressionResult{
			data:        records_data
			bytes_saved: 0
		}
	}

	// Skip compression for small payloads (threshold: 256 bytes)
	if records_data.len < 256 {
		return CompressionResult{
			data:        records_data
			bytes_saved: 0
		}
	}

	// Perform compression
	compressed := h.compression_service.compress(records_data, compression_type) or {
		h.logger.warn('Compression failed, returning uncompressed data', observability.field_string('topic',
			topic_name), observability.field_int('partition', partition), observability.field_string('compression_type',
			compression_type.str()), observability.field_err_str(err.str()))
		return CompressionResult{
			data:        records_data
			bytes_saved: 0
		}
	}

	// Return original data if compression increased the size
	if compressed.len >= records_data.len {
		h.logger.debug('Compression increased size, using uncompressed data', observability.field_string('topic',
			topic_name), observability.field_int('partition', partition), observability.field_int('original_size',
			records_data.len), observability.field_int('compressed_size', compressed.len))
		return CompressionResult{
			data:        records_data
			bytes_saved: 0
		}
	}

	// Compression successful — compute bytes saved
	bytes_saved := records_data.len - compressed.len

	h.logger.debug('Records compressed for fetch', observability.field_string('topic',
		topic_name), observability.field_int('partition', partition), observability.field_string('compression_type',
		compression_type.str()), observability.field_int('original_size', records_data.len),
		observability.field_int('compressed_size', compressed.len), observability.field_int('bytes_saved',
		bytes_saved))

	return CompressionResult{
		data:        compressed
		bytes_saved: bytes_saved
	}
}

// Legacy handler — uses SimpleFetchRequest to maintain compatibility with zerocopy_fetch.v.
fn (mut h Handler) handle_fetch(body []u8, version i16) ![]u8 {
	flexible := is_flexible_version(.fetch, version)
	simple_req := parse_fetch_request_simple(body, version, flexible)!

	// Convert SimpleFetchRequest to FetchRequest
	req := simple_fetch_to_fetch_request(simple_req)
	resp := h.process_fetch(req, version)!

	encoded := resp.encode(version)
	// Print response info for debugging
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

// simple_fetch_to_fetch_request converts a SimpleFetchRequest to a FetchRequest.
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

// SimpleFetchRequest parser

/// SimpleFetchRequest is a lightweight Fetch request struct
/// used for compatibility with zerocopy_fetch.v.
pub struct SimpleFetchRequest {
pub:
	replica_id       i32
	max_wait_ms      i32
	min_bytes        i32
	max_bytes        i32
	isolation_level  i8
	session_id       i32
	session_epoch    i32
	topics           []SimpleFetchTopic
	forgotten_topics []ForgottenTopic
	rack_id          string
}

/// SimpleFetchTopic holds topic data for a Fetch request.
pub struct SimpleFetchTopic {
pub:
	topic_id   []u8 // Topic UUID (v13+)
	name       string
	partitions []SimpleFetchPartition
}

/// SimpleFetchPartition holds partition data for a Fetch request.
pub struct SimpleFetchPartition {
pub:
	partition            i32
	current_leader_epoch i32
	fetch_offset         i64
	last_fetched_epoch   i32
	log_start_offset     i64
	partition_max_bytes  i32
}

/// ForgottenTopic holds a topic no longer tracked in a session.
pub struct ForgottenTopic {
pub:
	topic_id   []u8
	name       string
	partitions []i32
}

/// parse_fetch_request_simple is a lightweight Fetch request parser.
/// Returns the parsed request as a SimpleFetchRequest.
pub fn parse_fetch_request_simple(data []u8, version i16, flexible bool) !SimpleFetchRequest {
	mut reader := new_reader(data)

	// v0–14: replica_id (INT32) is the first field.
	// v15+: replica_id removed from body; replaced by replica_state in tagged fields.
	mut replica_id := i32(-1)
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!

	// max_bytes field added in v3+
	max_bytes := if version >= 3 { reader.read_i32()! } else { i32(0x7FFFFFFF) }

	// isolation_level field added in v4+
	isolation_level := if version >= 4 { reader.read_i8()! } else { i8(0) }

	// session_id and session_epoch fields added in v7+
	session_id := if version >= 7 { reader.read_i32()! } else { i32(0) }
	session_epoch := if version >= 7 { reader.read_i32()! } else { i32(-1) }

	// Parse topic array
	mut topics := []SimpleFetchTopic{}
	topic_count := if flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	for _ in 0 .. topic_count {
		// v13+ uses topic UUID
		mut topic_id := []u8{}
		mut topic_name := ''

		if version >= 13 {
			topic_id = reader.read_uuid()!
		}
		if version < 13 || !flexible {
			topic_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
		}

		// Parse partition array
		mut partitions := []SimpleFetchPartition{}
		partition_count := if flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			// current_leader_epoch field added in v9+
			current_leader_epoch := if version >= 9 { reader.read_i32()! } else { i32(-1) }

			fetch_offset := reader.read_i64()!

			// last_fetched_epoch field added in v12+
			last_fetched_epoch := if version >= 12 { reader.read_i32()! } else { i32(-1) }

			// log_start_offset field added in v5+
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

	// Parse forgotten topics array in v7+
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

	// rack_id field added in v11+
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
