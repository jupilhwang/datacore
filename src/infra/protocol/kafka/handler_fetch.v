// Fetch request/response types, parsing, encoding, and handler implementation
//
// This module implements the Kafka Fetch API.
// Used by consumers to pull messages from the broker.
// Supports offset-based data retrieval per topic/partition.
module kafka

import service.port
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

// parse_fetch_partition reads a single partition from a Fetch request.
fn parse_fetch_partition(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequestPartition {
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

	reader.skip_flex_tagged_fields(is_flexible)!

	return FetchRequestPartition{
		partition:           partition
		fetch_offset:        fetch_offset
		partition_max_bytes: partition_max_bytes
	}
}

// parse_fetch_topic reads a single topic entry from a Fetch request.
fn parse_fetch_topic(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequestTopic {
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

	partition_count := reader.read_flex_array_len(is_flexible)!

	mut partitions := []FetchRequestPartition{}
	for _ in 0 .. partition_count {
		partitions << parse_fetch_partition(mut reader, version, is_flexible)!
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return FetchRequestTopic{
		name:       name
		topic_id:   topic_id
		partitions: partitions
	}
}

// parse_fetch_forgotten_topics reads the forgotten topics array from a Fetch request.
fn parse_fetch_forgotten_topics(mut reader BinaryReader, version i16, is_flexible bool) ![]FetchRequestForgottenTopic {
	mut forgotten_topics_data := []FetchRequestForgottenTopic{}
	if version < 7 {
		return forgotten_topics_data
	}

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

	return forgotten_topics_data
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

	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topics := []FetchRequestTopic{}
	for _ in 0 .. topic_count {
		topics << parse_fetch_topic(mut reader, version, is_flexible)!
	}

	forgotten_topics_data := parse_fetch_forgotten_topics(mut reader, version, is_flexible)!

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

// FetchPartitionResult holds the result of processing a single partition fetch.
struct FetchPartitionResult {
pub:
	response     FetchResponsePartition
	record_count int
	total_bytes  i64
	bytes_saved  i64
}

// apply_fetch_schema_decoding applies schema decoding to fetched records if configured.
fn (mut h Handler) apply_fetch_schema_decoding(topic_name string, records []domain.Record, first_offset i64) []u8 {
	schema := h.get_topic_schema(topic_name) or { return []u8{} }

	h.logger.debug('Decoding records with schema', port.field_string('topic', topic_name),
		port.field_string('schema_type', domain.SchemaType(schema.schema_type).str()))

	mut decoded_records := []domain.Record{}
	for record in records {
		decoded_value := h.decode_record_with_schema(record.value, &schema) or {
			h.logger.warn('Failed to decode record with schema, returning raw data', port.field_string('topic',
				topic_name), port.field_err_str(err.str()))
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
	return encode_record_batch_zerocopy(decoded_records, first_offset)
}

// fetch_partition_data fetches data for a single partition and builds the response.
fn (mut h Handler) fetch_partition_data(topic_name string, p FetchRequestPartition, preferred_compression i16) FetchPartitionResult {
	result := h.storage.fetch(topic_name, int(p.partition), p.fetch_offset, p.partition_max_bytes) or {
		error_code := if err.str().contains('not found') {
			i16(ErrorCode.unknown_topic_or_partition)
		} else if err.str().contains('out of range') {
			i16(ErrorCode.offset_out_of_range)
		} else {
			i16(ErrorCode.unknown_server_error)
		}

		h.logger.debug('Fetch partition error', port.field_string('topic', topic_name),
			port.field_int('partition', p.partition), port.field_int('error_code', error_code))

		return FetchPartitionResult{
			response: FetchResponsePartition{
				partition_index:    p.partition
				error_code:         error_code
				high_watermark:     0
				last_stable_offset: 0
				log_start_offset:   0
				records:            []u8{}
			}
		}
	}

	records_data := encode_record_batch_zerocopy(result.records, result.first_offset)

	schema_decoded_data := h.apply_fetch_schema_decoding(topic_name, result.records, result.first_offset)
	records_for_compression := if schema_decoded_data.len > 0 {
		schema_decoded_data
	} else {
		records_data
	}

	compressed_result := h.compress_records_for_fetch(records_for_compression, preferred_compression,
		topic_name, p.partition)
	final_records_data := compressed_result.data

	h.logger.trace('Fetch partition success', port.field_string('topic', topic_name),
		port.field_int('partition', p.partition), port.field_int('records', result.records.len),
		port.field_bytes('response_size', final_records_data.len))

	return FetchPartitionResult{
		response:     FetchResponsePartition{
			partition_index:    p.partition
			error_code:         0
			high_watermark:     result.high_watermark
			last_stable_offset: result.last_stable_offset
			log_start_offset:   result.log_start_offset
			records:            final_records_data
		}
		record_count: result.records.len
		total_bytes:  final_records_data.len
		bytes_saved:  compressed_result.bytes_saved
	}
}

// process_fetch handles a Fetch request (frame-based).
// Retrieves messages from the requested topic/partition and builds the response.
// Compression support: compresses records according to topic configuration before sending.
fn (mut h Handler) process_fetch(req FetchRequest, version i16) !FetchResponse {
	start_time := time.now()

	h.logger.debug('Processing fetch request', port.field_int('version', version), port.field_int('topics',
		req.topics.len), port.field_int('max_wait_ms', req.max_wait_ms), port.field_int('max_bytes',
		req.max_bytes))

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

		// Kafka protocol compatibility: RecordBatch headers must be uncompressed
		// for Consumer parsing, so no re-compression is performed in the Fetch path.
		preferred_compression := port.compression_none

		mut partitions := []FetchResponsePartition{}
		for p in t.partitions {
			part_result := h.fetch_partition_data(topic_name, p, preferred_compression)
			total_records += part_result.record_count
			total_bytes += part_result.total_bytes
			total_bytes_saved += part_result.bytes_saved
			partitions << part_result.response
		}
		topics << FetchResponseTopic{
			name:       topic_name
			topic_id:   topic_id
			partitions: partitions
		}
	}

	elapsed := time.since(start_time)
	h.logger.debug('Fetch request completed', port.field_int('topics', topics.len), port.field_int('total_records',
		total_records), port.field_bytes('total_bytes', total_bytes), port.field_bytes('bytes_saved',
		total_bytes_saved), port.field_duration('latency', elapsed))

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
fn (mut h Handler) get_topic_compression_type(topic_name string) i16 {
	mut compression_type := port.compression_none

	if topic_meta := h.storage.get_topic(topic_name) {
		compression_str := topic_meta.config['compression.type']
		if compression_str.len > 0 && compression_str != 'none' {
			if ct := port.compression_type_from_string(compression_str) {
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
fn (mut h Handler) compress_records_for_fetch(records_data []u8, compression_type i16, topic_name string, partition i32) CompressionResult {
	if compression_type == port.compression_none || records_data.len == 0 {
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
		h.logger.warn('Compression failed, returning uncompressed data', port.field_string('topic',
			topic_name), port.field_int('partition', partition), port.field_string('compression_type',
			port.compression_type_name(compression_type)), port.field_err_str(err.str()))
		return CompressionResult{
			data:        records_data
			bytes_saved: 0
		}
	}

	// Return original data if compression increased the size
	if compressed.len >= records_data.len {
		h.logger.debug('Compression increased size, using uncompressed data', port.field_string('topic',
			topic_name), port.field_int('partition', partition), port.field_int('original_size',
			records_data.len), port.field_int('compressed_size', compressed.len))
		return CompressionResult{
			data:        records_data
			bytes_saved: 0
		}
	}

	// Compression successful — compute bytes saved
	bytes_saved := records_data.len - compressed.len

	h.logger.debug('Records compressed for fetch', port.field_string('topic', topic_name),
		port.field_int('partition', partition), port.field_string('compression_type',
		port.compression_type_name(compression_type)), port.field_int('original_size',
		records_data.len), port.field_int('compressed_size', compressed.len), port.field_int('bytes_saved',
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

// parse_simple_fetch_partition reads a single partition from a SimpleFetch request.
fn parse_simple_fetch_partition(mut reader BinaryReader, version i16, flexible bool) !SimpleFetchPartition {
	partition := reader.read_i32()!
	current_leader_epoch := if version >= 9 { reader.read_i32()! } else { i32(-1) }
	fetch_offset := reader.read_i64()!
	last_fetched_epoch := if version >= 12 { reader.read_i32()! } else { i32(-1) }
	log_start_offset := if version >= 5 { reader.read_i64()! } else { i64(-1) }
	partition_max_bytes := reader.read_i32()!

	if flexible {
		reader.skip_tagged_fields()!
	}

	return SimpleFetchPartition{
		partition:            partition
		current_leader_epoch: current_leader_epoch
		fetch_offset:         fetch_offset
		last_fetched_epoch:   last_fetched_epoch
		log_start_offset:     log_start_offset
		partition_max_bytes:  partition_max_bytes
	}
}

// parse_simple_fetch_topic reads a single topic entry from a SimpleFetch request.
fn parse_simple_fetch_topic(mut reader BinaryReader, version i16, flexible bool) !SimpleFetchTopic {
	mut topic_id := []u8{}
	mut topic_name := ''

	if version >= 13 {
		topic_id = reader.read_uuid()!
	}
	if version < 13 || !flexible {
		topic_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
	}

	mut partitions := []SimpleFetchPartition{}
	partition_count := if flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	for _ in 0 .. partition_count {
		partitions << parse_simple_fetch_partition(mut reader, version, flexible)!
	}

	if flexible {
		reader.skip_tagged_fields()!
	}

	return SimpleFetchTopic{
		topic_id:   topic_id
		name:       topic_name
		partitions: partitions
	}
}

// parse_simple_forgotten_topics reads the forgotten topics array from a SimpleFetch request.
fn parse_simple_forgotten_topics(mut reader BinaryReader, version i16, flexible bool) ![]ForgottenTopic {
	mut forgotten_topics := []ForgottenTopic{}
	if version < 7 {
		return forgotten_topics
	}

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

	return forgotten_topics
}

/// parse_fetch_request_simple is a lightweight Fetch request parser.
/// Returns the parsed request as a SimpleFetchRequest.
pub fn parse_fetch_request_simple(data []u8, version i16, flexible bool) !SimpleFetchRequest {
	mut reader := new_reader(data)

	mut replica_id := i32(-1)
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!
	max_bytes := if version >= 3 { reader.read_i32()! } else { i32(0x7FFFFFFF) }
	isolation_level := if version >= 4 { reader.read_i8()! } else { i8(0) }
	session_id := if version >= 7 { reader.read_i32()! } else { i32(0) }
	session_epoch := if version >= 7 { reader.read_i32()! } else { i32(-1) }

	mut topics := []SimpleFetchTopic{}
	topic_count := if flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}
	for _ in 0 .. topic_count {
		topics << parse_simple_fetch_topic(mut reader, version, flexible)!
	}

	forgotten_topics := parse_simple_forgotten_topics(mut reader, version, flexible)!

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
