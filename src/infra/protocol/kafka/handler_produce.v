// Produce request/response types, parsing, encoding, and handler implementation
//
// This module implements the Kafka Produce API.
// Used by producers to send messages to the broker.
// Supports transactional producers and various acks configurations.
module kafka

import domain
import infra.observability
import time

/// ProduceRequest is sent by a producer to deliver messages to the broker.
///
/// Supports sending to multiple topics and partitions simultaneously.
/// Transactional producers include a transactional_id.
pub struct ProduceRequest {
pub:
	transactional_id ?string
	acks             i16
	timeout_ms       i32
	topic_data       []ProduceRequestTopic
}

/// ProduceRequestTopic holds the topic data to be produced.
pub struct ProduceRequestTopic {
pub:
	name           string
	topic_id       []u8 // Topic UUID (v13+, 16 bytes)
	partition_data []ProduceRequestPartition
}

/// ProduceRequestPartition holds the partition data to be produced.
pub struct ProduceRequestPartition {
pub:
	index   i32
	records []u8
}

/// ProduceResponse contains the result of a produce operation.
pub struct ProduceResponse {
pub:
	topics           []ProduceResponseTopic
	throttle_time_ms i32
}

/// ProduceResponseTopic contains per-topic produce results.
pub struct ProduceResponseTopic {
pub:
	name       string
	topic_id   []u8 // Topic UUID (v13+)
	partitions []ProduceResponsePartition
}

/// ProduceResponsePartition contains per-partition produce results.
pub struct ProduceResponsePartition {
pub:
	index            i32
	error_code       i16
	base_offset      i64
	log_append_time  i64
	log_start_offset i64
}

// parse_produce_request parses a Produce request.
// Reads different fields depending on the version to build the ProduceRequest struct.
fn parse_produce_request(mut reader BinaryReader, version i16, is_flexible bool) !ProduceRequest {
	// transactional_id field added in v3+
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

	// Parse topic array
	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topic_data := []ProduceRequestTopic{}
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

		mut partition_data := []ProduceRequestPartition{}
		for _ in 0 .. partition_count {
			index := reader.read_i32()!
			// Read record batch data
			records := if is_flexible {
				reader.read_compact_bytes()!
			} else {
				reader.read_bytes()!
			}

			partition_data << ProduceRequestPartition{
				index:   index
				records: records
			}

			reader.skip_flex_tagged_fields(is_flexible)!
		}

		topic_data << ProduceRequestTopic{
			name:           name
			topic_id:       topic_id
			partition_data: partition_data
		}

		reader.skip_flex_tagged_fields(is_flexible)!
	}

	return ProduceRequest{
		transactional_id: transactional_id
		acks:             acks
		timeout_ms:       timeout_ms
		topic_data:       topic_data
	}
}

/// encode serializes the ProduceResponse to bytes.
/// Uses flexible or non-flexible format depending on the version.
pub fn (r ProduceResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

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
			writer.write_i32(p.index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.base_offset)
			// log_append_time field added in v2+
			if version >= 2 {
				writer.write_i64(p.log_append_time)
			}
			// log_start_offset field added in v5+
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// record_errors array added in v8+ (empty array)
			if version >= 8 {
				if is_flexible {
					writer.write_compact_array_len(0)
				} else {
					writer.write_array_len(0)
				}
			}
			// error_message field added in v8+ (null)
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

	// throttle_time_ms field added in v1+
	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// process_produce handles a Produce request (frame-based).
// Stores messages to the requested topic/partition and returns the result.
fn (mut h Handler) process_produce(req ProduceRequest, version i16) !ProduceResponse {
	start_time := time.now()
	mut total_records := 0
	mut total_bytes := i64(0)

	// Count records and bytes for logging
	for t in req.topic_data {
		for p in t.partition_data {
			total_bytes += p.records.len
		}
	}

	h.logger.debug('Processing produce request', observability.field_int('topics', req.topic_data.len),
		observability.field_int('acks', req.acks), observability.field_bytes('total_size',
		total_bytes))

	// Validate if this is a transactional producer
	if txn_id := req.transactional_id {
		if txn_id.len > 0 {
			h.logger.debug('Validating transaction', observability.field_string('txn_id',
				txn_id))

			if mut txn_coord := h.txn_coordinator {
				// Look up transaction metadata
				meta := txn_coord.get_transaction(txn_id) or {
					h.logger.warn('Transaction not found', observability.field_string('txn_id',
						txn_id))
					return h.build_produce_error_response_typed(req, ErrorCode.transactional_id_not_found)
				}

				// Validate transaction state
				if meta.state != .ongoing {
					h.logger.warn('Invalid transaction state', observability.field_string('txn_id',
						txn_id), observability.field_string('state', meta.state.str()))
					return h.build_produce_error_response_typed(req, ErrorCode.invalid_txn_state)
				}

				// Build partition lookup map for O(1) access
				mut partition_set := map[string]bool{}
				for tp in meta.topic_partitions {
					key := '${tp.topic}:${tp.partition}'
					partition_set[key] = true
				}

				// Verify that requested partitions are registered in the transaction
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

					// Use O(1) lookup instead of O(n) nested loop
					for p in t.partition_data {
						key := '${topic_name}:${int(p.index)}'
						if key !in partition_set {
							return h.build_produce_error_response_typed(req, ErrorCode.invalid_txn_state)
						}
					}
				}
			} else {
				// No transaction coordinator available
				return h.build_produce_error_response_typed(req, ErrorCode.coordinator_not_available)
			}
		}
	}

	// Store messages to each topic/partition
	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		// Resolve topic name and ID (v13+ UUID lookup)
		resolved := h.resolve_produce_topic(t, version) or {
			mut partitions := []ProduceResponsePartition{}
			for p in t.partition_data {
				partitions << new_error_partition(p.index, .unknown_topic_id)
			}
			topics << ProduceResponseTopic{
				name:       t.name
				topic_id:   t.topic_id.clone()
				partitions: partitions
			}
			continue
		}
		topic_name := resolved.name
		topic_id := resolved.id

		mut partitions := []ProduceResponsePartition{}
		for p in t.partition_data {
			// Decompress and parse RecordBatch
			parse_result := h.decompress_and_parse_partition(topic_name, int(p.index),
				p.records) or {
				partitions << new_error_partition(p.index, .corrupt_message)
				continue
			}
			parsed := parse_result.parsed
			original_compression_type := parse_result.original_compression_type

			total_records += parsed.records.len

			// Apply schema encoding if configured for this topic
			mut records_to_store := parsed.records.clone()
			if schema := h.get_topic_schema(topic_name) {
				h.logger.debug('Encoding records with schema', observability.field_string('topic',
					topic_name), observability.field_string('schema_type', domain.SchemaType(schema.schema_type).str()))

				mut encoded_records := []domain.Record{}
				for record in parsed.records {
					encoded_value := h.encode_record_with_schema(&record, &schema) or {
						h.logger.error('Failed to encode record with schema', observability.field_string('topic',
							topic_name), observability.field_err_str(err.str()))
						partitions << new_error_partition(p.index, .corrupt_message)
						continue
					}
					encoded_records << domain.Record{
						key:              record.key
						value:            encoded_value
						timestamp:        record.timestamp
						headers:          record.headers
						compression_type: record.compression_type
					}
				}
				records_to_store = encoded_records.clone()
			}

			// Preserve original compression type on each record (supports cross-broker fetch)
			if original_compression_type > 0 {
				for idx in 0 .. records_to_store.len {
					records_to_store[idx] = domain.Record{
						...records_to_store[idx]
						compression_type: original_compression_type
					}
				}
			}

			// Handle empty record batch
			if records_to_store.len == 0 {
				partitions << ProduceResponsePartition{
					index:            p.index
					error_code:       0
					base_offset:      0
					log_append_time:  -1
					log_start_offset: 0
				}
				continue
			}

			// Store records to storage (auto-creates topic if not found)
			result := h.store_with_auto_create(topic_name, p.index, records_to_store,
				req.acks) or {
				error_code := if err.str().contains('out of range') {
					ErrorCode.unknown_topic_or_partition
				} else {
					ErrorCode.unknown_server_error
				}
				partitions << new_error_partition(p.index, error_code)
				continue
			}

			// Success response
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

	elapsed := time.since(start_time)
	h.logger.debug('Produce request completed', observability.field_int('topics', topics.len),
		observability.field_int('total_records', total_records), observability.field_duration('latency',
		elapsed))

	return ProduceResponse{
		topics:           topics
		throttle_time_ms: default_throttle_time_ms
	}
}

// Legacy handler — delegates to process_produce.
fn (mut h Handler) handle_produce(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_produce_request(mut reader, version, is_flexible_version(.produce, version))!
	resp := h.process_produce(req, version)!
	return resp.encode(version)
}

// build_produce_error_response_typed builds a ProduceResponse with the given error code on all partitions (typed).
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
		throttle_time_ms: default_throttle_time_ms
	}
}

// build_produce_error_response builds a ProduceResponse with the given error code on all partitions (legacy, returns bytes).
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
		throttle_time_ms: default_throttle_time_ms
	}.encode(version)
}

// Note: Fetch (API Key 1) has been moved to handler_fetch.v
// Note: ListOffsets (API Key 2) has been moved to handler_list_offsets.v
// Note: RecordBatch encoding and CRC32-C calculation have been moved to record_batch.v
