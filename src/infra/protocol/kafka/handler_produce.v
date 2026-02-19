// Produce request/response types, parsing, encoding, and handler implementation
//
// This module implements the Kafka Produce API.
// Used by producers to send messages to the broker.
// Supports transactional producers and various acks configurations.
module kafka

import domain
import infra.compression
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
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		// v13+: resolve topic name from UUID
		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				// Topic UUID not found; return error response
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
			// Decompress and parse RecordBatch
			records_to_parse := p.records.clone()
			mut decompressed_data := []u8{}
			mut was_compressed := false
			mut outer_base_offset := i64(0)
			mut original_compression_type := u8(0)

			// Parse Kafka RecordBatch v2 header (61 bytes)
			if records_to_parse.len >= 61 {
				mut header_reader := new_reader(records_to_parse)
				outer_base_offset = header_reader.read_i64() or { 0 }
				batch_length := header_reader.read_i32() or { 0 }
				partition_leader_epoch := header_reader.read_i32() or { 0 }
				magic := header_reader.read_i8() or { 0 }

				// Detailed logging for RecordBatch header parsing
				h.logger.debug('RecordBatch header parsing', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(p.index)), observability.field_int('buffer_size',
					records_to_parse.len), observability.field_int('base_offset', int(outer_base_offset)),
					observability.field_int('batch_length', int(batch_length)), observability.field_int('leader_epoch',
					int(partition_leader_epoch)), observability.field_int('magic', int(magic)))

				// Inspect raw bytes - print first 32 bytes of header as hex
				header_preview_len := if records_to_parse.len > 32 {
					32
				} else {
					records_to_parse.len
				}
				header_hex := records_to_parse[..header_preview_len].hex()
				h.logger.debug('RecordBatch header raw bytes (hex)', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(p.index)), observability.field_string('header_hex',
					header_hex), observability.field_int('header_preview_bytes', header_preview_len))

				if magic == 2 && records_to_parse.len >= 61 {
					crc := header_reader.read_i32() or { 0 }
					attributes := header_reader.read_i16() or { 0 }
					last_offset_delta := header_reader.read_i32() or { 0 }
					base_timestamp := header_reader.read_i64() or { 0 }
					max_timestamp := header_reader.read_i64() or { 0 }
					producer_id := header_reader.read_i64() or { 0 }
					producer_epoch := header_reader.read_i16() or { 0 }
					base_sequence := header_reader.read_i32() or { 0 }

					// Compression type is stored in the lower 3 bits of attributes (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
					compression_type_val := attributes & 0x07
					timestamp_type := (attributes >> 3) & 0x01
					is_transactional := (attributes >> 4) & 0x01
					is_control := (attributes >> 5) & 0x01

					h.logger.debug('RecordBatch attributes detailed', observability.field_string('topic',
						topic_name), observability.field_int('partition', int(p.index)),
						observability.field_string('attributes_raw', u8(attributes).hex()),
						observability.field_int('attributes_int', int(attributes)), observability.field_int('compression_type_val',
						compression_type_val), observability.field_int('timestamp_type',
						int(timestamp_type)), observability.field_bool('is_transactional',
						is_transactional == 1), observability.field_bool('is_control',
						is_control == 1), observability.field_int('base_timestamp', int(base_timestamp)),
						observability.field_int('max_timestamp', int(max_timestamp)),
						observability.field_int('last_offset_delta', int(last_offset_delta)),
						observability.field_int('producer_id', int(producer_id)), observability.field_int('producer_epoch',
						int(producer_epoch)), observability.field_int('base_sequence',
						int(base_sequence)), observability.field_string('crc', int(crc).hex()))

					// Validate and convert compression type
					if compression_type_val > 4 {
						h.logger.error('Invalid compression type detected', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_int('compression_type_val', compression_type_val),
							observability.field_string('error', 'compression type must be 0-4'))
					} else if compression_type_val != 0 {
						// Compressed data — decompression required
						compression_type := unsafe { compression.CompressionType(compression_type_val) }

						h.logger.debug('Compression type detection', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_int('compression_type_val', compression_type_val),
							observability.field_string('compression_name', compression_type.str()))

						// Kafka compressed RecordBatch layout: header (61 bytes) + CRC (4 bytes) + compressed_records (nested RecordBatch)
						header_size := 65
						compressed_data := records_to_parse[header_size..]

						// Log compressed data details
						compressed_preview_len := if compressed_data.len > 64 {
							64
						} else {
							compressed_data.len
						}
						compressed_hex := compressed_data[..compressed_preview_len].hex()
						h.logger.debug('Compressed data details', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_string('compression_type', compression_type.str()),
							observability.field_int('total_record_size', records_to_parse.len),
							observability.field_int('header_size', header_size), observability.field_int('compressed_data_len',
							compressed_data.len), observability.field_string('compressed_data_start',
							compressed_hex))

						// Log before attempting decompression
						h.logger.debug('Starting decompression attempt', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_string('compression_type', compression_type.str()),
							observability.field_int('compressed_bytes', compressed_data.len))

						decompress_start := time.now()
						decompressed_data = h.compression_service.decompress(compressed_data,
							compression_type) or {
							decompress_elapsed := time.since(decompress_start)
							// Detailed error logging on decompression failure
							h.logger.error('Decompression failed with error', observability.field_string('topic',
								topic_name), observability.field_int('partition', int(p.index)),
								observability.field_string('compression_type', compression_type.str()),
								observability.field_int('compressed_bytes', compressed_data.len),
								observability.field_duration('decompress_time', decompress_elapsed),
								observability.field_err_str(err.str()))
							// Additional logging to diagnose decompression failure
							first_bytes := if compressed_data.len >= 8 {
								compressed_data[..8].hex()
							} else {
								compressed_data.hex()
							}
							last_bytes := if compressed_data.len >= 8 {
								compressed_data[compressed_data.len - 8..].hex()
							} else {
								''
							}
							h.logger.error('Compressed data diagnostics', observability.field_string('topic',
								topic_name), observability.field_int('partition', int(p.index)),
								observability.field_string('first_8_bytes', first_bytes),
								observability.field_string('last_8_bytes', last_bytes),
								observability.field_int('data_length', compressed_data.len))
							partitions << ProduceResponsePartition{
								index:            p.index
								error_code:       i16(ErrorCode.corrupt_message)
								base_offset:      -1
								log_append_time:  -1
								log_start_offset: -1
							}
							continue
						}
						decompress_time := time.since(decompress_start)
						was_compressed = true
						original_compression_type = u8(compression_type_val)

						// Detailed logging on successful decompression
						decompressed_preview_len := if decompressed_data.len > 32 {
							32
						} else {
							decompressed_data.len
						}
						decompressed_hex := decompressed_data[..decompressed_preview_len].hex()
						h.logger.debug('Decompression successful', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_string('compression_type', compression_type.str()),
							observability.field_int('compressed_size', compressed_data.len),
							observability.field_int('decompressed_size', decompressed_data.len),
							observability.field_string('decompressed_start', decompressed_hex),
							observability.field_duration('decompress_time', decompress_time))

						// Compute and log compression ratio metrics
						if compressed_data.len > 0 {
							ratio := f64(decompressed_data.len) / f64(compressed_data.len)
							savings_pct := (1.0 - (f64(compressed_data.len) / f64(decompressed_data.len))) * 100.0
							h.logger.debug('Compression ratio metrics', observability.field_string('topic',
								topic_name), observability.field_int('partition', int(p.index)),
								observability.field_string('compression_type', compression_type.str()),
								observability.field_int('compressed_size', compressed_data.len),
								observability.field_int('decompressed_size', decompressed_data.len),
								observability.field_float('ratio', ratio), observability.field_float('savings_percent',
								savings_pct), observability.field_duration('decompress_time',
								decompress_time))
						}
					} else {
						h.logger.debug('No compression (uncompressed records)', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_int('record_size', records_to_parse.len))
					}
				} else {
					// magic != 2: legacy MessageSet v0/v1
					h.logger.debug('Legacy message format detected (magic != 2)', observability.field_string('topic',
						topic_name), observability.field_int('partition', int(p.index)),
						observability.field_int('magic', int(magic)), observability.field_int('buffer_size',
						records_to_parse.len))
				}
			} else {
				// Data smaller than 61 bytes (too small for RecordBatch header)
				h.logger.warn('RecordBatch too small for header parsing', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(p.index)), observability.field_int('buffer_size',
					records_to_parse.len), observability.field_int('required_min_size',
					61))
			}

			// Parse RecordBatch from decompressed or original data
			mut parsed := ParsedRecordBatch{}
			if was_compressed {
				// Decompressed data is a nested (inner) RecordBatch.
				// It starts with last_offset_delta and requires a separate parser.
				mut nested_parsed := parse_nested_record_batch(decompressed_data) or {
					h.logger.error('Failed to parse nested record batch', observability.field_string('topic',
						topic_name), observability.field_int('partition', int(p.index)),
						observability.field_err_str(err.str()))
					partitions << ProduceResponsePartition{
						index:            p.index
						error_code:       i16(ErrorCode.corrupt_message)
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					}
					continue
				}
				// Use base_offset from the outer RecordBatch
				nested_parsed.base_offset = outer_base_offset
				parsed = nested_parsed
			} else {
				// Parse standard RecordBatch
				parsed = parse_record_batch(records_to_parse) or {
					h.logger.error('Failed to parse record batch', observability.field_string('topic',
						topic_name), observability.field_int('partition', int(p.index)),
						observability.field_err_str(err.str()))
					partitions << ProduceResponsePartition{
						index:            p.index
						error_code:       i16(ErrorCode.corrupt_message)
						base_offset:      -1
						log_append_time:  -1
						log_start_offset: -1
					}
					continue
				}
			}

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
						partitions << ProduceResponsePartition{
							index:            p.index
							error_code:       i16(ErrorCode.corrupt_message)
							base_offset:      -1
							log_append_time:  -1
							log_start_offset: -1
						}
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

			// Store records to storage
			result := h.storage.append(topic_name, int(p.index), records_to_store, req.acks) or {
				// Topic not found — attempt auto-creation
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
					// Retry after topic creation
					retry_result := h.storage.append(topic_name, int(p.index), records_to_store,
						req.acks) or {
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
					// Handle other errors
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
