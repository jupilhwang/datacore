// Helper functions for the Produce handler.
//
// Extracted from process_produce to reduce function size and improve readability.
// - new_error_partition: factory for error partition responses
// - resolve_produce_topic: topic name/ID resolution (v13+ UUID lookup)
// - decompress_and_parse_partition: RecordBatch parsing and decompression
// - store_with_auto_create: append with auto-topic-creation on not-found
module kafka

import domain
import infra.compression
import infra.observability
import time

// ResolvedTopic holds the resolved topic name and ID after UUID lookup.
struct ResolvedTopic {
	name string
	id   []u8
}

// DecompressParseResult holds the result of decompressing and parsing partition records.
struct DecompressParseResult {
	parsed                    ParsedRecordBatch
	original_compression_type u8
}

// new_error_partition creates a ProduceResponsePartition with the given error code
// and default -1 offsets. Centralizes the repeated error-partition construction pattern.
fn new_error_partition(index i32, code ErrorCode) ProduceResponsePartition {
	return ProduceResponsePartition{
		index:            index
		error_code:       i16(code)
		base_offset:      -1
		log_append_time:  -1
		log_start_offset: -1
	}
}

// resolve_produce_topic resolves the topic name and ID from a ProduceRequestTopic.
// For v13+, looks up the topic by UUID. Returns none if UUID not found.
fn (mut h Handler) resolve_produce_topic(t ProduceRequestTopic, version i16) ?ResolvedTopic {
	mut topic_name := t.name
	mut topic_id := t.topic_id.clone()

	if version >= 13 && t.topic_id.len == 16 {
		if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
			topic_name = topic_meta.name
			topic_id = topic_meta.topic_id.clone()
		} else {
			return none
		}
	}

	return ResolvedTopic{
		name: topic_name
		id:   topic_id
	}
}

// decompress_and_parse_partition parses and decompresses a partition's raw RecordBatch data.
// Returns the parsed records and original compression type.
fn (mut h Handler) decompress_and_parse_partition(topic_name string, partition_index i32, raw_records []u8) !DecompressParseResult {
	records_to_parse := raw_records.clone()
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

		h.logger.trace('RecordBatch processing', observability.field_string('topic', topic_name),
			observability.field_int('partition', int(partition_index)), observability.field_int('buffer_size',
			records_to_parse.len), observability.field_int('base_offset', int(outer_base_offset)),
			observability.field_int('batch_length', int(batch_length)), observability.field_int('magic',
			int(magic)))

		$if debug {
			h.logger.debug('RecordBatch header parsing', observability.field_string('topic',
				topic_name), observability.field_int('partition', int(partition_index)),
				observability.field_int('buffer_size', records_to_parse.len), observability.field_int('base_offset',
				int(outer_base_offset)), observability.field_int('batch_length', int(batch_length)),
				observability.field_int('leader_epoch', int(partition_leader_epoch)),
				observability.field_int('magic', int(magic)))
			header_preview_len := if records_to_parse.len > 32 {
				32
			} else {
				records_to_parse.len
			}
			header_hex := records_to_parse[..header_preview_len].hex()
			h.logger.debug('RecordBatch header raw bytes (hex)', observability.field_string('topic',
				topic_name), observability.field_int('partition', int(partition_index)),
				observability.field_string('header_hex', header_hex), observability.field_int('header_preview_bytes',
				header_preview_len))
		}

		if magic == 2 && records_to_parse.len >= 61 {
			decompressed_data, was_compressed, original_compression_type = h.parse_v2_record_batch_body(topic_name,
				partition_index, records_to_parse, mut header_reader)!
		} else {
			$if debug {
				h.logger.debug('Legacy message format detected (magic != 2)', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(partition_index)),
					observability.field_int('magic', int(magic)), observability.field_int('buffer_size',
					records_to_parse.len))
			}
		}
	} else {
		h.logger.warn('RecordBatch too small for header parsing', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_int('buffer_size',
			records_to_parse.len), observability.field_int('required_min_size', 61))
	}

	// Parse RecordBatch from decompressed or original data
	mut parsed := ParsedRecordBatch{}
	if was_compressed {
		mut nested_parsed := parse_nested_record_batch(decompressed_data) or {
			h.logger.error('Failed to parse nested record batch', observability.field_string('topic',
				topic_name), observability.field_int('partition', int(partition_index)),
				observability.field_err_str(err.str()))
			return error('failed to parse nested record batch: ${err}')
		}
		nested_parsed.base_offset = outer_base_offset
		parsed = nested_parsed
	} else {
		parsed = parse_record_batch(records_to_parse) or {
			h.logger.error('Failed to parse record batch', observability.field_string('topic',
				topic_name), observability.field_int('partition', int(partition_index)),
				observability.field_err_str(err.str()))
			return error('failed to parse record batch: ${err}')
		}
	}

	return DecompressParseResult{
		parsed:                    parsed
		original_compression_type: original_compression_type
	}
}

// parse_v2_record_batch_body parses the body of a RecordBatch v2 (magic=2),
// handling attribute extraction and decompression.
// Returns (decompressed_data, was_compressed, original_compression_type).
fn (mut h Handler) parse_v2_record_batch_body(topic_name string, partition_index i32, records_to_parse []u8, mut header_reader BinaryReader) !([]u8, bool, u8) {
	crc := header_reader.read_i32() or { 0 }
	attributes := header_reader.read_i16() or { 0 }
	last_offset_delta := header_reader.read_i32() or { 0 }
	base_timestamp := header_reader.read_i64() or { 0 }
	max_timestamp := header_reader.read_i64() or { 0 }
	producer_id := header_reader.read_i64() or { 0 }
	producer_epoch := header_reader.read_i16() or { 0 }
	base_sequence := header_reader.read_i32() or { 0 }
	// records_count field (4 bytes) must be consumed to advance reader to offset 61
	// where the actual record/compressed data begins
	records_count := header_reader.read_i32() or { 0 }

	$if debug {
		h.logger.debug('RecordBatch records_count', observability.field_string('topic',
			topic_name), observability.field_int('records_count', int(records_count)))
	}

	// Compression type is stored in the lower 3 bits of attributes
	// (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
	compression_type_val := attributes & 0x07
	timestamp_type := (attributes >> 3) & 0x01
	is_transactional := (attributes >> 4) & 0x01
	is_control := (attributes >> 5) & 0x01

	$if debug {
		h.logger.debug('RecordBatch attributes detailed', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_string('attributes_raw',
			u8(attributes).hex()), observability.field_int('attributes_int', int(attributes)),
			observability.field_int('compression_type_val', compression_type_val), observability.field_int('timestamp_type',
			int(timestamp_type)), observability.field_bool('is_transactional', is_transactional == 1),
			observability.field_bool('is_control', is_control == 1), observability.field_int('base_timestamp',
			int(base_timestamp)), observability.field_int('max_timestamp', int(max_timestamp)),
			observability.field_int('last_offset_delta', int(last_offset_delta)), observability.field_int('producer_id',
			int(producer_id)), observability.field_int('producer_epoch', int(producer_epoch)),
			observability.field_int('base_sequence', int(base_sequence)), observability.field_string('crc',
			int(crc).hex()))
	}

	// Validate and handle compression type
	if compression_type_val > 4 {
		h.logger.error('Invalid compression type detected', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_int('compression_type_val',
			compression_type_val), observability.field_string('error', 'compression type must be 0-4'))
		return []u8{}, false, u8(0)
	} else if compression_type_val != 0 {
		decompressed, comp_type := h.decompress_record_data(topic_name, partition_index,
			records_to_parse, compression_type_val)!
		return decompressed, true, comp_type
	} else {
		$if debug {
			h.logger.debug('No compression (uncompressed records)', observability.field_string('topic',
				topic_name), observability.field_int('partition', int(partition_index)),
				observability.field_int('record_size', records_to_parse.len))
		}
		return []u8{}, false, u8(0)
	}
}

// decompress_record_data decompresses the record data portion of a RecordBatch.
// The header (61 bytes) is stripped before decompression.
// Returns (decompressed_data, original_compression_type).
fn (mut h Handler) decompress_record_data(topic_name string, partition_index i32, records_to_parse []u8, compression_type_val int) !([]u8, u8) {
	compression_type := compression.compression_type_from_i16(i16(compression_type_val)) or {
		h.logger.error('Invalid compression type value', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_int('compression_type_val',
			compression_type_val), observability.field_string('error', err.msg()))
		return error('invalid compression type: ${err}')
	}

	$if debug {
		h.logger.debug('Compression type detection', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_int('compression_type_val',
			compression_type_val), observability.field_string('compression_name', compression_type.str()))
	}

	// RecordBatch v2 header layout (61 bytes total):
	// baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) + magic(1)
	// + crc(4) + attributes(2) + lastOffsetDelta(4) + firstTimestamp(8)
	// + maxTimestamp(8) + producerId(8) + producerEpoch(2)
	// + baseSequence(4) + recordsCount(4) = 61 bytes
	header_size := 61
	compressed_data := records_to_parse[header_size..]

	$if debug {
		compressed_preview_len := if compressed_data.len > 64 {
			64
		} else {
			compressed_data.len
		}
		compressed_hex := compressed_data[..compressed_preview_len].hex()
		h.logger.debug('Compressed data details', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_string('compression_type',
			compression_type.str()), observability.field_int('total_record_size', records_to_parse.len),
			observability.field_int('header_size', header_size), observability.field_int('compressed_data_len',
			compressed_data.len), observability.field_string('compressed_data_start',
			compressed_hex))
		h.logger.debug('Starting decompression attempt', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_string('compression_type',
			compression_type.str()), observability.field_int('compressed_bytes', compressed_data.len))
	}

	decompress_start := time.now()
	decompressed_data := h.compression_service.decompress(compressed_data, i16(compression_type)) or {
		decompress_elapsed := time.since(decompress_start)
		h.logger.error('Decompression failed with error', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_string('compression_type',
			compression_type.str()), observability.field_int('compressed_bytes', compressed_data.len),
			observability.field_duration('decompress_time', decompress_elapsed), observability.field_err_str(err.str()))
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
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_string('first_8_bytes',
			first_bytes), observability.field_string('last_8_bytes', last_bytes), observability.field_int('data_length',
			compressed_data.len))
		return error('decompression failed: ${err}')
	}
	decompress_time := time.since(decompress_start)

	$if debug {
		decompressed_preview_len := if decompressed_data.len > 32 {
			32
		} else {
			decompressed_data.len
		}
		decompressed_hex := decompressed_data[..decompressed_preview_len].hex()
		h.logger.debug('Decompression successful', observability.field_string('topic',
			topic_name), observability.field_int('partition', int(partition_index)), observability.field_string('compression_type',
			compression_type.str()), observability.field_int('compressed_size', compressed_data.len),
			observability.field_int('decompressed_size', decompressed_data.len), observability.field_string('decompressed_start',
			decompressed_hex), observability.field_duration('decompress_time', decompress_time))
		if compressed_data.len > 0 {
			ratio := f64(decompressed_data.len) / f64(compressed_data.len)
			savings_pct := (1.0 - (f64(compressed_data.len) / f64(decompressed_data.len))) * 100.0
			h.logger.debug('Compression ratio metrics', observability.field_string('topic',
				topic_name), observability.field_int('partition', int(partition_index)),
				observability.field_string('compression_type', compression_type.str()),
				observability.field_int('compressed_size', compressed_data.len), observability.field_int('decompressed_size',
				decompressed_data.len), observability.field_float('ratio', ratio), observability.field_float('savings_percent',
				savings_pct), observability.field_duration('decompress_time', decompress_time))
		}
	}

	return decompressed_data, u8(compression_type_val)
}

// store_with_auto_create appends records to storage, auto-creating the topic if not found.
fn (mut h Handler) store_with_auto_create(topic_name string, partition_index i32, records []domain.Record, acks i16) !domain.AppendResult {
	return h.storage.append(topic_name, int(partition_index), records, acks) or {
		if err.str().contains('not found') {
			num_partitions := if int(partition_index) >= 1 { int(partition_index) + 1 } else { 1 }
			h.storage.create_topic(topic_name, num_partitions, domain.TopicConfig{})!
			return h.storage.append(topic_name, int(partition_index), records, acks)
		}
		return err
	}
}
