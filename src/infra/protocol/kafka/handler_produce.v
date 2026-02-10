// 인프라 레이어 - Kafka Produce API 핸들러 (API Key 0)
// Produce 요청/응답 타입, 파싱, 인코딩 및 핸들러 구현
//
// 이 모듈은 Kafka Produce API를 구현합니다.
// 프로듀서가 브로커에 메시지를 전송할 때 사용되며,
// 트랜잭션 지원 및 다양한 acks 설정을 지원합니다.
module kafka

import domain
import infra.compression
import infra.observability
import time

// Produce (API Key 0) - 메시지 전송 API

/// Produce 요청 - 프로듀서가 브로커에 메시지를 전송하기 위한 요청
///
/// 여러 토픽과 파티션에 동시에 메시지를 전송할 수 있으며,
/// 트랜잭션 프로듀서의 경우 transactional_id를 포함합니다.
pub struct ProduceRequest {
pub:
	transactional_id ?string               // 트랜잭션 ID (트랜잭션 프로듀서용, v3+)
	acks             i16                   // 확인 수준 (-1: all, 0: none, 1: leader)
	timeout_ms       i32                   // 타임아웃 (밀리초)
	topic_data       []ProduceRequestTopic // 토픽별 데이터
}

/// Produce 요청 토픽 - 전송할 토픽 데이터
pub struct ProduceRequestTopic {
pub:
	name           string                    // 토픽 이름
	topic_id       []u8                      // 토픽 UUID (v13+, 16바이트)
	partition_data []ProduceRequestPartition // 파티션별 데이터
}

/// Produce 요청 파티션 - 전송할 파티션 데이터
pub struct ProduceRequestPartition {
pub:
	index   i32  // 파티션 인덱스
	records []u8 // RecordBatch 또는 MessageSet 데이터
}

/// Produce 응답 - 메시지 전송 결과
pub struct ProduceResponse {
pub:
	topics           []ProduceResponseTopic // 토픽별 응답
	throttle_time_ms i32                    // 스로틀링 시간 (밀리초)
}

/// Produce 응답 토픽 - 토픽별 전송 결과
pub struct ProduceResponseTopic {
pub:
	name       string                     // 토픽 이름
	topic_id   []u8                       // 토픽 UUID (v13+)
	partitions []ProduceResponsePartition // 파티션별 응답
}

/// Produce 응답 파티션 - 파티션별 전송 결과
pub struct ProduceResponsePartition {
pub:
	index            i32 // 파티션 인덱스
	error_code       i16 // 에러 코드
	base_offset      i64 // 첫 번째 메시지의 오프셋
	log_append_time  i64 // 로그 추가 시간 (밀리초, -1: 사용 안 함)
	log_start_offset i64 // 로그 시작 오프셋
}

// Produce 요청을 파싱합니다.
// 버전에 따라 다른 필드들을 읽어 ProduceRequest 구조체를 생성합니다.
fn parse_produce_request(mut reader BinaryReader, version i16, is_flexible bool) !ProduceRequest {
	// v3+에서 transactional_id 필드 추가
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

	// 토픽 배열 파싱
	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topic_data := []ProduceRequestTopic{}
	for _ in 0 .. topic_count {
		mut name := ''
		mut topic_id := []u8{}

		// v13+에서는 토픽 이름 대신 UUID 사용
		if version >= 13 {
			topic_id = reader.read_uuid()!
		} else if is_flexible {
			name = reader.read_compact_string()!
		} else {
			name = reader.read_string()!
		}

		// 파티션 배열 파싱
		partition_count := reader.read_flex_array_len(is_flexible)!

		mut partition_data := []ProduceRequestPartition{}
		for _ in 0 .. partition_count {
			index := reader.read_i32()!
			// 레코드 배치 데이터 읽기
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

/// Produce 응답을 바이트 배열로 인코딩합니다.
/// 버전에 따라 flexible 또는 non-flexible 형식으로 인코딩합니다.
pub fn (r ProduceResponse) encode(version i16) []u8 {
	is_flexible := version >= 9
	mut writer := new_writer()

	// 토픽 배열 인코딩
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		// v13+에서는 토픽 이름 대신 UUID 사용
		if version >= 13 {
			writer.write_uuid(t.topic_id)
		} else if is_flexible {
			writer.write_compact_string(t.name)
		} else {
			writer.write_string(t.name)
		}

		// 파티션 배열 인코딩
		if is_flexible {
			writer.write_compact_array_len(t.partitions.len)
		} else {
			writer.write_array_len(t.partitions.len)
		}

		for p in t.partitions {
			writer.write_i32(p.index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.base_offset)
			// v2+에서 log_append_time 필드 추가
			if version >= 2 {
				writer.write_i64(p.log_append_time)
			}
			// v5+에서 log_start_offset 필드 추가
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// v8+에서 record_errors 배열 추가 (빈 배열)
			if version >= 8 {
				if is_flexible {
					writer.write_compact_array_len(0)
				} else {
					writer.write_array_len(0)
				}
			}
			// v8+에서 error_message 필드 추가 (null)
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

	// v1+에서 throttle_time_ms 필드 추가
	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// Produce 요청을 처리합니다 (Frame 기반).
// 요청된 토픽/파티션에 메시지를 저장하고 결과를 응답으로 반환합니다.
fn (mut h Handler) process_produce(req ProduceRequest, version i16) !ProduceResponse {
	start_time := time.now()
	mut total_records := 0
	mut total_bytes := i64(0)

	// 로깅을 위한 레코드 및 바이트 수 계산
	for t in req.topic_data {
		for p in t.partition_data {
			total_bytes += p.records.len
		}
	}

	h.logger.debug('Processing produce request', observability.field_int('topics', req.topic_data.len),
		observability.field_int('acks', req.acks), observability.field_bytes('total_size',
		total_bytes))

	// 트랜잭션 프로듀서인 경우 유효성 검증
	if txn_id := req.transactional_id {
		if txn_id.len > 0 {
			h.logger.debug('Validating transaction', observability.field_string('txn_id',
				txn_id))

			if mut txn_coord := h.txn_coordinator {
				// 트랜잭션 메타데이터 조회
				meta := txn_coord.get_transaction(txn_id) or {
					h.logger.warn('Transaction not found', observability.field_string('txn_id',
						txn_id))
					return h.build_produce_error_response_typed(req, ErrorCode.transactional_id_not_found)
				}

				// 트랜잭션 상태 검증
				if meta.state != .ongoing {
					h.logger.warn('Invalid transaction state', observability.field_string('txn_id',
						txn_id), observability.field_string('state', meta.state.str()))
					return h.build_produce_error_response_typed(req, ErrorCode.invalid_txn_state)
				}

				// O(1) 조회를 위한 파티션 룩업 맵 생성
				mut partition_set := map[string]bool{}
				for tp in meta.topic_partitions {
					key := '${tp.topic}:${tp.partition}'
					partition_set[key] = true
				}

				// 요청된 파티션이 트랜잭션에 등록되어 있는지 확인
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

					// O(n) 중첩 루프 대신 O(1) 룩업 사용
					for p in t.partition_data {
						key := '${topic_name}:${int(p.index)}'
						if key !in partition_set {
							return h.build_produce_error_response_typed(req, ErrorCode.invalid_txn_state)
						}
					}
				}
			} else {
				// 트랜잭션 코디네이터가 없는 경우
				return h.build_produce_error_response_typed(req, ErrorCode.coordinator_not_available)
			}
		}
	}

	// 각 토픽/파티션에 메시지 저장
	mut topics := []ProduceResponseTopic{}
	for t in req.topic_data {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()

		// v13+에서는 토픽 UUID로 토픽 이름 조회
		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			} else {
				// 토픽 UUID를 찾을 수 없는 경우 에러 응답
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
			// 압축 해제 및 RecordBatch 파싱
			records_to_parse := p.records.clone()
			mut decompressed_data := []u8{}
			mut was_compressed := false
			mut outer_base_offset := i64(0)
			mut original_compression_type := u8(0)

			// Kafka RecordBatch v2 헤더 파싱 (61바이트)
			if records_to_parse.len >= 61 {
				mut header_reader := new_reader(records_to_parse)
				outer_base_offset = header_reader.read_i64() or { 0 }
				batch_length := header_reader.read_i32() or { 0 }
				partition_leader_epoch := header_reader.read_i32() or { 0 }
				magic := header_reader.read_i8() or { 0 }

				// RecordBatch 헤더 파싱 상세 로깅
				h.logger.debug('RecordBatch header parsing', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(p.index)), observability.field_int('buffer_size',
					records_to_parse.len), observability.field_int('base_offset', int(outer_base_offset)),
					observability.field_int('batch_length', int(batch_length)), observability.field_int('leader_epoch',
					int(partition_leader_epoch)), observability.field_int('magic', int(magic)))

				// 원시 바이트 검사 - 헤더의 처음 32바이트를 hex로 출력
				header_preview_len := if records_to_parse.len > 32 {
					32
				} else {
					records_to_parse.len
				}
				header_hex := records_to_parse[..header_preview_len].hex()
				h.logger.debug('RecordBatch header raw bytes (hex)', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(p.index)), observability.field_string('header_hex',
					header_hex), observability.field_int('header_preview_bytes', header_preview_len))

				if magic == 2 && records_to_parse.len >= 61 { // RecordBatch v2
					crc := header_reader.read_i32() or { 0 }
					attributes := header_reader.read_i16() or { 0 }
					last_offset_delta := header_reader.read_i32() or { 0 }
					base_timestamp := header_reader.read_i64() or { 0 }
					max_timestamp := header_reader.read_i64() or { 0 }
					producer_id := header_reader.read_i64() or { 0 }
					producer_epoch := header_reader.read_i16() or { 0 }
					base_sequence := header_reader.read_i32() or { 0 }

					// 압축 타입은 attributes의 하위 3비트에 저장됨 (0: none, 1: gzip, 2: snappy, 3: lz4, 4: zstd)
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

					// 압축 타입 검증 및 변환
					if compression_type_val > 4 {
						h.logger.error('Invalid compression type detected', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_int('compression_type_val', compression_type_val),
							observability.field_string('error', 'compression type must be 0-4'))
					} else if compression_type_val != 0 {
						// 압축된 데이터 - 압축 해제 필요
						compression_type := unsafe { compression.CompressionType(compression_type_val) }

						h.logger.debug('Compression type detection', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_int('compression_type_val', compression_type_val),
							observability.field_string('compression_name', compression_type.str()))

						// Kafka 압축 RecordBatch: header(61 bytes) + CRC(4 bytes) + compressed_records (nested RecordBatch)
						header_size := 65 // 61 bytes header + 4 bytes CRC
						compressed_data := records_to_parse[header_size..]

						// 압축된 데이터 상세 정보 로깅
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

						// 압축 해제 시도 전 로깅
						h.logger.debug('Starting decompression attempt', observability.field_string('topic',
							topic_name), observability.field_int('partition', int(p.index)),
							observability.field_string('compression_type', compression_type.str()),
							observability.field_int('compressed_bytes', compressed_data.len))

						decompress_start := time.now()
						decompressed_data = h.compression_service.decompress(compressed_data,
							compression_type) or {
							decompress_elapsed := time.since(decompress_start)
							// 압축 해제 실패 시 상세 오류 로깅
							h.logger.error('Decompression failed with error', observability.field_string('topic',
								topic_name), observability.field_int('partition', int(p.index)),
								observability.field_string('compression_type', compression_type.str()),
								observability.field_int('compressed_bytes', compressed_data.len),
								observability.field_duration('decompress_time', decompress_elapsed),
								observability.field_err_str(err.str()))
							// 압축 해제 실패 원인 분석을 위한 추가 로깅
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

						// 압축 해제 성공 상세 로깅
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

						// 압축률 메트릭 계산 및 로깅
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
						h.logger.debug('No compression detected (uncompressed records)',
							observability.field_string('topic', topic_name), observability.field_int('partition',
							int(p.index)), observability.field_int('record_size', records_to_parse.len))
					}
				} else {
					// magic != 2 인 경우 (legacy MessageSet v0/v1)
					h.logger.debug('Legacy message format detected (magic != 2)', observability.field_string('topic',
						topic_name), observability.field_int('partition', int(p.index)),
						observability.field_int('magic', int(magic)), observability.field_int('buffer_size',
						records_to_parse.len))
				}
			} else {
				// 61바이트 미만의 데이터 (RecordBatch 헤더보다 작음)
				h.logger.warn('RecordBatch too small for header parsing', observability.field_string('topic',
					topic_name), observability.field_int('partition', int(p.index)), observability.field_int('buffer_size',
					records_to_parse.len), observability.field_int('required_min_size',
					61))
			}

			// 압축 해제된 데이터 또는 원본 데이터로 RecordBatch 파싱
			mut parsed := ParsedRecordBatch{}
			if was_compressed {
				// 압축 해제된 데이터: 중첩 RecordBatch (Inner RecordBatch)
				// last_offset_delta로 시작하므로 별도 파싱 필요
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
				// 외부 RecordBatch의 base_offset 사용
				nested_parsed.base_offset = outer_base_offset
				parsed = nested_parsed
			} else {
				// 일반 RecordBatch 파싱
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

			// 스키마 인코딩이 필요한 경우 처리
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

			// 원본 압축 타입을 각 레코드에 보존 (크로스 브로커 fetch 지원)
			if original_compression_type > 0 {
				for idx in 0 .. records_to_store.len {
					records_to_store[idx] = domain.Record{
						...records_to_store[idx]
						compression_type: original_compression_type
					}
				}
			}

			// 빈 레코드 배치 처리
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

			// 스토리지에 레코드 저장
			result := h.storage.append(topic_name, int(p.index), records_to_store, req.acks) or {
				// 토픽이 존재하지 않으면 자동 생성 시도
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
					// 토픽 생성 후 재시도
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
					// 기타 에러 처리
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

			// 성공 응답
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

// 레거시 핸들러 - process_produce에 위임
fn (mut h Handler) handle_produce(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_produce_request(mut reader, version, is_flexible_version(.produce, version))!
	resp := h.process_produce(req, version)!
	return resp.encode(version)
}

// 모든 파티션에 에러 코드를 설정한 ProduceResponse를 생성합니다 (타입 기반).
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

// 모든 파티션에 에러 코드를 설정한 ProduceResponse를 생성합니다 (레거시, 바이트 배열 반환).
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

// 참고: Fetch (API Key 1)는 handler_fetch.v로 이동됨
// 참고: ListOffsets (API Key 2)는 handler_list_offsets.v로 이동됨
// 참고: RecordBatch 인코딩 및 CRC32-C 계산은 record_batch.v로 이동됨
