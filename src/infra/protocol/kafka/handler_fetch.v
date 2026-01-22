// 인프라 레이어 - Kafka Fetch API 핸들러 (API Key 1)
// Fetch 요청/응답 타입, 파싱, 인코딩 및 핸들러 구현
//
// 이 모듈은 Kafka Fetch API를 구현합니다.
// 컨슈머가 브로커로부터 메시지를 가져올 때 사용되며,
// 토픽/파티션별 오프셋 기반 데이터 조회를 지원합니다.
module kafka

import infra.observability
import time

// ============================================================================
// Fetch (API Key 1) - 메시지 조회 API
// ============================================================================

/// Fetch 요청 - 컨슈머가 브로커에서 메시지를 가져오기 위한 요청
///
/// 여러 토픽과 파티션에서 동시에 데이터를 가져올 수 있으며,
/// max_wait_ms와 min_bytes를 통해 long polling을 지원합니다.
pub struct FetchRequest {
pub:
	replica_id            i32                          // 복제본 ID (-1: 컨슈머, 0+: 팔로워 브로커)
	max_wait_ms           i32                          // 최대 대기 시간 (밀리초)
	min_bytes             i32                          // 최소 응답 바이트 수
	max_bytes             i32                          // 최대 응답 바이트 수
	isolation_level       i8                           // 격리 수준 (0: read_uncommitted, 1: read_committed)
	topics                []FetchRequestTopic          // 조회할 토픽 목록
	forgotten_topics_data []FetchRequestForgottenTopic // 세션에서 제거할 토픽 (v7+)
}

/// Fetch 요청에서 잊혀진 토픽 - 세션 기반 fetch에서 더 이상 필요없는 토픽
pub struct FetchRequestForgottenTopic {
pub:
	name       string // 토픽 이름
	topic_id   []u8   // 토픽 UUID (v13+)
	partitions []i32  // 잊혀진 파티션 목록
}

/// Fetch 요청 토픽 - 조회할 토픽 정보
pub struct FetchRequestTopic {
pub:
	name       string                  // 토픽 이름
	topic_id   []u8                    // 토픽 UUID (v13+, 16바이트)
	partitions []FetchRequestPartition // 조회할 파티션 목록
}

/// Fetch 요청 파티션 - 조회할 파티션 정보
pub struct FetchRequestPartition {
pub:
	partition           i32 // 파티션 인덱스
	fetch_offset        i64 // 조회 시작 오프셋
	partition_max_bytes i32 // 파티션당 최대 바이트 수
}

/// Fetch 응답 - 조회된 메시지 데이터를 포함하는 응답
pub struct FetchResponse {
pub:
	throttle_time_ms i32                  // 스로틀링 시간 (밀리초)
	error_code       i16                  // 최상위 에러 코드
	session_id       i32                  // 세션 ID (v7+)
	topics           []FetchResponseTopic // 응답 토픽 목록
}

/// Fetch 응답 토픽 - 토픽별 조회 결과
pub struct FetchResponseTopic {
pub:
	name       string                   // 토픽 이름
	topic_id   []u8                     // 토픽 UUID (v13+)
	partitions []FetchResponsePartition // 파티션별 응답
}

/// Fetch 응답 파티션 - 파티션별 조회 결과
pub struct FetchResponsePartition {
pub:
	partition_index    i32  // 파티션 인덱스
	error_code         i16  // 에러 코드
	high_watermark     i64  // 하이 워터마크 (커밋된 최신 오프셋)
	last_stable_offset i64  // 마지막 안정 오프셋 (트랜잭션 완료된 오프셋)
	log_start_offset   i64  // 로그 시작 오프셋
	records            []u8 // 레코드 배치 데이터
}

// Fetch 요청을 파싱합니다.
// 버전에 따라 다른 필드들을 읽어 FetchRequest 구조체를 생성합니다.
fn parse_fetch_request(mut reader BinaryReader, version i16, is_flexible bool) !FetchRequest {
	mut replica_id := i32(-1)
	// v15+에서는 replica_id가 본문에서 제거되고 tagged field로 이동
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!

	// v3+에서 max_bytes 필드 추가
	mut max_bytes := i32(0x7fffffff)
	if version >= 3 {
		max_bytes = reader.read_i32()!
	}

	// v4+에서 isolation_level 필드 추가
	mut isolation_level := i8(0)
	if version >= 4 {
		isolation_level = reader.read_i8()!
	}

	// v7+에서 세션 기반 fetch 지원
	if version >= 7 {
		_ = reader.read_i32()! // session_id (무상태 모드에서는 무시)
		_ = reader.read_i32()! // session_epoch (무상태 모드에서는 무시)
	}

	// 토픽 배열 파싱
	topic_count := reader.read_flex_array_len(is_flexible)!

	mut topics := []FetchRequestTopic{}
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

		mut partitions := []FetchRequestPartition{}
		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			// v9+에서 current_leader_epoch 필드 추가 (무시)
			if version >= 9 {
				_ = reader.read_i32()! // current_leader_epoch
			}

			fetch_offset := reader.read_i64()!

			// v5+에서 log_start_offset 필드 추가 (무시)
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

	// v7+에서 잊혀진 토픽 데이터 파싱 (세션 기반 fetch용)
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

	// v11+에서 rack_id 필드 추가 (무시)
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

/// Fetch 응답을 바이트 배열로 인코딩합니다.
/// 버전에 따라 flexible 또는 non-flexible 형식으로 인코딩합니다.
pub fn (r FetchResponse) encode(version i16) []u8 {
	is_flexible := version >= 12
	mut writer := new_writer()

	// v1+에서 throttle_time_ms 필드 추가
	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}
	// v7+에서 최상위 에러 코드와 세션 ID 추가
	if version >= 7 {
		writer.write_i16(r.error_code)
		writer.write_i32(r.session_id)
	}

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
			writer.write_i32(p.partition_index)
			writer.write_i16(p.error_code)
			writer.write_i64(p.high_watermark)
			// v4+에서 last_stable_offset 필드 추가
			if version >= 4 {
				writer.write_i64(p.last_stable_offset)
			}
			// v5+에서 log_start_offset 필드 추가
			if version >= 5 {
				writer.write_i64(p.log_start_offset)
			}
			// v4+에서 aborted_transactions 배열 추가 (빈 배열)
			if version >= 4 {
				if is_flexible {
					writer.write_uvarint(1) // compact array: 길이 + 1
				} else {
					writer.write_array_len(0)
				}
			}
			// v11+에서 preferred_read_replica 필드 추가
			if version >= 11 {
				writer.write_i32(-1) // -1: 선호 복제본 없음
			}
			// 레코드 배치 데이터 인코딩
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

// Fetch 요청을 처리합니다 (Frame 기반).
// 요청된 토픽/파티션에서 메시지를 조회하여 응답을 생성합니다.
fn (mut h Handler) process_fetch(req FetchRequest, version i16) !FetchResponse {
	start_time := time.now()

	h.logger.debug('Processing fetch request', observability.field_int('version', version),
		observability.field_int('topics', req.topics.len), observability.field_int('max_wait_ms',
		req.max_wait_ms), observability.field_int('max_bytes', req.max_bytes))

	mut topics := []FetchResponseTopic{}
	mut total_records := 0
	mut total_bytes := i64(0)

	for t in req.topics {
		mut topic_name := t.name
		mut topic_id := t.topic_id.clone()
		// v13+에서는 토픽 UUID로 토픽 이름을 조회
		if version >= 13 && t.topic_id.len == 16 {
			if topic_meta := h.storage.get_topic_by_id(t.topic_id) {
				topic_name = topic_meta.name
				topic_id = topic_meta.topic_id.clone()
			}
		}

		// 각 파티션에서 데이터 조회
		mut partitions := []FetchResponsePartition{}
		for p in t.partitions {
			// 스토리지에서 메시지 조회
			result := h.storage.fetch(topic_name, int(p.partition), p.fetch_offset, p.partition_max_bytes) or {
				// 에러 발생 시 적절한 에러 코드 설정
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

			// 조회된 레코드를 RecordBatch 형식으로 인코딩
			records_data := encode_record_batch_zerocopy(result.records, p.fetch_offset)
			total_records += result.records.len
			total_bytes += records_data.len

			h.logger.trace('Fetch partition success', observability.field_string('topic',
				topic_name), observability.field_int('partition', p.partition), observability.field_int('records',
				result.records.len), observability.field_bytes('response_size', records_data.len))

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
	h.logger.debug('Fetch request completed', observability.field_int('topics', topics.len),
		observability.field_int('total_records', total_records), observability.field_bytes('total_bytes',
		total_bytes), observability.field_duration('latency', elapsed))

	return FetchResponse{
		throttle_time_ms: 0
		error_code:       0
		session_id:       0
		topics:           topics
	}
}

// 레거시 핸들러 - SimpleFetchRequest를 사용하여 zerocopy_fetch.v와 호환성 유지
fn (mut h Handler) handle_fetch(body []u8, version i16) ![]u8 {
	flexible := is_flexible_version(.fetch, version)
	simple_req := parse_fetch_request_simple(body, version, flexible)!

	// SimpleFetchRequest를 FetchRequest로 변환
	req := simple_fetch_to_fetch_request(simple_req)
	resp := h.process_fetch(req, version)!

	encoded := resp.encode(version)
	// 디버그용 응답 정보 출력
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

// SimpleFetchRequest를 FetchRequest로 변환합니다.
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
// SimpleFetchRequest 파서 (Fetch 요청 파싱용)
// ============================================================================

/// SimpleFetchRequest - 경량 Fetch 요청 구조체
/// zerocopy_fetch.v와의 호환성을 위해 사용됩니다.
pub struct SimpleFetchRequest {
pub:
	replica_id       i32                // 복제본 ID (-1: 컨슈머)
	max_wait_ms      i32                // 최대 대기 시간
	min_bytes        i32                // 최소 응답 바이트
	max_bytes        i32                // 최대 응답 바이트
	isolation_level  i8                 // 격리 수준
	session_id       i32                // 세션 ID (무상태: 무시, 항상 0으로 응답)
	session_epoch    i32                // 세션 에포크 (무상태: 무시)
	topics           []SimpleFetchTopic // 조회할 토픽 목록
	forgotten_topics []ForgottenTopic   // 잊혀진 토픽 (무상태: 무시)
	rack_id          string             // 랙 ID (v11+)
}

/// SimpleFetchTopic - Fetch 요청의 토픽 데이터
pub struct SimpleFetchTopic {
pub:
	topic_id   []u8                   // 토픽 UUID (v13+)
	name       string                 // 토픽 이름
	partitions []SimpleFetchPartition // 파티션 목록
}

/// SimpleFetchPartition - Fetch 요청의 파티션 데이터
pub struct SimpleFetchPartition {
pub:
	partition            i32 // 파티션 인덱스
	current_leader_epoch i32 // 현재 리더 에포크 (v9+)
	fetch_offset         i64 // 조회 시작 오프셋
	last_fetched_epoch   i32 // 마지막 조회 에포크 (v12+)
	log_start_offset     i64 // 로그 시작 오프셋 (v5+)
	partition_max_bytes  i32 // 파티션당 최대 바이트
}

/// ForgottenTopic - 세션 추적용 잊혀진 토픽
pub struct ForgottenTopic {
pub:
	topic_id   []u8   // 토픽 UUID
	name       string // 토픽 이름
	partitions []i32  // 잊혀진 파티션 목록
}

/// 경량 Fetch 요청 파서
/// SimpleFetchRequest 구조체로 파싱하여 반환합니다.
pub fn parse_fetch_request_simple(data []u8, version i16, flexible bool) !SimpleFetchRequest {
	mut reader := new_reader(data)

	// v15+에서는 replica_id가 본문에서 제거되고 tagged field의 replica_state로 대체됨
	// v0-14: replica_id (INT32)가 첫 번째 필드
	// v15+: replica_id가 본문에 없음 (tagged field에 replica_state로 존재)
	mut replica_id := i32(-1) // 컨슈머의 기본값
	if version < 15 {
		replica_id = reader.read_i32()!
	}

	max_wait_ms := reader.read_i32()!
	min_bytes := reader.read_i32()!

	// v3+에서 max_bytes 필드 추가
	max_bytes := if version >= 3 { reader.read_i32()! } else { i32(0x7FFFFFFF) }

	// v4+에서 isolation_level 필드 추가
	isolation_level := if version >= 4 { reader.read_i8()! } else { i8(0) }

	// v7+에서 session_id와 session_epoch 필드 추가
	session_id := if version >= 7 { reader.read_i32()! } else { i32(0) }
	session_epoch := if version >= 7 { reader.read_i32()! } else { i32(-1) }

	// 토픽 배열 파싱
	mut topics := []SimpleFetchTopic{}
	topic_count := if flexible {
		reader.read_compact_array_len()!
	} else {
		reader.read_array_len()!
	}

	for _ in 0 .. topic_count {
		// v13+에서는 토픽 UUID 사용
		mut topic_id := []u8{}
		mut topic_name := ''

		if version >= 13 {
			topic_id = reader.read_uuid()!
		}
		if version < 13 || !flexible {
			topic_name = if flexible { reader.read_compact_string()! } else { reader.read_string()! }
		}

		// 파티션 배열 파싱
		mut partitions := []SimpleFetchPartition{}
		partition_count := if flexible {
			reader.read_compact_array_len()!
		} else {
			reader.read_array_len()!
		}

		for _ in 0 .. partition_count {
			partition := reader.read_i32()!

			// v9+에서 current_leader_epoch 필드 추가
			current_leader_epoch := if version >= 9 { reader.read_i32()! } else { i32(-1) }

			fetch_offset := reader.read_i64()!

			// v12+에서 last_fetched_epoch 필드 추가
			last_fetched_epoch := if version >= 12 { reader.read_i32()! } else { i32(-1) }

			// v5+에서 log_start_offset 필드 추가
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

	// v7+에서 잊혀진 토픽 배열 파싱
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

	// v11+에서 rack_id 필드 추가
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
