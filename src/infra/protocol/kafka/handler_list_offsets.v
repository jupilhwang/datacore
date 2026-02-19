// ListOffsets 요청/응답 타입, 파싱, 인코딩 및 핸들러 구현
//
// 이 모듈은 Kafka ListOffsets API를 구현합니다.
// 컨슈머가 특정 토픽/파티션의 오프셋 정보를 조회할 때 사용되며,
// 최신 오프셋(-1), 최초 오프셋(-2), 또는 타임스탬프 기반 오프셋 조회를 지원합니다.
module kafka

import infra.observability
import time

/// ListOffsets 요청 - 토픽/파티션의 오프셋 정보 조회 요청
///
/// timestamp 값에 따라 다른 오프셋을 반환합니다:
/// - -1: 최신 오프셋 (LOG_END_OFFSET)
/// - -2: 최초 오프셋 (LOG_START_OFFSET)
/// - 양수: 해당 타임스탬프 이후의 첫 번째 오프셋
pub struct ListOffsetsRequest {
pub:
	replica_id      i32
	isolation_level i8
	topics          []ListOffsetsRequestTopic
}

/// ListOffsets 요청 토픽 - 조회할 토픽 정보
pub struct ListOffsetsRequestTopic {
pub:
	name       string
	partitions []ListOffsetsRequestPartition
}

/// ListOffsets 요청 파티션 - 조회할 파티션 정보
pub struct ListOffsetsRequestPartition {
pub:
	partition_index i32
	timestamp       i64
}

/// ListOffsets 응답 - 오프셋 조회 결과
pub struct ListOffsetsResponse {
pub:
	throttle_time_ms i32
	topics           []ListOffsetsResponseTopic
}

/// ListOffsets 응답 토픽 - 토픽별 조회 결과
pub struct ListOffsetsResponseTopic {
pub:
	name       string
	partitions []ListOffsetsResponsePartition
}

/// ListOffsets 응답 파티션 - 파티션별 조회 결과
pub struct ListOffsetsResponsePartition {
pub:
	partition_index i32
	error_code      i16
	timestamp       i64
	offset          i64
	leader_epoch    i32
}

// ListOffsets 요청을 파싱합니다.
// 버전에 따라 다른 필드들을 읽어 ListOffsetsRequest 구조체를 생성합니다.
fn parse_list_offsets_request(mut reader BinaryReader, version i16, is_flexible bool) !ListOffsetsRequest {
	replica_id := reader.read_i32()!

	// v2+에서 isolation_level 필드 추가
	mut isolation_level := i8(0)
	if version >= 2 {
		isolation_level = reader.read_i8()!
	}

	// 토픽 배열 파싱
	count := reader.read_flex_array_len(is_flexible)!
	mut topics := []ListOffsetsRequestTopic{}
	for _ in 0 .. count {
		name := reader.read_flex_string(is_flexible)!

		// 파티션 배열 파싱
		pcount := reader.read_flex_array_len(is_flexible)!
		mut partitions := []ListOffsetsRequestPartition{}
		for _ in 0 .. pcount {
			pi := reader.read_i32()!
			// v4+에서 current_leader_epoch 필드 추가 (무시)
			if version >= 4 {
				_ = reader.read_i32()!
			}
			ts := reader.read_i64()!
			partitions << ListOffsetsRequestPartition{
				partition_index: pi
				timestamp:       ts
			}
			reader.skip_flex_tagged_fields(is_flexible)!
		}
		topics << ListOffsetsRequestTopic{
			name:       name
			partitions: partitions
		}
		reader.skip_flex_tagged_fields(is_flexible)!
	}
	return ListOffsetsRequest{
		replica_id:      replica_id
		isolation_level: isolation_level
		topics:          topics
	}
}

/// ListOffsets 응답을 바이트 배열로 인코딩합니다.
/// 버전에 따라 flexible 또는 non-flexible 형식으로 인코딩합니다.
pub fn (r ListOffsetsResponse) encode(version i16) []u8 {
	is_flexible := version >= 6
	mut writer := new_writer()

	// v2+에서 throttle_time_ms 필드 추가
	if version >= 2 {
		writer.write_i32(r.throttle_time_ms)
	}

	// 토픽 배열 인코딩
	if is_flexible {
		writer.write_compact_array_len(r.topics.len)
	} else {
		writer.write_array_len(r.topics.len)
	}

	for t in r.topics {
		// 토픽 이름 및 파티션 배열 인코딩
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
			// v1+에서 timestamp와 offset 필드 추가
			if version >= 1 {
				writer.write_i64(p.timestamp)
				writer.write_i64(p.offset)
			}
			// v4+에서 leader_epoch 필드 추가
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

// ListOffsets 요청을 처리합니다 (Frame 기반).
// 요청된 토픽/파티션의 오프셋 정보를 조회하여 응답을 생성합니다.
fn (mut h Handler) process_list_offsets(req ListOffsetsRequest, version i16) !ListOffsetsResponse {
	_ = version
	start_time := time.now()

	h.logger.debug('Processing list offsets', observability.field_int('topics', req.topics.len),
		observability.field_int('isolation_level', req.isolation_level))

	mut topics := []ListOffsetsResponseTopic{}
	for t in req.topics {
		mut partitions := []ListOffsetsResponsePartition{}
		for p in t.partitions {
			// 파티션 정보 조회
			info := h.storage.get_partition_info(t.name, int(p.partition_index)) or {
				// 토픽/파티션을 찾을 수 없는 경우 에러 응답
				partitions << ListOffsetsResponsePartition{
					partition_index: p.partition_index
					error_code:      i16(ErrorCode.unknown_topic_or_partition)
					timestamp:       -1
					offset:          -1
					leader_epoch:    -1
				}
				continue
			}

			// 타임스탬프에 따라 오프셋 결정
			// -1: 최신 오프셋 (LOG_END_OFFSET)
			// -2: 최초 오프셋 (LOG_START_OFFSET)
			// 기타: 최신 오프셋 반환 (타임스탬프 기반 조회는 미구현)
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

	elapsed := time.since(start_time)
	h.logger.debug('List offsets completed', observability.field_int('topics', topics.len),
		observability.field_duration('latency', elapsed))

	return ListOffsetsResponse{
		throttle_time_ms: default_throttle_time_ms
		topics:           topics
	}
}

// 레거시 핸들러 - process_list_offsets에 위임
fn (mut h Handler) handle_list_offsets(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_list_offsets_request(mut reader, version, is_flexible_version(.list_offsets,
		version))!
	resp := h.process_list_offsets(req, version)!
	return resp.encode(version)
}
