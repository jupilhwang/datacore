// Kafka 프로토콜 - FindCoordinator (API Key 10)
// 요청/응답 타입, 파싱, 인코딩 및 핸들러
//
// 버전 히스토리:
// - v0: 기본 FindCoordinator
// - v1: KeyType 추가 (GROUP=0, TRANSACTION=1)
// - v2: v1과 동일
// - v3: Flexible 버전
// - v4: 배치 조회 지원 (CoordinatorKeys, KIP-699)
// - v5: TRANSACTION_ABORTABLE 에러 코드 지원 (KIP-890)
// - v6: Share Groups 지원 (KeyType=2, KIP-932)
module kafka

import domain
import infra.observability
import time

// FindCoordinator (API Key 10)

/// CoordinatorKeyType은 코디네이터 키 타입을 정의합니다.
/// v1부터 지원되며, v6에서 SHARE 타입이 추가되었습니다.
pub enum CoordinatorKeyType as i8 {
	group       = 0 // 컨슈머 그룹 코디네이터
	transaction = 1 // 트랜잭션 코디네이터
	share       = 2 // Share Group 코디네이터 (v6, KIP-932)
}

pub struct FindCoordinatorRequest {
pub:
	key              string
	key_type         i8
	coordinator_keys []string
}

pub struct FindCoordinatorResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    ?string
	node_id          i32
	host             string
	port             i32
	coordinators     []FindCoordinatorResponseNode
}

pub struct FindCoordinatorResponseNode {
pub:
	key           string
	node_id       i32
	host          string
	port          i32
	error_code    i16
	error_message ?string
}

fn parse_find_coordinator_request(mut reader BinaryReader, version i16, is_flexible bool) !FindCoordinatorRequest {
	mut key := ''
	mut coordinator_keys := []string{}
	mut key_type := i8(0)

	if version <= 3 {
		key = reader.read_flex_string(is_flexible)!
	}

	if version >= 1 {
		key_type = reader.read_i8()!
	}

	if version >= 4 {
		keys_len := reader.read_flex_array_len(is_flexible)!
		if keys_len > 0 {
			for _ in 0 .. keys_len {
				k := reader.read_flex_string(is_flexible)!
				coordinator_keys << k
			}
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return FindCoordinatorRequest{
		key:              key
		key_type:         key_type
		coordinator_keys: coordinator_keys
	}
}

pub fn (r FindCoordinatorResponse) encode(version i16) []u8 {
	is_flexible := version >= 3
	mut writer := new_writer()

	if version >= 1 {
		writer.write_i32(r.throttle_time_ms)
	}

	if version <= 3 {
		writer.write_i16(r.error_code)
		if version >= 1 {
			if is_flexible {
				writer.write_compact_nullable_string(r.error_message)
			} else {
				writer.write_nullable_string(r.error_message)
			}
		}

		writer.write_i32(r.node_id)
		if is_flexible {
			writer.write_compact_string(r.host)
		} else {
			writer.write_string(r.host)
		}
		writer.write_i32(r.port)
	} else {
		if is_flexible {
			writer.write_compact_array_len(r.coordinators.len)
		} else {
			writer.write_array_len(r.coordinators.len)
		}
		for node in r.coordinators {
			if is_flexible {
				writer.write_compact_string(node.key)
			} else {
				writer.write_string(node.key)
			}
			writer.write_i32(node.node_id)
			if is_flexible {
				writer.write_compact_string(node.host)
			} else {
				writer.write_string(node.host)
			}
			writer.write_i32(node.port)
			writer.write_i16(node.error_code)
			if is_flexible {
				writer.write_compact_nullable_string(node.error_message)
			} else {
				writer.write_nullable_string(node.error_message)
			}
			if is_flexible {
				writer.write_tagged_fields()
			}
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

/// coordinator_key_type_str은 코디네이터 키 타입을 문자열로 변환합니다.
fn coordinator_key_type_str(key_type i8) string {
	return match key_type {
		0 { 'GROUP' }
		1 { 'TRANSACTION' }
		2 { 'SHARE' } // v6, KIP-932
		else { 'UNKNOWN' }
	}
}

fn (mut h Handler) handle_find_coordinator(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_find_coordinator_request(mut reader, version, is_flexible_version(.find_coordinator,
		version))!

	key_type_str := coordinator_key_type_str(req.key_type)
	h.logger.debug('Processing find coordinator', observability.field_string('key', req.key),
		observability.field_string('key_type', key_type_str), observability.field_int('coordinator_keys',
		req.coordinator_keys.len), observability.field_int('version', version))

	resp := h.process_find_coordinator(req, version)!

	elapsed := time.since(start_time)
	h.logger.debug('Find coordinator completed', observability.field_int('node_id', h.broker_id),
		observability.field_duration('latency', elapsed))

	return resp.encode(version)
}

// compute_coordinator_broker는 group_id를 기반으로 코디네이터 브로커를 결정합니다.
// 파티션 할당 서비스가 있으면 사용하고, 없으면 해시 기반으로 브로커를 선택합니다.
fn (mut h Handler) compute_coordinator_broker(key string, key_type i8) (i32, string, i32) {
	// 멀티 브로커 모드에서 활성 브로커 목록 조회
	mut active_brokers := []domain.BrokerInfo{}
	if mut registry := h.broker_registry {
		active_brokers = registry.list_active_brokers() or { []domain.BrokerInfo{} }
	}

	// 브로커가 없거나 싱글 브로커 모드인 경우 자신을 반환
	if active_brokers.len == 0 {
		return h.broker_id, h.host, h.broker_port
	}

	// 파티션 할당 서비스가 있고 GROUP 타입인 경우 할당 기반 코디네이터 선택
	if key_type == i8(CoordinatorKeyType.group) {
		if mut assigner := h.partition_assigner {
			// group_id를 해시하여 파티션 번호 결정 (0-999 범위)
			mut hash := i32(0)
			for c in key.bytes() {
				hash = (hash * 31 + i32(c)) & 0x7FFFFFFF // 양수로 유지
			}
			partition := hash % 1000

			// 할당 서비스에서 해당 파티션의 리더 브로커 조회
			coordinator_id := assigner.get_partition_leader('__consumer_offsets', partition) or {
				// 할당이 없으면 해시 기반으로 브로커 선택
				active_brokers[hash % active_brokers.len].broker_id
			}

			// 선택된 브로커 정보 찾기
			for broker in active_brokers {
				if broker.broker_id == coordinator_id {
					return broker.broker_id, broker.host, broker.port
				}
			}
		}
	}

	// 기본: 해시 기반 브로커 선택 (라운드 로빈)
	mut hash := i32(0)
	for c in key.bytes() {
		hash = (hash * 31 + i32(c)) & 0x7FFFFFFF
	}
	selected_idx := hash % active_brokers.len
	selected := active_brokers[selected_idx]
	return selected.broker_id, selected.host, selected.port
}

fn (mut h Handler) process_find_coordinator(req FindCoordinatorRequest, version i16) !FindCoordinatorResponse {
	// v4+: 배치 조회 응답 (v5, v6 포함)
	// v5는 TRANSACTION_ABORTABLE 에러 코드 지원 추가 (KIP-890)
	// v6는 Share Groups 지원 추가 (KIP-932)
	if version >= 4 {
		mut keys := req.coordinator_keys.clone()
		if keys.len == 0 && req.key.len > 0 {
			keys << req.key
		}

		mut coordinators := []FindCoordinatorResponseNode{}
		for key in keys {
			// Share Group (v6)의 경우 키 형식 검증: "groupId:topicId:partition"
			if version >= 6 && req.key_type == i8(CoordinatorKeyType.share) {
				// Share Group 키 형식 검증
				parts := key.split(':')
				if parts.len != 3 {
					coordinators << FindCoordinatorResponseNode{
						key:           key
						node_id:       -1
						host:          ''
						port:          0
						error_code:    i16(ErrorCode.invalid_request)
						error_message: 'Invalid share group key format. Expected: groupId:topicId:partition'
					}
					continue
				}
			}

			// 코디네이터 브로커 결정 (할당 기반 또는 해시 기반)
			node_id, host, port := h.compute_coordinator_broker(key, req.key_type)

			coordinators << FindCoordinatorResponseNode{
				key:           key
				node_id:       node_id
				host:          host
				port:          port
				error_code:    0
				error_message: none
			}
		}

		return FindCoordinatorResponse{
			throttle_time_ms: 0
			coordinators:     coordinators
		}
	}

	// v0-v3: 단일 응답
	// 코디네이터 브로커 결정 (할당 기반 또는 해시 기반)
	node_id, host, port := h.compute_coordinator_broker(req.key, req.key_type)

	return FindCoordinatorResponse{
		throttle_time_ms: 0
		error_code:       0
		error_message:    none
		node_id:          node_id
		host:             host
		port:             port
		coordinators:     []FindCoordinatorResponseNode{}
	}
}
