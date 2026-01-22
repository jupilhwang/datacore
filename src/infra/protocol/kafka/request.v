// 인프라 레이어 - Kafka 요청 파싱
// 공통 요청 구조체 및 파싱 함수
//
// 이 모듈은 Kafka 프로토콜의 요청 헤더 파싱을 담당합니다.
// 요청 헤더 버전(v0, v1, v2)에 따라 다른 형식을 지원하며,
// flexible 버전(Kafka 2.4+)의 tagged fields도 처리합니다.
module kafka

/// 요청 헤더 v0 (대부분의 요청에서 사용)
///
/// 참고: v0은 client_id에 STRING(non-null)을 사용하고,
/// v1+는 NULLABLE_STRING을 사용합니다.
pub struct RequestHeader {
pub:
	api_key        i16    // API 키 (요청 유형 식별)
	api_version    i16    // API 버전
	correlation_id i32    // 요청-응답 매칭을 위한 상관 ID
	client_id      string // 클라이언트 ID (빈 문자열 = null/없음)
}

/// 요청 헤더 v2 (flexible 버전, Kafka 2.4+)
///
/// 참고: client_id는 여전히 NULLABLE_STRING이며, compact가 아닙니다!
/// tag_buffer만 compact 형식입니다.
pub struct RequestHeaderV2 {
pub:
	api_key        i16           // API 키
	api_version    i16           // API 버전
	correlation_id i32           // 상관 ID
	client_id      string        // 클라이언트 ID
	tagged_fields  []TaggedField // 태그된 필드 목록
}

/// 태그된 필드 - flexible 버전에서 확장 데이터를 전달
pub struct TaggedField {
pub:
	tag  u64  // 태그 번호
	data []u8 // 필드 데이터
}

/// 일반 요청 래퍼 - 헤더와 본문을 포함
pub struct Request {
pub:
	header RequestHeader // 요청 헤더
	body   []u8          // 요청 본문 (API별로 다른 형식)
}

/// 주어진 API에 대한 요청 헤더 버전을 반환합니다.
///
/// 반환값: 0 = v0 (레거시), 1 = v1 (non-flexible), 2 = v2 (flexible, tag_buffer 포함)
///
/// 참고: ApiVersions 요청은 일반 규칙을 따릅니다 (v3+는 헤더 v2 사용)
///       ApiVersions 응답만 특별합니다 (항상 헤더 v0, KIP-511)
pub fn get_request_header_version(api_key ApiKey, api_version i16) i16 {
	// SaslHandshake와 ApiVersions는 항상 헤더 v1 사용 (non-flexible 헤더)
	// ApiVersions의 경우, 이는 KIP-511에 따른 하위 호환성 보장을 위함
	if api_key == .sasl_handshake {
		return 1
	}
	// ApiVersions의 경우, v3+는 헤더 v2 (Flexible) 사용
	// v0-v2는 헤더 v1 (Non-flexible) 사용
	if api_key == .api_versions {
		return if api_version >= 3 { i16(2) } else { i16(1) }
	}
	// 다른 모든 API는 일반 규칙을 따름:
	// - Flexible API 버전은 헤더 v2 사용
	// - Non-flexible API 버전은 헤더 v1 사용
	return if is_flexible_version(api_key, api_version) { i16(2) } else { i16(1) }
}

/// 주어진 API에 대한 응답 헤더 버전을 반환합니다.
///
/// 반환값: 0 = v0 (tag_buffer 없음), 1 = v1 (tag_buffer 포함)
///
/// 중요: ApiVersions 응답은 항상 헤더 v0입니다! (KIP-511 특별 규칙)
/// 이는 클라이언트가 서버의 기능을 알기 전에도 ApiVersions 응답을
/// 항상 파싱할 수 있도록 보장합니다.
pub fn get_response_header_version(api_key ApiKey, api_version i16) i16 {
	// ApiVersions 응답은 항상 헤더 v0 사용 (KIP-511)
	// 이 특별 규칙이 적용되는 유일한 API
	if api_key == .api_versions {
		return 0
	}
	// 다른 모든 API: flexible 버전은 헤더 v1, non-flexible은 헤더 v0 사용
	return if is_flexible_version(api_key, api_version) { i16(1) } else { i16(0) }
}

/// 원시 바이트에서 요청을 파싱합니다.
///
/// 형식: [헤더][본문] (크기 필드는 TCP 레이어에서 이미 제거됨)
pub fn parse_request(data []u8) !Request {
	if data.len < 8 {
		return error('request too short')
	}

	mut reader := new_reader(data)

	// 헤더 파싱
	api_key := reader.read_i16()!
	api_version := reader.read_i16()!
	correlation_id := reader.read_i32()!

	// flexible 버전(v2 헤더)인지 확인
	api_key_enum := unsafe { ApiKey(api_key) }
	header_version := get_request_header_version(api_key_enum, api_version)
	is_flexible_header := header_version >= 2

	// 요청 헤더 v2 (flexible)에서도 client_id는 여전히 NULLABLE_STRING (compact 아님!)
	client_id := reader.read_nullable_string()!

	if is_flexible_header {
		// 헤더의 tagged fields (tag_buffer) 건너뛰기
		reader.skip_tagged_fields()!
	}

	// 나머지 데이터가 요청 본문
	body := reader.data[reader.pos..].clone()

	return Request{
		header: RequestHeader{
			api_key:        api_key
			api_version:    api_version
			correlation_id: correlation_id
			client_id:      client_id
		}
		body:   body
	}
}

/// API 버전이 flexible 인코딩을 사용하는지 확인합니다.
///
/// Flexible 버전은 Kafka 2.4에서 도입되었으며,
/// 각 API마다 고유한 임계값이 있습니다.
pub fn is_flexible_version(api_key ApiKey, version i16) bool {
	return match api_key {
		.produce { version >= 9 }
		.fetch { version >= 12 }
		.list_offsets { version >= 6 }
		.metadata { version >= 9 }
		.offset_commit { version >= 8 }
		.offset_fetch { version >= 6 }
		.find_coordinator { version >= 3 }
		.join_group { version >= 6 }
		.heartbeat { version >= 4 }
		.leave_group { version >= 4 }
		.sync_group { version >= 4 }
		.describe_groups { version >= 5 }
		.list_groups { version >= 3 }
		.sasl_handshake { false } // v0-v1은 절대 flexible이 아님
		.api_versions { version >= 3 }
		.create_topics { version >= 5 }
		.delete_topics { version >= 4 }
		.delete_records { version >= 2 }
		.init_producer_id { version >= 2 }
		.describe_configs { version >= 4 }
		.alter_configs { version >= 2 }
		.create_partitions { version >= 2 }
		.add_partitions_to_txn { version >= 3 }
		.add_offsets_to_txn { version >= 3 }
		.end_txn { version >= 3 }
		.write_txn_markers { version >= 1 }
		.txn_offset_commit { version >= 3 }
		.sasl_authenticate { version >= 2 } // v2+는 flexible
		.delete_groups { version >= 2 }
		.describe_acls { version >= 2 } // v2+는 flexible
		.create_acls { version >= 2 } // v2+는 flexible
		.delete_acls { version >= 2 } // v2+는 flexible
		.describe_cluster { version >= 0 }
		.consumer_group_heartbeat { version >= 0 }
		.consumer_group_describe { version >= 0 }
		.share_group_heartbeat { version >= 0 }
		.share_group_describe { version >= 0 }
		.share_fetch { version >= 0 }
		.share_acknowledge { version >= 0 }
		else { false }
	}
}
