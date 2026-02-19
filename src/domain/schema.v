// Schema Registry 도메인 엔티티를 정의합니다.
// 메시지 스키마의 등록, 버전 관리, 호환성 검사를 지원합니다.
module domain

import time

/// SchemaType은 스키마의 유형을 나타냅니다.
/// avro: Apache Avro 스키마
/// json: JSON Schema
/// protobuf: Protocol Buffers 스키마
pub enum SchemaType {
	avro
	json
	protobuf
}

/// Schema는 등록된 스키마를 나타냅니다.
/// id: 전역 고유 스키마 ID
/// schema_type: 스키마 유형 (AVRO, JSON, PROTOBUF)
/// schema_str: 원시 스키마 정의 문자열
/// references: 다른 스키마에 대한 참조
/// fingerprint: 중복 제거를 위한 스키마 지문
pub struct Schema {
pub:
	id          int               // 전역 고유 스키마 ID
	schema_type SchemaType        // AVRO, JSON, PROTOBUF
	schema_str  string            // 원시 스키마 정의
	references  []SchemaReference // 다른 스키마 참조
	fingerprint string            // 중복 제거용 지문
}

/// SchemaReference는 다른 스키마에 대한 참조를 나타냅니다.
/// name: 참조 이름
/// subject: 참조된 스키마의 서브젝트
/// version: 참조된 스키마의 버전
pub struct SchemaReference {
pub:
	name    string // 참조 이름
	subject string // 참조된 스키마의 서브젝트
	version int    // 참조된 스키마의 버전
}

/// SchemaVersion은 서브젝트 하위의 스키마 버전을 나타냅니다.
/// version: 버전 번호 (1부터 시작)
/// schema_id: 전역 스키마 ID
/// subject: 서브젝트 이름 (예: "orders-value")
/// compatibility: 호환성 수준
/// created_at: 생성 시간
pub struct SchemaVersion {
pub:
	version       int    // 버전 번호 (1-based)
	schema_id     int    // 전역 스키마 ID
	subject       string // 서브젝트 이름 (예: "orders-value")
	compatibility CompatibilityLevel
	created_at    time.Time
}

/// CompatibilityLevel은 스키마 호환성 규칙을 정의합니다.
/// none: 호환성 검사 없음
/// backward: 새 스키마가 이전 데이터를 읽을 수 있음
/// forward: 이전 스키마가 새 데이터를 읽을 수 있음
/// full: 양방향 호환
pub enum CompatibilityLevel {
	none     // 호환성 검사 없음
	backward // 새 스키마가 이전 데이터를 읽을 수 있음
	backward_transitive
	forward // 이전 스키마가 새 데이터를 읽을 수 있음
	forward_transitive
	full // 양방향 호환
	full_transitive
}

/// SubjectConfig는 서브젝트에 대한 설정을 나타냅니다.
/// compatibility: 호환성 수준
/// alias: 서브젝트 별칭
/// normalize: 비교 전 스키마 정규화 여부
pub struct SubjectConfig {
pub:
	compatibility CompatibilityLevel = .backward
	alias         string // 서브젝트 별칭
	normalize     bool   // 비교 전 스키마 정규화 여부
}

/// SchemaInfo는 API 응답을 위한 스키마 정보를 나타냅니다.
pub struct SchemaInfo {
pub:
	id          int
	schema_type string
	schema_str  string
	subject     string
	version     int
	created_at  i64 // Unix 타임스탬프
}

/// SubjectVersion은 서브젝트와 버전 목록을 나타냅니다.
pub struct SubjectVersion {
pub:
	subject  string
	versions []int
}

// 새 서브젝트의 기본 호환성 수준
pub const default_compatibility = CompatibilityLevel.backward

/// str은 SchemaType을 문자열로 변환합니다.
pub fn (st SchemaType) str() string {
	return match st {
		.avro { 'AVRO' }
		.json { 'JSON' }
		.protobuf { 'PROTOBUF' }
	}
}

/// schema_type_from_str은 문자열을 SchemaType으로 변환합니다.
pub fn schema_type_from_str(s string) !SchemaType {
	return match s.to_upper() {
		'AVRO' { .avro }
		'JSON' { .json }
		'PROTOBUF' { .protobuf }
		else { error('unknown schema type: ${s}') }
	}
}

/// str은 CompatibilityLevel을 문자열로 변환합니다.
pub fn (cl CompatibilityLevel) str() string {
	return match cl {
		.none { 'NONE' }
		.backward { 'BACKWARD' }
		.backward_transitive { 'BACKWARD_TRANSITIVE' }
		.forward { 'FORWARD' }
		.forward_transitive { 'FORWARD_TRANSITIVE' }
		.full { 'FULL' }
		.full_transitive { 'FULL_TRANSITIVE' }
	}
}

/// compatibility_from_str은 문자열을 CompatibilityLevel로 변환합니다.
pub fn compatibility_from_str(s string) !CompatibilityLevel {
	return match s.to_upper() {
		'NONE' { .none }
		'BACKWARD' { .backward }
		'BACKWARD_TRANSITIVE' { .backward_transitive }
		'FORWARD' { .forward }
		'FORWARD_TRANSITIVE' { .forward_transitive }
		'FULL' { .full }
		'FULL_TRANSITIVE' { .full_transitive }
		else { error('unknown compatibility level: ${s}') }
	}
}
