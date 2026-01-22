// Interface Layer - Schema Registry REST API
// 인터페이스 레이어 - 스키마 레지스트리 REST API
//
// Kafka Schema Registry 호환 REST 엔드포인트를 제공합니다.
// Avro, JSON Schema, Protobuf 스키마의 등록, 조회, 호환성 검사를
// 지원합니다.
//
// 주요 엔드포인트:
// - /subjects - 서브젝트 관리
// - /schemas - 스키마 조회
// - /config - 호환성 설정
// - /compatibility - 호환성 검사
module rest

import domain
import service.schema
import json

/// SchemaAPI는 Schema Registry용 REST API 핸들러를 제공합니다.
pub struct SchemaAPI {
mut:
	registry &schema.SchemaRegistry
}

/// new_schema_api는 새로운 Schema REST API 핸들러를 생성합니다.
pub fn new_schema_api(registry &schema.SchemaRegistry) &SchemaAPI {
	return &SchemaAPI{
		registry: registry
	}
}

// ============================================================================
// API 요청/응답 구조체
// ============================================================================

// RegisterSchemaRequest는 POST /subjects/{subject}/versions 요청 구조체입니다.
struct RegisterSchemaRequest {
	schema      string             @[json: 'schema']     // 스키마 문자열
	schema_type string             @[json: 'schemaType'] // 스키마 타입 (AVRO, JSON, PROTOBUF)
	references  []ReferenceRequest @[json: 'references'] // 참조 스키마
}

// ReferenceRequest는 스키마 참조 정보를 담는 구조체입니다.
struct ReferenceRequest {
	name    string @[json: 'name']    // 참조 이름
	subject string @[json: 'subject'] // 참조 서브젝트
	version int    @[json: 'version'] // 참조 버전
}

// SchemaResponse는 GET /schemas/ids/{id} 응답 구조체입니다.
struct SchemaResponse {
	schema      string @[json: 'schema']     // 스키마 문자열
	schema_type string @[json: 'schemaType'] // 스키마 타입
}

// RegisterResponse는 POST /subjects/{subject}/versions 응답 구조체입니다.
struct RegisterResponse {
	id int @[json: 'id'] // 등록된 스키마 ID
}

// VersionResponse는 GET /subjects/{subject}/versions/{version} 응답 구조체입니다.
struct VersionResponse {
	subject     string @[json: 'subject']    // 서브젝트 이름
	id          int    @[json: 'id']         // 스키마 ID
	version     int    @[json: 'version']    // 버전 번호
	schema      string @[json: 'schema']     // 스키마 문자열
	schema_type string @[json: 'schemaType'] // 스키마 타입
}

// CompatibilityRequest는 PUT /config/{subject} 요청 구조체입니다.
struct CompatibilityRequest {
	compatibility string @[json: 'compatibility'] // 호환성 레벨
}

// CompatibilityResponse는 GET /config/{subject} 응답 구조체입니다.
struct CompatibilityResponse {
	compatibility_level string @[json: 'compatibilityLevel'] // 호환성 레벨
}

// CompatibilityCheckResponse는 POST /compatibility/subjects/{subject}/versions/{version} 응답 구조체입니다.
struct CompatibilityCheckResponse {
	is_compatible bool @[json: 'is_compatible'] // 호환 여부
}

// ErrorResponse는 에러 응답 구조체입니다.
struct ErrorResponse {
	error_code int    @[json: 'error_code'] // 에러 코드
	message    string @[json: 'message']    // 에러 메시지
}

// ============================================================================
// HTTP 엔드포인트 핸들러
// ============================================================================

/// handle_request는 요청을 적절한 핸들러로 라우팅합니다.
pub fn (mut api SchemaAPI) handle_request(method string, path string, body string) (int, string) {
	// 경로 구성요소 파싱
	parts := path.trim_left('/').split('/')

	if parts.len == 0 {
		return api.error_response(404, 40401, 'Not found')
	}

	return match parts[0] {
		'subjects' { api.handle_subjects(method, parts[1..], body) }
		'schemas' { api.handle_schemas(method, parts[1..], body) }
		'config' { api.handle_config(method, parts[1..], body) }
		'compatibility' { api.handle_compatibility(method, parts[1..], body) }
		else { api.error_response(404, 40401, 'Not found') }
	}
}

// ============================================================================
// Subjects 엔드포인트
// ============================================================================

// handle_subjects는 /subjects 엔드포인트를 처리합니다.
fn (mut api SchemaAPI) handle_subjects(method string, parts []string, body string) (int, string) {
	// GET /subjects - 모든 서브젝트 목록
	if parts.len == 0 {
		if method == 'GET' {
			return api.list_subjects()
		}
		return api.error_response(405, 40501, 'Method not allowed')
	}

	subject := parts[0]

	// /subjects/{subject}
	if parts.len == 1 {
		return match method {
			'GET' { api.get_subject(subject) }
			'DELETE' { api.delete_subject(subject) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	// /subjects/{subject}/versions
	if parts.len == 2 && parts[1] == 'versions' {
		return match method {
			'GET' { api.list_versions(subject) }
			'POST' { api.register_schema(subject, body) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	// /subjects/{subject}/versions/{version}
	if parts.len == 3 && parts[1] == 'versions' {
		version := parts[2].int()
		return match method {
			'GET' { api.get_schema_by_version(subject, version) }
			'DELETE' { api.delete_version(subject, version) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	// /subjects/{subject}/versions/{version}/schema
	if parts.len == 4 && parts[1] == 'versions' && parts[3] == 'schema' {
		version := parts[2].int()
		return match method {
			'GET' { api.get_raw_schema(subject, version) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	return api.error_response(404, 40401, 'Not found')
}

// ============================================================================
// Schemas 엔드포인트
// ============================================================================

// handle_schemas는 /schemas 엔드포인트를 처리합니다.
fn (mut api SchemaAPI) handle_schemas(method string, parts []string, body string) (int, string) {
	// /schemas/ids/{id}
	if parts.len == 2 && parts[0] == 'ids' {
		schema_id := parts[1].int()
		return match method {
			'GET' { api.get_schema_by_id(schema_id) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	// /schemas/ids/{id}/schema
	if parts.len == 3 && parts[0] == 'ids' && parts[2] == 'schema' {
		schema_id := parts[1].int()
		return match method {
			'GET' { api.get_raw_schema_by_id(schema_id) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	return api.error_response(404, 40401, 'Not found')
}

// ============================================================================
// Config 엔드포인트
// ============================================================================

// handle_config는 /config 엔드포인트를 처리합니다.
fn (mut api SchemaAPI) handle_config(method string, parts []string, body string) (int, string) {
	// GET/PUT /config - 전역 설정
	if parts.len == 0 {
		return match method {
			'GET' { api.get_global_config() }
			'PUT' { api.set_global_config(body) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	// GET/PUT /config/{subject} - 서브젝트 설정
	if parts.len == 1 {
		subject := parts[0]
		return match method {
			'GET' { api.get_subject_config(subject) }
			'PUT' { api.set_subject_config(subject, body) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	return api.error_response(404, 40401, 'Not found')
}

// ============================================================================
// Compatibility 엔드포인트
// ============================================================================

// handle_compatibility는 /compatibility 엔드포인트를 처리합니다.
fn (mut api SchemaAPI) handle_compatibility(method string, parts []string, body string) (int, string) {
	// POST /compatibility/subjects/{subject}/versions/{version}
	if parts.len >= 4 && parts[0] == 'subjects' && parts[2] == 'versions' {
		subject := parts[1]
		version := parts[3].int()

		if method == 'POST' {
			return api.check_compatibility(subject, version, body)
		}
	}

	return api.error_response(404, 40401, 'Not found')
}

// ============================================================================
// 핸들러 구현
// ============================================================================

// list_subjects는 모든 서브젝트 목록을 반환합니다.
fn (mut api SchemaAPI) list_subjects() (int, string) {
	subjects := api.registry.list_subjects()
	return 200, json.encode(subjects)
}

// get_subject는 서브젝트의 버전 목록을 반환합니다.
fn (mut api SchemaAPI) get_subject(subject string) (int, string) {
	versions := api.registry.list_versions(subject) or {
		return api.error_response(404, 40401, 'Subject not found: ${subject}')
	}
	return 200, json.encode(versions)
}

// delete_subject는 서브젝트를 삭제합니다.
fn (mut api SchemaAPI) delete_subject(subject string) (int, string) {
	deleted := api.registry.delete_subject(subject) or {
		return api.error_response(404, 40401, 'Subject not found: ${subject}')
	}
	return 200, json.encode(deleted)
}

// list_versions는 서브젝트의 버전 목록을 반환합니다.
fn (mut api SchemaAPI) list_versions(subject string) (int, string) {
	versions := api.registry.list_versions(subject) or {
		return api.error_response(404, 40401, 'Subject not found: ${subject}')
	}
	return 200, json.encode(versions)
}

// register_schema는 새 스키마를 등록합니다.
fn (mut api SchemaAPI) register_schema(subject string, body string) (int, string) {
	req := json.decode(RegisterSchemaRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request: ${err}')
	}

	schema_type := domain.schema_type_from_str(if req.schema_type.len > 0 {
		req.schema_type
	} else {
		'AVRO'
	}) or { return api.error_response(400, 40001, 'Invalid schema type') }

	schema_id := api.registry.register(subject, req.schema, schema_type) or {
		return api.error_response(409, 40901, 'Schema registration failed: ${err}')
	}

	resp := RegisterResponse{
		id: schema_id
	}
	return 200, json.encode(resp)
}

// get_schema_by_version은 버전으로 스키마를 조회합니다.
fn (mut api SchemaAPI) get_schema_by_version(subject string, version int) (int, string) {
	schema_data := api.registry.get_schema_by_subject(subject, version) or {
		return api.error_response(404, 40402, 'Version not found: ${subject}/${version}')
	}

	// 버전 정보 가져오기
	versions := api.registry.list_versions(subject) or { []int{} }
	actual_version := if version == -1 { versions.len } else { version }

	resp := VersionResponse{
		subject:     subject
		id:          schema_data.id
		version:     actual_version
		schema:      schema_data.schema_str
		schema_type: schema_data.schema_type.str()
	}
	return 200, json.encode(resp)
}

// delete_version은 특정 버전을 삭제합니다.
fn (mut api SchemaAPI) delete_version(subject string, version int) (int, string) {
	schema_id := api.registry.delete_version(subject, version) or {
		return api.error_response(404, 40402, 'Version not found')
	}
	return 200, json.encode(schema_id)
}

// get_raw_schema는 원시 스키마 문자열을 반환합니다.
fn (mut api SchemaAPI) get_raw_schema(subject string, version int) (int, string) {
	schema_data := api.registry.get_schema_by_subject(subject, version) or {
		return api.error_response(404, 40402, 'Version not found')
	}
	return 200, schema_data.schema_str
}

// get_schema_by_id는 ID로 스키마를 조회합니다.
fn (mut api SchemaAPI) get_schema_by_id(schema_id int) (int, string) {
	schema_data := api.registry.get_schema(schema_id) or {
		return api.error_response(404, 40403, 'Schema not found')
	}

	resp := SchemaResponse{
		schema:      schema_data.schema_str
		schema_type: schema_data.schema_type.str()
	}
	return 200, json.encode(resp)
}

// get_raw_schema_by_id는 ID로 원시 스키마 문자열을 반환합니다.
fn (mut api SchemaAPI) get_raw_schema_by_id(schema_id int) (int, string) {
	schema_data := api.registry.get_schema(schema_id) or {
		return api.error_response(404, 40403, 'Schema not found')
	}
	return 200, schema_data.schema_str
}

// get_global_config는 전역 호환성 설정을 반환합니다.
fn (mut api SchemaAPI) get_global_config() (int, string) {
	// 전역 호환성 설정 반환
	config := api.registry.get_global_config()
	resp := CompatibilityResponse{
		compatibility_level: config.compatibility.str()
	}
	return 200, json.encode(resp)
}

// set_global_config는 전역 호환성 설정을 변경합니다.
fn (mut api SchemaAPI) set_global_config(body string) (int, string) {
	req := json.decode(CompatibilityRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request: ${err}')
	}

	level := domain.compatibility_from_str(req.compatibility) or {
		return api.error_response(400, 40001, 'Invalid compatibility level: ${req.compatibility}')
	}

	// 전역 설정 업데이트
	api.registry.set_global_config(domain.SubjectConfig{
		compatibility: level
	})

	resp := CompatibilityResponse{
		compatibility_level: level.str()
	}
	return 200, json.encode(resp)
}

// get_subject_config는 서브젝트의 호환성 설정을 반환합니다.
fn (mut api SchemaAPI) get_subject_config(subject string) (int, string) {
	compat := api.registry.get_compatibility(subject)
	resp := CompatibilityResponse{
		compatibility_level: compat.str()
	}
	return 200, json.encode(resp)
}

// set_subject_config는 서브젝트의 호환성 설정을 변경합니다.
fn (mut api SchemaAPI) set_subject_config(subject string, body string) (int, string) {
	req := json.decode(CompatibilityRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request')
	}

	level := domain.compatibility_from_str(req.compatibility) or {
		return api.error_response(400, 40001, 'Invalid compatibility level')
	}

	api.registry.set_compatibility(subject, level)

	resp := CompatibilityResponse{
		compatibility_level: level.str()
	}
	return 200, json.encode(resp)
}

// check_compatibility는 스키마 호환성을 검사합니다.
fn (mut api SchemaAPI) check_compatibility(subject string, version int, body string) (int, string) {
	req := json.decode(RegisterSchemaRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request')
	}

	schema_type := domain.schema_type_from_str(if req.schema_type.len > 0 {
		req.schema_type
	} else {
		'AVRO'
	}) or { return api.error_response(400, 40001, 'Invalid schema type') }

	is_compatible := api.registry.test_compatibility(subject, req.schema, schema_type) or {
		resp := CompatibilityCheckResponse{
			is_compatible: false
		}
		return 200, json.encode(resp)
	}

	resp := CompatibilityCheckResponse{
		is_compatible: is_compatible
	}
	return 200, json.encode(resp)
}

// error_response는 에러 응답을 생성합니다.
fn (api &SchemaAPI) error_response(status int, error_code int, message string) (int, string) {
	resp := ErrorResponse{
		error_code: error_code
		message:    message
	}
	return status, json.encode(resp)
}
