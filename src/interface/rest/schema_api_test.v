// SchemaAPI 유닛 테스트
// MockSchemaRegistry를 사용하여 DIP 준수를 검증한다.
// SchemaAPI가 concrete SchemaRegistry 대신 SchemaRegistryRestPort 인터페이스에
// 의존함을 증명하는 테스트.
module rest

import domain

// MockSchemaRegistry는 SchemaRegistryRestPort 인터페이스의 인메모리 구현이다.
struct MockSchemaRegistry {
mut:
	schemas         map[int]domain.Schema
	subjects        map[string][]int
	next_id         int = 1
	global_config   domain.SubjectConfig
	subject_configs map[string]domain.CompatibilityLevel
}

fn new_mock_schema_registry() &MockSchemaRegistry {
	return &MockSchemaRegistry{
		global_config: domain.SubjectConfig{
			compatibility: .backward
		}
	}
}

// SchemaRegistryRestPort 인터페이스 구현

fn (mut m MockSchemaRegistry) list_subjects() []string {
	mut result := []string{}
	for subject, _ in m.subjects {
		result << subject
	}
	return result
}

fn (mut m MockSchemaRegistry) list_versions(subject string) ![]int {
	return m.subjects[subject] or { return error('subject not found: ${subject}') }
}

fn (mut m MockSchemaRegistry) delete_subject(subject string) ![]int {
	versions := m.subjects[subject] or { return error('subject not found: ${subject}') }
	m.subjects.delete(subject)
	return versions
}

fn (mut m MockSchemaRegistry) register(subject string, schema_str string, schema_type domain.SchemaType) !int {
	id := m.next_id
	m.next_id += 1
	m.schemas[id] = domain.Schema{
		id:          id
		schema_type: schema_type
		schema_str:  schema_str
	}
	if subject in m.subjects {
		m.subjects[subject] << id
	} else {
		m.subjects[subject] = [id]
	}
	return id
}

fn (mut m MockSchemaRegistry) get_schema_by_subject(subject string, version int) !domain.Schema {
	versions := m.subjects[subject] or { return error('subject not found: ${subject}') }
	if version == -1 {
		if versions.len == 0 {
			return error('no versions')
		}
		return m.schemas[versions[versions.len - 1]] or { return error('schema not found') }
	}
	idx := version - 1
	if idx < 0 || idx >= versions.len {
		return error('version not found')
	}
	return m.schemas[versions[idx]] or { return error('schema not found') }
}

fn (mut m MockSchemaRegistry) get_schema(schema_id int) !domain.Schema {
	return m.schemas[schema_id] or { return error('schema not found: ${schema_id}') }
}

fn (mut m MockSchemaRegistry) delete_version(subject string, version int) !int {
	mut versions := m.subjects[subject] or { return error('subject not found') }
	idx := version - 1
	if idx < 0 || idx >= versions.len {
		return error('version not found')
	}
	versions.delete(idx)
	m.subjects[subject] = versions
	return version
}

fn (mut m MockSchemaRegistry) get_global_config() domain.SubjectConfig {
	return m.global_config
}

fn (mut m MockSchemaRegistry) set_global_config(config domain.SubjectConfig) {
	m.global_config = config
}

fn (mut m MockSchemaRegistry) get_compatibility(subject string) domain.CompatibilityLevel {
	return m.subject_configs[subject] or { return m.global_config.compatibility }
}

fn (mut m MockSchemaRegistry) set_compatibility(subject string, level domain.CompatibilityLevel) {
	m.subject_configs[subject] = level
}

fn (mut m MockSchemaRegistry) test_compatibility(subject string, schema_str string, schema_type domain.SchemaType) !bool {
	return true
}

// 테스트 케이스

fn test_schema_api_uses_port_interface() {
	// DIP 검증: Mock이 SchemaRegistryRestPort를 구현하고
	// SchemaAPI에 주입 가능한지 확인
	mock := new_mock_schema_registry()
	mut api := new_schema_api(mock)

	// GET /subjects - 빈 목록 반환
	status, body := api.handle_request('GET', '/subjects', '')
	assert status == 200
	assert body == '[]'
}

fn test_schema_api_register_and_get() {
	mock := new_mock_schema_registry()
	mut api := new_schema_api(mock)

	// POST /subjects/test-value/versions - 스키마 등록
	register_body := '{"schema":"{\\"type\\":\\"string\\"}","schemaType":"AVRO"}'
	status, body := api.handle_request('POST', '/subjects/test-value/versions', register_body)
	assert status == 200
	assert body.contains('"id"')

	// GET /subjects - 등록된 subject 확인
	status2, body2 := api.handle_request('GET', '/subjects', '')
	assert status2 == 200
	assert body2.contains('test-value')
}

fn test_schema_api_get_schema_by_id() {
	mock := new_mock_schema_registry()
	mut api := new_schema_api(mock)

	// 스키마 등록
	register_body := '{"schema":"{\\"type\\":\\"string\\"}","schemaType":"AVRO"}'
	api.handle_request('POST', '/subjects/test-value/versions', register_body)

	// GET /schemas/ids/1
	status, body := api.handle_request('GET', '/schemas/ids/1', '')
	assert status == 200
	assert body.contains('"schema"')
}

fn test_schema_api_config_endpoints() {
	mock := new_mock_schema_registry()
	mut api := new_schema_api(mock)

	// GET /config - 전역 설정 조회
	status, body := api.handle_request('GET', '/config', '')
	assert status == 200
	assert body.contains('compatibilityLevel')

	// PUT /config - 전역 설정 변경
	config_body := '{"compatibility":"FULL"}'
	status2, _ := api.handle_request('PUT', '/config', config_body)
	assert status2 == 200
}

fn test_schema_api_not_found() {
	mock := new_mock_schema_registry()
	mut api := new_schema_api(mock)

	// 존재하지 않는 경로
	status, body := api.handle_request('GET', '/unknown', '')
	assert status == 404
	assert body.contains('error_code')
}

fn test_schema_api_subject_not_found() {
	mock := new_mock_schema_registry()
	mut api := new_schema_api(mock)

	// 존재하지 않는 subject의 versions 조회
	status, body := api.handle_request('GET', '/subjects/nonexistent/versions', '')
	assert status == 404
	assert body.contains('Subject not found')
}
