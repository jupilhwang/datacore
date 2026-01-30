// 서비스 레이어 - 스키마 레지스트리
// 스키마 등록, 버전 관리, 호환성 검사를 관리합니다.
// Confluent Schema Registry와 호환되는 API를 제공합니다.
module schema

import domain
import service.port
import sync
import time
import crypto.md5

/// SchemaRegistry는 스키마 관리 기능을 제공합니다.
/// 스키마 등록, 조회, 버전 관리, 호환성 검사를 담당합니다.
pub struct SchemaRegistry {
mut:
	// 인메모리 캐시
	schemas         map[int]domain.Schema             // schema_id -> Schema
	subjects        map[string][]domain.SchemaVersion // subject -> versions
	subject_configs map[string]domain.SubjectConfig   // subject -> config
	// 버전 조회 캐시 (v0.28.0 최적화)
	version_map map[string]map[int]int // subject -> (version -> versions 배열 인덱스)

	// 전역 상태
	next_id       int                  // 다음 스키마 ID
	global_config domain.SubjectConfig // 전역 기본 설정

	// 스레드 안전성
	lock sync.RwMutex

	// 영구 저장을 위한 스토리지
	storage port.StoragePort

	// 설정
	default_compat domain.CompatibilityLevel

	// 부팅 복구 상태
	recovered bool
}

/// RegistryConfig는 레지스트리 설정을 담습니다.
pub struct RegistryConfig {
pub:
	default_compatibility domain.CompatibilityLevel = .backward // 기본 호환성 수준
	auto_register         bool                      = true      // 자동 등록 여부
	normalize_schemas     bool                      = true      // 스키마 정규화 여부
}

/// RegistryStats는 레지스트리 통계를 담습니다.
pub struct RegistryStats {
pub:
	total_schemas  int // 총 스키마 수
	total_subjects int // 총 subject 수
	next_id        int // 다음 스키마 ID
}

// 스키마 저장을 위한 내부 토픽
pub const schemas_topic = '__schemas'

/// new_registry는 새로운 스키마 레지스트리를 생성합니다.
pub fn new_registry(storage port.StoragePort, config RegistryConfig) &SchemaRegistry {
	return &SchemaRegistry{
		schemas:         map[int]domain.Schema{}
		subjects:        map[string][]domain.SchemaVersion{}
		subject_configs: map[string]domain.SubjectConfig{}
		version_map:     map[string]map[int]int{}
		next_id:         1
		global_config:   domain.SubjectConfig{
			compatibility: config.default_compatibility
			normalize:     config.normalize_schemas
		}
		storage:         storage
		default_compat:  config.default_compatibility
		recovered:       false
	}
}

/// load_from_storage는 __schemas 토픽에서 스키마 레지스트리 상태를 복구합니다.
/// 요청을 받기 전 브로커 시작 시 호출해야 합니다.
pub fn (mut r SchemaRegistry) load_from_storage() ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if r.recovered {
		return
	}

	// __schemas 토픽에서 읽기 시도
	r.storage.get_topic(schemas_topic) or {
		// 토픽이 아직 없음 - 첫 부팅
		r.recovered = true
		return
	}

	// __schemas 토픽에서 모든 레코드 가져오기
	fetch_result := r.storage.fetch(schemas_topic, 0, 0, 100 * 1024 * 1024) or {
		// 비어있거나 오류 - 새로 시작
		r.recovered = true
		return
	}

	mut max_id := 0

	// 각 레코드를 처리하여 상태 재구성
	for record in fetch_result.records {
		record_str := record.value.bytestr()

		// 저장된 스키마 레코드 파싱
		// 형식: {"subject":"...", "version":..., "id":..., "schemaType":"...", "schema":"..."}

		subject := extract_json_string(record_str, 'subject') or { continue }
		version := extract_json_int(record_str, 'version') or { continue }
		schema_id := extract_json_int(record_str, 'id') or { continue }
		schema_type_str := extract_json_string(record_str, 'schemaType') or { 'AVRO' }
		schema_str := extract_json_string(record_str, 'schema') or { continue }

		schema_type := domain.schema_type_from_str(schema_type_str) or { domain.SchemaType.avro }

		// next_id를 위해 최대 ID 추적
		if schema_id > max_id {
			max_id = schema_id
		}

		// 스키마 캐시 재구성
		if schema_id !in r.schemas {
			r.schemas[schema_id] = domain.Schema{
				id:          schema_id
				schema_type: schema_type
				schema_str:  schema_str
				fingerprint: compute_fingerprint(normalize_schema(schema_str))
			}
		}

		// subject 버전 재구성
		schema_version := domain.SchemaVersion{
			version:       version
			schema_id:     schema_id
			subject:       subject
			compatibility: r.default_compat
			created_at:    record.timestamp
		}

		if subject in r.subjects {
			// 버전이 이미 존재하는지 확인
			mut exists := false
			for v in r.subjects[subject] {
				if v.version == version {
					exists = true
					break
				}
			}
			if !exists {
				r.subjects[subject] << schema_version
			}
		} else {
			r.subjects[subject] = [schema_version]
		}
	}

	// next_id를 복구된 최대 ID 이후로 설정
	r.next_id = max_id + 1
	r.recovered = true
}

/// get_global_config는 전역 호환성 설정을 반환합니다.
pub fn (mut r SchemaRegistry) get_global_config() domain.SubjectConfig {
	r.lock.rlock()
	defer { r.lock.runlock() }
	return r.global_config
}

/// set_global_config는 전역 호환성 설정을 업데이트합니다.
pub fn (mut r SchemaRegistry) set_global_config(config domain.SubjectConfig) {
	r.lock.@lock()
	defer { r.lock.unlock() }
	r.global_config = config
	r.default_compat = config.compatibility
}

/// register는 subject에 새 스키마를 등록합니다.
/// 반환값: 스키마 ID (중복이면 기존 ID, 새로우면 새 ID)
pub fn (mut r SchemaRegistry) register(subject string, schema_str string, schema_type domain.SchemaType) !int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	// 스키마 유효성 검사
	validate_schema(schema_type, schema_str)!

	// 정규화 및 지문 계산
	normalized := normalize_schema(schema_str)
	fingerprint := compute_fingerprint(normalized)

	// 이 subject에 동일한 스키마가 이미 존재하는지 확인
	if existing_id := r.find_schema_by_fingerprint(subject, fingerprint) {
		return existing_id
	}

	// 호환성 수준 가져오기
	config := r.subject_configs[subject] or { r.global_config }

	// 기존 버전과의 호환성 검사
	r.check_compatibility_internal(subject, schema_str, schema_type, config.compatibility)!

	// 새 스키마 생성
	schema_id := r.next_id
	r.next_id += 1

	schema := domain.Schema{
		id:          schema_id
		schema_type: schema_type
		schema_str:  schema_str
		fingerprint: fingerprint
	}

	// 버전 번호 결정
	versions := r.subjects[subject] or { []domain.SchemaVersion{} }
	version_num := versions.len + 1

	version := domain.SchemaVersion{
		version:       version_num
		schema_id:     schema_id
		subject:       subject
		compatibility: config.compatibility
		created_at:    time.now()
	}

	// 스키마 및 버전 저장
	r.schemas[schema_id] = schema

	if subject in r.subjects {
		r.subjects[subject] << version
	} else {
		r.subjects[subject] = [version]
	}

	// O(1) 조회를 위한 version_map 업데이트 (v0.28.0 최적화)
	if subject !in r.version_map {
		r.version_map[subject] = map[int]int{}
	}
	r.version_map[subject][version_num] = versions.len // 배열 인덱스

	// 스토리지에 영구 저장 (잠금 해제 작업)
	r.persist_schema(subject, schema, version) or {
		// 오류 로그 남기지만 실패하지 않음 - 인메모리 상태는 업데이트됨
	}

	return schema_id
}

/// get_schema는 ID로 스키마를 조회합니다.
pub fn (mut r SchemaRegistry) get_schema(schema_id int) !domain.Schema {
	r.lock.rlock()
	defer { r.lock.runlock() }

	return r.schemas[schema_id] or { return error('schema not found: ${schema_id}') }
}

/// get_schema_by_subject는 subject와 버전으로 스키마를 조회합니다.
/// version -1을 사용하면 최신 버전을 가져옵니다.
pub fn (mut r SchemaRegistry) get_schema_by_subject(subject string, version int) !domain.Schema {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	// version -1은 최신을 의미
	if version == -1 {
		if versions.len == 0 {
			return error('no versions for subject: ${subject}')
		}
		latest := versions[versions.len - 1]
		return r.schemas[latest.schema_id] or { return error('schema not found') }
	}

	// version_map을 사용한 O(1) 조회 (v0.28.0 최적화)
	if vm := r.version_map[subject] {
		if idx := vm[version] {
			if idx < versions.len {
				return r.schemas[versions[idx].schema_id] or { return error('schema not found') }
			}
		}
	}

	// 하위 호환성을 위한 선형 검색 폴백
	for v in versions {
		if v.version == version {
			return r.schemas[v.schema_id] or { return error('schema not found') }
		}
	}

	return error('version ${version} not found for subject ${subject}')
}

/// get_latest_version은 subject의 최신 스키마 버전을 조회합니다.
pub fn (mut r SchemaRegistry) get_latest_version(subject string) !domain.SchemaVersion {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }
	if versions.len == 0 {
		return error('no versions for subject: ${subject}')
	}

	return versions[versions.len - 1]
}

/// list_subjects는 등록된 모든 subject를 반환합니다.
pub fn (mut r SchemaRegistry) list_subjects() []string {
	r.lock.rlock()
	defer { r.lock.runlock() }

	mut result := []string{}
	for subject, _ in r.subjects {
		result << subject
	}
	return result
}

/// list_versions는 subject의 모든 버전을 반환합니다.
pub fn (mut r SchemaRegistry) list_versions(subject string) ![]int {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	mut result := []int{}
	for v in versions {
		result << v.version
	}
	return result
}

/// delete_subject는 subject와 모든 버전을 삭제합니다.
pub fn (mut r SchemaRegistry) delete_subject(subject string) ![]int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	mut deleted := []int{}
	for v in versions {
		deleted << v.version
	}

	r.subjects.delete(subject)
	r.subject_configs.delete(subject)
	r.version_map.delete(subject) // version_map 정리 (v0.28.0)

	return deleted
}

/// delete_version은 subject의 특정 버전을 삭제합니다.
pub fn (mut r SchemaRegistry) delete_version(subject string, version int) !int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	mut versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	for i, v in versions {
		if v.version == version {
			versions.delete(i)
			r.subjects[subject] = versions

			// 이 subject의 version_map 재구성 (v0.28.0)
			if subject in r.version_map {
				r.version_map[subject].delete(version)
				// 삭제된 버전 이후의 인덱스 업데이트
				for j := i; j < versions.len; j++ {
					r.version_map[subject][versions[j].version] = j
				}
			}

			return version
		}
	}

	return error('version ${version} not found for subject ${subject}')
}

/// get_compatibility는 subject의 호환성 수준을 반환합니다.
pub fn (mut r SchemaRegistry) get_compatibility(subject string) domain.CompatibilityLevel {
	r.lock.rlock()
	defer { r.lock.runlock() }

	config := r.subject_configs[subject] or { return r.default_compat }
	return config.compatibility
}

/// set_compatibility는 subject의 호환성 수준을 설정합니다.
pub fn (mut r SchemaRegistry) set_compatibility(subject string, level domain.CompatibilityLevel) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	existing := r.subject_configs[subject] or { domain.SubjectConfig{} }
	// 업데이트된 호환성으로 새 설정 생성 (SubjectConfig 필드는 불변)
	r.subject_configs[subject] = domain.SubjectConfig{
		compatibility: level
		alias:         existing.alias
		normalize:     existing.normalize
	}
}

/// test_compatibility는 스키마가 subject와 호환되는지 테스트합니다.
pub fn (mut r SchemaRegistry) test_compatibility(subject string, schema_str string, schema_type domain.SchemaType) !bool {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return true } // 버전 없음 = 호환
	if versions.len == 0 {
		return true
	}

	config := r.subject_configs[subject] or {
		domain.SubjectConfig{
			compatibility: r.default_compat
		}
	}

	// 잠금을 다시 획득하지 않고 check_compatibility_internal 사용
	return r.check_compatibility_internal(subject, schema_str, schema_type, config.compatibility)
}

/// get_stats는 레지스트리 통계를 반환합니다.
pub fn (mut r SchemaRegistry) get_stats() RegistryStats {
	r.lock.rlock()
	defer { r.lock.runlock() }

	return RegistryStats{
		total_schemas:  r.schemas.len
		total_subjects: r.subjects.len
		next_id:        r.next_id
	}
}

// 비공개 헬퍼 메서드

/// find_schema_by_fingerprint는 지문으로 스키마를 찾습니다.
fn (r &SchemaRegistry) find_schema_by_fingerprint(subject string, fingerprint string) ?int {
	versions := r.subjects[subject] or { return none }

	for v in versions {
		if schema := r.schemas[v.schema_id] {
			if schema.fingerprint == fingerprint {
				return v.schema_id
			}
		}
	}
	return none
}

/// check_compatibility_internal은 내부 호환성 검사를 수행합니다.
fn (r &SchemaRegistry) check_compatibility_internal(subject string, schema_str string, schema_type domain.SchemaType, level domain.CompatibilityLevel) !bool {
	if level == .none {
		return true
	}

	versions := r.subjects[subject] or { return true }
	if versions.len == 0 {
		return true
	}

	// 호환성 수준에 따라 검사할 스키마 가져오기
	schemas_to_check := match level {
		.backward, .forward, .full {
			// 최신 버전만 검사
			[versions[versions.len - 1]]
		}
		.backward_transitive, .forward_transitive, .full_transitive {
			// 모든 버전 검사
			versions
		}
		.none {
			[]domain.SchemaVersion{}
		}
	}

	for v in schemas_to_check {
		existing := r.schemas[v.schema_id] or { continue }

		compatible := match level {
			.backward, .backward_transitive {
				// 새 스키마가 이전 스키마로 작성된 데이터를 읽을 수 있음
				check_backward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.forward, .forward_transitive {
				// 이전 스키마가 새 스키마로 작성된 데이터를 읽을 수 있음
				check_forward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.full, .full_transitive {
				// 양방향

				check_backward_compatible(existing.schema_str, schema_str, schema_type)
					&& check_forward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.none {
				true
			}
		}

		if !compatible {
			return error('schema is not ${level.str()} compatible with version ${v.version}')
		}
	}

	return true
}

/// persist_schema는 스키마를 __schemas 토픽에 저장합니다.
fn (mut r SchemaRegistry) persist_schema(subject string, schema domain.Schema, version domain.SchemaVersion) ! {
	// __schemas 토픽에 저장할 레코드 생성
	// 형식: 스키마 세부 정보가 포함된 JSON
	record_data := '{"subject":"${subject}","version":${version.version},"id":${schema.id},"schemaType":"${schema.schema_type.str()}","schema":${escape_json_string(schema.schema_str)}}'

	record := domain.Record{
		key:       subject.bytes()
		value:     record_data.bytes()
		timestamp: time.now()
	}

	// __schemas 토픽 존재 확인 (없으면 생성)
	r.storage.get_topic(schemas_topic) or {
		// 단일 파티션으로 내부 토픽 생성
		r.storage.create_topic(schemas_topic, 1, domain.TopicConfig{
			retention_ms:   -1 // 영구 보존
			cleanup_policy: 'compact'
		}) or {
			// 다른 스레드에서 이미 생성되었을 수 있음
		}
	}

	// __schemas 토픽에 추가
	r.storage.append(schemas_topic, 0, [record]) or {
		return error('failed to persist schema: ${err}')
	}
}

// 유틸리티 함수

/// normalize_schema는 일관된 지문 생성을 위해 공백을 제거합니다.
fn normalize_schema(schema_str string) string {
	return schema_str.replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
		'')
}

/// compute_fingerprint는 스키마 문자열의 MD5 해시를 계산합니다.
fn compute_fingerprint(schema_str string) string {
	hash := md5.sum(schema_str.bytes())
	return hash.hex()
}
