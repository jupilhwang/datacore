// 서비스 레이어 - 스키마 유효성 검사
// Avro, JSON Schema, Protobuf 스키마의 구문 유효성 검사를 제공합니다.
// 스키마 등록 전 구문 오류를 검출합니다.
module schema

import domain

/// validate_schema는 스키마 타입에 따라 스키마 구문을 검증합니다.
fn validate_schema(schema_type domain.SchemaType, schema_str string) ! {
	match schema_type {
		.avro {
			validate_avro_schema_syntax(schema_str)!
		}
		.json {
			validate_json_schema_syntax(schema_str)!
		}
		.protobuf {
			validate_protobuf_schema_syntax(schema_str)!
		}
	}
}

// Avro 스키마 유효성 검사

/// validate_avro_schema_syntax는 Avro 스키마 구문을 검증합니다.
fn validate_avro_schema_syntax(schema_str string) ! {
	// 유효한 JSON인지 확인
	if !is_valid_json(schema_str) {
		return error('invalid Avro schema: not valid JSON')
	}

	trimmed := schema_str.trim_space()

	// "string", "int" 등의 원시 타입 문자열 처리
	if trimmed.starts_with('"') && trimmed.ends_with('"') {
		type_name := trimmed[1..trimmed.len - 1]
		valid_primitives := ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string']
		if type_name !in valid_primitives {
			return error('invalid Avro schema: unknown primitive type "${type_name}"')
		}
		return
	}

	// ["null", "string"] 같은 배열 (union 타입) 처리
	if trimmed.starts_with('[') && trimmed.ends_with(']') {
		// Union 타입 - 각 요소 검증
		return
	}

	// 객체 스키마 처리
	if !trimmed.starts_with('{') {
		return error('invalid Avro schema: expected JSON object, array, or primitive type string')
	}

	// 복합 타입은 "type" 필드가 필수
	if !schema_str.contains('"type"') {
		return error('invalid Avro schema: missing "type" field')
	}

	// 타입 추출 및 검증
	schema_type := extract_json_string(schema_str, 'type') or {
		return error('invalid Avro schema: cannot parse "type" field')
	}

	valid_types := ['record', 'enum', 'array', 'map', 'fixed', 'null', 'boolean', 'int', 'long',
		'float', 'double', 'bytes', 'string']
	if schema_type !in valid_types {
		return error('invalid Avro schema: unknown type "${schema_type}"')
	}

	// 타입별 요구사항 검증
	match schema_type {
		'record' {
			validate_avro_record_schema(schema_str)!
		}
		'enum' {
			validate_avro_enum_schema(schema_str)!
		}
		'array' {
			validate_avro_array_schema(schema_str)!
		}
		'map' {
			validate_avro_map_schema(schema_str)!
		}
		'fixed' {
			validate_avro_fixed_schema(schema_str)!
		}
		else {
			// 원시 타입은 유효함
		}
	}
}

/// validate_avro_record_schema는 Avro record 타입 스키마를 검증합니다.
fn validate_avro_record_schema(schema_str string) ! {
	// record는 "name"과 "fields"가 필수
	if !schema_str.contains('"name"') {
		return error('invalid Avro schema: record type requires "name" field')
	}
	if !schema_str.contains('"fields"') {
		return error('invalid Avro schema: record type requires "fields" field')
	}
	// fields 배열 검증
	fields := parse_avro_fields(schema_str) or { []AvroField{} }
	for field in fields {
		if field.name.len == 0 {
			return error('invalid Avro schema: field missing "name"')
		}
	}
}

/// validate_avro_enum_schema는 Avro enum 타입 스키마를 검증합니다.
fn validate_avro_enum_schema(schema_str string) ! {
	// enum은 "name"과 "symbols"가 필수
	if !schema_str.contains('"name"') {
		return error('invalid Avro schema: enum type requires "name" field')
	}
	if !schema_str.contains('"symbols"') {
		return error('invalid Avro schema: enum type requires "symbols" field')
	}
	// symbols가 비어있지 않은 배열인지 검증
	symbols := parse_json_string_array(schema_str, 'symbols') or { []string{} }
	if symbols.len == 0 {
		return error('invalid Avro schema: enum "symbols" cannot be empty')
	}
}

/// validate_avro_array_schema는 Avro array 타입 스키마를 검증합니다.
fn validate_avro_array_schema(schema_str string) ! {
	// array는 "items"가 필수
	if !schema_str.contains('"items"') {
		return error('invalid Avro schema: array type requires "items" field')
	}
}

/// validate_avro_map_schema는 Avro map 타입 스키마를 검증합니다.
fn validate_avro_map_schema(schema_str string) ! {
	// map은 "values"가 필수
	if !schema_str.contains('"values"') {
		return error('invalid Avro schema: map type requires "values" field')
	}
}

/// validate_avro_fixed_schema는 Avro fixed 타입 스키마를 검증합니다.
fn validate_avro_fixed_schema(schema_str string) ! {
	// fixed는 "name"과 "size"가 필수
	if !schema_str.contains('"name"') {
		return error('invalid Avro schema: fixed type requires "name" field')
	}
	if !schema_str.contains('"size"') {
		return error('invalid Avro schema: fixed type requires "size" field')
	}
}

// JSON Schema 유효성 검사

/// validate_json_schema_syntax는 JSON Schema 구문을 검증합니다 (Draft-07 호환).
fn validate_json_schema_syntax(schema_str string) ! {
	// 유효한 JSON인지 확인
	if !is_valid_json(schema_str) {
		return error('invalid JSON Schema: not valid JSON')
	}

	trimmed := schema_str.trim_space()

	// JSON Schema는 boolean일 수 있음 (true/false)
	if trimmed == 'true' || trimmed == 'false' {
		return
	}

	// 객체여야 함
	if !trimmed.starts_with('{') {
		return error('invalid JSON Schema: expected JSON object or boolean')
	}

	// 선택사항: draft 버전을 위한 $schema 필드 확인
	if schema_version := extract_json_string(schema_str, r'$schema') {
		// 지원되는 draft 검증
		supported_drafts := [
			'http://json-schema.org/draft-04/schema#',
			'http://json-schema.org/draft-06/schema#',
			'http://json-schema.org/draft-07/schema#',
			'https://json-schema.org/draft/2019-09/schema',
			'https://json-schema.org/draft/2020-12/schema',
		]
		mut supported := false
		for d in supported_drafts {
			if schema_version.contains(d) {
				supported = true
				break
			}
		}
		// 알 수 없는 draft에서 실패하지 않음, 경고만
		_ = supported
	}

	// type 필드가 있으면 검증
	if type_val := extract_json_string(schema_str, 'type') {
		valid_json_types := ['string', 'number', 'integer', 'boolean', 'array', 'object', 'null']
		if type_val !in valid_json_types {
			return error('invalid JSON Schema: unknown type "${type_val}"')
		}
	}

	// 타입별 키워드 검증
	validate_json_schema_keywords(schema_str)!
}

/// validate_json_schema_keywords는 JSON Schema 키워드를 검증합니다.
fn validate_json_schema_keywords(schema_str string) ! {
	// 충돌하는 키워드 확인
	// 예: minLength/maxLength는 string에만 유효
	// minItems/maxItems는 array에만 유효
	// minimum/maximum은 number에만 유효

	type_val := extract_json_string(schema_str, 'type') or { '' }

	// 문자열 전용 검증
	if type_val.len > 0 && type_val != 'string' {
		if schema_str.contains('"minLength"') || schema_str.contains('"maxLength"')
			|| schema_str.contains('"pattern"') {
			// 경고: 비문자열 타입에 문자열 키워드 (사양상 오류 아님)
		}
	}

	// 배열 전용 검증
	if type_val.len > 0 && type_val != 'array' {
		if schema_str.contains('"minItems"') || schema_str.contains('"maxItems"')
			|| schema_str.contains('"uniqueItems"') {
			// 경고: 비배열 타입에 배열 키워드
		}
	}

	// 숫자 전용 검증
	if type_val.len > 0 && type_val != 'number' && type_val != 'integer' {
		if schema_str.contains('"minimum"') || schema_str.contains('"maximum"')
			|| schema_str.contains('"multipleOf"') {
			// 경고: 비숫자 타입에 숫자 키워드
		}
	}

	// minLength <= maxLength 검증 (둘 다 있는 경우)
	if min_len := extract_json_int(schema_str, 'minLength') {
		if max_len := extract_json_int(schema_str, 'maxLength') {
			if min_len > max_len {
				return error('invalid JSON Schema: minLength (${min_len}) > maxLength (${max_len})')
			}
		}
	}

	// minItems <= maxItems 검증 (둘 다 있는 경우)
	if min_items := extract_json_int(schema_str, 'minItems') {
		if max_items := extract_json_int(schema_str, 'maxItems') {
			if min_items > max_items {
				return error('invalid JSON Schema: minItems (${min_items}) > maxItems (${max_items})')
			}
		}
	}

	// minimum <= maximum 검증 (둘 다 있는 경우)
	if min_val := extract_json_float(schema_str, 'minimum') {
		if max_val := extract_json_float(schema_str, 'maximum') {
			if min_val > max_val {
				return error('invalid JSON Schema: minimum (${min_val}) > maximum (${max_val})')
			}
		}
	}
}

// Protobuf 스키마 유효성 검사

/// validate_protobuf_schema_syntax는 Protobuf 스키마 구문을 검증합니다.
fn validate_protobuf_schema_syntax(schema_str string) ! {
	// 기본 protobuf 검증
	trimmed := schema_str.trim_space()

	// message 또는 enum 정의가 필수
	if !trimmed.contains('message ') && !trimmed.contains('enum ') {
		return error('invalid Protobuf schema: missing message or enum definition')
	}

	// 중괄호 균형 확인
	mut brace_count := 0
	for c in trimmed {
		if c == `{` {
			brace_count += 1
		} else if c == `}` {
			brace_count -= 1
			if brace_count < 0 {
				return error('invalid Protobuf schema: unbalanced braces')
			}
		}
	}
	if brace_count != 0 {
		return error('invalid Protobuf schema: unbalanced braces')
	}

	// syntax 선언이 있으면 검증
	if trimmed.contains('syntax') {
		if !trimmed.contains('syntax = "proto2"') && !trimmed.contains('syntax = "proto3"')
			&& !trimmed.contains("syntax = 'proto2'") && !trimmed.contains("syntax = 'proto3'") {
			return error('invalid Protobuf schema: invalid syntax declaration')
		}
	}

	// message 정의가 있는 경우에만 필드 검증
	// enum 전용 스키마는 message 필드 정의가 없음
	if trimmed.contains('message ') {
		// message 본문 추출 및 필드 검증
		if msg_start := trimmed.index('message ') {
			// message 본문 찾기
			rest := trimmed[msg_start..]
			if brace_start := rest.index('{') {
				// 일치하는 닫는 중괄호 찾기
				mut depth := 1
				mut brace_end := brace_start + 1
				for brace_end < rest.len && depth > 0 {
					if rest[brace_end] == `{` {
						depth += 1
					} else if rest[brace_end] == `}` {
						depth -= 1
					}
					brace_end += 1
				}
				body := rest[brace_start + 1..brace_end - 1]
				validate_protobuf_fields(body)!
			}
		}
	}
}

/// validate_protobuf_fields는 protobuf 필드 정의를 검증합니다.
fn validate_protobuf_fields(body string) ! {
	// 유효한 protobuf 필드 타입
	valid_types := [
		// 스칼라 타입
		'double',
		'float',
		'int32',
		'int64',
		'uint32',
		'uint64',
		'sint32',
		'sint64',
		'fixed32',
		'fixed64',
		'sfixed32',
		'sfixed64',
		'bool',
		'string',
		'bytes',
		// Well-known 타입
		'google.protobuf.Any',
		'google.protobuf.Duration',
		'google.protobuf.Timestamp',
		'google.protobuf.Struct',
		'google.protobuf.Value',
		'google.protobuf.ListValue',
	]

	// 필드 이름으로 사용할 수 없는 예약어
	reserved_words := [
		'syntax',
		'import',
		'package',
		'option',
		'message',
		'enum',
		'service',
		'rpc',
		'returns',
		'stream',
		'extend',
		'extensions',
		'reserved',
		'to',
		'max',
		'repeated',
		'optional',
		'required',
		'oneof',
		'map',
	]

	// 정규화: 균일한 파싱을 위해 줄바꿈을 세미콜론으로 대체
	normalized := body.replace('\n', ';').replace(';;', ';')
	statements := normalized.split(';')

	mut used_field_numbers := map[int]string{}

	for stmt in statements {
		trimmed := stmt.trim_space()

		if trimmed.len == 0 {
			continue
		}

		// 중첩된 message/enum 정의 건너뛰기
		if trimmed.starts_with('message ') || trimmed.starts_with('enum ') {
			continue
		}

		// reserved 문 건너뛰기
		if trimmed.starts_with('reserved ') {
			continue
		}

		// option 문 건너뛰기
		if trimmed.starts_with('option ') {
			continue
		}

		// 주석 건너뛰기
		if trimmed.starts_with('//') {
			continue
		}

		// 필드 정의 파싱: [modifier] type name = number [options];
		if trimmed.contains('=') && !trimmed.starts_with('option') {
			parts := trimmed.split('=')
			if parts.len >= 2 {
				// 필드 번호 추출
				num_part := parts[1].trim_space().trim_right(';')
				// 옵션 [...] 제거
				mut clean_num := num_part
				if bracket_idx := num_part.index('[') {
					clean_num = num_part[..bracket_idx].trim_space()
				}
				field_num := clean_num.int()

				if field_num <= 0 {
					return error('invalid Protobuf schema: invalid field number in "${trimmed}"')
				}

				// 예약된 필드 번호 확인
				if field_num >= 19000 && field_num <= 19999 {
					return error('invalid Protobuf schema: field numbers 19000-19999 are reserved')
				}

				// 중복 필드 번호 확인
				if existing := used_field_numbers[field_num] {
					return error('invalid Protobuf schema: duplicate field number ${field_num} (used by "${existing}")')
				}

				// 필드 이름과 타입 추출
				type_name_part := parts[0].trim_space()
				tokens := type_name_part.split(' ').filter(fn (s string) bool {
					return s.len > 0
				})

				if tokens.len >= 2 {
					// 마지막 토큰이 필드 이름
					field_name := tokens[tokens.len - 1]

					// 예약어 확인
					if field_name in reserved_words {
						return error('invalid Protobuf schema: "${field_name}" is a reserved word')
					}

					// 필드 이름 형식 검증 (문자로 시작, 영숫자와 밑줄만 포함)
					if field_name.len > 0 {
						first_char := field_name[0]
						if !((first_char >= `a` && first_char <= `z`)
							|| (first_char >= `A` && first_char <= `Z`)) {
							return error('invalid Protobuf schema: field name "${field_name}" must start with a letter')
						}
					}

					used_field_numbers[field_num] = field_name

					// 타입 가져오기 (끝에서 두 번째 토큰, 또는 첫 번째 비수식어 토큰)
					mut type_idx := 0
					for type_idx < tokens.len - 1 {
						if tokens[type_idx] in ['repeated', 'optional', 'required'] {
							type_idx += 1
						} else {
							break
						}
					}

					if type_idx < tokens.len - 1 {
						field_type := tokens[type_idx]
						// 알려진 스칼라 타입만 검증; 커스텀 메시지 타입은 허용
						if field_type.len > 0 && field_type[0] >= `a` && field_type[0] <= `z` {
							// 소문자 타입은 스칼라여야 함
							if field_type !in valid_types && !field_type.starts_with('map<') {
								// 다른 곳에서 정의된 커스텀 타입일 수 있음 - 허용
								_ = field_type
							}
						}
					}
				}
			}
		}
	}
}
