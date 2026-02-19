// Avro, JSON Schema, Protobuf 스키마의 호환성 검사를 제공합니다.
module schema

import domain

// 메인 호환성 검사 함수

/// check_backward_compatible는 새 스키마가 이전 스키마로 작성된 데이터를 읽을 수 있는지 검사합니다.
fn check_backward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	match schema_type {
		.avro {
			return check_avro_backward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_backward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_backward_compatible(old_schema, new_schema)
		}
	}
}

/// check_forward_compatible는 이전 스키마가 새 스키마로 작성된 데이터를 읽을 수 있는지 검사합니다.
fn check_forward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	match schema_type {
		.avro {
			return check_avro_forward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_forward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_forward_compatible(old_schema, new_schema)
		}
	}
}
