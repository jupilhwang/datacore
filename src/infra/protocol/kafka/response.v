// 공통 응답 헤더 및 응답 빌드 함수
module kafka

/// 응답 헤더 v0 (non-flexible)
/// 사용처: ApiVersions (항상), non-flexible API들
pub struct ResponseHeader {
pub:
	correlation_id i32
}

/// 응답 헤더 v1 (flexible)
/// 사용처: flexible API들
/// correlation_id 뒤에 tag_buffer 포함
pub struct ResponseHeaderV1 {
pub:
	correlation_id i32
	// tag_buffer는 별도로 작성됨 (최소 1바이트: 빈 경우 0x00)
}

/// 헤더가 있는 응답을 빌드합니다 (non-flexible, 응답 헤더 v0).
/// 사용처: ApiVersions (항상), SaslHandshake, non-flexible API 버전들
/// 참고: 헤더에 tag_buffer 없음!
pub fn build_response(correlation_id i32, body []u8) []u8 {
	mut writer := new_writer_with_capacity(4 + 4 + body.len)

	// 크기 (size 필드 자체를 제외한 전체 길이)
	writer.write_i32(i32(4 + body.len))
	// Correlation ID
	writer.write_i32(correlation_id)
	// non-flexible 응답 헤더에는 tag_buffer 없음!
	// 본문
	writer.write_raw(body)

	return writer.bytes()
}

/// flexible 응답을 빌드합니다 (응답 헤더 v1, tag_buffer 포함).
/// 사용처: flexible API 버전들 (항상 non-flexible인 ApiVersions 제외)
/// 중요: tag_buffer는 최소 1바이트 (빈 태그의 경우 0x00)
pub fn build_flexible_response(correlation_id i32, body []u8) []u8 {
	mut writer := new_writer_with_capacity(4 + 4 + 1 + body.len)

	// 크기 (size 필드 자체를 제외한 전체 길이)
	// = correlation_id(4) + tag_buffer(1, 최소) + body
	writer.write_i32(i32(4 + 1 + body.len))
	// Correlation ID
	writer.write_i32(correlation_id)
	// Tag buffer (빈 경우 = 0x00, num_tags=0을 의미)
	writer.write_uvarint(0)
	// 본문
	writer.write_raw(body)

	return writer.bytes()
}

/// API 키와 버전에 따라 적절한 헤더로 응답을 빌드합니다.
/// 응답 빌드에 권장되는 함수입니다.
pub fn build_response_auto(api_key ApiKey, api_version i16, correlation_id i32, body []u8) []u8 {
	response_header_version := get_response_header_version(api_key, api_version)
	if response_header_version >= 1 {
		return build_flexible_response(correlation_id, body)
	}
	return build_response(correlation_id, body)
}
