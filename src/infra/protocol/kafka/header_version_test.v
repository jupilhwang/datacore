module kafka

// Request/Response Header 버전 함수 테스트
// KIP-511 및 Apache Kafka 공식 사양 기반

// ============================================
// Request Header 버전 테스트
// ============================================

fn test_api_versions_request_header_version() {
	// ApiVersions Request는 항상 Header v1 사용 (KIP-511)
	// 본문이 flexible(v3+)이더라도 헤더는 그렇지 않음

	// v0: non-flexible → Header v1
	assert get_request_header_version(.api_versions, 0) == 1
	// v1: non-flexible → Header v1
	assert get_request_header_version(.api_versions, 1) == 1
	// v2: non-flexible → Header v1
	assert get_request_header_version(.api_versions, 2) == 1
	// v3+: 본문이 flexible이더라도 Header는 여전히 v1 (KIP-511) - 잘못됨!
	// ApiVersions Request v3+는 Header v2 (Flexible) 사용
	// Response Header만 항상 v0
	assert get_request_header_version(.api_versions, 3) == 2
	assert get_request_header_version(.api_versions, 4) == 2
}

fn test_api_versions_response_header_version() {
	// ApiVersions Response는 항상 Header v0 (KIP-511)
	// 이 특별한 동작을 가진 유일한 API
	assert get_response_header_version(.api_versions, 0) == 0
	assert get_response_header_version(.api_versions, 1) == 0
	assert get_response_header_version(.api_versions, 2) == 0
	assert get_response_header_version(.api_versions, 3) == 0 // v3+도 Header v0 사용!
	assert get_response_header_version(.api_versions, 4) == 0
}

fn test_sasl_handshake_header_version() {
	// SaslHandshake는 절대 flexible하지 않음 (v0-v1)
	// Request Header: 항상 v1
	// Response Header: 항상 v0
	assert get_request_header_version(.sasl_handshake, 0) == 1
	assert get_request_header_version(.sasl_handshake, 1) == 1
	assert get_response_header_version(.sasl_handshake, 0) == 0
	assert get_response_header_version(.sasl_handshake, 1) == 0
}

fn test_sasl_authenticate_header_version() {
	// SaslAuthenticate: v0-v1 non-flexible, v2+ flexible
	// v0-v1: Request Header v1, Response Header v0
	assert get_request_header_version(.sasl_authenticate, 0) == 1
	assert get_request_header_version(.sasl_authenticate, 1) == 1
	assert get_response_header_version(.sasl_authenticate, 0) == 0
	assert get_response_header_version(.sasl_authenticate, 1) == 0

	// v2+: Request Header v2, Response Header v1
	assert get_request_header_version(.sasl_authenticate, 2) == 2
	assert get_response_header_version(.sasl_authenticate, 2) == 1
}

fn test_metadata_header_version() {
	// Metadata: v0-v8 non-flexible, v9+ flexible
	// v8: non-flexible → Request Header v1, Response Header v0
	assert get_request_header_version(.metadata, 8) == 1
	assert get_response_header_version(.metadata, 8) == 0

	// v9+: flexible → Request Header v2, Response Header v1
	assert get_request_header_version(.metadata, 9) == 2
	assert get_request_header_version(.metadata, 12) == 2
	assert get_response_header_version(.metadata, 9) == 1
	assert get_response_header_version(.metadata, 12) == 1
}

fn test_produce_header_version() {
	// Produce: v0-v8 non-flexible, v9+ flexible
	assert get_request_header_version(.produce, 8) == 1
	assert get_response_header_version(.produce, 8) == 0

	assert get_request_header_version(.produce, 9) == 2
	assert get_request_header_version(.produce, 11) == 2
	assert get_response_header_version(.produce, 9) == 1
	assert get_response_header_version(.produce, 11) == 1
}

fn test_fetch_header_version() {
	// Fetch: v0-v11 non-flexible, v12+ flexible
	assert get_request_header_version(.fetch, 11) == 1
	assert get_response_header_version(.fetch, 11) == 0

	assert get_request_header_version(.fetch, 12) == 2
	assert get_request_header_version(.fetch, 16) == 2
	assert get_response_header_version(.fetch, 12) == 1
	assert get_response_header_version(.fetch, 16) == 1
}

// ============================================
// is_flexible_version 테스트
// ============================================

fn test_is_flexible_version_api_versions() {
	// ApiVersions: v0-v2 non-flexible, v3+ flexible
	assert is_flexible_version(.api_versions, 0) == false
	assert is_flexible_version(.api_versions, 1) == false
	assert is_flexible_version(.api_versions, 2) == false
	assert is_flexible_version(.api_versions, 3) == true
	assert is_flexible_version(.api_versions, 4) == true
}

fn test_is_flexible_version_metadata() {
	// Metadata: v0-v8 non-flexible, v9+ flexible
	assert is_flexible_version(.metadata, 8) == false
	assert is_flexible_version(.metadata, 9) == true
}

fn test_is_flexible_version_produce() {
	// Produce: v0-v8 non-flexible, v9+ flexible
	assert is_flexible_version(.produce, 8) == false
	assert is_flexible_version(.produce, 9) == true
}

fn test_is_flexible_version_fetch() {
	// Fetch: v0-v11 non-flexible, v12+ flexible
	assert is_flexible_version(.fetch, 11) == false
	assert is_flexible_version(.fetch, 12) == true
}

fn test_is_flexible_version_sasl() {
	// SaslHandshake: 절대 flexible하지 않음
	assert is_flexible_version(.sasl_handshake, 0) == false
	assert is_flexible_version(.sasl_handshake, 1) == false

	// SaslAuthenticate: v2+ flexible
	assert is_flexible_version(.sasl_authenticate, 1) == false
	assert is_flexible_version(.sasl_authenticate, 2) == true
}

// ============================================
// 엣지 케이스 및 회귀 테스트
// ============================================

fn test_header_version_symmetry_exception() {
	// ApiVersions는 특별함:
	// Request: v3+에서 Header v2 (flexible)
	// Response: Header v0 (항상)

	// v3의 경우:
	req_header := get_request_header_version(.api_versions, 3)
	resp_header := get_response_header_version(.api_versions, 3)

	assert req_header == 2
	assert resp_header == 0
	assert req_header != resp_header
}

fn test_all_other_apis_symmetric_flexibility() {
	// 다른 모든 API의 경우, Request와 Response 헤더 버전은
	// flexibility 측면에서 일치해야 함 (둘 다 flexible이거나 둘 다 non-flexible)

	// Produce v9 테스트 (flexible)
	assert get_request_header_version(.produce, 9) == 2
	assert get_response_header_version(.produce, 9) == 1

	// Fetch v12 테스트 (flexible)
	assert get_request_header_version(.fetch, 12) == 2
	assert get_response_header_version(.fetch, 12) == 1

	// Metadata v9 테스트 (flexible)
	assert get_request_header_version(.metadata, 9) == 2
	assert get_response_header_version(.metadata, 9) == 1

	// FindCoordinator v3 테스트 (flexible)
	assert get_request_header_version(.find_coordinator, 3) == 2
	assert get_response_header_version(.find_coordinator, 3) == 1
}
