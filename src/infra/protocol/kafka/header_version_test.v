module kafka

// Request/Response Header version function tests
// Based on KIP-511 and the official Apache Kafka specification

// Request Header version tests

fn test_api_versions_request_header_version() {
	// ApiVersions Request always uses Header v1 (KIP-511)
	// Even if the body is flexible (v3+), the header is not

	// v0: non-flexible → Header v1
	assert get_request_header_version(.api_versions, 0) == 1
	// v1: non-flexible → Header v1
	assert get_request_header_version(.api_versions, 1) == 1
	// v2: non-flexible → Header v1
	assert get_request_header_version(.api_versions, 2) == 1
	// v3+: Header v2 (Flexible) — body is flexible, and so is the header
	// ApiVersions Request v3+ uses Header v2 (Flexible)
	// Only the Response Header is always v0
	assert get_request_header_version(.api_versions, 3) == 2
	assert get_request_header_version(.api_versions, 4) == 2
}

fn test_api_versions_response_header_version() {
	// ApiVersions Response always uses Header v0 (KIP-511)
	// This is the only API with this special behavior
	assert get_response_header_version(.api_versions, 0) == 0
	assert get_response_header_version(.api_versions, 1) == 0
	assert get_response_header_version(.api_versions, 2) == 0
	assert get_response_header_version(.api_versions, 3) == 0
	assert get_response_header_version(.api_versions, 4) == 0
}

fn test_sasl_handshake_header_version() {
	// SaslHandshake is never flexible (v0-v1)
	// Request Header: always v1
	// Response Header: always v0
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

// is_flexible_version tests

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
	// SaslHandshake: never flexible
	assert is_flexible_version(.sasl_handshake, 0) == false
	assert is_flexible_version(.sasl_handshake, 1) == false

	// SaslAuthenticate: v2+ flexible
	assert is_flexible_version(.sasl_authenticate, 1) == false
	assert is_flexible_version(.sasl_authenticate, 2) == true
}

// Edge cases and regression tests

fn test_header_version_symmetry_exception() {
	// ApiVersions is special:
	// Request: Header v2 (flexible) for v3+
	// Response: Header v0 (always)

	// For v3:
	req_header := get_request_header_version(.api_versions, 3)
	resp_header := get_response_header_version(.api_versions, 3)

	assert req_header == 2
	assert resp_header == 0
	assert req_header != resp_header
}

fn test_all_other_apis_symmetric_flexibility() {
	// For all other APIs, Request and Response header versions should match
	// in terms of flexibility (both flexible or both non-flexible)

	// Produce v9 test (flexible)
	assert get_request_header_version(.produce, 9) == 2
	assert get_response_header_version(.produce, 9) == 1

	// Fetch v12 test (flexible)
	assert get_request_header_version(.fetch, 12) == 2
	assert get_response_header_version(.fetch, 12) == 1

	// Metadata v9 test (flexible)
	assert get_request_header_version(.metadata, 9) == 2
	assert get_response_header_version(.metadata, 9) == 1

	// FindCoordinator v3 test (flexible)
	assert get_request_header_version(.find_coordinator, 3) == 2
	assert get_response_header_version(.find_coordinator, 3) == 1
}
