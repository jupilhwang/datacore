// Tests for SSE handler CORS origin resolution and request parsing
module http

// -- resolve_cors_origin: wildcard mode --

fn test_resolve_cors_origin_returns_wildcard_when_origins_empty() {
	origin := resolve_cors_origin([]string{}, 'https://example.com') or {
		assert false, 'expected wildcard, got none'
		return
	}
	assert origin == '*'
}

fn test_resolve_cors_origin_returns_wildcard_when_star_in_origins() {
	origin := resolve_cors_origin(['*'], 'https://example.com') or {
		assert false, 'expected wildcard, got none'
		return
	}
	assert origin == '*'
}

fn test_resolve_cors_origin_returns_wildcard_when_request_origin_empty_and_no_origins() {
	origin := resolve_cors_origin([]string{}, '') or {
		assert false, 'expected wildcard, got none'
		return
	}
	assert origin == '*'
}

// -- resolve_cors_origin: specific origin matched --

fn test_resolve_cors_origin_returns_matched_origin() {
	allowed := ['https://example.com', 'https://app.example.com']
	origin := resolve_cors_origin(allowed, 'https://example.com') or {
		assert false, 'expected matched origin, got none'
		return
	}
	assert origin == 'https://example.com'
}

fn test_resolve_cors_origin_matches_second_origin_in_list() {
	allowed := ['https://example.com', 'https://app.example.com']
	origin := resolve_cors_origin(allowed, 'https://app.example.com') or {
		assert false, 'expected matched origin, got none'
		return
	}
	assert origin == 'https://app.example.com'
}

// -- resolve_cors_origin: unmatched origin blocked --

fn test_resolve_cors_origin_returns_none_for_unmatched_origin() {
	allowed := ['https://example.com']
	if _ := resolve_cors_origin(allowed, 'https://evil.com') {
		assert false, 'expected none for unmatched origin'
	}
}

fn test_resolve_cors_origin_returns_none_for_empty_origin_with_specific_allowed() {
	allowed := ['https://example.com']
	if _ := resolve_cors_origin(allowed, '') {
		assert false, 'expected none for empty request origin with specific allowed origins'
	}
}

// -- parse_sse_request: origin header extraction --

fn test_parse_sse_request_extracts_origin_header() {
	headers := {
		'Origin':     'https://example.com'
		'User-Agent': 'test-agent'
	}
	request := parse_sse_request('/v1/topics/test-topic/sse', map[string]string{}, headers,
		'127.0.0.1') or {
		assert false, 'expected successful parse, got: ${err}'
		return
	}
	assert request.origin == 'https://example.com'
}

fn test_parse_sse_request_origin_defaults_to_empty_when_missing() {
	headers := {
		'User-Agent': 'test-agent'
	}
	request := parse_sse_request('/v1/topics/test-topic/sse', map[string]string{}, headers,
		'127.0.0.1') or {
		assert false, 'expected successful parse, got: ${err}'
		return
	}
	assert request.origin == ''
}
