// Kafka 프로토콜 - SASL 작업
// SaslHandshake, SaslAuthenticate
// 요청/응답 타입, 파싱, 인코딩 및 핸들러
//
// 지원 메커니즘:
// - PLAIN: 단순 사용자명/비밀번호 (TLS 권장)
// - SCRAM-SHA-256: Challenge-Response 기반 인증
module kafka

import domain
import infra.observability
import time

// ============================================================================
// SaslHandshake 요청 (API Key 17)
// ============================================================================
// 클라이언트와 브로커 간 SASL 메커니즘 협상에 사용
// v0: 기본 메커니즘 협상
// v1: 메커니즘 활성화/비활성화 플래그 추가

pub struct SaslHandshakeRequest {
pub:
	mechanism string // 클라이언트가 선택한 SASL 메커니즘
}

fn parse_sasl_handshake_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslHandshakeRequest {
	// SaslHandshake는 flexible 버전이 아님 (v0-v1만 지원)
	mechanism := reader.read_string()!

	return SaslHandshakeRequest{
		mechanism: mechanism
	}
}

// ============================================================================
// SaslAuthenticate 요청 (API Key 36)
// ============================================================================
// 메커니즘 핸드셰이크 후 SASL 인증 수행에 사용
// v0: 기본 인증
// v1: 세션 수명 추가
// v2: Flexible 버전

pub struct SaslAuthenticateRequest {
pub:
	auth_bytes []u8 // 클라이언트의 SASL 인증 바이트
}

fn parse_sasl_authenticate_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslAuthenticateRequest {
	auth_bytes := if is_flexible {
		reader.read_compact_bytes()!
	} else {
		reader.read_bytes()!
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return SaslAuthenticateRequest{
		auth_bytes: auth_bytes
	}
}

// ============================================================================
// SaslHandshake 응답 (API Key 17)
// ============================================================================
// 브로커가 지원하는 SASL 메커니즘 목록 반환

pub struct SaslHandshakeResponse {
pub:
	error_code i16      // 에러 코드 (0 = 에러 없음, 33 = 지원하지 않는 메커니즘)
	mechanisms []string // 브로커가 활성화한 SASL 메커니즘 목록
}

pub fn (r SaslHandshakeResponse) encode(version i16) []u8 {
	// SaslHandshake는 flexible 버전이 아님 (v0-v1만 지원)
	mut writer := new_writer()

	// error_code: INT16
	writer.write_i16(r.error_code)

	// mechanisms: ARRAY[STRING]
	writer.write_array_len(r.mechanisms.len)
	for m in r.mechanisms {
		writer.write_string(m)
	}

	return writer.bytes()
}

// ============================================================================
// SaslAuthenticate 응답 (API Key 36)
// ============================================================================
// SASL 인증 결과 반환

pub struct SaslAuthenticateResponse {
pub:
	error_code          i16     // 에러 코드 (0 = 성공, 58 = SASL_AUTHENTICATION_FAILED)
	error_message       ?string // 인증 실패 시 에러 메시지
	auth_bytes          []u8    // 서버의 SASL 인증 바이트 (다단계 인증용)
	session_lifetime_ms i64     // v1+: 세션 수명 (밀리초, 0 = 수명 없음)
}

pub fn (r SaslAuthenticateResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// error_code: INT16
	writer.write_i16(r.error_code)

	// error_message: NULLABLE_STRING / COMPACT_NULLABLE_STRING
	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
	} else {
		writer.write_nullable_string(r.error_message)
	}

	// auth_bytes: BYTES / COMPACT_BYTES
	if is_flexible {
		writer.write_compact_bytes(r.auth_bytes)
	} else {
		writer.write_bytes(r.auth_bytes)
	}

	// session_lifetime_ms: INT64 (v1+)
	if version >= 1 {
		writer.write_i64(r.session_lifetime_ms)
	}

	// Tagged fields for flexible versions
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ============================================================================
// SASL Handlers
// ============================================================================

// Handle SaslHandshake (API Key 17)
// Handles SASL mechanism negotiation
fn (mut h Handler) handle_sasl_handshake(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	is_flexible := is_flexible_version(.sasl_handshake, version)

	req := parse_sasl_handshake_request(mut reader, version, is_flexible)!

	h.logger.info('SASL handshake request', observability.field_string('mechanism', req.mechanism))

	// Get supported mechanisms from auth manager
	mut supported_mechanisms := []string{}
	mut error_code := i16(0)

	if auth_mgr := h.auth_manager {
		// Convert SaslMechanism enum to strings
		for m in auth_mgr.supported_mechanisms() {
			supported_mechanisms << m.str()
		}

		// Check if the requested mechanism is supported
		if !auth_mgr.is_mechanism_supported(req.mechanism) {
			error_code = i16(ErrorCode.unsupported_sasl_mechanism)
		}
	} else {
		// No auth manager configured - return PLAIN as default supported mechanism
		// This is for backward compatibility when auth is not enabled
		supported_mechanisms = ['PLAIN']
		if req.mechanism.to_upper() != 'PLAIN' {
			error_code = i16(ErrorCode.unsupported_sasl_mechanism)
		}
	}

	response := SaslHandshakeResponse{
		error_code: error_code
		mechanisms: supported_mechanisms
	}

	elapsed := time.since(start_time)
	h.logger.info('SASL handshake completed', observability.field_string('mechanism',
		req.mechanism), observability.field_int('error_code', error_code), observability.field_duration('latency',
		elapsed))

	return response.encode(version)
}

// Handle SaslAuthenticate (API Key 36)
// Handles SASL authentication
// 지원 메커니즘: PLAIN, SCRAM-SHA-256
fn (mut h Handler) handle_sasl_authenticate(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	is_flexible := is_flexible_version(.sasl_authenticate, version)

	req := parse_sasl_authenticate_request(mut reader, version, is_flexible)!

	h.logger.debug('SASL authenticate request', observability.field_bytes('auth_bytes_len',
		req.auth_bytes.len))

	// Perform authentication
	if mut auth_mgr := h.auth_manager {
		// 인증 데이터에서 메커니즘 감지
		// PLAIN: [authzid]\0[authcid]\0[password] 형식
		// SCRAM: client-first-message (n,,n=username,r=nonce) 형식
		mechanism := detect_sasl_mechanism(req.auth_bytes)

		h.logger.debug('Detected SASL mechanism', observability.field_string('mechanism',
			mechanism.str()))

		result := auth_mgr.authenticate(mechanism, req.auth_bytes) or {
			// Authentication error
			elapsed := time.since(start_time)
			h.logger.warn('SASL authentication error', observability.field_string('mechanism',
				mechanism.str()), observability.field_err_str(err.msg()), observability.field_duration('latency',
				elapsed))

			response := SaslAuthenticateResponse{
				error_code:          i16(ErrorCode.sasl_authentication_failed)
				error_message:       'Authentication failed: ${err.msg()}'
				auth_bytes:          []u8{}
				session_lifetime_ms: 0
			}
			return response.encode(version)
		}

		if result.error_code == .none {
			// Authentication successful or challenge response
			elapsed := time.since(start_time)

			if result.complete {
				h.logger.info('SASL authentication successful', observability.field_string('mechanism',
					mechanism.str()), observability.field_duration('latency', elapsed))
			} else {
				h.logger.debug('SASL authentication step completed', observability.field_string('mechanism',
					mechanism.str()), observability.field_duration('latency', elapsed))
			}

			response := SaslAuthenticateResponse{
				error_code:          0
				error_message:       none
				auth_bytes:          result.challenge // For SCRAM, this is the server's challenge
				session_lifetime_ms: 0                // No session lifetime limit
			}
			return response.encode(version)
		} else {
			// Authentication failed
			elapsed := time.since(start_time)
			h.logger.warn('SASL authentication failed', observability.field_string('mechanism',
				mechanism.str()), observability.field_string('error', result.error_message),
				observability.field_duration('latency', elapsed))

			response := SaslAuthenticateResponse{
				error_code:          i16(result.error_code)
				error_message:       result.error_message
				auth_bytes:          []u8{}
				session_lifetime_ms: 0
			}
			return response.encode(version)
		}
	} else {
		// No auth manager - authentication not configured
		// Return error to indicate auth is required but not available
		elapsed := time.since(start_time)
		h.logger.warn('SASL authentication not configured', observability.field_duration('latency',
			elapsed))

		response := SaslAuthenticateResponse{
			error_code:          i16(ErrorCode.illegal_sasl_state)
			error_message:       'SASL authentication not configured'
			auth_bytes:          []u8{}
			session_lifetime_ms: 0
		}
		return response.encode(version)
	}
}

/// detect_sasl_mechanism은 인증 바이트에서 SASL 메커니즘을 감지합니다.
/// PLAIN: 바이트에 null(\0)이 포함됨
/// SCRAM: "n,," 또는 "y,," 또는 "p="로 시작 (GS2 헤더)
fn detect_sasl_mechanism(auth_bytes []u8) domain.SaslMechanism {
	if auth_bytes.len == 0 {
		return .plain
	}

	// SCRAM client-first-message는 GS2 헤더로 시작
	// n,, (채널 바인딩 없음)
	// y,, (서버가 채널 바인딩을 지원하지 않음)
	// p=... (채널 바인딩 사용)
	auth_str := auth_bytes.bytestr()
	if auth_str.starts_with('n,,') || auth_str.starts_with('y,,') || auth_str.starts_with('p=') {
		return .scram_sha_256
	}

	// PLAIN: null 바이트 포함 확인
	for b in auth_bytes {
		if b == 0 {
			return .plain
		}
	}

	// 기본값: PLAIN
	return .plain
}
