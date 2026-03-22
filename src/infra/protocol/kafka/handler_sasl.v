// Kafka protocol - SASL operations
// SaslHandshake, SaslAuthenticate
// Request/response types, parsing, encoding, and handlers
//
// Supported mechanisms:
// - PLAIN: Simple username/password (TLS recommended)
// - SCRAM-SHA-256: Challenge-Response based authentication
module kafka

import domain
import infra.observability
import time

// SaslHandshake request (API Key 17)

// Used for SASL mechanism negotiation between client and broker
// v0: Basic mechanism negotiation
// v1: Added mechanism enable/disable flags

/// SaslHandshakeRequest holds the SASL mechanism name for mechanism negotiation between client and broker.
pub struct SaslHandshakeRequest {
pub:
	mechanism string
}

fn parse_sasl_handshake_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslHandshakeRequest {
	// SaslHandshake is non-flexible (only v0-v1 supported)
	mechanism := reader.read_string()!

	return SaslHandshakeRequest{
		mechanism: mechanism
	}
}

// SaslAuthenticate request (API Key 36)

// Used for SASL authentication after mechanism handshake
// v0: Basic authentication
// v1: Added session lifetime
// v2: Flexible version

/// SaslAuthenticateRequest holds the SASL authentication bytes for a SaslAuthenticate request.
pub struct SaslAuthenticateRequest {
pub:
	auth_bytes []u8
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

// SaslHandshake response (API Key 17)

// Returns the list of SASL mechanisms supported by the broker

/// SaslHandshakeResponse returns the list of SASL mechanisms supported by the broker.
pub struct SaslHandshakeResponse {
pub:
	error_code i16
	mechanisms []string
}

/// encode serializes the SaslHandshakeResponse into bytes.
pub fn (r SaslHandshakeResponse) encode(version i16) []u8 {
	// SaslHandshake is non-flexible (only v0-v1 supported)
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

// SaslAuthenticate response (API Key 36)

// Returns the SASL authentication result

/// SaslAuthenticateResponse holds the SASL authentication result.
pub struct SaslAuthenticateResponse {
pub:
	error_code          i16
	error_message       ?string
	auth_bytes          []u8
	session_lifetime_ms i64
}

/// encode serializes the SaslAuthenticateResponse into bytes.
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

// SASL Handlers

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

	// Store the negotiated mechanism for use in handle_sasl_authenticate
	if error_code == 0 {
		upper := req.mechanism.to_upper()
		h.negotiated_mechanism = match upper {
			'SCRAM-SHA-256' { domain.SaslMechanism.scram_sha_256 }
			'SCRAM-SHA-512' { domain.SaslMechanism.scram_sha_512 }
			'OAUTHBEARER' { domain.SaslMechanism.oauthbearer }
			else { domain.SaslMechanism.plain }
		}
	}

	elapsed := time.since(start_time)
	h.logger.info('SASL handshake completed', observability.field_string('mechanism',
		req.mechanism), observability.field_int('error_code', error_code), observability.field_duration('latency',
		elapsed))

	return response.encode(version)
}

// SaslAuthenticateResult holds the result of a SASL authenticate handler call.
// It combines the encoded response bytes with an optional principal for connection update.
struct SaslAuthenticateResult {
	response_bytes []u8
	principal      ?domain.Principal
}

// Handle SaslAuthenticate (API Key 36)
// Handles SASL authentication
// Supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER
// Returns encoded response bytes and an optional principal for connection authentication.
fn (mut h Handler) handle_sasl_authenticate(body []u8, version i16) !SaslAuthenticateResult {
	start_time := time.now()
	mut reader := new_reader(body)
	is_flexible := is_flexible_version(.sasl_authenticate, version)

	req := parse_sasl_authenticate_request(mut reader, version, is_flexible)!

	h.logger.debug('SASL authenticate request', observability.field_bytes('auth_bytes_len',
		req.auth_bytes.len))

	// Perform authentication
	if mut auth_mgr := h.auth_manager {
		// Use the mechanism negotiated during SaslHandshake; fall back to detection
		// only when no handshake was performed (e.g., legacy clients).
		mechanism := h.negotiated_mechanism or { detect_sasl_mechanism(req.auth_bytes) }

		h.logger.debug('SASL authenticate mechanism', observability.field_string('mechanism',
			mechanism.str()))

		result := auth_mgr.authenticate(mechanism, req.auth_bytes) or {
			// Authentication error
			elapsed := time.since(start_time)
			h.logger.warn('SASL authentication error', observability.field_string('mechanism',
				mechanism.str()), observability.field_err_str(err.msg()), observability.field_duration('latency',
				elapsed))

			if mut al := h.audit_logger {
				al.log_auth_failure('', err.msg())
			}

			response := SaslAuthenticateResponse{
				error_code:          i16(ErrorCode.sasl_authentication_failed)
				error_message:       'Authentication failed'
				auth_bytes:          []u8{}
				session_lifetime_ms: 0
			}
			return SaslAuthenticateResult{
				response_bytes: response.encode(version)
				principal:      none
			}
		}

		if result.error_code == .none {
			elapsed := time.since(start_time)

			if result.complete {
				h.logger.info('SASL authentication successful', observability.field_string('mechanism',
					mechanism.str()), observability.field_duration('latency', elapsed))

				principal_name := if p := result.principal {
					p.name
				} else {
					'unknown'
				}
				if mut al := h.audit_logger {
					al.log_auth_success('', principal_name, mechanism.str())
				}
			} else {
				h.logger.debug('SASL authentication step completed', observability.field_string('mechanism',
					mechanism.str()), observability.field_duration('latency', elapsed))
			}

			response := SaslAuthenticateResponse{
				error_code:          0
				error_message:       none
				auth_bytes:          result.challenge
				session_lifetime_ms: 0
			}
			return SaslAuthenticateResult{
				response_bytes: response.encode(version)
				principal:      result.principal
			}
		} else {
			elapsed := time.since(start_time)
			h.logger.warn('SASL authentication failed', observability.field_string('mechanism',
				mechanism.str()), observability.field_string('error', result.error_message),
				observability.field_duration('latency', elapsed))

			if mut al := h.audit_logger {
				al.log_auth_failure('', result.error_message)
			}

			response := SaslAuthenticateResponse{
				error_code:          i16(result.error_code)
				error_message:       'Authentication failed'
				auth_bytes:          []u8{}
				session_lifetime_ms: 0
			}
			return SaslAuthenticateResult{
				response_bytes: response.encode(version)
				principal:      none
			}
		}
	} else {
		// No auth manager - authentication not configured
		elapsed := time.since(start_time)
		h.logger.warn('SASL authentication not configured', observability.field_duration('latency',
			elapsed))

		response := SaslAuthenticateResponse{
			error_code:          i16(ErrorCode.illegal_sasl_state)
			error_message:       'SASL authentication not configured'
			auth_bytes:          []u8{}
			session_lifetime_ms: 0
		}
		return SaslAuthenticateResult{
			response_bytes: response.encode(version)
			principal:      none
		}
	}
}

/// detect_sasl_mechanism detects the SASL mechanism from authentication bytes.
/// PLAIN: bytes contain null (\0)
/// SCRAM: starts with "n,,", "y,,", or "p=" (GS2 header)
/// OAUTHBEARER: contains auth=Bearer token
fn detect_sasl_mechanism(auth_bytes []u8) domain.SaslMechanism {
	if auth_bytes.len == 0 {
		return .plain
	}

	auth_str := auth_bytes.bytestr()

	// OAUTHBEARER: contains the auth=Bearer attribute (RFC 7628)
	if auth_str.contains('auth=Bearer ') {
		return .oauthbearer
	}

	// SCRAM client-first-message starts with a GS2 header
	// n,, (no channel binding)
	// y,, (server does not support channel binding)
	// p=... (channel binding in use)
	// The mechanism variant (SHA-256 vs SHA-512) is determined by the
	// SaslHandshake negotiation; here we default to SCRAM-SHA-256.
	// In the handler, the connection's negotiated mechanism should be used instead.
	if auth_str.starts_with('n,,') || auth_str.starts_with('y,,') || auth_str.starts_with('p=') {
		return .scram_sha_256
	}

	// PLAIN: check for null bytes
	for b in auth_bytes {
		if b == 0 {
			return .plain
		}
	}

	// Default: PLAIN
	return .plain
}
