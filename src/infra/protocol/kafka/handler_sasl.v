// Kafka Protocol - SASL Operations
// SaslHandshake, SaslAuthenticate
// Request/Response types, parsing, encoding, and handlers
module kafka

// ============================================================================
// SaslHandshake Request (API Key 17)
// ============================================================================
// Used to negotiate SASL mechanism between client and broker
// v0: Basic mechanism negotiation
// v1: Adds mechanism enable/disable flags

pub struct SaslHandshakeRequest {
pub:
	mechanism string // The SASL mechanism chosen by the client
}

fn parse_sasl_handshake_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslHandshakeRequest {
	// SaslHandshake is never flexible (v0-v1 only)
	mechanism := reader.read_string()!

	return SaslHandshakeRequest{
		mechanism: mechanism
	}
}

// ============================================================================
// SaslAuthenticate Request (API Key 36)
// ============================================================================
// Used to perform SASL authentication after mechanism handshake
// v0: Basic authentication
// v1: Adds session lifetime
// v2: Flexible versions

pub struct SaslAuthenticateRequest {
pub:
	auth_bytes []u8 // The SASL authentication bytes from the client
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
// SaslHandshake Response (API Key 17)
// ============================================================================
// Returns the list of SASL mechanisms supported by the broker

pub struct SaslHandshakeResponse {
pub:
	error_code i16      // Error code (0 = no error, 33 = unsupported mechanism)
	mechanisms []string // List of SASL mechanisms enabled by the broker
}

pub fn (r SaslHandshakeResponse) encode(version i16) []u8 {
	// SaslHandshake is never flexible (v0-v1 only)
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
// SaslAuthenticate Response (API Key 36)
// ============================================================================
// Returns the result of SASL authentication

pub struct SaslAuthenticateResponse {
pub:
	error_code          i16     // Error code (0 = success, 58 = SASL_AUTHENTICATION_FAILED)
	error_message       ?string // Error message if authentication failed
	auth_bytes          []u8    // SASL authentication bytes from server (for multi-step)
	session_lifetime_ms i64     // v1+: Session lifetime in milliseconds (0 = no lifetime)
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
	mut reader := new_reader(body)
	is_flexible := is_flexible_version(.sasl_handshake, version)

	req := parse_sasl_handshake_request(mut reader, version, is_flexible)!

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

	return response.encode(version)
}

// Handle SaslAuthenticate (API Key 36)
// Handles SASL authentication
fn (mut h Handler) handle_sasl_authenticate(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	is_flexible := is_flexible_version(.sasl_authenticate, version)

	req := parse_sasl_authenticate_request(mut reader, version, is_flexible)!

	// Perform authentication
	if mut auth_mgr := h.auth_manager {
		result := auth_mgr.authenticate(.plain, req.auth_bytes) or {
			// Authentication error
			response := SaslAuthenticateResponse{
				error_code:          i16(ErrorCode.sasl_authentication_failed)
				error_message:       'Authentication failed: ${err.msg()}'
				auth_bytes:          []u8{}
				session_lifetime_ms: 0
			}
			return response.encode(version)
		}

		if result.error_code == .none {
			// Authentication successful
			response := SaslAuthenticateResponse{
				error_code:          0
				error_message:       none
				auth_bytes:          result.challenge // For SCRAM, this would be the server's challenge
				session_lifetime_ms: 0                // No session lifetime limit
			}
			return response.encode(version)
		} else {
			// Authentication failed
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
		response := SaslAuthenticateResponse{
			error_code:          i16(ErrorCode.illegal_sasl_state)
			error_message:       'SASL authentication not configured'
			auth_bytes:          []u8{}
			session_lifetime_ms: 0
		}
		return response.encode(version)
	}
}
