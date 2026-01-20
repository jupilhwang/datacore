// Infra Layer - Kafka Protocol Handler - SASL Operations
// SaslHandshake, SaslAuthenticate handlers
module kafka

// ============================================================================
// SaslHandshake Handler (API Key 17)
// ============================================================================
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

// ============================================================================
// SaslAuthenticate Handler (API Key 36)
// ============================================================================
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
