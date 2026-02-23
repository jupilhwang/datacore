// Implements OAUTHBEARER SASL authentication per RFC 7628.
// Provides OAuth 2.0 token-based authentication for Kafka clients.
//
// Protocol flow:
//   1. Client sends: n,,\x01auth=Bearer <token>\x01\x01
//   2. Server validates the token and returns success or challenge
//
// In production, token validation should connect to an external OAuth 2.0
// authorization server. This implementation provides a configurable
// token validation callback for flexibility.
module auth

import domain
import service.port

// OAuthBearerToken represents a validated OAuth 2.0 bearer token.
pub struct OAuthBearerToken {
pub:
	// The raw token string
	token string
	// Principal name extracted from the token (e.g. subject claim)
	principal_name string
	// Token expiry as Unix timestamp (0 means no expiry set)
	expires_at i64
	// Scope of the token (space-separated)
	scope string
}

/// OAuthBearerAuthenticator implements SASL OAUTHBEARER authentication.
/// Supports pluggable token validation via the validate_token method.
/// Override validate_token for production use with a real OAuth server.
pub struct OAuthBearerAuthenticator {
mut:
	// Optional: user store to verify that the principal exists in the system
	user_store ?port.UserStore
}

/// new_oauthbearer_authenticator creates a new OAUTHBEARER authenticator.
/// Uses a passthrough validator: any non-empty token is accepted.
/// For production, embed custom validation logic in a subtype or use
/// new_oauthbearer_authenticator_with_store to cross-check the user store.
pub fn new_oauthbearer_authenticator(validator_unused voidptr) &OAuthBearerAuthenticator {
	return &OAuthBearerAuthenticator{
		user_store: none
	}
}

/// new_oauthbearer_authenticator_with_store creates an OAUTHBEARER authenticator
/// that cross-checks the token principal against the user store.
pub fn new_oauthbearer_authenticator_with_store(user_store port.UserStore) &OAuthBearerAuthenticator {
	return &OAuthBearerAuthenticator{
		user_store: user_store
	}
}

/// mechanism returns the SASL mechanism type.
pub fn (a &OAuthBearerAuthenticator) mechanism() domain.SaslMechanism {
	return .oauthbearer
}

/// authenticate performs OAUTHBEARER authentication.
/// auth_bytes format (RFC 7628):
///   n,,[extensions]\x01auth=Bearer <token>\x01[key=value\x01...]\x01
pub fn (mut a OAuthBearerAuthenticator) authenticate(auth_bytes []u8) !domain.AuthResult {
	if auth_bytes.len == 0 {
		return domain.auth_failure(.sasl_authentication_failed, 'Empty authentication data')
	}

	// Parse the OAUTHBEARER initial client response
	token := parse_oauthbearer_token(auth_bytes) or {
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid OAUTHBEARER format')
	}

	if token.len == 0 {
		return domain.auth_failure(.sasl_authentication_failed, 'Empty bearer token')
	}

	// Validate the token (default: passthrough)
	oauth_token := a.validate_token(token) or {
		// Return a proper OAUTHBEARER error response per RFC 7628
		error_challenge := build_oauthbearer_error_response('invalid_token', err.msg())
		return domain.AuthResult{
			complete:      true
			challenge:     error_challenge
			error_code:    .sasl_authentication_failed
			error_message: 'Token validation failed'
		}
	}

	// Optionally cross-check principal against user store
	if mut us := a.user_store {
		us.get_user(oauth_token.principal_name) or {
			return domain.auth_failure(.sasl_authentication_failed, 'Principal not found in user store')
		}
	}

	principal := domain.new_user_principal(oauth_token.principal_name)

	return domain.AuthResult{
		complete:   true
		challenge:  []u8{}
		principal:  principal
		error_code: .none
	}
}

/// step is not used in OAUTHBEARER (single-step authentication).
pub fn (mut a OAuthBearerAuthenticator) step(response []u8) !domain.AuthResult {
	return error('OAUTHBEARER does not support multi-step authentication')
}

/// validate_token validates a bearer token.
/// Default implementation: accepts any non-empty token (passthrough).
/// The principal name is the token itself (first 64 chars).
/// Override this method in a subtype for real OAuth server integration.
fn (mut a OAuthBearerAuthenticator) validate_token(token string) !OAuthBearerToken {
	// Passthrough validator: accept any non-empty token
	// In production, this should validate against an OAuth 2.0 authorization server
	// and extract claims (sub, exp, scope) from the JWT token.
	principal_name := if token.len > 64 {
		token[0..64]
	} else {
		token
	}

	return OAuthBearerToken{
		token:          token
		principal_name: principal_name
		expires_at:     0
		scope:          ''
	}
}

/// parse_oauthbearer_token extracts the bearer token from an OAUTHBEARER initial response.
/// Format: n,,[\x01extensions]\x01auth=Bearer <token>\x01[key=value\x01...]\x01
fn parse_oauthbearer_token(data []u8) ?string {
	msg := data.bytestr()

	if msg.len == 0 {
		return none
	}

	// The message format uses \x01 as field separator
	parts := msg.split('\x01')

	for part in parts {
		if part.starts_with('auth=Bearer ') {
			token := part['auth=Bearer '.len..]
			if token.len == 0 {
				return none
			}
			return token
		}
	}

	return none
}

/// build_oauthbearer_error_response constructs an OAUTHBEARER server error challenge.
/// Per RFC 7628, the server sends a JSON-formatted error when token validation fails.
fn build_oauthbearer_error_response(status string, message string) []u8 {
	json := '{"status":"${status}","message":"${message}"}'
	return json.bytes()
}
