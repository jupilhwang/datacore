module domain

/// SaslMechanism represents supported SASL authentication mechanisms.
/// plain: PLAIN - username/password (TLS recommended)
/// scram_sha_256: SCRAM-SHA-256 - Challenge-Response
/// scram_sha_512: SCRAM-SHA-512 - Challenge-Response
/// oauthbearer: OAUTHBEARER - OAuth 2.0 token
pub enum SaslMechanism {
	plain
	scram_sha_256
	scram_sha_512
	oauthbearer
}

/// str converts a SASL mechanism to a string.
pub fn (m SaslMechanism) str() string {
	return match m {
		.plain { 'PLAIN' }
		.scram_sha_256 { 'SCRAM-SHA-256' }
		.scram_sha_512 { 'SCRAM-SHA-512' }
		.oauthbearer { 'OAUTHBEARER' }
	}
}

/// sasl_mechanism_from_str parses a string into a SaslMechanism.
pub fn sasl_mechanism_from_str(s string) ?SaslMechanism {
	return match s.to_upper() {
		'PLAIN' { SaslMechanism.plain }
		'SCRAM-SHA-256' { SaslMechanism.scram_sha_256 }
		'SCRAM-SHA-512' { SaslMechanism.scram_sha_512 }
		'OAUTHBEARER' { SaslMechanism.oauthbearer }
		else { none }
	}
}

/// Principal represents an authenticated user identity.
/// name: username (e.g. "alice")
/// principal_type: principal type ("User", "ServiceAccount", etc.)
pub struct Principal {
pub:
	name           string
	principal_type string
}

/// anonymous_principal constant.
pub const anonymous_principal = Principal{
	name:           'ANONYMOUS'
	principal_type: 'User'
}

/// new_user_principal creates a new user principal.
pub fn new_user_principal(name string) Principal {
	return Principal{
		name:           name
		principal_type: 'User'
	}
}

/// AuthState represents the current authentication state of a connection.
/// initial: authentication not yet started
/// handshake_complete: SASL handshake complete, awaiting authentication
/// authenticated: authentication successful
/// failed: authentication failed
pub enum AuthState {
	initial
	handshake_complete
	authenticated
	failed
}

/// AuthConnection is an interface for connection types that support authentication.
/// This interface is implemented by connection types in the interface layer.
pub interface AuthConnection {
	is_authenticated() bool
mut:
	set_authenticated(principal Principal)
}

/// User represents a stored user for authentication.
/// username: username
/// password_hash: password hash (stored as plaintext for PLAIN; hashing required in production)
/// mechanism: SASL mechanism
/// created_at: creation time (Unix timestamp)
/// updated_at: modification time (Unix timestamp)
pub struct User {
pub:
	username      string
	password_hash string
	mechanism     SaslMechanism
	created_at    i64
	updated_at    i64
}

/// ScramCredentials holds credentials for SCRAM authentication (P2)
pub struct ScramCredentials {
pub:
	username   string
	salt       []u8
	iterations int
	stored_key []u8
	server_key []u8
}

/// AuthResult represents the result of an authentication step.
/// complete: true when authentication is complete
/// challenge: next challenge for the client (for SCRAM)
/// principal: set when complete=true and authentication succeeded
/// error_code: error code if failed
/// error_message: human-readable error message
pub struct AuthResult {
pub:
	complete      bool
	challenge     []u8
	principal     ?Principal
	error_code    ErrorCode
	error_message string
}

/// auth_success creates a successful authentication result.
pub fn auth_success(principal Principal) AuthResult {
	return AuthResult{
		complete:   true
		principal:  principal
		error_code: .none
	}
}

/// auth_failure creates a failed authentication result.
pub fn auth_failure(code ErrorCode, message string) AuthResult {
	return AuthResult{
		complete:      true
		error_code:    code
		error_message: message
	}
}

/// AuthError represents an authentication-related error.
pub struct AuthError {
pub:
	code    ErrorCode
	message string
}

/// msg returns the error message.
pub fn (e AuthError) msg() string {
	return e.message
}
