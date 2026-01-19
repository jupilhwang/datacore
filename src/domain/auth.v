// Domain Layer - Authentication Domain Models
module domain

// SaslMechanism represents supported SASL authentication mechanisms
pub enum SaslMechanism {
	plain         // PLAIN - username/password (TLS recommended)
	scram_sha_256 // SCRAM-SHA-256 - Challenge-Response
	scram_sha_512 // SCRAM-SHA-512 - Challenge-Response
	oauthbearer   // OAUTHBEARER - OAuth 2.0 token
}

// Convert SASL mechanism to string
pub fn (m SaslMechanism) str() string {
	return match m {
		.plain { 'PLAIN' }
		.scram_sha_256 { 'SCRAM-SHA-256' }
		.scram_sha_512 { 'SCRAM-SHA-512' }
		.oauthbearer { 'OAUTHBEARER' }
	}
}

// Parse SASL mechanism from string
pub fn sasl_mechanism_from_str(s string) ?SaslMechanism {
	return match s.to_upper() {
		'PLAIN' { SaslMechanism.plain }
		'SCRAM-SHA-256' { SaslMechanism.scram_sha_256 }
		'SCRAM-SHA-512' { SaslMechanism.scram_sha_512 }
		'OAUTHBEARER' { SaslMechanism.oauthbearer }
		else { none }
	}
}

// Principal represents an authenticated user identity
pub struct Principal {
pub:
	name           string // Username (e.g., "alice")
	principal_type string // "User", "ServiceAccount", etc.
}

// Anonymous principal for unauthenticated connections
pub const anonymous_principal = Principal{
	name:           'ANONYMOUS'
	principal_type: 'User'
}

// Create a new user principal
pub fn new_user_principal(name string) Principal {
	return Principal{
		name:           name
		principal_type: 'User'
	}
}

// AuthState represents the current authentication state of a connection
pub enum AuthState {
	initial            // No authentication started
	handshake_complete // SASL handshake done, waiting for authenticate
	authenticated      // Successfully authenticated
	failed             // Authentication failed
}

// User represents a stored user for authentication
pub struct User {
pub:
	username      string
	password_hash string // For PLAIN: stored as-is (should be hashed in production)
	mechanism     SaslMechanism
	created_at    i64 // Unix timestamp
	updated_at    i64 // Unix timestamp
}

// ScramCredentials for SCRAM authentication (P2)
pub struct ScramCredentials {
pub:
	username   string
	salt       []u8
	iterations int
	stored_key []u8
	server_key []u8
}

// AuthResult represents the result of an authentication step
pub struct AuthResult {
pub:
	complete      bool       // True if authentication is complete
	challenge     []u8       // Next challenge for client (SCRAM)
	principal     ?Principal // Set when complete=true and successful
	error_code    ErrorCode  // Error code if failed
	error_message string     // Human-readable error message
}

// Create a successful authentication result
pub fn auth_success(principal Principal) AuthResult {
	return AuthResult{
		complete:   true
		principal:  principal
		error_code: .none
	}
}

// Create a failed authentication result
pub fn auth_failure(code ErrorCode, message string) AuthResult {
	return AuthResult{
		complete:      true
		error_code:    code
		error_message: message
	}
}

// AuthError represents authentication-specific errors
pub struct AuthError {
pub:
	code    ErrorCode
	message string
}

pub fn (e AuthError) msg() string {
	return e.message
}
