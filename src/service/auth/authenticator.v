// Service Layer - SASL Authentication Service
module auth

import domain
import service.port

// AuthService manages authentication for the broker
pub struct AuthService {
mut:
	user_store     port.UserStore
	mechanisms     []domain.SaslMechanism
	authenticators map[string]port.SaslAuthenticator
}

// new_auth_service creates a new authentication service
pub fn new_auth_service(user_store port.UserStore, mechanisms []domain.SaslMechanism) &AuthService {
	mut authenticators := map[string]port.SaslAuthenticator{}

	for mech in mechanisms {
		match mech {
			.plain {
				authenticators[mech.str()] = new_plain_authenticator(user_store)
			}
			else {
				// Other mechanisms will be implemented later
			}
		}
	}

	return &AuthService{
		user_store:     user_store
		mechanisms:     mechanisms
		authenticators: authenticators
	}
}

// supported_mechanisms returns the list of supported SASL mechanisms
pub fn (s &AuthService) supported_mechanisms() []domain.SaslMechanism {
	return s.mechanisms
}

// supported_mechanism_strings returns mechanism names as strings
pub fn (s &AuthService) supported_mechanism_strings() []string {
	mut result := []string{}
	for m in s.mechanisms {
		result << m.str()
	}
	return result
}

// is_mechanism_supported checks if a mechanism is supported
pub fn (s &AuthService) is_mechanism_supported(mechanism string) bool {
	return mechanism.to_upper() in s.authenticators
}

// get_authenticator returns the authenticator for a mechanism
pub fn (mut s AuthService) get_authenticator(mechanism domain.SaslMechanism) !port.SaslAuthenticator {
	if auth := s.authenticators[mechanism.str()] {
		return auth
	}
	return error('unsupported mechanism: ${mechanism.str()}')
}

// authenticate authenticates using the specified mechanism
pub fn (mut s AuthService) authenticate(mechanism domain.SaslMechanism, auth_bytes []u8) !domain.AuthResult {
	mut auth := s.get_authenticator(mechanism)!
	return auth.authenticate(auth_bytes)
}

// PlainAuthenticator implements SASL PLAIN authentication
pub struct PlainAuthenticator {
mut:
	user_store port.UserStore
}

// new_plain_authenticator creates a new PLAIN authenticator
pub fn new_plain_authenticator(user_store port.UserStore) &PlainAuthenticator {
	return &PlainAuthenticator{
		user_store: user_store
	}
}

// mechanism returns the SASL mechanism
pub fn (a &PlainAuthenticator) mechanism() domain.SaslMechanism {
	return .plain
}

// authenticate performs PLAIN authentication
// PLAIN format: [authzid]\0[authcid]\0[password]
// authzid: authorization identity (optional, usually empty)
// authcid: authentication identity (username)
// password: the password
pub fn (mut a PlainAuthenticator) authenticate(auth_bytes []u8) !domain.AuthResult {
	// Parse PLAIN auth data
	parts := parse_plain_auth(auth_bytes) or {
		return domain.auth_failure(.sasl_authentication_failed, 'invalid PLAIN format')
	}

	username := parts.username
	password := parts.password

	// Validate credentials
	valid := a.user_store.validate_password(username, password) or {
		return domain.auth_failure(.sasl_authentication_failed, 'authentication failed')
	}

	if !valid {
		return domain.auth_failure(.sasl_authentication_failed, 'invalid credentials')
	}

	// Return success with principal
	return domain.auth_success(domain.new_user_principal(username))
}

// step is not used for PLAIN (single-step authentication)
pub fn (mut a PlainAuthenticator) step(response []u8) !domain.AuthResult {
	return error('PLAIN does not support multi-step authentication')
}

// PlainAuthData represents parsed PLAIN authentication data
struct PlainAuthData {
	authzid  string // Authorization identity (optional)
	username string // Authentication identity
	password string // Password
}

// parse_plain_auth parses SASL PLAIN authentication bytes
// Format: [authzid]\0[authcid]\0[password]
fn parse_plain_auth(data []u8) ?PlainAuthData {
	if data.len == 0 {
		return none
	}

	// Find null bytes
	mut null_positions := []int{}
	for i, b in data {
		if b == 0 {
			null_positions << i
		}
	}

	// Must have exactly 2 null bytes
	if null_positions.len != 2 {
		return none
	}

	authzid := data[0..null_positions[0]].bytestr()
	username := data[null_positions[0] + 1..null_positions[1]].bytestr()
	password := data[null_positions[1] + 1..].bytestr()

	// Username and password must not be empty
	if username.len == 0 || password.len == 0 {
		return none
	}

	return PlainAuthData{
		authzid:  authzid
		username: username
		password: password
	}
}
