// Unit Tests - Service Layer: SASL Authentication
module auth

import infra.auth as infra_auth

// Helper: Create PLAIN auth bytes
fn make_plain_auth(authzid string, username string, password string) []u8 {
	mut data := []u8{}
	data << authzid.bytes()
	data << u8(0)
	data << username.bytes()
	data << u8(0)
	data << password.bytes()
	return data
}

// ============================================================================
// PlainAuthenticator Tests
// ============================================================================

fn test_plain_authenticator_success() {
	// Setup: Create user store with test user
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	// Create authenticator
	mut auth := new_plain_authenticator(store)

	// Test authentication
	auth_bytes := make_plain_auth('', 'alice', 'secret123')
	result := auth.authenticate(auth_bytes) or { panic(err) }

	assert result.error_code == .none
	assert result.complete == true
	if principal := result.principal {
		assert principal.name == 'alice'
		assert principal.principal_type == 'User'
	} else {
		assert false, 'principal should be set'
	}
}

fn test_plain_authenticator_wrong_password() {
	// Setup
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	mut auth := new_plain_authenticator(store)

	// Test with wrong password
	auth_bytes := make_plain_auth('', 'alice', 'wrongpassword')
	result := auth.authenticate(auth_bytes) or { panic(err) }

	assert result.error_code == .sasl_authentication_failed
	assert result.complete == true
	assert result.principal == none
}

fn test_plain_authenticator_unknown_user() {
	// Setup
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	mut auth := new_plain_authenticator(store)

	// Test with unknown user
	auth_bytes := make_plain_auth('', 'bob', 'secret123')
	result := auth.authenticate(auth_bytes) or { panic(err) }

	assert result.error_code == .sasl_authentication_failed
}

fn test_plain_authenticator_empty_username() {
	mut store := infra_auth.new_memory_user_store()
	mut auth := new_plain_authenticator(store)

	// Test with empty username
	auth_bytes := make_plain_auth('', '', 'password')
	result := auth.authenticate(auth_bytes) or { panic(err) }

	assert result.error_code == .sasl_authentication_failed
}

fn test_plain_authenticator_empty_password() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	mut auth := new_plain_authenticator(store)

	// Test with empty password
	auth_bytes := make_plain_auth('', 'alice', '')
	result := auth.authenticate(auth_bytes) or { panic(err) }

	assert result.error_code == .sasl_authentication_failed
}

fn test_plain_authenticator_invalid_format() {
	mut store := infra_auth.new_memory_user_store()
	mut auth := new_plain_authenticator(store)

	// Test with invalid format (no null bytes)
	result := auth.authenticate('invalid_format'.bytes()) or { panic(err) }

	assert result.error_code == .sasl_authentication_failed
}

fn test_plain_authenticator_with_authzid() {
	// Setup
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	mut auth := new_plain_authenticator(store)

	// Test with authzid (authorization identity)
	auth_bytes := make_plain_auth('alice', 'alice', 'secret123')
	result := auth.authenticate(auth_bytes) or { panic(err) }

	assert result.error_code == .none
	if principal := result.principal {
		assert principal.name == 'alice'
	}
}

// ============================================================================
// AuthService Tests
// ============================================================================

fn test_auth_service_supported_mechanisms() {
	mut store := infra_auth.new_memory_user_store()
	service := new_auth_service(store, [.plain])

	mechanisms := service.supported_mechanisms()
	assert mechanisms.len == 1
	assert mechanisms[0] == .plain
}

fn test_auth_service_is_mechanism_supported() {
	mut store := infra_auth.new_memory_user_store()
	service := new_auth_service(store, [.plain])

	assert service.is_mechanism_supported('PLAIN') == true
	assert service.is_mechanism_supported('plain') == true
	assert service.is_mechanism_supported('SCRAM-SHA-256') == false
}

fn test_auth_service_authenticate() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('testuser', 'testpass', .plain) or { panic(err) }

	mut service := new_auth_service(store, [.plain])

	auth_bytes := make_plain_auth('', 'testuser', 'testpass')
	result := service.authenticate(.plain, auth_bytes) or { panic(err) }

	assert result.error_code == .none
	if principal := result.principal {
		assert principal.name == 'testuser'
	}
}

fn test_auth_service_get_authenticator() {
	mut store := infra_auth.new_memory_user_store()
	mut service := new_auth_service(store, [.plain])

	// Should succeed for PLAIN
	auth := service.get_authenticator(.plain) or { panic(err) }
	assert auth.mechanism() == .plain

	// Should fail for unsupported mechanism
	service.get_authenticator(.scram_sha_256) or {
		assert err.msg().contains('unsupported')
		return
	}
	assert false, 'should have returned error'
}

// ============================================================================
// Parse PLAIN Auth Tests
// ============================================================================

fn test_parse_plain_auth_valid() {
	data := make_plain_auth('', 'user', 'pass')
	result := parse_plain_auth(data) or {
		assert false, 'should parse successfully'
		return
	}

	assert result.authzid == ''
	assert result.username == 'user'
	assert result.password == 'pass'
}

fn test_parse_plain_auth_with_authzid() {
	data := make_plain_auth('admin', 'user', 'pass')
	result := parse_plain_auth(data) or {
		assert false, 'should parse successfully'
		return
	}

	assert result.authzid == 'admin'
	assert result.username == 'user'
	assert result.password == 'pass'
}

fn test_parse_plain_auth_empty_data() {
	if _ := parse_plain_auth([]u8{}) {
		assert false, 'should fail on empty data'
	}
}

fn test_parse_plain_auth_missing_null() {
	// Only one null byte
	data := 'user\x00pass'.bytes()
	if _ := parse_plain_auth(data) {
		assert false, 'should fail with only one null byte'
	}
}

fn test_parse_plain_auth_too_many_nulls() {
	// Three null bytes
	data := 'a\x00b\x00c\x00d'.bytes()
	if _ := parse_plain_auth(data) {
		assert false, 'should fail with three null bytes'
	}
}
