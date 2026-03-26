// SCRAM-SHA-512 Authentication Tests
module auth

import encoding.base64
import infra.auth as infra_auth

// Mechanism Tests

fn test_scram_sha512_mechanism_returns_correct_type() {
	mut store := infra_auth.new_memory_user_store()
	auth := new_scram_sha512_authenticator(store)

	assert auth.mechanism() == .scram_sha_512
}

// Key Derivation Tests (SHA-512)

fn test_pbkdf2_sha512_produces_64_byte_output() {
	password := 'password'.bytes()
	salt := 'salt'.bytes()
	result := pbkdf2_sha512(password, salt, 1)

	// SHA-512 output is 64 bytes
	assert result.len == 64
}

fn test_pbkdf2_sha512_deterministic() {
	password := 'password'.bytes()
	salt := 'salt'.bytes()

	result1 := pbkdf2_sha512(password, salt, 100)
	result2 := pbkdf2_sha512(password, salt, 100)

	assert result1 == result2
}

fn test_pbkdf2_sha512_different_iterations_produce_different_output() {
	password := 'password'.bytes()
	salt := 'salt'.bytes()

	result1 := pbkdf2_sha512(password, salt, 100)
	result2 := pbkdf2_sha512(password, salt, 200)

	assert result1 != result2
}

fn test_pbkdf2_sha512_differs_from_sha256() {
	password := 'password'.bytes()
	salt := 'salt'.bytes()

	result_256 := pbkdf2_sha256(password, salt, 100)
	result_512 := pbkdf2_sha512(password, salt, 100)

	// Different lengths: SHA-256=32 bytes, SHA-512=64 bytes
	assert result_256.len == 32
	assert result_512.len == 64
}

// HMAC-SHA-512 Tests

fn test_hmac_sha512_deterministic() {
	key := 'key'.bytes()
	message := 'message'.bytes()

	result1 := hmac_sha512(key, message)
	result2 := hmac_sha512(key, message)

	assert result1 == result2
	// HMAC-SHA-512 output is 64 bytes
	assert result1.len == 64
}

fn test_hmac_sha512_differs_from_hmac_sha256() {
	key := 'key'.bytes()
	message := 'message'.bytes()

	result_256 := hmac_sha256(key, message)
	result_512 := hmac_sha512(key, message)

	assert result_256.len == 32
	assert result_512.len == 64
}

// Stored/Server/Client Key Tests (SHA-512)

fn test_compute_stored_key_sha512_produces_64_bytes() {
	salted_password := []u8{len: 64, init: u8(index + 1)}
	result := compute_stored_key_sha512(salted_password)

	assert result.len == 64
}

fn test_compute_server_key_sha512_produces_64_bytes() {
	salted_password := []u8{len: 64, init: u8(index + 1)}
	result := compute_server_key_sha512(salted_password)

	assert result.len == 64
}

fn test_compute_client_key_from_salted_sha512_produces_64_bytes() {
	salted_password := []u8{len: 64, init: u8(index + 1)}
	result := compute_client_key_from_salted_sha512(salted_password)

	assert result.len == 64
}

fn test_sha512_keys_differ_from_sha256_keys() {
	salted_password_32 := []u8{len: 32, init: u8(index + 1)}
	salted_password_64 := []u8{len: 64, init: u8(index + 1)}

	stored_256 := compute_stored_key(salted_password_32)
	stored_512 := compute_stored_key_sha512(salted_password_64)

	assert stored_256.len == 32
	assert stored_512.len == 64
}

// Challenge-Response Flow Tests

fn test_scram_sha512_authenticate_returns_challenge() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .scram_sha_512) or {
		assert false, 'user creation should not fail'
		return
	}

	mut auth := new_scram_sha512_authenticator(store)

	// Send client-first-message
	client_first := 'n,,n=alice,r=client_nonce_abc123'
	result := auth.authenticate(client_first.bytes()) or {
		assert false, 'authenticate should not fail: ${err}'
		return
	}

	// Should return a challenge, not complete
	assert result.complete == false
	assert result.error_code == .none
	assert result.challenge.len > 0

	// Verify server-first-message format
	server_first := result.challenge.bytestr()
	assert server_first.contains('r=')
	assert server_first.contains('s=')
	assert server_first.contains('i=')
	// Combined nonce should start with client nonce
	assert server_first.contains('r=client_nonce_abc123')
}

fn test_scram_sha512_authenticate_unknown_user() {
	mut store := infra_auth.new_memory_user_store()

	mut auth := new_scram_sha512_authenticator(store)

	client_first := 'n,,n=unknown_user,r=client_nonce_abc123'
	result := auth.authenticate(client_first.bytes()) or {
		assert false, 'should return AuthResult, not error'
		return
	}

	assert result.error_code == .sasl_authentication_failed
}

fn test_scram_sha512_authenticate_invalid_message_format() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .scram_sha_512) or {
		assert false, 'user creation should not fail'
		return
	}

	mut auth := new_scram_sha512_authenticator(store)

	// Invalid message format
	result := auth.authenticate('invalid_message'.bytes()) or {
		assert false, 'should return AuthResult, not error'
		return
	}

	assert result.error_code == .sasl_authentication_failed
}

fn test_scram_sha512_reuse_after_authenticate_fails() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .scram_sha_512) or {
		assert false, 'user creation should not fail'
		return
	}

	mut auth := new_scram_sha512_authenticator(store)

	// First authenticate call
	client_first := 'n,,n=alice,r=nonce1'
	auth.authenticate(client_first.bytes()) or {
		assert false, 'first authenticate should not fail: ${err}'
		return
	}

	// Second authenticate call should fail (already in use)
	result := auth.authenticate(client_first.bytes()) or {
		assert false, 'should return AuthResult, not error'
		return
	}

	assert result.error_code == .illegal_sasl_state
}

fn test_scram_sha512_step_without_authenticate_fails() {
	mut store := infra_auth.new_memory_user_store()
	mut auth := new_scram_sha512_authenticator(store)

	// Calling step without calling authenticate first should fail
	result := auth.step('c=biws,r=nonce,p=proof'.bytes()) or {
		assert false, 'should return AuthResult, not error'
		return
	}

	assert result.error_code == .illegal_sasl_state
}

fn test_scram_sha512_full_authentication_success() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .scram_sha_512) or {
		assert false, 'user creation should not fail'
		return
	}
	register_scram_sha512_credentials(mut store, 'alice', 'secret123') or {
		assert false, 'scram credential registration should not fail'
		return
	}

	mut auth := new_scram_sha512_authenticator(store)

	// Step 1: Client-first-message
	client_nonce := 'test_nonce_12345'
	client_first := 'n,,n=alice,r=${client_nonce}'
	result1 := auth.authenticate(client_first.bytes()) or {
		assert false, 'authenticate should not fail: ${err}'
		return
	}

	assert result1.complete == false
	assert result1.error_code == .none

	// Parse server-first-message to extract combined nonce and salt
	server_first := result1.challenge.bytestr()
	mut combined_nonce := ''
	mut salt_b64 := ''
	mut iterations := 0
	for part in server_first.split(',') {
		if part.starts_with('r=') {
			combined_nonce = part[2..]
		} else if part.starts_with('s=') {
			salt_b64 = part[2..]
		} else if part.starts_with('i=') {
			iterations = part[2..].int()
		}
	}

	assert combined_nonce.starts_with(client_nonce)
	assert salt_b64.len > 0
	assert iterations > 0

	// Compute client proof (simulating client-side SCRAM-SHA-512)
	salt := base64.decode(salt_b64)
	salted_password := pbkdf2_sha512('secret123'.bytes(), salt, iterations)
	client_key := hmac_sha512(salted_password, 'Client Key'.bytes())

	stored_key_val := compute_stored_key_sha512(salted_password)

	// Build auth_message
	client_first_bare := 'n=alice,r=${client_nonce}'
	channel_binding := base64.encode('n,,'.bytes())
	client_final_without_proof := 'c=${channel_binding},r=${combined_nonce}'
	auth_message := '${client_first_bare},${server_first},${client_final_without_proof}'

	client_signature := hmac_sha512(stored_key_val, auth_message.bytes())
	client_proof := xor_bytes(client_key, client_signature)
	proof_b64 := base64.encode(client_proof)

	// Step 2: Client-final-message
	client_final := '${client_final_without_proof},p=${proof_b64}'
	result2 := auth.step(client_final.bytes()) or {
		assert false, 'step should not fail: ${err}'
		return
	}

	assert result2.complete == true
	assert result2.error_code == .none
	if principal := result2.principal {
		assert principal.name == 'alice'
		assert principal.principal_type == 'User'
	} else {
		assert false, 'principal should be set on successful auth'
	}

	// Verify server-final-message contains server signature
	server_final := result2.challenge.bytestr()
	assert server_final.starts_with('v=')
}

fn test_scram_sha512_wrong_password_fails_at_step() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'correct_password', .scram_sha_512) or {
		assert false, 'user creation should not fail'
		return
	}

	mut auth := new_scram_sha512_authenticator(store)

	// Step 1: Client-first-message
	client_nonce := 'test_nonce_wrong'
	client_first := 'n,,n=alice,r=${client_nonce}'
	result1 := auth.authenticate(client_first.bytes()) or {
		assert false, 'authenticate should not fail: ${err}'
		return
	}

	// Parse server response
	server_first := result1.challenge.bytestr()
	mut combined_nonce := ''
	mut salt_b64 := ''
	mut iterations := 0
	for part in server_first.split(',') {
		if part.starts_with('r=') {
			combined_nonce = part[2..]
		} else if part.starts_with('s=') {
			salt_b64 = part[2..]
		} else if part.starts_with('i=') {
			iterations = part[2..].int()
		}
	}

	// Compute proof with WRONG password
	salt := base64.decode(salt_b64)
	salted_password := pbkdf2_sha512('wrong_password'.bytes(), salt, iterations)
	client_key := hmac_sha512(salted_password, 'Client Key'.bytes())
	stored_key_val := compute_stored_key_sha512(salted_password)

	client_first_bare := 'n=alice,r=${client_nonce}'
	channel_binding := base64.encode('n,,'.bytes())
	client_final_without_proof := 'c=${channel_binding},r=${combined_nonce}'
	auth_message := '${client_first_bare},${server_first},${client_final_without_proof}'

	client_signature := hmac_sha512(stored_key_val, auth_message.bytes())
	client_proof := xor_bytes(client_key, client_signature)
	proof_b64 := base64.encode(client_proof)

	// Step 2: Client-final-message with wrong proof
	client_final := '${client_final_without_proof},p=${proof_b64}'
	result2 := auth.step(client_final.bytes()) or {
		assert false, 'step should return AuthResult, not error'
		return
	}

	assert result2.error_code == .sasl_authentication_failed
	assert result2.principal == none
}

fn test_scram_sha512_nonce_mismatch_fails() {
	mut store := infra_auth.new_memory_user_store()
	store.create_user('alice', 'secret123', .scram_sha_512) or {
		assert false, 'user creation should not fail'
		return
	}

	mut auth := new_scram_sha512_authenticator(store)

	// Step 1: Authenticate
	client_first := 'n,,n=alice,r=real_nonce'
	auth.authenticate(client_first.bytes()) or {
		assert false, 'authenticate should not fail: ${err}'
		return
	}

	// Step 2: Send client-final with mismatched nonce
	client_final := 'c=biws,r=completely_different_nonce,p=fakeproof'
	result := auth.step(client_final.bytes()) or {
		assert false, 'step should return AuthResult, not error'
		return
	}

	assert result.error_code == .sasl_authentication_failed
}
