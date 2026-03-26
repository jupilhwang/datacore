/// Unit tests - Infrastructure layer: Password hashing
module auth

/// test_hash_password_produces_salt_colon_hash_format verifies the salt:hash output format.
fn test_hash_password_produces_salt_colon_hash_format() {
	result := hash_password('mypassword')
	parts := result.split(':')
	assert parts.len == 2, 'hash should be in salt:hash format, got: ${result}'
	assert parts[0].len > 0, 'salt part should not be empty'
	assert parts[1].len > 0, 'hash part should not be empty'
}

/// test_hash_password_different_each_time verifies salt randomness produces unique hashes.
fn test_hash_password_different_each_time() {
	hash1 := hash_password('samepassword')
	hash2 := hash_password('samepassword')
	assert hash1 != hash2, 'different salts should produce different hashes'
}

/// test_verify_password_correct_succeeds verifies that correct password passes verification.
fn test_verify_password_correct_succeeds() {
	stored := hash_password('mypassword')
	assert verify_password('mypassword', stored) == true, 'correct password should verify successfully'
}

/// test_verify_password_wrong_fails verifies that wrong password fails verification.
fn test_verify_password_wrong_fails() {
	stored := hash_password('mypassword')
	assert verify_password('wrongpassword', stored) == false, 'wrong password should fail verification'
}

/// test_stored_password_is_not_plaintext verifies that create_user does not store plaintext.
fn test_stored_password_is_not_plaintext() {
	mut store := new_memory_user_store()
	password := 'secret123'
	store.create_user('alice', password, .plain) or { panic(err) }

	user := store.get_user('alice') or { panic(err) }
	assert user.password_hash != password, 'stored password should not be plaintext'
	assert user.password_hash.contains(':'), 'stored hash should be in salt:hash format'
}

/// test_validate_password_uses_hashing verifies that validate_password works with hashed storage.
fn test_validate_password_uses_hashing() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	valid := store.validate_password('alice', 'secret123') or { panic(err) }
	assert valid == true, 'correct password should validate after hashing'

	invalid := store.validate_password('alice', 'wrongpass') or { panic(err) }
	assert invalid == false, 'wrong password should fail validation after hashing'
}

/// test_update_password_stores_hash verifies that update_password stores hashed value.
fn test_update_password_stores_hash() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'oldpass', .plain) or { panic(err) }

	store.update_password('alice', 'newpass') or { panic(err) }

	user := store.get_user('alice') or { panic(err) }
	assert user.password_hash != 'newpass', 'updated password should not be stored as plaintext'
	assert user.password_hash.contains(':'), 'updated hash should be in salt:hash format'
}
