/// Infrastructure layer - Password hashing utilities
/// Provides salted SHA-256 hashing for secure password storage.
module auth

import crypto.sha256
import rand

/// hash_password hashes a password with a random salt.
/// Returns the result in 'salt:hash' format where salt is 32 hex chars.
fn hash_password(password string) string {
	salt := rand.hex(32)
	hash := compute_hash(salt, password)
	return '${salt}:${hash}'
}

/// verify_password checks whether a plaintext password matches a stored hash.
/// The stored_hash must be in 'salt:hash' format.
fn verify_password(password string, stored_hash string) bool {
	parts := stored_hash.split(':')
	if parts.len != 2 {
		return false
	}
	salt := parts[0]
	expected_hash := parts[1]
	actual_hash := compute_hash(salt, password)
	return actual_hash == expected_hash
}

/// compute_hash computes SHA-256 hex digest of salt concatenated with password.
fn compute_hash(salt string, password string) string {
	return sha256.hexhash('${salt}${password}')
}
