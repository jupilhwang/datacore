// Implements SCRAM-SHA-256 and SCRAM-SHA-512 authentication per RFC 5802 and RFC 7677.
// Salted Challenge Response Authentication Mechanism (SCRAM)
module auth

import domain
import service.port
import crypto.sha256
import crypto.sha512
import crypto.hmac
import encoding.base64
import rand

// SCRAM-SHA-256 Constants

/// Default number of iterations used in SCRAM authentication
const default_iterations = 4096

/// SCRAM nonce length (bytes)
const nonce_length = 24

// SCRAM-SHA-256 Authenticator

/// ScramState represents the current state of SCRAM authentication.
pub enum ScramState {
	initial
	client_first_sent
	server_first_sent
	completed
	failed
}

/// ScramSha256Authenticator implements SCRAM-SHA-256 authentication.
/// Complies with RFC 5802 and RFC 7677.
pub struct ScramSha256Authenticator {
mut:
	user_store      port.UserStore
	state           ScramState
	username        string
	client_nonce    string
	server_nonce    string
	combined_nonce  string
	salt            []u8
	iterations      int
	auth_message    string
	salted_password []u8
	stored_key      []u8
	server_key      []u8
}

/// new_scram_sha256_authenticator - creates a new SCRAM-SHA-256 authenticator
/// new_scram_sha256_authenticator - creates a new SCRAM-SHA-256 authenticator
fn new_scram_sha256_authenticator(user_store port.UserStore) &ScramSha256Authenticator {
	return &ScramSha256Authenticator{
		user_store: user_store
		state:      .initial
		iterations: default_iterations
	}
}

/// mechanism - returns the SASL mechanism type
/// mechanism - returns the SASL mechanism type
pub fn (a &ScramSha256Authenticator) mechanism() domain.SaslMechanism {
	return .scram_sha_256
}

/// authenticate - handles the first step of SCRAM-SHA-256 authentication
/// authenticate - handles the first step of SCRAM-SHA-256 authentication
pub fn (mut a ScramSha256Authenticator) authenticate(auth_bytes []u8) !domain.AuthResult {
	if a.state != .initial {
		return domain.auth_failure(.illegal_sasl_state, 'SCRAM authenticator already in use')
	}

	// Parse client-first-message
	client_first := parse_client_first_message(auth_bytes.bytestr()) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid client-first-message format')
	}

	a.username = client_first.username
	a.client_nonce = client_first.nonce

	// Look up user
	user := a.user_store.get_user(a.username) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Authentication failed')
	}

	// SCRAM credentials: generate consistent salt based on username
	// In production, should be loaded from ScramCredentials table
	// Here we generate a deterministic salt based on username for consistency
	a.salt = generate_user_salt(a.username)
	a.iterations = default_iterations

	// Derive keys from password
	salted_password := pbkdf2_sha256(user.password_hash.bytes(), a.salt, a.iterations)
	a.salted_password = salted_password
	a.stored_key = compute_stored_key(salted_password)
	a.server_key = compute_server_key(salted_password)

	// Generate server nonce
	a.server_nonce = generate_nonce()
	a.combined_nonce = a.client_nonce + a.server_nonce

	// Build server-first-message
	server_first := build_server_first_message(a.combined_nonce, a.salt, a.iterations)

	// Construct auth_message (used later for signature verification)
	// auth_message = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
	a.auth_message = client_first.bare + ',' + server_first

	a.state = .server_first_sent

	return domain.AuthResult{
		complete:   false
		challenge:  server_first.bytes()
		error_code: .none
	}
}

/// step - handles subsequent steps of SCRAM authentication
/// step - handles subsequent steps of SCRAM authentication
pub fn (mut a ScramSha256Authenticator) step(response []u8) !domain.AuthResult {
	if a.state != .server_first_sent {
		return domain.auth_failure(.illegal_sasl_state, 'Invalid SCRAM state for step')
	}

	// Parse client-final-message
	client_final := parse_client_final_message(response.bytestr()) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid client-final-message format')
	}

	// Verify nonce
	if client_final.nonce != a.combined_nonce {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Nonce mismatch')
	}

	// Complete auth_message
	a.auth_message = a.auth_message + ',' + client_final.without_proof

	// Compute ClientKey (derived from salted_password)
	client_key := compute_client_key_from_salted(a.salted_password)

	// Verify client signature
	// ClientSignature = HMAC(StoredKey, AuthMessage)
	client_signature := hmac_sha256(a.stored_key, a.auth_message.bytes())

	// ClientProof = ClientKey XOR ClientSignature
	// Therefore ClientKey = ClientProof XOR ClientSignature
	// Verify: received ClientProof XOR ClientSignature == ClientKey

	// Base64-decoded client proof
	// V's base64.decode doesn't return errors, so validate by result length
	client_proof := base64.decode(client_final.proof)
	if client_proof.len == 0 && client_final.proof.len > 0 {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid proof encoding')
	}

	// Recover ClientKey from the proof sent by client
	// RecoveredClientKey = ClientProof XOR ClientSignature
	recovered_client_key := xor_bytes(client_proof, client_signature)

	// Compare recovered ClientKey with computed ClientKey
	if !constant_time_compare(recovered_client_key, client_key) {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Authentication failed')
	}

	// Compute server signature and build server-final-message
	server_signature := hmac_sha256(a.server_key, a.auth_message.bytes())
	server_final := 'v=' + base64.encode(server_signature)

	a.state = .completed

	return domain.AuthResult{
		complete:   true
		challenge:  server_final.bytes()
		principal:  domain.new_user_principal(a.username)
		error_code: .none
	}
}

// SCRAM Message Parsing

/// ClientFirstMessage represents a parsed client-first-message.
struct ClientFirstMessage {
	gs2_header string
	username   string
	nonce      string
	bare       string
}

/// parse_client_first_message parses a client-first-message.
/// Format: gs2-header + client-first-message-bare
/// gs2-header: n,, or y,, or p=... (channel binding)
/// client-first-message-bare: n=username,r=nonce
fn parse_client_first_message(msg string) ?ClientFirstMessage {
	if msg == '' {
		return none
	}

	// Find GS2 header (after the second ',')
	// In format n,,n=user,r=nonce, n,, is the gs2-header
	mut comma_count := 0
	mut gs2_end := 0

	for i, c in msg {
		if c == `,` {
			comma_count++
			if comma_count == 2 {
				gs2_end = i + 1
				break
			}
		}
	}

	if comma_count < 2 {
		return none
	}

	gs2_header := msg[0..gs2_end]
	bare := msg[gs2_end..]

	// Parse bare message: n=username,r=nonce[,extensions...]
	mut username := ''
	mut nonce := ''

	for part in bare.split(',') {
		if part.starts_with('n=') {
			username = part[2..]
		} else if part.starts_with('r=') {
			nonce = part[2..]
		}
	}

	if username == '' || nonce == '' {
		return none
	}

	return ClientFirstMessage{
		gs2_header: gs2_header
		username:   username
		nonce:      nonce
		bare:       bare
	}
}

/// ClientFinalMessage represents a parsed client-final-message.
struct ClientFinalMessage {
	channel_binding string
	nonce           string
	proof           string
	without_proof   string
}

/// parse_client_final_message parses a client-final-message.
/// Format: c=channel-binding,r=nonce,p=proof
fn parse_client_final_message(msg string) ?ClientFinalMessage {
	if msg == '' {
		return none
	}

	mut channel_binding := ''
	mut nonce := ''
	mut proof := ''
	mut without_proof_parts := []string{}

	for part in msg.split(',') {
		if part.starts_with('c=') {
			channel_binding = part[2..]
			without_proof_parts << part
		} else if part.starts_with('r=') {
			nonce = part[2..]
			without_proof_parts << part
		} else if part.starts_with('p=') {
			proof = part[2..]
		}
	}

	if channel_binding == '' || nonce == '' || proof == '' {
		return none
	}

	return ClientFinalMessage{
		channel_binding: channel_binding
		nonce:           nonce
		proof:           proof
		without_proof:   without_proof_parts.join(',')
	}
}

/// build_server_first_message builds a server-first-message.
/// Format: r=combined-nonce,s=salt,i=iterations
fn build_server_first_message(combined_nonce string, salt []u8, iterations int) string {
	salt_b64 := base64.encode(salt)
	return 'r=${combined_nonce},s=${salt_b64},i=${iterations}'
}

// Cryptographic Functions

/// generate_nonce generates a random nonce for SCRAM authentication.
fn generate_nonce() string {
	mut bytes := []u8{len: nonce_length}
	for i in 0 .. nonce_length {
		bytes[i] = u8(rand.int_in_range(0, 256) or { 0 })
	}
	return base64.encode(bytes)
}

/// generate_salt generates a random salt for SCRAM authentication.
fn generate_salt() []u8 {
	mut bytes := []u8{len: 16}
	for i in 0 .. 16 {
		bytes[i] = u8(rand.int_in_range(0, 256) or { 0 })
	}
	return bytes
}

/// generate_user_salt generates a deterministic salt based on username.
/// Always returns the same salt for the same username to ensure authentication consistency.
/// In production, a random salt should be generated at user creation time and stored.
fn generate_user_salt(username string) []u8 {
	// Generate deterministic salt by SHA-256 hashing the username
	// This is a temporary solution; production code should use salts stored in DB
	hash := sha256.sum256(username.bytes())
	return hash[0..16].clone()
}

/// pbkdf2_sha256 implements the PBKDF2-SHA-256 key derivation function.
/// Implementation per RFC 8018
fn pbkdf2_sha256(password []u8, salt []u8, iterations int) []u8 {
	// PBKDF2 with SHA-256
	// DK = T1 || T2 || ... || Tdklen/hlen
	// Ti = F(Password, Salt, c, i)
	// F(Password, Salt, c, i) = U1 ^ U2 ^ ... ^ Uc
	// U1 = PRF(Password, Salt || INT(i))
	// U2 = PRF(Password, U1)
	// ...
	// Uc = PRF(Password, Uc-1)

	// Append block number 1 (big-endian)
	mut salt_with_int := salt.clone()
	salt_with_int << u8(0)
	salt_with_int << u8(0)
	salt_with_int << u8(0)
	salt_with_int << u8(1)

	// U1 = HMAC(password, salt || INT(1))
	mut u := hmac_sha256(password, salt_with_int)
	mut result := u.clone()

	// U2...Uc
	for _ in 1 .. iterations {
		u = hmac_sha256(password, u)
		for i in 0 .. result.len {
			result[i] ^= u[i]
		}
	}

	return result
}

/// hmac_sha256 computes HMAC-SHA-256.
fn hmac_sha256(key []u8, message []u8) []u8 {
	return hmac.new(key, message, sha256.sum256, sha256.block_size).bytestr().bytes()
}

/// compute_stored_key computes the SCRAM StoredKey.
/// StoredKey = H(ClientKey)
fn compute_stored_key(salted_password []u8) []u8 {
	client_key := hmac_sha256(salted_password, 'Client Key'.bytes())
	sum := sha256.sum256(client_key)
	return sum[..].clone()
}

/// compute_server_key computes the SCRAM ServerKey.
/// ServerKey = HMAC(SaltedPassword, "Server Key")
fn compute_server_key(salted_password []u8) []u8 {
	return hmac_sha256(salted_password, 'Server Key'.bytes())
}

/// compute_client_key_from_salted computes ClientKey from SaltedPassword.
/// ClientKey = HMAC(SaltedPassword, "Client Key")
fn compute_client_key_from_salted(salted_password []u8) []u8 {
	return hmac_sha256(salted_password, 'Client Key'.bytes())
}

/// xor_bytes XORs two byte arrays.
fn xor_bytes(a []u8, b []u8) []u8 {
	if a.len != b.len {
		return []u8{}
	}
	mut result := []u8{len: a.len}
	for i in 0 .. a.len {
		result[i] = a[i] ^ b[i]
	}
	return result
}

/// constant_time_compare compares two byte arrays in constant time.
/// Used to prevent timing attacks
fn constant_time_compare(a []u8, b []u8) bool {
	// Handle length difference in constant time
	// If lengths differ, still compare up to shorter length to keep timing consistent
	mut result := u8(0)

	// Reflect length difference in result (non-zero if lengths differ)
	if a.len != b.len {
		result = 1
	}

	// Compare up to shorter length (0 if both empty)
	min_len := if a.len < b.len { a.len } else { b.len }
	for i in 0 .. min_len {
		result |= a[i] ^ b[i]
	}

	return result == 0
}

// SCRAM-SHA-512 Authenticator

/// ScramSha512Authenticator implements SCRAM-SHA-512 authentication.
/// Per RFC 5802 and RFC 7677, uses SHA-512 instead of SHA-256.
pub struct ScramSha512Authenticator {
mut:
	user_store      port.UserStore
	state           ScramState
	username        string
	client_nonce    string
	server_nonce    string
	combined_nonce  string
	salt            []u8
	iterations      int
	auth_message    string
	salted_password []u8
	stored_key      []u8
	server_key      []u8
}

/// new_scram_sha512_authenticator creates a new SCRAM-SHA-512 authenticator.
fn new_scram_sha512_authenticator(user_store port.UserStore) &ScramSha512Authenticator {
	return &ScramSha512Authenticator{
		user_store: user_store
		state:      .initial
		iterations: default_iterations
	}
}

/// mechanism returns the SASL mechanism type.
pub fn (a &ScramSha512Authenticator) mechanism() domain.SaslMechanism {
	return .scram_sha_512
}

/// authenticate handles the first step of SCRAM-SHA-512 authentication.
pub fn (mut a ScramSha512Authenticator) authenticate(auth_bytes []u8) !domain.AuthResult {
	if a.state != .initial {
		return domain.auth_failure(.illegal_sasl_state, 'SCRAM-SHA-512 authenticator already in use')
	}

	// Parse client-first-message
	client_first := parse_client_first_message(auth_bytes.bytestr()) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid client-first-message format')
	}

	a.username = client_first.username
	a.client_nonce = client_first.nonce

	// Look up user
	user := a.user_store.get_user(a.username) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Authentication failed')
	}

	// Generate deterministic salt based on username
	a.salt = generate_user_salt(a.username)
	a.iterations = default_iterations

	// Derive keys from password using SHA-512
	salted_password := pbkdf2_sha512(user.password_hash.bytes(), a.salt, a.iterations)
	a.salted_password = salted_password
	a.stored_key = compute_stored_key_sha512(salted_password)
	a.server_key = compute_server_key_sha512(salted_password)

	// Generate server nonce
	a.server_nonce = generate_nonce()
	a.combined_nonce = a.client_nonce + a.server_nonce

	// Build server-first-message
	server_first := build_server_first_message(a.combined_nonce, a.salt, a.iterations)

	a.auth_message = client_first.bare + ',' + server_first
	a.state = .server_first_sent

	return domain.AuthResult{
		complete:   false
		challenge:  server_first.bytes()
		error_code: .none
	}
}

/// step handles subsequent steps of SCRAM-SHA-512 authentication.
pub fn (mut a ScramSha512Authenticator) step(response []u8) !domain.AuthResult {
	if a.state != .server_first_sent {
		return domain.auth_failure(.illegal_sasl_state, 'Invalid SCRAM state for step')
	}

	// Parse client-final-message
	client_final := parse_client_final_message(response.bytestr()) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid client-final-message format')
	}

	// Verify nonce
	if client_final.nonce != a.combined_nonce {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Nonce mismatch')
	}

	// Complete auth_message
	a.auth_message = a.auth_message + ',' + client_final.without_proof

	// Compute ClientKey (derived from salted_password using SHA-512)
	client_key := compute_client_key_from_salted_sha512(a.salted_password)

	// ClientSignature = HMAC-SHA-512(StoredKey, AuthMessage)
	client_signature := hmac_sha512(a.stored_key, a.auth_message.bytes())

	// Decode client proof
	client_proof := base64.decode(client_final.proof)
	if client_proof.len == 0 && client_final.proof.len > 0 {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid proof encoding')
	}

	// Recover ClientKey from proof
	recovered_client_key := xor_bytes(client_proof, client_signature)

	if !constant_time_compare(recovered_client_key, client_key) {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Authentication failed')
	}

	// Build server-final-message
	server_signature := hmac_sha512(a.server_key, a.auth_message.bytes())
	server_final := 'v=' + base64.encode(server_signature)

	a.state = .completed

	return domain.AuthResult{
		complete:   true
		challenge:  server_final.bytes()
		principal:  domain.new_user_principal(a.username)
		error_code: .none
	}
}

// SHA-512 Cryptographic Functions

/// hmac_sha512 computes HMAC-SHA-512.
fn hmac_sha512(key []u8, message []u8) []u8 {
	return hmac.new(key, message, sha512.sum512, sha512.block_size).bytestr().bytes()
}

/// pbkdf2_sha512 implements the PBKDF2-SHA-512 key derivation function.
fn pbkdf2_sha512(password []u8, salt []u8, iterations int) []u8 {
	mut salt_with_int := salt.clone()
	salt_with_int << u8(0)
	salt_with_int << u8(0)
	salt_with_int << u8(0)
	salt_with_int << u8(1)

	mut u := hmac_sha512(password, salt_with_int)
	mut result := u.clone()

	for _ in 1 .. iterations {
		u = hmac_sha512(password, u)
		for i in 0 .. result.len {
			result[i] ^= u[i]
		}
	}

	return result
}

/// compute_stored_key_sha512 computes the SCRAM StoredKey with SHA-512.
/// StoredKey = SHA-512(ClientKey)
fn compute_stored_key_sha512(salted_password []u8) []u8 {
	client_key := hmac_sha512(salted_password, 'Client Key'.bytes())
	sum := sha512.sum512(client_key)
	return sum[..].clone()
}

/// compute_server_key_sha512 computes the SCRAM ServerKey with SHA-512.
fn compute_server_key_sha512(salted_password []u8) []u8 {
	return hmac_sha512(salted_password, 'Server Key'.bytes())
}

/// compute_client_key_from_salted_sha512 computes ClientKey from SaltedPassword using SHA-512.
fn compute_client_key_from_salted_sha512(salted_password []u8) []u8 {
	return hmac_sha512(salted_password, 'Client Key'.bytes())
}
