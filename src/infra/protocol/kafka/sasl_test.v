// Unit Tests - Infra Layer: SASL Protocol
module kafka

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
// SaslHandshake Request/Response Tests
// ============================================================================

fn test_sasl_handshake_request_parse_v0() {
	// Build a SaslHandshake request with mechanism "PLAIN"
	mut writer := new_writer()
	writer.write_string('PLAIN')

	mut reader := new_reader(writer.bytes())

	// Parse - v0 is never flexible
	mechanism := reader.read_string()!

	assert mechanism == 'PLAIN'
}

fn test_sasl_handshake_request_parse_v1() {
	// Build a SaslHandshake request with mechanism "SCRAM-SHA-256"
	mut writer := new_writer()
	writer.write_string('SCRAM-SHA-256')

	mut reader := new_reader(writer.bytes())

	mechanism := reader.read_string()!

	assert mechanism == 'SCRAM-SHA-256'
}

fn test_sasl_handshake_response_encode_v0_success() {
	response := SaslHandshakeResponse{
		error_code: 0
		mechanisms: ['PLAIN', 'SCRAM-SHA-256']
	}

	bytes := response.encode(0)

	mut reader := new_reader(bytes)

	// error_code: INT16
	error_code := reader.read_i16()!
	assert error_code == 0

	// mechanisms: ARRAY[STRING]
	array_len := reader.read_array_len()!
	assert array_len == 2

	m1 := reader.read_string()!
	assert m1 == 'PLAIN'

	m2 := reader.read_string()!
	assert m2 == 'SCRAM-SHA-256'
}

fn test_sasl_handshake_response_encode_v0_unsupported() {
	response := SaslHandshakeResponse{
		error_code: i16(ErrorCode.unsupported_sasl_mechanism)
		mechanisms: ['PLAIN']
	}

	bytes := response.encode(0)

	mut reader := new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 33 // UNSUPPORTED_SASL_MECHANISM

	array_len := reader.read_array_len()!
	assert array_len == 1
}

// ============================================================================
// SaslAuthenticate Request/Response Tests
// ============================================================================

fn test_sasl_authenticate_request_parse_v0() {
	// Build PLAIN auth bytes
	auth_bytes := make_plain_auth('', 'testuser', 'testpass')

	// Build request with BYTES field
	mut writer := new_writer()
	writer.write_bytes(auth_bytes)

	mut reader := new_reader(writer.bytes())

	// Parse auth bytes (non-flexible, v0)
	parsed := reader.read_bytes()!

	assert parsed == auth_bytes
}

fn test_sasl_authenticate_request_parse_v2_flexible() {
	// Build PLAIN auth bytes
	auth_bytes := make_plain_auth('', 'user', 'pass')

	// Build request with COMPACT_BYTES (flexible version)
	mut writer := new_writer()
	writer.write_compact_bytes(auth_bytes)
	writer.write_tagged_fields()

	mut reader := new_reader(writer.bytes())

	// Parse auth bytes (flexible, v2)
	parsed := reader.read_compact_bytes()!

	assert parsed == auth_bytes
}

fn test_sasl_authenticate_response_encode_v0_success() {
	response := SaslAuthenticateResponse{
		error_code:          0
		error_message:       none
		auth_bytes:          []u8{}
		session_lifetime_ms: 0
	}

	bytes := response.encode(0)

	mut reader := new_reader(bytes)

	// error_code: INT16
	error_code := reader.read_i16()!
	assert error_code == 0

	// error_message: NULLABLE_STRING
	error_msg := reader.read_nullable_string()!
	assert error_msg == '' // null/empty

	// auth_bytes: BYTES
	auth_data := reader.read_bytes()!
	assert auth_data.len == 0
}

fn test_sasl_authenticate_response_encode_v1_with_lifetime() {
	response := SaslAuthenticateResponse{
		error_code:          0
		error_message:       none
		auth_bytes:          []u8{}
		session_lifetime_ms: 3600000 // 1 hour
	}

	bytes := response.encode(1)

	mut reader := new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 0

	_ := reader.read_nullable_string()!
	_ := reader.read_bytes()!

	// session_lifetime_ms: INT64 (v1+)
	lifetime := reader.read_i64()!
	assert lifetime == 3600000
}

fn test_sasl_authenticate_response_encode_v0_failure() {
	response := SaslAuthenticateResponse{
		error_code:          i16(ErrorCode.sasl_authentication_failed)
		error_message:       'Invalid credentials'
		auth_bytes:          []u8{}
		session_lifetime_ms: 0
	}

	bytes := response.encode(0)

	mut reader := new_reader(bytes)

	error_code := reader.read_i16()!
	assert error_code == 58 // SASL_AUTHENTICATION_FAILED

	error_msg := reader.read_nullable_string()!
	assert error_msg == 'Invalid credentials'
}

fn test_sasl_authenticate_response_encode_v2_flexible() {
	response := SaslAuthenticateResponse{
		error_code:          0
		error_message:       none
		auth_bytes:          [u8(1), 2, 3]
		session_lifetime_ms: 7200000
	}

	bytes := response.encode(2)

	mut reader := new_reader(bytes)

	// error_code: INT16
	error_code := reader.read_i16()!
	assert error_code == 0

	// error_message: COMPACT_NULLABLE_STRING
	error_msg := reader.read_compact_nullable_string()!
	assert error_msg == ''

	// auth_bytes: COMPACT_BYTES
	auth_data := reader.read_compact_bytes()!
	assert auth_data == [u8(1), 2, 3]

	// session_lifetime_ms: INT64
	lifetime := reader.read_i64()!
	assert lifetime == 7200000
}

// ============================================================================
// PLAIN Auth Parsing Tests
// ============================================================================

fn test_plain_auth_bytes_format() {
	// PLAIN format: [authzid]\0[authcid]\0[password]
	auth_bytes := make_plain_auth('', 'alice', 'secret123')

	// Verify format
	assert auth_bytes[0] == 0 // empty authzid followed by null

	// Find second null
	mut null_count := 0
	mut second_null_pos := 0
	for i, b in auth_bytes {
		if b == 0 {
			null_count += 1
			if null_count == 2 {
				second_null_pos = i
				break
			}
		}
	}

	assert null_count == 2

	// Extract username
	username := auth_bytes[1..second_null_pos].bytestr()
	assert username == 'alice'

	// Extract password
	password := auth_bytes[second_null_pos + 1..].bytestr()
	assert password == 'secret123'
}

fn test_plain_auth_bytes_with_authzid() {
	auth_bytes := make_plain_auth('admin', 'alice', 'secret123')

	// Find null positions
	mut null_positions := []int{}
	for i, b in auth_bytes {
		if b == 0 {
			null_positions << i
		}
	}

	assert null_positions.len == 2

	authzid := auth_bytes[0..null_positions[0]].bytestr()
	assert authzid == 'admin'

	username := auth_bytes[null_positions[0] + 1..null_positions[1]].bytestr()
	assert username == 'alice'

	password := auth_bytes[null_positions[1] + 1..].bytestr()
	assert password == 'secret123'
}
