// SCRAM-SHA-256 인증 테스트
module auth

// Client First Message 파싱 테스트

fn test_parse_client_first_message_valid() {
	// 표준 SCRAM client-first-message 형식
	msg := 'n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL'
	result := parse_client_first_message(msg) or {
		assert false, 'Should parse valid message'
		return
	}

	assert result.gs2_header == 'n,,'
	assert result.username == 'user'
	assert result.nonce == 'fyko+d2lbbFgONRv9qkxdawL'
	assert result.bare == 'n=user,r=fyko+d2lbbFgONRv9qkxdawL'
}

fn test_parse_client_first_message_with_authzid() {
	// authzid가 포함된 메시지 (드물지만 유효)
	msg := 'n,a=admin,n=user,r=nonce123'
	result := parse_client_first_message(msg) or {
		assert false, 'Should parse message with authzid'
		return
	}

	assert result.username == 'user'
	assert result.nonce == 'nonce123'
}

fn test_parse_client_first_message_empty() {
	result := parse_client_first_message('')
	assert result == none
}

fn test_parse_client_first_message_invalid_format() {
	// GS2 헤더 누락
	result := parse_client_first_message('n=user,r=nonce')
	assert result == none
}

fn test_parse_client_first_message_missing_username() {
	result := parse_client_first_message('n,,r=nonce')
	assert result == none
}

fn test_parse_client_first_message_missing_nonce() {
	result := parse_client_first_message('n,,n=user')
	assert result == none
}

// Client Final Message 파싱 테스트

fn test_parse_client_final_message_valid() {
	msg := 'c=biws,r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHuB,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts='
	result := parse_client_final_message(msg) or {
		assert false, 'Should parse valid message'
		return
	}

	assert result.channel_binding == 'biws'
	assert result.nonce == 'fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHuB'
	assert result.proof == 'v0X8v3Bz2T0CJGbJQyF0X+HI4Ts='
	assert result.without_proof == 'c=biws,r=fyko+d2lbbFgONRv9qkxdawLHo+Vgk7qvUOKUwuWLIWg4l/9SraGMHuB'
}

fn test_parse_client_final_message_empty() {
	result := parse_client_final_message('')
	assert result == none
}

fn test_parse_client_final_message_missing_proof() {
	result := parse_client_final_message('c=biws,r=nonce')
	assert result == none
}

// Server First Message 생성 테스트

fn test_build_server_first_message() {
	salt := [u8(1), 2, 3, 4, 5, 6, 7, 8]
	msg := build_server_first_message('combined_nonce', salt, 4096)

	assert msg.contains('r=combined_nonce')
	assert msg.contains('i=4096')
	assert msg.contains('s=')
}

// Cryptographic Functions 테스트

fn test_pbkdf2_sha256_basic() {
	// 간단한 테스트 - 결과가 32바이트(SHA-256 출력)인지 확인
	password := 'password'.bytes()
	salt := 'salt'.bytes()
	result := pbkdf2_sha256(password, salt, 1)

	assert result.len == 32
}

fn test_pbkdf2_sha256_deterministic() {
	// 같은 입력에 대해 같은 출력
	password := 'password'.bytes()
	salt := 'salt'.bytes()

	result1 := pbkdf2_sha256(password, salt, 100)
	result2 := pbkdf2_sha256(password, salt, 100)

	assert result1 == result2
}

fn test_pbkdf2_sha256_different_iterations() {
	// 다른 iteration 횟수는 다른 결과
	password := 'password'.bytes()
	salt := 'salt'.bytes()

	result1 := pbkdf2_sha256(password, salt, 100)
	result2 := pbkdf2_sha256(password, salt, 200)

	assert result1 != result2
}

fn test_hmac_sha256_deterministic() {
	key := 'key'.bytes()
	message := 'message'.bytes()

	result1 := hmac_sha256(key, message)
	result2 := hmac_sha256(key, message)

	assert result1 == result2
	assert result1.len == 32
}

fn test_compute_stored_key() {
	salted_password := [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
	result := compute_stored_key(salted_password)

	assert result.len == 32
}

fn test_compute_server_key() {
	salted_password := [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
	result := compute_server_key(salted_password)

	assert result.len == 32
}

fn test_compute_client_key_from_salted() {
	salted_password := [u8(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32]
	result := compute_client_key_from_salted(salted_password)

	assert result.len == 32
}

// XOR 및 상수 시간 비교 테스트

fn test_xor_bytes_same_length() {
	a := [u8(0xFF), 0x00, 0xAA]
	b := [u8(0x00), 0xFF, 0x55]
	result := xor_bytes(a, b)

	assert result == [u8(0xFF), 0xFF, 0xFF]
}

fn test_xor_bytes_different_length() {
	a := [u8(1), 2, 3]
	b := [u8(1), 2]
	result := xor_bytes(a, b)

	assert result.len == 0
}

fn test_xor_bytes_self_inverse() {
	// XOR은 자기 자신과 XOR하면 0
	a := [u8(0x12), 0x34, 0x56]
	result := xor_bytes(a, a)

	assert result == [u8(0), 0, 0]
}

fn test_constant_time_compare_equal() {
	a := [u8(1), 2, 3, 4, 5]
	b := [u8(1), 2, 3, 4, 5]

	assert constant_time_compare(a, b) == true
}

fn test_constant_time_compare_different() {
	a := [u8(1), 2, 3, 4, 5]
	b := [u8(1), 2, 3, 4, 6]

	assert constant_time_compare(a, b) == false
}

fn test_constant_time_compare_different_length() {
	a := [u8(1), 2, 3]
	b := [u8(1), 2, 3, 4]

	assert constant_time_compare(a, b) == false
}

fn test_constant_time_compare_empty() {
	a := []u8{}
	b := []u8{}

	assert constant_time_compare(a, b) == true
}

// Salt 생성 테스트

fn test_generate_user_salt_deterministic() {
	// 같은 사용자명에 대해 같은 salt
	salt1 := generate_user_salt('testuser')
	salt2 := generate_user_salt('testuser')

	assert salt1 == salt2
	assert salt1.len == 16
}

fn test_generate_user_salt_different_users() {
	// 다른 사용자명에 대해 다른 salt
	salt1 := generate_user_salt('user1')
	salt2 := generate_user_salt('user2')

	assert salt1 != salt2
}

fn test_generate_nonce_length() {
	nonce := generate_nonce()
	// base64 인코딩된 24바이트 = 32자
	assert nonce.len == 32
}

fn test_generate_salt_length() {
	salt := generate_salt()
	assert salt.len == 16
}
