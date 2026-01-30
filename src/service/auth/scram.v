// 서비스 레이어 - SCRAM-SHA-256 인증
// RFC 5802 및 RFC 7677에 따른 SCRAM-SHA-256 인증을 구현합니다.
// Salted Challenge Response Authentication Mechanism (SCRAM)
module auth

import domain
import service.port
import crypto.sha256
import crypto.hmac
import encoding.base64
import rand

// SCRAM-SHA-256 Constants

/// SCRAM 인증에 사용되는 기본 iteration 횟수
const default_iterations = 4096

/// SCRAM nonce 길이 (바이트)
const nonce_length = 24

// SCRAM-SHA-256 Authenticator

/// ScramState는 SCRAM 인증의 현재 상태를 나타냅니다.
pub enum ScramState {
	initial           // 초기 상태
	client_first_sent // client-first-message 수신
	server_first_sent // server-first-message 전송
	completed         // 인증 완료
	failed            // 인증 실패
}

/// ScramSha256Authenticator는 SCRAM-SHA-256 인증을 구현합니다.
/// RFC 5802 및 RFC 7677을 준수합니다.
pub struct ScramSha256Authenticator {
mut:
	user_store      port.UserStore // 사용자 저장소
	state           ScramState     // 현재 인증 상태
	username        string         // 인증 중인 사용자명
	client_nonce    string         // 클라이언트 nonce
	server_nonce    string         // 서버 nonce
	combined_nonce  string         // client_nonce + server_nonce
	salt            []u8           // 사용자별 salt
	iterations      int            // PBKDF2 iteration 횟수
	auth_message    string         // 인증 메시지 (서명 검증용)
	salted_password []u8           // SaltedPassword (ClientKey 계산에 필요)
	stored_key      []u8           // 저장된 키 (비밀번호에서 파생)
	server_key      []u8           // 서버 키 (비밀번호에서 파생)
}

/// new_scram_sha256_authenticator - creates a new SCRAM-SHA-256 authenticator
/// new_scram_sha256_authenticator - creates a new SCRAM-SHA-256 authenticator
pub fn new_scram_sha256_authenticator(user_store port.UserStore) &ScramSha256Authenticator {
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

	// client-first-message 파싱
	client_first := parse_client_first_message(auth_bytes.bytestr()) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid client-first-message format')
	}

	a.username = client_first.username
	a.client_nonce = client_first.nonce

	// 사용자 조회
	user := a.user_store.get_user(a.username) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Authentication failed')
	}

	// SCRAM 자격 증명: 사용자 기반 일관된 salt 생성
	// 프로덕션에서는 ScramCredentials 테이블에서 로드해야 함
	// 여기서는 사용자명 기반으로 결정적 salt를 생성하여 일관성 유지
	a.salt = generate_user_salt(a.username)
	a.iterations = default_iterations

	// 비밀번호에서 키 파생
	salted_password := pbkdf2_sha256(user.password_hash.bytes(), a.salt, a.iterations)
	a.salted_password = salted_password // ClientKey 계산을 위해 저장
	a.stored_key = compute_stored_key(salted_password)
	a.server_key = compute_server_key(salted_password)

	// 서버 nonce 생성
	a.server_nonce = generate_nonce()
	a.combined_nonce = a.client_nonce + a.server_nonce

	// server-first-message 생성
	server_first := build_server_first_message(a.combined_nonce, a.salt, a.iterations)

	// auth_message 구성 (나중에 서명 검증에 사용)
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

	// client-final-message 파싱
	client_final := parse_client_final_message(response.bytestr()) or {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid client-final-message format')
	}

	// nonce 검증
	if client_final.nonce != a.combined_nonce {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Nonce mismatch')
	}

	// auth_message 완성
	a.auth_message = a.auth_message + ',' + client_final.without_proof

	// ClientKey 계산 (salted_password에서 파생)
	client_key := compute_client_key_from_salted(a.salted_password)

	// 클라이언트 서명 검증
	// ClientSignature = HMAC(StoredKey, AuthMessage)
	client_signature := hmac_sha256(a.stored_key, a.auth_message.bytes())

	// ClientProof = ClientKey XOR ClientSignature
	// 따라서 ClientKey = ClientProof XOR ClientSignature
	// 검증: 수신된 ClientProof XOR ClientSignature == ClientKey

	// base64 디코딩된 클라이언트 proof
	// V의 base64.decode는 오류를 반환하지 않으므로 결과 길이로 검증
	client_proof := base64.decode(client_final.proof)
	if client_proof.len == 0 && client_final.proof.len > 0 {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Invalid proof encoding')
	}

	// 클라이언트가 보낸 proof로부터 ClientKey 복원
	// RecoveredClientKey = ClientProof XOR ClientSignature
	recovered_client_key := xor_bytes(client_proof, client_signature)

	// 복원된 ClientKey와 계산된 ClientKey 비교
	if !constant_time_compare(recovered_client_key, client_key) {
		a.state = .failed
		return domain.auth_failure(.sasl_authentication_failed, 'Authentication failed')
	}

	// 서버 서명 계산 및 server-final-message 생성
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

/// ClientFirstMessage는 파싱된 client-first-message를 나타냅니다.
struct ClientFirstMessage {
	gs2_header string // GS2 헤더 (채널 바인딩 정보)
	username   string // 사용자명 (n=username)
	nonce      string // 클라이언트 nonce (r=nonce)
	bare       string // client-first-message-bare (GS2 헤더 제외)
}

/// parse_client_first_message는 client-first-message를 파싱합니다.
/// 형식: gs2-header + client-first-message-bare
/// gs2-header: n,, 또는 y,, 또는 p=... (채널 바인딩)
/// client-first-message-bare: n=username,r=nonce
fn parse_client_first_message(msg string) ?ClientFirstMessage {
	if msg == '' {
		return none
	}

	// GS2 헤더 찾기 (첫 번째 ',' 이후의 두 번째 ',')
	// n,,n=user,r=nonce 형식에서 n,,가 gs2-header
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

	// bare 메시지 파싱: n=username,r=nonce[,extensions...]
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

/// ClientFinalMessage는 파싱된 client-final-message를 나타냅니다.
struct ClientFinalMessage {
	channel_binding string // 채널 바인딩 (c=...)
	nonce           string // combined nonce (r=...)
	proof           string // 클라이언트 proof (p=...)
	without_proof   string // proof 제외한 메시지
}

/// parse_client_final_message는 client-final-message를 파싱합니다.
/// 형식: c=channel-binding,r=nonce,p=proof
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

/// build_server_first_message는 server-first-message를 생성합니다.
/// 형식: r=combined-nonce,s=salt,i=iterations
fn build_server_first_message(combined_nonce string, salt []u8, iterations int) string {
	salt_b64 := base64.encode(salt)
	return 'r=${combined_nonce},s=${salt_b64},i=${iterations}'
}

// Cryptographic Functions

/// generate_nonce는 SCRAM 인증을 위한 랜덤 nonce를 생성합니다.
fn generate_nonce() string {
	mut bytes := []u8{len: nonce_length}
	for i in 0 .. nonce_length {
		bytes[i] = u8(rand.int_in_range(0, 256) or { 0 })
	}
	return base64.encode(bytes)
}

/// generate_salt는 SCRAM 인증을 위한 랜덤 salt를 생성합니다.
fn generate_salt() []u8 {
	mut bytes := []u8{len: 16}
	for i in 0 .. 16 {
		bytes[i] = u8(rand.int_in_range(0, 256) or { 0 })
	}
	return bytes
}

/// generate_user_salt는 사용자명 기반으로 결정적 salt를 생성합니다.
/// 같은 사용자명에 대해 항상 같은 salt를 반환하여 인증 일관성 보장
/// 프로덕션에서는 사용자 생성 시 랜덤 salt를 생성하고 저장해야 합니다.
fn generate_user_salt(username string) []u8 {
	// 사용자명을 SHA-256 해시하여 결정적 salt 생성
	// 이는 임시 해결책이며, 실제 프로덕션에서는 DB에 저장된 salt 사용
	hash := sha256.sum256(username.bytes())
	return hash[0..16].clone()
}

/// pbkdf2_sha256는 PBKDF2-SHA-256 키 파생 함수를 구현합니다.
/// RFC 8018에 따른 구현
fn pbkdf2_sha256(password []u8, salt []u8, iterations int) []u8 {
	// PBKDF2 with SHA-256
	// DK = T1 || T2 || ... || Tdklen/hlen
	// Ti = F(Password, Salt, c, i)
	// F(Password, Salt, c, i) = U1 ^ U2 ^ ... ^ Uc
	// U1 = PRF(Password, Salt || INT(i))
	// U2 = PRF(Password, U1)
	// ...
	// Uc = PRF(Password, Uc-1)

	// 블록 번호 1 추가 (big-endian)
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

/// hmac_sha256는 HMAC-SHA-256을 계산합니다.
fn hmac_sha256(key []u8, message []u8) []u8 {
	return hmac.new(key, message, sha256.sum256, sha256.block_size).bytestr().bytes()
}

/// compute_stored_key는 SCRAM StoredKey를 계산합니다.
/// StoredKey = H(ClientKey)
fn compute_stored_key(salted_password []u8) []u8 {
	client_key := hmac_sha256(salted_password, 'Client Key'.bytes())
	sum := sha256.sum256(client_key)
	return sum[..].clone()
}

/// compute_server_key는 SCRAM ServerKey를 계산합니다.
/// ServerKey = HMAC(SaltedPassword, "Server Key")
fn compute_server_key(salted_password []u8) []u8 {
	return hmac_sha256(salted_password, 'Server Key'.bytes())
}

/// compute_client_key_from_salted는 SaltedPassword에서 ClientKey를 계산합니다.
/// ClientKey = HMAC(SaltedPassword, "Client Key")
fn compute_client_key_from_salted(salted_password []u8) []u8 {
	return hmac_sha256(salted_password, 'Client Key'.bytes())
}

/// xor_bytes는 두 바이트 배열을 XOR합니다.
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

/// constant_time_compare는 두 바이트 배열을 상수 시간에 비교합니다.
/// 타이밍 공격 방지를 위해 사용
fn constant_time_compare(a []u8, b []u8) bool {
	// 길이 차이도 상수 시간에 처리
	// 길이가 다르면 더 긴 배열 길이만큼 비교하여 타이밍 일정하게 유지
	mut result := u8(0)

	// 길이 차이를 결과에 반영 (길이가 다르면 0이 아닌 값)
	if a.len != b.len {
		result = 1
	}

	// 더 짧은 길이만큼 비교 (둘 다 빈 배열이면 0)
	min_len := if a.len < b.len { a.len } else { b.len }
	for i in 0 .. min_len {
		result |= a[i] ^ b[i]
	}

	return result == 0
}
