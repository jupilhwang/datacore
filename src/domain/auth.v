// 도메인 레이어 - 인증 도메인 모델
// SASL 인증 및 사용자 관리 관련 데이터 구조를 정의합니다.
module domain

/// SaslMechanism은 지원하는 SASL 인증 메커니즘을 나타냅니다.
/// plain: PLAIN - 사용자명/비밀번호 (TLS 권장)
/// scram_sha_256: SCRAM-SHA-256 - Challenge-Response
/// scram_sha_512: SCRAM-SHA-512 - Challenge-Response
/// oauthbearer: OAUTHBEARER - OAuth 2.0 토큰
pub enum SaslMechanism {
	plain         // PLAIN - 사용자명/비밀번호 (TLS 권장)
	scram_sha_256 // SCRAM-SHA-256 - Challenge-Response
	scram_sha_512 // SCRAM-SHA-512 - Challenge-Response
	oauthbearer   // OAUTHBEARER - OAuth 2.0 토큰
}

/// str은 SASL 메커니즘을 문자열로 변환합니다.
pub fn (m SaslMechanism) str() string {
	return match m {
		.plain { 'PLAIN' }
		.scram_sha_256 { 'SCRAM-SHA-256' }
		.scram_sha_512 { 'SCRAM-SHA-512' }
		.oauthbearer { 'OAUTHBEARER' }
	}
}

/// sasl_mechanism_from_str은 문자열을 SaslMechanism으로 파싱합니다.
pub fn sasl_mechanism_from_str(s string) ?SaslMechanism {
	return match s.to_upper() {
		'PLAIN' { SaslMechanism.plain }
		'SCRAM-SHA-256' { SaslMechanism.scram_sha_256 }
		'SCRAM-SHA-512' { SaslMechanism.scram_sha_512 }
		'OAUTHBEARER' { SaslMechanism.oauthbearer }
		else { none }
	}
}

/// Principal은 인증된 사용자 ID를 나타냅니다.
/// name: 사용자명 (예: "alice")
/// principal_type: 주체 유형 ("User", "ServiceAccount" 등)
pub struct Principal {
pub:
	name           string // 사용자명 (예: "alice")
	principal_type string // "User", "ServiceAccount" 등
}

// 인증되지 않은 연결을 위한 익명 주체
pub const anonymous_principal = Principal{
	name:           'ANONYMOUS'
	principal_type: 'User'
}

/// new_user_principal은 새로운 사용자 주체를 생성합니다.
pub fn new_user_principal(name string) Principal {
	return Principal{
		name:           name
		principal_type: 'User'
	}
}

/// AuthState는 연결의 현재 인증 상태를 나타냅니다.
/// initial: 인증이 시작되지 않음
/// handshake_complete: SASL 핸드셰이크 완료, 인증 대기
/// authenticated: 인증 성공
/// failed: 인증 실패
pub enum AuthState {
	initial            // 인증이 시작되지 않음
	handshake_complete // SASL 핸드셰이크 완료, 인증 대기
	authenticated      // 인증 성공
	failed             // 인증 실패
}

/// User는 인증을 위해 저장된 사용자를 나타냅니다.
/// username: 사용자명
/// password_hash: 비밀번호 해시 (PLAIN의 경우 평문으로 저장, 운영 환경에서는 해싱 필요)
/// mechanism: SASL 메커니즘
/// created_at: 생성 시간 (Unix 타임스탬프)
/// updated_at: 수정 시간 (Unix 타임스탬프)
pub struct User {
pub:
	username      string
	password_hash string // PLAIN의 경우: 평문 저장 (운영 환경에서는 해싱 필요)
	mechanism     SaslMechanism
	created_at    i64 // Unix 타임스탬프
	updated_at    i64 // Unix 타임스탬프
}

/// ScramCredentials는 SCRAM 인증을 위한 자격 증명입니다 (P2)
pub struct ScramCredentials {
pub:
	username   string
	salt       []u8
	iterations int
	stored_key []u8
	server_key []u8
}

/// AuthResult는 인증 단계의 결과를 나타냅니다.
/// complete: 인증이 완료되면 true
/// challenge: 클라이언트를 위한 다음 challenge (SCRAM용)
/// principal: complete=true이고 성공한 경우 설정됨
/// error_code: 실패한 경우 에러 코드
/// error_message: 사람이 읽을 수 있는 에러 메시지
pub struct AuthResult {
pub:
	complete      bool       // 인증이 완료되면 true
	challenge     []u8       // 클라이언트를 위한 다음 challenge (SCRAM)
	principal     ?Principal // complete=true이고 성공한 경우 설정됨
	error_code    ErrorCode  // 실패한 경우 에러 코드
	error_message string     // 사람이 읽을 수 있는 에러 메시지
}

/// auth_success는 성공적인 인증 결과를 생성합니다.
pub fn auth_success(principal Principal) AuthResult {
	return AuthResult{
		complete:   true
		principal:  principal
		error_code: .none
	}
}

/// auth_failure는 실패한 인증 결과를 생성합니다.
pub fn auth_failure(code ErrorCode, message string) AuthResult {
	return AuthResult{
		complete:      true
		error_code:    code
		error_message: message
	}
}

/// AuthError는 인증 관련 에러를 나타냅니다.
pub struct AuthError {
pub:
	code    ErrorCode
	message string
}

pub fn (e AuthError) msg() string {
	return e.message
}
