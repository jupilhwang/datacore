// 서비스 레이어 - SASL 인증 서비스
// SASL 인증 메커니즘을 관리하고 사용자 인증을 처리합니다.
// PLAIN, SCRAM 등 다양한 인증 방식을 지원합니다.
module auth

import domain
import service.port

/// AuthService는 브로커의 인증을 관리합니다.
/// 지원되는 SASL 메커니즘과 인증자를 관리합니다.
pub struct AuthService {
mut:
	user_store     port.UserStore                    // 사용자 저장소
	mechanisms     []domain.SaslMechanism            // 지원되는 SASL 메커니즘 목록
	authenticators map[string]port.SaslAuthenticator // 메커니즘별 인증자 맵
}

/// new_auth_service는 새로운 인증 서비스를 생성합니다.
/// 지정된 메커니즘에 대한 인증자를 초기화합니다.
pub fn new_auth_service(user_store port.UserStore, mechanisms []domain.SaslMechanism) &AuthService {
	mut authenticators := map[string]port.SaslAuthenticator{}

	// 각 메커니즘에 대한 인증자 생성
	for mech in mechanisms {
		match mech {
			.plain {
				authenticators[mech.str()] = new_plain_authenticator(user_store)
			}
			.scram_sha_256 {
				authenticators[mech.str()] = new_scram_sha256_authenticator(user_store)
			}
			else {
				// 다른 메커니즘은 추후 구현 예정 (SCRAM-SHA-512, OAUTHBEARER)
			}
		}
	}

	return &AuthService{
		user_store:     user_store
		mechanisms:     mechanisms
		authenticators: authenticators
	}
}

/// supported_mechanisms는 지원되는 SASL 메커니즘 목록을 반환합니다.
pub fn (s &AuthService) supported_mechanisms() []domain.SaslMechanism {
	return s.mechanisms
}

/// supported_mechanism_strings는 메커니즘 이름을 문자열 배열로 반환합니다.
pub fn (s &AuthService) supported_mechanism_strings() []string {
	mut result := []string{}
	for m in s.mechanisms {
		result << m.str()
	}
	return result
}

/// is_mechanism_supported는 지정된 메커니즘이 지원되는지 확인합니다.
pub fn (s &AuthService) is_mechanism_supported(mechanism string) bool {
	return mechanism.to_upper() in s.authenticators
}

/// get_authenticator는 지정된 메커니즘의 인증자를 반환합니다.
pub fn (mut s AuthService) get_authenticator(mechanism domain.SaslMechanism) !port.SaslAuthenticator {
	if auth := s.authenticators[mechanism.str()] {
		return auth
	}
	return error('unsupported mechanism: ${mechanism.str()}')
}

/// authenticate는 지정된 메커니즘을 사용하여 인증을 수행합니다.
pub fn (mut s AuthService) authenticate(mechanism domain.SaslMechanism, auth_bytes []u8) !domain.AuthResult {
	mut auth := s.get_authenticator(mechanism)!
	return auth.authenticate(auth_bytes)
}

/// PlainAuthenticator는 SASL PLAIN 인증을 구현합니다.
/// 사용자 이름과 비밀번호를 평문으로 전송하는 간단한 인증 방식입니다.
pub struct PlainAuthenticator {
mut:
	user_store port.UserStore // 사용자 저장소
}

/// new_plain_authenticator는 새로운 PLAIN 인증자를 생성합니다.
pub fn new_plain_authenticator(user_store port.UserStore) &PlainAuthenticator {
	return &PlainAuthenticator{
		user_store: user_store
	}
}

/// mechanism은 SASL 메커니즘 타입을 반환합니다.
pub fn (a &PlainAuthenticator) mechanism() domain.SaslMechanism {
	return .plain
}

/// authenticate는 PLAIN 인증을 수행합니다.
/// PLAIN 형식: [authzid]\0[authcid]\0[password]
/// authzid: 권한 부여 ID (선택사항, 보통 비어있음)
/// authcid: 인증 ID (사용자 이름)
/// password: 비밀번호
pub fn (mut a PlainAuthenticator) authenticate(auth_bytes []u8) !domain.AuthResult {
	// PLAIN 인증 데이터 파싱
	parts := parse_plain_auth(auth_bytes) or {
		return domain.auth_failure(.sasl_authentication_failed, 'invalid PLAIN format')
	}

	username := parts.username
	password := parts.password

	// 자격 증명 검증
	valid := a.user_store.validate_password(username, password) or {
		return domain.auth_failure(.sasl_authentication_failed, 'authentication failed')
	}

	if !valid {
		return domain.auth_failure(.sasl_authentication_failed, 'invalid credentials')
	}

	// 성공 시 principal과 함께 반환
	return domain.auth_success(domain.new_user_principal(username))
}

/// step은 PLAIN에서는 사용되지 않습니다 (단일 단계 인증).
pub fn (mut a PlainAuthenticator) step(response []u8) !domain.AuthResult {
	return error('PLAIN does not support multi-step authentication')
}

/// PlainAuthData는 파싱된 PLAIN 인증 데이터를 나타냅니다.
struct PlainAuthData {
	authzid  string // 권한 부여 ID (선택사항)
	username string // 인증 ID (사용자 이름)
	password string // 비밀번호
}

/// parse_plain_auth는 SASL PLAIN 인증 바이트를 파싱합니다.
/// 형식: [authzid]\0[authcid]\0[password]
fn parse_plain_auth(data []u8) ?PlainAuthData {
	if data.len == 0 {
		return none
	}

	// null 바이트 위치 찾기
	mut null_positions := []int{}
	for i, b in data {
		if b == 0 {
			null_positions << i
		}
	}

	// 정확히 2개의 null 바이트가 있어야 함
	if null_positions.len != 2 {
		return none
	}

	// 각 부분 추출
	authzid := data[0..null_positions[0]].bytestr()
	username := data[null_positions[0] + 1..null_positions[1]].bytestr()
	password := data[null_positions[1] + 1..].bytestr()

	// 사용자 이름과 비밀번호는 비어있으면 안 됨
	if username.len == 0 || password.len == 0 {
		return none
	}

	return PlainAuthData{
		authzid:  authzid
		username: username
		password: password
	}
}
