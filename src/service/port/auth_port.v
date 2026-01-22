// 서비스 레이어 - 인증 포트 인터페이스
// 유스케이스 레이어에서 정의하고 어댑터 레이어에서 구현하는 인증 관련 인터페이스입니다.
// SASL 인증 및 ACL 관리를 위한 추상화를 제공합니다.
module port

import domain

/// UserStore는 사용자 저장 및 인증을 위한 인터페이스입니다.
/// infra/auth에서 구현됩니다.
pub interface UserStore {
mut:
	/// 사용자명으로 사용자를 조회합니다.
	get_user(username string) !domain.User

	/// 새로운 사용자를 생성합니다.
	create_user(username string, password string, mechanism domain.SaslMechanism) !domain.User

	/// 사용자 비밀번호를 업데이트합니다.
	update_password(username string, new_password string) !

	/// 사용자를 삭제합니다.
	delete_user(username string) !

	/// 모든 사용자 목록을 반환합니다.
	list_users() ![]domain.User

	/// PLAIN 인증을 위한 비밀번호를 검증합니다.
	/// 반환값: 비밀번호가 일치하면 true
	validate_password(username string, password string) !bool
}

/// SaslAuthenticator는 SASL 인증을 위한 인터페이스입니다.
pub interface SaslAuthenticator {
	/// 지원하는 메커니즘을 반환합니다.
	mechanism() domain.SaslMechanism
mut:
	/// 제공된 인증 바이트로 인증을 수행합니다.
	/// PLAIN의 경우: auth_bytes는 [authzid]\0[authcid]\0[password] 형식
	/// 반환값: 성공 시 principal을 포함한 AuthResult
	authenticate(auth_bytes []u8) !domain.AuthResult

	/// SCRAM용: 챌린지-응답의 다음 단계를 처리합니다.
	/// 반환값: 챌린지 또는 최종 결과를 포함한 AuthResult
	step(response []u8) !domain.AuthResult
}

/// AuthManager는 연결에 대한 인증을 관리합니다.
pub interface AuthManager {
	/// 지원되는 SASL 메커니즘 목록을 반환합니다.
	supported_mechanisms() []domain.SaslMechanism

	/// 특정 메커니즘이 지원되는지 확인합니다.
	is_mechanism_supported(mechanism string) bool
mut:
	/// 특정 메커니즘에 대한 인증자를 반환합니다.
	get_authenticator(mechanism domain.SaslMechanism) !SaslAuthenticator

	/// 지정된 메커니즘을 사용하여 인증을 수행합니다.
	authenticate(mechanism domain.SaslMechanism, auth_bytes []u8) !domain.AuthResult
}

/// AclManager는 접근 제어 목록(ACL)을 관리합니다.
pub interface AclManager {
mut:
	/// ACL을 생성합니다.
	create_acls(acls []domain.AclBinding) ![]domain.AclCreateResult

	/// 필터와 일치하는 ACL을 삭제합니다.
	delete_acls(filters []domain.AclBindingFilter) ![]domain.AclDeleteResult

	/// 필터와 일치하는 ACL을 조회합니다.
	describe_acls(filter domain.AclBindingFilter) ![]domain.AclBinding

	/// 작업에 대한 권한을 확인합니다.
	/// 반환값: 권한이 있으면 true
	authorize(principal string, host string, operation domain.AclOperation, resource domain.ResourcePattern) !bool
}
