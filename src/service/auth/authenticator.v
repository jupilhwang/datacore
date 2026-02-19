// SASL 인증 메커니즘을 관리하고 사용자 인증을 처리합니다.
// PLAIN, SCRAM 등 다양한 인증 방식을 지원합니다.
module auth

import domain
import service.port
import infra.observability
import sync
import time

// 로깅 (Logging)

/// log_message는 구조화된 로그 메시지를 출력합니다.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	mut logger := observability.get_named_logger('auth.${component}')
	match level {
		.debug { logger.debug(message) }
		.info { logger.info(message) }
		.warn { logger.warn(message) }
		.error { logger.error(message) }
		else {}
	}
}

// 메트릭 (Metrics)

/// AuthMetrics는 인증 작업의 메트릭을 추적합니다.
pub struct AuthMetrics {
mut:
	// 인증 메트릭
	auth_attempts i64
	auth_success  i64
	auth_failures i64
	// 메커니즘별 메트릭
	mechanism_attempts map[string]i64
	mechanism_success  map[string]i64
	// 지연 시간 메트릭 (밀리초)
	auth_latency_sum   i64
	auth_latency_count i64
	// 락
	lock sync.Mutex
}

/// 새로운 AuthMetrics를 생성합니다.
pub fn new_auth_metrics() &AuthMetrics {
	return &AuthMetrics{
		mechanism_attempts: map[string]i64{}
		mechanism_success:  map[string]i64{}
	}
}

/// 인증 시도를 기록합니다.
pub fn (mut m AuthMetrics) record_auth_attempt(mechanism string, latency_ms i64, success bool) {
	m.lock.lock()
	defer { m.lock.unlock() }

	m.auth_attempts++
	m.auth_latency_sum += latency_ms
	m.auth_latency_count++

	// 메커니즘별 카운트
	if mechanism !in m.mechanism_attempts {
		m.mechanism_attempts[mechanism] = 0
		m.mechanism_success[mechanism] = 0
	}
	m.mechanism_attempts[mechanism]++

	if success {
		m.auth_success++
		m.mechanism_success[mechanism]++
	} else {
		m.auth_failures++
	}
}

/// 메트릭을 초기화합니다.
pub fn (mut m AuthMetrics) reset() {
	m.lock.lock()
	defer { m.lock.unlock() }

	m.auth_attempts = 0
	m.auth_success = 0
	m.auth_failures = 0
	m.mechanism_attempts.clear()
	m.mechanism_success.clear()
	m.auth_latency_sum = 0
	m.auth_latency_count = 0
}

/// 메트릭 요약을 문자열로 반환합니다.
pub fn (mut m AuthMetrics) get_summary() string {
	m.lock.lock()
	defer { m.lock.unlock() }

	mut result := '[Auth Metrics]\n'
	result += '  Total: ${m.auth_attempts} attempts, ${m.auth_success} success, ${m.auth_failures} failures'

	if m.auth_attempts > 0 {
		success_rate := (f64(m.auth_success) / f64(m.auth_attempts)) * 100.0
		result += ' (${success_rate:.1f}% success)\n'
	} else {
		result += '\n'
	}

	// 평균 지연 시간
	if m.auth_latency_count > 0 {
		avg_latency := f64(m.auth_latency_sum) / f64(m.auth_latency_count)
		result += '  Avg Latency: ${avg_latency:.2f}ms\n'
	}

	// 메커니즘별 통계
	if m.mechanism_attempts.len > 0 {
		result += '  By Mechanism:\n'
		for mech, count in m.mechanism_attempts {
			success := m.mechanism_success[mech] or { 0 }
			rate := if count > 0 { (f64(success) / f64(count)) * 100.0 } else { 0.0 }
			result += '    ${mech}: ${count} attempts, ${success} success (${rate:.1f}%)\n'
		}
	}

	return result
}

/// AuthService는 브로커의 인증을 관리합니다.
/// 지원되는 SASL 메커니즘과 인증자를 관리합니다.
pub struct AuthService {
mut:
	user_store     port.UserStore
	mechanisms     []domain.SaslMechanism
	authenticators map[string]port.SaslAuthenticator
	metrics        &AuthMetrics
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

	metrics := new_auth_metrics()

	return &AuthService{
		user_store:     user_store
		mechanisms:     mechanisms
		authenticators: authenticators
		metrics:        metrics
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
	start_time := time.now()
	mech_str := mechanism.str()

	mut auth := s.get_authenticator(mechanism) or {
		// 메트릭: 실패 기록
		elapsed_ms := time.since(start_time).milliseconds()
		s.metrics.record_auth_attempt(mech_str, elapsed_ms, false)
		log_message(.error, 'Auth', 'Unsupported mechanism', {
			'mechanism': mech_str
		})
		return err
	}

	result := auth.authenticate(auth_bytes) or {
		// 메트릭: 실패 기록
		elapsed_ms := time.since(start_time).milliseconds()
		s.metrics.record_auth_attempt(mech_str, elapsed_ms, false)
		log_message(.warn, 'Auth', 'Authentication failed', {
			'mechanism': mech_str
			'error':     err.msg()
		})
		return err
	}

	// 메트릭: 성공 기록
	elapsed_ms := time.since(start_time).milliseconds()
	s.metrics.record_auth_attempt(mech_str, elapsed_ms, true)

	log_message(.info, 'Auth', 'Authentication successful', {
		'mechanism': mech_str
		'user':      if principal := result.principal { principal.name } else { 'unknown' }
	})

	return result
}

/// PlainAuthenticator는 SASL PLAIN 인증을 구현합니다.
/// 사용자 이름과 비밀번호를 평문으로 전송하는 간단한 인증 방식입니다.
pub struct PlainAuthenticator {
mut:
	user_store port.UserStore
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
	authzid  string
	username string
	password string
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

// 메트릭 조회 (Metrics Query)

/// get_metrics_summary는 인증 메트릭 요약을 반환합니다.
pub fn (mut s AuthService) get_metrics_summary() string {
	return s.metrics.get_summary()
}

/// get_metrics는 인증 메트릭 구조체를 반환합니다.
pub fn (mut s AuthService) get_metrics() &AuthMetrics {
	return s.metrics
}

/// reset_metrics는 모든 인증 메트릭을 초기화합니다.
pub fn (mut s AuthService) reset_metrics() {
	s.metrics.reset()
}
