/// Interface Layer - Connection Manager
/// 인터페이스 레이어 - 연결 관리자
///
/// 이 모듈은 클라이언트 연결을 스레드 안전하게 관리합니다.
/// 연결 수 제한, IP별 제한, 유휴 연결 정리 등의 기능을 제공합니다.
///
/// 주요 기능:
/// - 스레드 안전한 연결 등록/해제
/// - 최대 연결 수 및 IP별 연결 수 제한
/// - 유휴 연결 자동 정리
/// - 연결 메트릭 수집
/// - 인증 상태 관리
module server

import domain
import net
import sync
import time

/// ClientConnection은 클라이언트 연결 정보를 담는 구조체입니다.
/// 연결 상태, 통계, 인증 정보 등을 포함합니다.
pub struct ClientConnection {
pub mut:
	fd             int       // 파일 디스크립터
	remote_addr    string    // 원격 주소
	connected_at   time.Time // 연결 시간
	last_active_at time.Time // 마지막 활동 시간
	request_count  u64       // 요청 수
	bytes_received u64       // 수신 바이트
	bytes_sent     u64       // 송신 바이트
	client_id      string    // 클라이언트 ID
	api_version    i16       // 클라이언트 선호 API 버전
	client_sw_name string    // 클라이언트 소프트웨어 이름
	client_sw_ver  string    // 클라이언트 소프트웨어 버전
	// 인증 상태
	auth_state     domain.AuthState  // 현재 인증 상태
	principal      ?domain.Principal // 인증된 주체 (있는 경우)
	sasl_mechanism ?string           // 사용 중인 SASL 메커니즘
}

/// ConnectionMetrics는 연결 통계를 추적하는 구조체입니다.
pub struct ConnectionMetrics {
pub mut:
	active_connections   int // 현재 활성 연결 수
	total_connections    u64 // 총 연결 수 (누적)
	rejected_connections u64 // 거부된 연결 수
	rejected_max_total   u64 // 거부됨: 최대 연결 수 도달
	rejected_max_per_ip  u64 // 거부됨: IP별 최대 연결 수 도달
	total_bytes_received u64 // 총 수신 바이트
	total_bytes_sent     u64 // 총 송신 바이트
	total_requests       u64 // 총 처리된 요청 수
}

/// ConnectionManager는 스레드 안전하게 클라이언트 연결을 관리합니다.
/// RwMutex를 사용하여 동시 읽기와 배타적 쓰기를 지원합니다.
pub struct ConnectionManager {
mut:
	connections map[int]&ClientConnection // fd를 키로 하는 연결 맵
	config      ServerConfig              // 서버 설정 참조
	ip_counts   map[string]int            // IP별 연결 수 추적
	metrics     ConnectionMetrics         // 연결 통계
	lock        sync.RwMutex              // connections와 ip_counts 보호용 읽기-쓰기 락
}

/// new_connection_manager는 새로운 연결 관리자를 생성합니다.
pub fn new_connection_manager(config ServerConfig) &ConnectionManager {
	return &ConnectionManager{
		config:      config
		connections: map[int]&ClientConnection{}
		ip_counts:   map[string]int{}
		metrics:     ConnectionMetrics{}
	}
}

/// accept는 새로운 연결을 수락합니다 (스레드 안전).
/// 최대 연결 수 또는 IP별 제한에 도달하면 에러를 반환합니다.
pub fn (mut cm ConnectionManager) accept(mut conn net.TcpConn) !&ClientConnection {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	// 최대 연결 수 제한 확인
	if cm.connections.len >= cm.config.max_connections {
		cm.metrics.rejected_connections += 1
		cm.metrics.rejected_max_total += 1
		conn.close() or {}
		return error('max connections reached: ${cm.connections.len}/${cm.config.max_connections}')
	}

	// 피어 주소 가져오기
	addr := conn.peer_addr() or { return error('cannot get peer address') }
	ip := extract_ip(addr.str())

	// IP별 연결 제한 확인
	ip_count := cm.ip_counts[ip] or { 0 }
	if ip_count >= cm.config.max_connections_per_ip {
		cm.metrics.rejected_connections += 1
		cm.metrics.rejected_max_per_ip += 1
		conn.close() or {}
		return error('max connections per IP reached: ${ip} has ${ip_count}/${cm.config.max_connections_per_ip}')
	}

	// 클라이언트 연결 생성
	fd := conn.sock.handle
	now := time.now()
	client := &ClientConnection{
		fd:             fd
		remote_addr:    addr.str()
		connected_at:   now
		last_active_at: now
	}

	// 연결 등록
	cm.connections[fd] = client
	cm.ip_counts[ip] = ip_count + 1
	cm.metrics.active_connections = cm.connections.len
	cm.metrics.total_connections += 1

	return client
}

/// close는 연결을 닫습니다 (스레드 안전).
pub fn (mut cm ConnectionManager) close(fd int) {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	cm.close_internal(fd)
}

/// close_internal은 락 없이 연결을 닫습니다.
/// 주의: 이 함수는 락이 이미 획득된 상태에서만 호출해야 합니다.
/// 내부 헬퍼 함수로, 외부에서 직접 호출하면 안 됩니다.
fn (mut cm ConnectionManager) close_internal(fd int) {
	if client := cm.connections[fd] {
		// 제거 전 총 통계 업데이트
		cm.metrics.total_bytes_received += client.bytes_received
		cm.metrics.total_bytes_sent += client.bytes_sent
		cm.metrics.total_requests += client.request_count

		// IP 카운트 감소
		ip := extract_ip(client.remote_addr)
		if count := cm.ip_counts[ip] {
			if count > 1 {
				cm.ip_counts[ip] = count - 1
			} else {
				cm.ip_counts.delete(ip)
			}
		}

		cm.connections.delete(fd)
		cm.metrics.active_connections = cm.connections.len
	}
}

/// cleanup_idle는 유휴 연결을 제거합니다 (스레드 안전).
/// 제거된 연결 수를 반환합니다.
pub fn (mut cm ConnectionManager) cleanup_idle() int {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	now := time.now()
	mut to_close := []int{}

	for fd, client in cm.connections {
		idle_ms := (now - client.last_active_at).milliseconds()
		if idle_ms > cm.config.idle_timeout_ms {
			to_close << fd
		}
	}

	for fd in to_close {
		cm.close_internal(fd)
	}

	return to_close.len
}

/// get_connection은 fd로 연결을 반환합니다 (스레드 안전, 읽기 전용).
pub fn (mut cm ConnectionManager) get_connection(fd int) ?&ClientConnection {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	return cm.connections[fd] or { return none }
}

/// update_client_info는 클라이언트 메타데이터를 업데이트합니다 (스레드 안전).
pub fn (mut cm ConnectionManager) update_client_info(fd int, client_id string, sw_name string, sw_version string) {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	if mut client := cm.connections[fd] {
		client.client_id = client_id
		client.client_sw_name = sw_name
		client.client_sw_ver = sw_version
	}
}

/// active_count는 활성 연결 수를 반환합니다 (스레드 안전).
pub fn (mut cm ConnectionManager) active_count() int {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	return cm.connections.len
}

/// get_metrics는 연결 메트릭의 스냅샷을 반환합니다 (스레드 안전).
pub fn (mut cm ConnectionManager) get_metrics() ConnectionMetrics {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	return cm.metrics
}

/// get_all_connections는 모든 활성 연결을 반환합니다 (스레드 안전).
pub fn (mut cm ConnectionManager) get_all_connections() []ClientConnection {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	mut result := []ClientConnection{cap: cm.connections.len}
	for _, client in cm.connections {
		result << *client
	}
	return result
}

/// close_all은 모든 연결을 닫습니다 (종료 시 사용).
/// 닫힌 연결 수를 반환합니다.
pub fn (mut cm ConnectionManager) close_all() int {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	count := cm.connections.len

	for fd, _ in cm.connections {
		cm.close_internal(fd)
	}

	return count
}

/// extract_ip는 주소 문자열에서 IP를 추출합니다.
/// IPv4 (예: 127.0.0.1:9092) 및 IPv6 (예: [::1]:9092) 형식을 지원합니다.
fn extract_ip(addr string) string {
	// IPv6 주소 처리 (예: [::1]:9092 -> ::1)
	if addr.starts_with('[') {
		if end := addr.index(']:') {
			return addr[1..end]
		}
	}
	// IPv4 주소 처리 (예: 127.0.0.1:9092 -> 127.0.0.1)
	parts := addr.split(':')
	if parts.len >= 1 {
		return parts[0]
	}
	return addr
}

// ============================================================================
// 인증 헬퍼 메서드
// ============================================================================

/// is_authenticated는 연결이 인증되었는지 확인합니다.
pub fn (c &ClientConnection) is_authenticated() bool {
	return c.auth_state == .authenticated
}

/// set_authenticated는 연결을 주어진 주체로 인증된 상태로 설정합니다.
pub fn (mut c ClientConnection) set_authenticated(principal domain.Principal) {
	c.auth_state = .authenticated
	c.principal = principal
}

/// set_handshake_complete는 SASL 핸드셰이크가 완료되었음을 표시합니다.
pub fn (mut c ClientConnection) set_handshake_complete(mechanism string) {
	c.auth_state = .handshake_complete
	c.sasl_mechanism = mechanism
}

/// reset_auth는 인증 상태를 초기화합니다.
pub fn (mut c ClientConnection) reset_auth() {
	c.auth_state = .initial
	c.principal = none
	c.sasl_mechanism = none
}

/// get_principal은 인증된 주체를 반환합니다.
pub fn (c &ClientConnection) get_principal() ?domain.Principal {
	return c.principal
}

/// requires_auth는 연결에 인증이 필요한지 확인합니다.
/// 요청 처리 전 SASL이 필요한지 판단하는 데 사용됩니다.
pub fn (c &ClientConnection) requires_auth(auth_required bool) bool {
	if !auth_required {
		return false
	}
	return c.auth_state != .authenticated
}
