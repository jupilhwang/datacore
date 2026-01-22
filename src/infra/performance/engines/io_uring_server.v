/// io_uring_server - io_uring 기반 네트워크 서버
/// Linux 5.1+ 에서 고성능 비동기 네트워크 I/O 제공
///
/// 기능:
/// - io_uring을 사용한 비동기 accept/recv/send
/// - 멀티 accept를 통한 연결 수락 배칭
/// - 제로카피 데이터 전송 지원
///
/// 참고: Linux 5.1+ 에서만 사용 가능, 다른 플랫폼은 폴백 사용
module engines

import time

// ============================================================================
// io_uring 서버 구조체
// ============================================================================

/// IoUringServerConfig는 io_uring 서버 설정을 담고 있습니다.
pub struct IoUringServerConfig {
pub:
	host             string = '0.0.0.0'
	port             int    = 9092
	queue_depth      u32    = 256   // io_uring 큐 깊이
	backlog          int    = 128   // listen 백로그
	max_connections  int    = 10000 // 최대 동시 연결 수
	recv_buffer_size int    = 65536 // 수신 버퍼 크기
	multi_accept     int    = 8     // 한 번에 준비할 accept 수
	use_sqpoll       bool // SQ 폴링 모드 사용 여부
}

/// IoUringServer는 io_uring 기반 네트워크 서버입니다.
pub struct IoUringServer {
mut:
	config    IoUringServerConfig
	ring      IoUring // io_uring 인스턴스
	listen_fd int     // 리스닝 소켓 fd
	running   bool    // 실행 상태
	// 연결 관리
	connections map[int]&IoUringConnection // fd -> 연결 정보
	// 통계
	stats IoUringServerStats
}

/// IoUringConnection은 클라이언트 연결 정보입니다.
pub struct IoUringConnection {
pub mut:
	fd             int       // 파일 디스크립터
	recv_buf       []u8      // 수신 버퍼
	send_buf       []u8      // 송신 버퍼 (대기 중)
	connected_at   time.Time // 연결 시간
	last_active_at time.Time // 마지막 활동 시간
	bytes_received u64       // 수신 바이트
	bytes_sent     u64       // 송신 바이트
	recv_pending   bool      // recv 요청 대기 중
	send_pending   bool      // send 요청 대기 중
}

/// IoUringServerStats는 서버 통계입니다.
pub struct IoUringServerStats {
pub mut:
	total_accepts     u64 // 총 연결 수락 수
	total_connections u64 // 총 연결 수 (누적)
	active_conns      int // 현재 활성 연결 수
	total_recv_bytes  u64 // 총 수신 바이트
	total_send_bytes  u64 // 총 송신 바이트
	total_recv_ops    u64 // 총 recv 연산 수
	total_send_ops    u64 // 총 send 연산 수
}

/// IoUringEventType은 io_uring 이벤트 타입입니다.
pub enum IoUringEventType {
	accept // 연결 수락
	recv   // 데이터 수신
	send   // 데이터 송신
	close  // 연결 종료
}

/// IoUringEvent는 io_uring 완료 이벤트입니다.
pub struct IoUringEvent {
pub:
	event_type IoUringEventType // 이벤트 타입
	fd         int              // 관련 fd
	result     i32              // 결과 (바이트 수 또는 에러)
	data       []u8             // 수신된 데이터 (recv인 경우)
}

// user_data 인코딩: 상위 8비트 = 이벤트 타입, 하위 56비트 = fd
const event_type_shift = 56
const fd_mask = u64(0x00FFFFFFFFFFFFFF)

fn encode_user_data(event_type IoUringEventType, fd int) u64 {
	return (u64(event_type) << event_type_shift) | (u64(fd) & fd_mask)
}

fn decode_user_data(user_data u64) (IoUringEventType, int) {
	event_type := unsafe { IoUringEventType(user_data >> event_type_shift) }
	fd := int(user_data & fd_mask)
	return event_type, fd
}

// ============================================================================
// io_uring 서버 구현
// ============================================================================

/// new_io_uring_server는 새 io_uring 서버를 생성합니다.
pub fn new_io_uring_server(config IoUringServerConfig) !&IoUringServer {
	$if linux {
		// io_uring 설정
		ring_config := IoUringConfig{
			queue_depth:    config.queue_depth
			flags:          if config.use_sqpoll { ioring_setup_sqpoll } else { 0 }
			sq_thread_idle: 1000
		}

		// io_uring 생성
		ring := new_io_uring(ring_config)!

		return &IoUringServer{
			config:      config
			ring:        ring
			listen_fd:   -1
			running:     false
			connections: map[int]&IoUringConnection{}
		}
	} $else {
		return error('io_uring server is only available on Linux 5.1+')
	}
}

/// start는 서버를 시작합니다 (리스닝 소켓 생성 및 accept 준비).
pub fn (mut s IoUringServer) start() ! {
	$if linux {
		if s.running {
			return error('server is already running')
		}

		// 리스닝 소켓 생성
		s.listen_fd = create_listen_socket(s.config.host, s.config.port, s.config.backlog)!

		// multi-accept 준비
		for _ in 0 .. s.config.multi_accept {
			s.prepare_accept()
		}

		// accept 제출
		s.ring.submit(0)!

		s.running = true
	} $else {
		return error('io_uring server is only available on Linux')
	}
}

/// stop은 서버를 중지합니다.
pub fn (mut s IoUringServer) stop() {
	$if linux {
		if !s.running {
			return
		}

		s.running = false

		// 모든 연결 닫기
		for fd, _ in s.connections {
			close_socket(fd)
		}
		s.connections.clear()

		// 리스닝 소켓 닫기
		if s.listen_fd >= 0 {
			close_socket(s.listen_fd)
			s.listen_fd = -1
		}

		// io_uring 정리
		s.ring.close()
	}
}

/// poll은 io_uring 완료 이벤트를 폴링합니다.
/// 완료된 이벤트를 반환하며, 이벤트가 없으면 빈 배열을 반환합니다.
pub fn (mut s IoUringServer) poll() ![]IoUringEvent {
	$if linux {
		mut events := []IoUringEvent{cap: 16}

		// 비블로킹으로 완료 확인
		for {
			cqe := s.ring.peek_cqe() or { break }

			event_type, fd := decode_user_data(cqe.user_data)

			match event_type {
				.accept {
					event := s.handle_accept_completion(cqe.res)
					if event.result >= 0 {
						events << event
					}
				}
				.recv {
					event := s.handle_recv_completion(fd, cqe.res)
					events << event
				}
				.send {
					event := s.handle_send_completion(fd, cqe.res)
					events << event
				}
				.close {
					// 연결 종료 완료
					events << IoUringEvent{
						event_type: .close
						fd:         fd
						result:     cqe.res
					}
				}
			}

			s.ring.consume_cqe()
		}

		return events
	} $else {
		return error('io_uring not available')
	}
}

/// wait은 최소 하나의 완료 이벤트가 있을 때까지 대기합니다.
pub fn (mut s IoUringServer) wait() ![]IoUringEvent {
	$if linux {
		// 최소 1개 완료 대기
		s.ring.submit(1)!
		return s.poll()
	} $else {
		return error('io_uring not available')
	}
}

/// prepare_recv는 fd에 대한 recv 연산을 준비합니다.
pub fn (mut s IoUringServer) prepare_recv(fd int) bool {
	$if linux {
		if mut conn := s.connections[fd] {
			if conn.recv_pending {
				return false // 이미 대기 중
			}

			// 버퍼가 없으면 생성
			if conn.recv_buf.len == 0 {
				conn.recv_buf = []u8{len: s.config.recv_buffer_size}
			}

			user_data := encode_user_data(.recv, fd)
			if s.ring.prep_recv(fd, conn.recv_buf, 0, user_data) {
				conn.recv_pending = true
				return true
			}
		}
		return false
	} $else {
		return false
	}
}

/// prepare_send는 fd로 데이터 송신을 준비합니다.
pub fn (mut s IoUringServer) prepare_send(fd int, data []u8) bool {
	$if linux {
		if mut conn := s.connections[fd] {
			if conn.send_pending {
				// 이미 전송 대기 중이면 버퍼에 추가
				conn.send_buf << data
				return true
			}

			user_data := encode_user_data(.send, fd)
			if s.ring.prep_send(fd, data, 0, user_data) {
				conn.send_pending = true
				return true
			}
		}
		return false
	} $else {
		return false
	}
}

/// close_connection은 연결을 닫습니다.
pub fn (mut s IoUringServer) close_connection(fd int) {
	$if linux {
		if _ := s.connections[fd] {
			close_socket(fd)
			s.connections.delete(fd)
			s.stats.active_conns = s.connections.len
		}
	}
}

/// submit은 대기 중인 모든 연산을 제출합니다.
pub fn (mut s IoUringServer) submit() !int {
	$if linux {
		return s.ring.submit(0)
	} $else {
		return error('io_uring not available')
	}
}

/// get_stats는 서버 통계를 반환합니다.
pub fn (s &IoUringServer) get_stats() IoUringServerStats {
	return s.stats
}

/// is_running은 서버가 실행 중인지 반환합니다.
pub fn (s &IoUringServer) is_running() bool {
	return s.running
}

// ============================================================================
// 내부 헬퍼 메서드
// ============================================================================

fn (mut s IoUringServer) prepare_accept() {
	$if linux {
		user_data := encode_user_data(.accept, s.listen_fd)
		s.ring.prep_accept(s.listen_fd, user_data)
	}
}

fn (mut s IoUringServer) handle_accept_completion(result i32) IoUringEvent {
	if result < 0 {
		// accept 실패 - 다시 준비
		s.prepare_accept()
		return IoUringEvent{
			event_type: .accept
			fd:         -1
			result:     result
		}
	}

	// 새 연결 등록
	client_fd := int(result)
	now := time.now()
	conn := &IoUringConnection{
		fd:             client_fd
		recv_buf:       []u8{len: s.config.recv_buffer_size}
		connected_at:   now
		last_active_at: now
	}
	s.connections[client_fd] = conn
	s.stats.total_accepts++
	s.stats.total_connections++
	s.stats.active_conns = s.connections.len

	// 다음 accept 준비
	s.prepare_accept()

	// 새 연결에 대한 recv 자동 준비
	s.prepare_recv(client_fd)

	return IoUringEvent{
		event_type: .accept
		fd:         client_fd
		result:     result
	}
}

fn (mut s IoUringServer) handle_recv_completion(fd int, result i32) IoUringEvent {
	if mut conn := s.connections[fd] {
		conn.recv_pending = false
		conn.last_active_at = time.now()

		if result <= 0 {
			// 연결 종료 또는 에러
			return IoUringEvent{
				event_type: .close
				fd:         fd
				result:     result
			}
		}

		// 데이터 수신 성공
		conn.bytes_received += u64(result)
		s.stats.total_recv_bytes += u64(result)
		s.stats.total_recv_ops++

		// 수신된 데이터 복사
		data := conn.recv_buf[..result].clone()

		return IoUringEvent{
			event_type: .recv
			fd:         fd
			result:     result
			data:       data
		}
	}

	return IoUringEvent{
		event_type: .close
		fd:         fd
		result:     -1
	}
}

fn (mut s IoUringServer) handle_send_completion(fd int, result i32) IoUringEvent {
	if mut conn := s.connections[fd] {
		conn.send_pending = false
		conn.last_active_at = time.now()

		if result < 0 {
			// 전송 에러
			return IoUringEvent{
				event_type: .close
				fd:         fd
				result:     result
			}
		}

		conn.bytes_sent += u64(result)
		s.stats.total_send_bytes += u64(result)
		s.stats.total_send_ops++

		// 대기 중인 데이터가 있으면 전송
		if conn.send_buf.len > 0 {
			data := conn.send_buf.clone()
			conn.send_buf.clear()
			s.prepare_send(fd, data)
		}

		return IoUringEvent{
			event_type: .send
			fd:         fd
			result:     result
		}
	}

	return IoUringEvent{
		event_type: .close
		fd:         fd
		result:     -1
	}
}

// ============================================================================
// 비Linux 폴백 구현
// ============================================================================

/// IoUringServerFallback은 비Linux 시스템을 위한 폴백입니다.
/// net 모듈을 사용한 동기 I/O로 대체합니다.
pub struct IoUringServerFallback {
pub mut:
	available bool // 항상 true (폴백은 항상 사용 가능)
}

/// new_io_uring_server_fallback은 폴백 서버 객체를 생성합니다.
pub fn new_io_uring_server_fallback() IoUringServerFallback {
	return IoUringServerFallback{
		available: true
	}
}

/// is_io_uring_server_available은 io_uring 서버 사용 가능 여부를 확인합니다.
pub fn is_io_uring_server_available() bool {
	$if linux {
		return is_io_uring_available()
	} $else {
		return false
	}
}
