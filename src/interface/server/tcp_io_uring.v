/// Interface Layer - io_uring 기반 TCP 서버
/// Linux 5.1+ 에서 io_uring을 사용한 고성능 TCP 서버
///
/// 이 모듈은 Linux에서 io_uring을 사용하여 네트워크 I/O를 처리합니다.
/// 비-Linux 플랫폼에서는 자동으로 기존 net 모듈 기반 서버로 폴백됩니다.
///
/// 주요 기능:
/// - io_uring 기반 비동기 accept/recv/send
/// - 기존 RequestHandler 인터페이스와 호환
/// - 플랫폼 자동 감지 및 폴백
module server

import infra.performance.engines
import sync
import time

// io_uring 통합 서버

/// IoUringTcpServer는 io_uring 기반 TCP 서버입니다.
/// Linux에서는 io_uring을 사용하고, 다른 플랫폼에서는 폴백을 사용합니다.
pub struct IoUringTcpServer {
mut:
	config     ServerConfig   // 서버 설정
	state      ServerState    // 현재 서버 상태
	handler    RequestHandler // 요청 핸들러
	state_lock sync.Mutex     // 상태 변경 동기화용 뮤텍스
	// io_uring 관련 (Linux 전용)
	uring_server ?&engines.IoUringServer // io_uring 서버 인스턴스
	// 연결 정보 관리
	conn_info map[int]&IoUringConnInfo // fd -> 연결 정보
	// 통계
	metrics IoUringServerMetrics
}

/// IoUringConnInfo는 io_uring 서버의 연결 정보입니다.
struct IoUringConnInfo {
mut:
	fd             int       // 파일 디스크립터
	remote_addr    string    // 원격 주소
	connected_at   time.Time // 연결 시간
	last_active_at time.Time // 마지막 활동 시간
	request_count  u64       // 요청 수
	bytes_received u64       // 수신 바이트
	bytes_sent     u64       // 송신 바이트
	// 요청 파싱 상태
	recv_buf      []u8 // 수신 버퍼
	recv_offset   int  // 현재 버퍼 오프셋
	expected_size int  // 예상 요청 크기 (-1 = 미결정)
}

/// IoUringServerMetrics는 io_uring 서버 메트릭입니다.
pub struct IoUringServerMetrics {
pub mut:
	active_connections   int  // 현재 활성 연결 수
	total_connections    u64  // 총 연결 수 (누적)
	total_requests       u64  // 총 요청 수
	total_bytes_received u64  // 총 수신 바이트
	total_bytes_sent     u64  // 총 송신 바이트
	io_uring_enabled     bool // io_uring 활성화 여부
}

/// new_io_uring_tcp_server - creates an io_uring based TCP server
/// new_io_uring_tcp_server - creates an io_uring based TCP server
pub fn new_io_uring_tcp_server(config ServerConfig, handler RequestHandler) &IoUringTcpServer {
	return &IoUringTcpServer{
		config:    config
		state:     .stopped
		handler:   handler
		conn_info: map[int]&IoUringConnInfo{}
		metrics:   IoUringServerMetrics{}
	}
}

/// start - starts the io_uring TCP server
/// start - starts the io_uring TCP server
pub fn (mut s IoUringTcpServer) start() ! {
	s.state_lock.@lock()
	if s.state != .stopped {
		s.state_lock.unlock()
		return error('server is already running or stopping')
	}
	s.state = .starting
	s.state_lock.unlock()

	// io_uring 사용 가능 여부 확인
	$if linux {
		if s.config.use_io_uring && engines.is_io_uring_server_available() {
			s.start_io_uring_mode()!
			return
		}
	}

	// io_uring 사용 불가 - 에러 반환 (기존 서버 사용 권장)
	s.state_lock.@lock()
	s.state = .stopped
	s.state_lock.unlock()
	return error('io_uring not available, use standard Server instead')
}

/// start_io_uring_mode는 io_uring 모드로 서버를 시작합니다.
fn (mut s IoUringTcpServer) start_io_uring_mode() ! {
	$if linux {
		// io_uring 서버 설정
		uring_config := engines.IoUringServerConfig{
			host:             s.config.host
			port:             s.config.port
			queue_depth:      s.config.io_uring_queue_depth
			backlog:          128
			max_connections:  s.config.max_connections
			recv_buffer_size: 65536
			multi_accept:     8
			use_sqpoll:       s.config.io_uring_sqpoll
		}

		// io_uring 서버 생성 및 시작
		mut uring_server := engines.new_io_uring_server(uring_config)!
		uring_server.start()!

		s.uring_server = uring_server
		s.metrics.io_uring_enabled = true

		s.state_lock.@lock()
		s.state = .running
		s.state_lock.unlock()

		println('╔═══════════════════════════════════════════════════════════╗')
		println('║        DataCore Kafka-Compatible Broker (io_uring)       ║')
		println('╠═══════════════════════════════════════════════════════════╣')
		println('║  Listening: ${s.config.host}:${s.config.port}                              ║')
		println('║  Broker ID: ${s.config.broker_id}                                          ║')
		println('║  Mode: io_uring (Linux 5.1+)                              ║')
		println('║  Queue Depth: ${s.config.io_uring_queue_depth}                                        ║')
		println('╚═══════════════════════════════════════════════════════════╝')

		// 이벤트 루프 실행
		s.io_uring_event_loop()
	}
}

/// io_uring_event_loop는 io_uring 이벤트 루프입니다.
fn (mut s IoUringTcpServer) io_uring_event_loop() {
	$if linux {
		mut uring := s.uring_server or { return }

		for s.is_running() {
			// 이벤트 대기
			events := uring.wait() or {
				if s.is_running() {
					eprintln('[io_uring] wait error: ${err}')
				}
				continue
			}

			for event in events {
				match event.event_type {
					.accept {
						s.handle_io_uring_accept(event.fd)
					}
					.recv {
						s.handle_io_uring_recv(event.fd, event.data)
					}
					.send {
						// 송신 완료 처리 (추가 작업 불필요)
					}
					.close {
						s.handle_io_uring_close(event.fd)
					}
				}
			}

			// 제출
			uring.submit() or {}
		}
	}
}

/// handle_io_uring_accept는 새 연결을 처리합니다.
fn (mut s IoUringTcpServer) handle_io_uring_accept(client_fd int) {
	if client_fd < 0 {
		return
	}

	now := time.now()
	conn := &IoUringConnInfo{
		fd:             client_fd
		remote_addr:    'unknown' // io_uring에서는 주소 정보 별도 처리 필요
		connected_at:   now
		last_active_at: now
		recv_buf:       []u8{cap: 65536}
		expected_size:  -1
	}

	s.conn_info[client_fd] = conn
	s.metrics.active_connections = s.conn_info.len
	s.metrics.total_connections++

	println('[io_uring] New connection: fd=${client_fd}')
}

/// handle_io_uring_recv는 수신 데이터를 처리합니다.
fn (mut s IoUringTcpServer) handle_io_uring_recv(fd int, data []u8) {
	$if linux {
		mut uring := s.uring_server or { return }

		if data.len == 0 {
			// 연결 종료
			s.handle_io_uring_close(fd)
			return
		}

		mut conn := s.conn_info[fd] or {
			// 알 수 없는 연결
			uring.close_connection(fd)
			return
		}

		conn.last_active_at = time.now()
		conn.bytes_received += u64(data.len)
		s.metrics.total_bytes_received += u64(data.len)

		// 버퍼에 데이터 추가
		conn.recv_buf << data

		// 완전한 요청이 있는지 확인하고 처리
		s.process_recv_buffer(fd, mut conn, mut uring)

		// 다음 recv 준비
		uring.prepare_recv(fd)
	}
}

/// process_recv_buffer는 수신 버퍼에서 완전한 요청을 처리합니다.
fn (mut s IoUringTcpServer) process_recv_buffer(fd int, mut conn IoUringConnInfo, mut uring engines.IoUringServer) {
	for {
		// 요청 크기를 아직 모르는 경우
		if conn.expected_size < 0 {
			if conn.recv_buf.len < 4 {
				break // 아직 크기 헤더가 완전히 도착하지 않음
			}

			// 빅엔디안으로 요청 크기 읽기
			conn.expected_size = int(u32(conn.recv_buf[0]) << 24 | u32(conn.recv_buf[1]) << 16 | u32(conn.recv_buf[2]) << 8 | u32(conn.recv_buf[3]))

			if conn.expected_size <= 0 || conn.expected_size > s.config.max_request_size {
				eprintln('[io_uring] Invalid request size: ${conn.expected_size} from fd=${fd}')
				uring.close_connection(fd)
				s.conn_info.delete(fd)
				s.metrics.active_connections = s.conn_info.len
				return
			}
		}

		// 완전한 요청이 도착했는지 확인
		total_needed := 4 + conn.expected_size
		if conn.recv_buf.len < total_needed {
			break // 아직 완전한 요청이 아님
		}

		// 요청 데이터 추출
		request_data := conn.recv_buf[4..total_needed].clone()

		// 버퍼에서 처리된 데이터 제거
		conn.recv_buf = conn.recv_buf[total_needed..].clone()
		conn.expected_size = -1

		// 요청 처리
		conn.request_count++
		s.metrics.total_requests++

		response := s.handler.handle_request(request_data) or {
			eprintln('[io_uring] Error handling request from fd=${fd}: ${err}')
			// 최소 에러 응답 생성
			s.create_error_response(request_data)
		}

		// 응답 전송
		conn.bytes_sent += u64(response.len)
		s.metrics.total_bytes_sent += u64(response.len)
		uring.prepare_send(fd, response)
	}
}

/// create_error_response는 에러 응답을 생성합니다.
fn (s &IoUringTcpServer) create_error_response(request_data []u8) []u8 {
	if request_data.len < 8 {
		return []u8{}
	}

	correlation_id := i32(u32(request_data[4]) << 24 | u32(request_data[5]) << 16 | u32(request_data[6]) << 8 | u32(request_data[7]))

	mut error_resp := []u8{len: 8}
	error_resp[0] = 0
	error_resp[1] = 0
	error_resp[2] = 0
	error_resp[3] = 4 // size = 4 (correlation_id만)
	error_resp[4] = u8(correlation_id >> 24)
	error_resp[5] = u8(correlation_id >> 16)
	error_resp[6] = u8(correlation_id >> 8)
	error_resp[7] = u8(correlation_id)
	return error_resp
}

/// handle_io_uring_close는 연결 종료를 처리합니다.
fn (mut s IoUringTcpServer) handle_io_uring_close(fd int) {
	$if linux {
		if mut uring := s.uring_server {
			uring.close_connection(fd)
		}
	}

	if _ := s.conn_info[fd] {
		println('[io_uring] Connection closed: fd=${fd}')
		s.conn_info.delete(fd)
		s.metrics.active_connections = s.conn_info.len
	}
}

/// stop - stops the server
/// stop - stops the server
pub fn (mut s IoUringTcpServer) stop() {
	s.state_lock.@lock()
	if s.state != .running {
		s.state_lock.unlock()
		return
	}
	s.state = .stopping
	s.state_lock.unlock()

	println('\n[io_uring] Initiating graceful shutdown...')

	$if linux {
		if mut uring := s.uring_server {
			uring.stop()
		}
	}

	s.conn_info.clear()

	s.state_lock.@lock()
	s.state = .stopped
	s.state_lock.unlock()

	println('[io_uring] Server stopped')
	println('  Total connections: ${s.metrics.total_connections}')
	println('  Total requests: ${s.metrics.total_requests}')
	println('  Total bytes received: ${format_bytes(s.metrics.total_bytes_received)}')
	println('  Total bytes sent: ${format_bytes(s.metrics.total_bytes_sent)}')
}

/// is_running - checks if the server is running
/// is_running - checks if the server is running
pub fn (mut s IoUringTcpServer) is_running() bool {
	s.state_lock.@lock()
	defer { s.state_lock.unlock() }
	return s.state == .running
}

/// get_metrics - returns server metrics
/// get_metrics - returns server metrics
pub fn (s &IoUringTcpServer) get_metrics() IoUringServerMetrics {
	return s.metrics
}

// io_uring 사용 가능 여부 확인 함수

/// is_io_uring_available - checks if io_uring is available on the current platform
/// is_io_uring_available - checks if io_uring is available on the current platform
pub fn is_io_uring_available() bool {
	$if linux {
		return engines.is_io_uring_server_available()
	} $else {
		return false
	}
}

/// get_recommended_server_mode - returns the recommended server mode
/// get_recommended_server_mode - returns the recommended server mode
pub fn get_recommended_server_mode() string {
	$if linux {
		if engines.is_io_uring_server_available() {
			return 'io_uring'
		}
		return 'standard (io_uring not available)'
	} $else {
		return 'standard (non-Linux platform)'
	}
}
