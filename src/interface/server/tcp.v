/// Interface Layer - TCP Server
/// 인터페이스 레이어 - TCP 서버
///
/// 이 모듈은 Kafka 호환 TCP 서버를 구현합니다.
/// V 코루틴을 활용한 논블로킹 I/O 기반으로 동작하며,
/// 고성능 동시 연결 처리를 지원합니다.
///
/// 주요 기능:
/// - 논블로킹 I/O 기반 TCP 연결 수락
/// - 워커 풀을 통한 동시 연결 핸들러 제한
/// - 요청 파이프라이닝 지원
/// - 유휴 연결 자동 정리
/// - 우아한 종료(Graceful Shutdown) 지원
module server

import net
import sync
import time
import infra.observability

// 로깅 (Logging)

/// log_message는 구조화된 로그 메시지를 출력합니다.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	mut logger := observability.get_named_logger('tcp.${component}')
	match level {
		.debug { logger.debug(message) }
		.info { logger.info(message) }
		.warn { logger.warn(message) }
		.error { logger.error(message) }
		else {}
	}
}

/// ServerConfig는 서버 설정을 담는 구조체입니다.
/// TCP 서버의 동작을 제어하는 다양한 옵션을 포함합니다.
pub struct ServerConfig {
pub:
	host                   string = '0.0.0.0'
	port                   int    = 9092
	broker_id              int    = 1
	cluster_id             string = 'datacore-cluster'
	max_connections        int    = 10000     // 최대 동시 연결 수
	max_connections_per_ip int    = 100       // IP당 최대 연결 수
	idle_timeout_ms        int    = 600000    // 유휴 타임아웃 (10분)
	request_timeout_ms     int    = 30000     // 요청 타임아웃 (30초)
	max_request_size       int    = 104857600 // 최대 요청 크기 (100MB)
	max_pending_requests   int    = 100       // 요청 파이프라이닝 제한
	shutdown_timeout_ms    int    = 30000     // 우아한 종료 타임아웃
	// 워커 풀 설정 (v0.28.0)
	max_concurrent_handlers int = 1000 // 최대 동시 연결 핸들러 수
	handler_acquire_timeout int = 5000 // 핸들러 슬롯 획득 타임아웃 (ms)
	// io_uring 설정 (v0.32.0)
	use_io_uring         bool = true // io_uring 사용 여부 (Linux 전용)
	io_uring_queue_depth u32  = 256  // io_uring 큐 깊이
	io_uring_sqpoll      bool // SQ 폴링 모드 사용 여부 (기본 false)
	// NUMA 설정 (v0.33.0)
	numa_enabled      bool // NUMA 인식 모드 활성화 (Linux 전용, 기본 false)
	numa_bind_workers bool = true // 워커를 NUMA 노드에 바인딩
	// TCP 최적화 설정 (v0.42.0)
	tcp_nodelay       bool = true   // TCP_NODELAY 활성화 (Nagle 알고리즘 비활성화)
	tcp_send_buf_size int  = 262144 // TCP 송신 버퍼 크기 (256KB)
	tcp_recv_buf_size int  = 262144 // TCP 수신 버퍼 크기 (256KB)
}

/// RequestHandler는 프로토콜 요청을 처리하는 인터페이스입니다.
/// Kafka 프로토콜 요청을 받아 응답을 생성합니다.
pub interface RequestHandler {
mut:
	handle_request(data []u8) ![]u8
}

/// ServerState는 서버의 현재 상태를 나타내는 열거형입니다.
pub enum ServerState {
	stopped  // 정지됨
	starting // 시작 중
	running  // 실행 중
	stopping // 종료 중
}

/// Server는 논블로킹 I/O 기반 TCP 서버입니다.
/// Kafka 호환 프로토콜을 처리하며, 워커 풀을 통해
/// 동시 연결 수를 제어합니다.
pub struct Server {
mut:
	config        ServerConfig       // 서버 설정
	state         ServerState        // 현재 서버 상태
	conn_mgr      &ConnectionManager // 연결 관리자
	handler       RequestHandler     // 요청 핸들러
	shutdown_chan chan bool          // 종료 신호 채널
	state_lock    sync.Mutex         // 상태 변경 동기화용 뮤텍스
	worker_pool   &WorkerPool        // 연결 핸들러용 워커 풀 (v0.28.0)
}

/// new_server는 새로운 TCP 서버를 생성합니다.
/// 설정과 요청 핸들러를 받아 서버 인스턴스를 반환합니다.
pub fn new_server(config ServerConfig, handler RequestHandler) &Server {
	// 서버 설정에 따른 워커 풀 구성 생성
	pool_config := WorkerPoolConfig{
		max_workers:       config.max_concurrent_handlers
		acquire_timeout:   config.handler_acquire_timeout
		numa_aware:        config.numa_enabled      // v0.33.0
		numa_bind_workers: config.numa_bind_workers // v0.33.0
	}

	return &Server{
		config:        config
		state:         .stopped // 초기 상태: 정지됨
		conn_mgr:      new_connection_manager(config) // 연결 관리자 초기화
		handler:       handler
		shutdown_chan: chan bool{cap: 1} // 버퍼 크기 1의 종료 채널
		worker_pool:   new_worker_pool(pool_config) // 워커 풀 초기화
	}
}

/// start는 TCP 서버를 시작합니다.
/// 서버가 이미 실행 중이면 에러를 반환합니다.
/// 이 메서드는 블로킹되며, stop()이 호출될 때까지 실행됩니다.
pub fn (mut s Server) start() ! {
	s.state_lock.@lock()
	if s.state != .stopped {
		s.state_lock.unlock()
		return error('server is already running or stopping')
	}
	s.state = .starting
	s.state_lock.unlock()

	// TCP 리스너 생성
	mut listener := net.listen_tcp(.ip, '${s.config.host}:${s.config.port}')!

	s.state_lock.@lock()
	s.state = .running
	s.state_lock.unlock()

	println('╔═══════════════════════════════════════════════════════════╗')
	println('║             DataCore Kafka-Compatible Broker              ║')
	println('╠═══════════════════════════════════════════════════════════╣')
	println('║  Listening: ${s.config.host}:${s.config.port}                              ║')
	println('║  Broker ID: ${s.config.broker_id}                                          ║')
	println('║  Cluster:   ${s.config.cluster_id}                         ║')
	println('║  Max Connections: ${s.config.max_connections}                                  ║')
	println('║  Max Handlers: ${s.config.max_concurrent_handlers}                                     ║')
	println('╚═══════════════════════════════════════════════════════════╝')

	// 백그라운드 작업 시작
	spawn s.cleanup_loop()
	spawn s.stats_loop()

	// 연결 수락 루프 (메인 루프)
	for s.is_running() {
		// 주기적으로 종료 신호를 확인하기 위해 타임아웃과 함께 수락
		mut conn := listener.accept() or {
			// 종료해야 하는지 확인
			if !s.is_running() {
				break
			}
			continue
		}

		// 워커 슬롯 획득 시도 (타임아웃 적용)
		// 고부하 상황에서 고루틴 폭발을 방지
		if !s.worker_pool.acquire() {
			// 슬롯 획득 실패 - 연결 거부
			eprintln('[Connection] Rejected: worker pool exhausted (${s.worker_pool.active_count()}/${s.config.max_concurrent_handlers} active)')
			conn.close() or {}
			continue
		}

		// 각 연결을 별도의 코루틴에서 처리 (논블로킹)
		// handle_connection 반환 시 워커 슬롯 해제
		spawn s.handle_connection_with_pool(mut conn)
	}

	listener.close() or {}
}

/// stop은 우아한 종료를 시작합니다.
/// 기존 연결이 완료될 때까지 대기하며, 타임아웃 시 강제 종료합니다.
pub fn (mut s Server) stop() {
	s.state_lock.@lock()
	if s.state != .running {
		s.state_lock.unlock()
		return
	}
	s.state = .stopping
	s.state_lock.unlock()

	println('\n[DataCore] Initiating graceful shutdown...')

	// 종료 신호 전송 (상태 변경으로 수락 루프 종료)
	select {
		s.shutdown_chan <- true {}
		else {}
	}

	// 워커 풀 종료
	s.worker_pool.shutdown()

	// 기존 연결 드레인 대기 (타임아웃 적용)
	start_time := time.now()
	for {
		active := s.conn_mgr.active_count()
		if active == 0 {
			break
		}

		elapsed := (time.now() - start_time).milliseconds()
		if elapsed > s.config.shutdown_timeout_ms {
			println('[DataCore] Shutdown timeout reached, forcing close of ${active} connections')
			s.conn_mgr.close_all()
			break
		}

		println('[DataCore] Waiting for ${active} connections to close...')
		time.sleep(1 * time.second)
	}

	s.state_lock.@lock()
	s.state = .stopped
	s.state_lock.unlock()

	// 최종 통계 출력
	metrics := s.conn_mgr.get_metrics()
	pool_metrics := s.worker_pool.get_metrics()
	println('[DataCore] Server stopped')
	println('  Total connections: ${metrics.total_connections}')
	println('  Total requests: ${metrics.total_requests}')
	println('  Total bytes received: ${format_bytes(metrics.total_bytes_received)}')
	println('  Total bytes sent: ${format_bytes(metrics.total_bytes_sent)}')
	println('  Peak workers: ${pool_metrics.peak_workers}')
	println('  Worker timeouts: ${pool_metrics.total_timeouts}')
}

/// is_running은 서버가 실행 중인지 확인합니다.
pub fn (mut s Server) is_running() bool {
	s.state_lock.@lock()
	defer { s.state_lock.unlock() }
	return s.state == .running
}

/// get_state는 현재 서버 상태를 반환합니다.
pub fn (mut s Server) get_state() ServerState {
	s.state_lock.@lock()
	defer { s.state_lock.unlock() }
	return s.state
}

/// get_metrics는 서버 메트릭을 반환합니다.
pub fn (mut s Server) get_metrics() ConnectionMetrics {
	return s.conn_mgr.get_metrics()
}

/// get_worker_pool_metrics는 워커 풀 메트릭을 반환합니다.
pub fn (mut s Server) get_worker_pool_metrics() WorkerPoolMetrics {
	return s.worker_pool.get_metrics()
}

// TCP 최적화 헬퍼 함수 (TCP_NODELAY 설정)
fn set_tcp_nodelay(sock int) {
	$if linux {
		// Linux: TCP_NODELAY 설정으로 Nagle 알고리즘 비활성화
		// 소켓 지연(latency) 감소를 위해 즉시 전송
		flag := 1
		unsafe {
			C.setsockopt(sock, C.IPPROTO_TCP, C.TCP_NODELAY, &flag, sizeof(int))
		}
	}
}

// TCP 버퍼 크기 최적화
fn set_tcp_buffers(sock int, send_buf int, recv_buf int) {
	$if linux {
		unsafe {
			if send_buf > 0 {
				C.setsockopt(sock, C.SOL_SOCKET, C.SO_SNDBUF, &send_buf, sizeof(int))
			}
			if recv_buf > 0 {
				C.setsockopt(sock, C.SOL_SOCKET, C.SO_RCVBUF, &recv_buf, sizeof(int))
			}
		}
	}
}

/// handle_connection_with_pool은 연결을 처리하고 완료 시 워커 슬롯을 해제합니다.
/// defer를 사용하여 함수 종료 시 반드시 워커 슬롯이 반환되도록 보장합니다.
fn (mut s Server) handle_connection_with_pool(mut conn net.TcpConn) {
	// 함수 종료 시 워커 슬롯 해제 보장 (RAII 패턴)
	defer {
		s.worker_pool.release()
	}

	// TCP 최적화: Nagle 알고리즘 비활성화 (지연 감소)
	// 소켓 핸들 얻기 (V의 net 모듈 내부 구조 활용)
	$if linux {
		unsafe {
			// net.TcpConn의 sock 필드는 TcpSocket을 포함하고 handle은 파일 디스크립터
			sock_ptr := &int(voidptr(&conn.sock) + sizeof(voidptr))
			set_tcp_nodelay(*sock_ptr)
			// TCP 버퍼 크기 최적화 (256KB 송신, 256KB 수신)
			set_tcp_buffers(*sock_ptr, 262144, 262144)
		}
	}

	// NUMA 노드에 워커 바인딩 (v0.33.0)
	// 라운드로빈 방식으로 워커를 NUMA 노드에 분배
	s.worker_pool.bind_worker_to_numa()

	s.handle_connection(mut conn)
}

/// handle_connection은 단일 클라이언트 연결을 처리합니다.
/// Kafka 프로토콜에 따라 요청을 읽고 응답을 전송합니다.
/// 지속 연결(persistent connection)을 지원하며, 연결이 닫히거나
/// 에러가 발생할 때까지 요청-응답 루프를 유지합니다.
fn (mut s Server) handle_connection(mut conn net.TcpConn) {
	// 연결 관리자에 연결 등록
	mut client := s.conn_mgr.accept(mut conn) or {
		eprintln('[Connection] Rejected: ${err}')
		return
	}

	client_addr := client.remote_addr
	println('[Connection] New connection from ${client_addr}')

	defer {
		println('[Connection] Closed: ${client_addr}')
		s.conn_mgr.close(client.fd)
		conn.close() or {}
	}

	// 이 연결에 대한 요청 파이프라인 생성
	mut pipeline := new_pipeline(s.config.max_pending_requests)

	// 요청 처리 루프 (지속 연결)
	for s.is_running() {
		// 요청 타임아웃 확인
		if pipeline.has_timed_out(s.config.request_timeout_ms) {
			eprintln('[Connection] Request timeout for ${client_addr}')
			break
		}

		// 요청 크기 읽기 (4바이트, 빅엔디안)
		mut size_buf := []u8{len: 4}
		bytes_read := conn.read(mut size_buf) or { break }
		if bytes_read != 4 {
			break
		}

		request_size := int(u32(size_buf[0]) << 24 | u32(size_buf[1]) << 16 | u32(size_buf[2]) << 8 | u32(size_buf[3]))

		// 요청 크기 유효성 검사
		if request_size <= 0 {
			eprintln('[Connection] Invalid request size: ${request_size} from ${client_addr}')
			break
		}

		if request_size > s.config.max_request_size {
			eprintln('[Connection] Request too large: ${request_size} > ${s.config.max_request_size} from ${client_addr}')
			break
		}

		// 요청 본문 읽기
		mut request_buf := []u8{len: request_size}
		mut total_read := 0
		for total_read < request_size {
			n := conn.read(mut request_buf[total_read..]) or { break }
			if n == 0 {
				break
			}
			total_read += n
		}

		if total_read != request_size {
			eprintln('[Connection] Incomplete request: expected ${request_size}, got ${total_read} from ${client_addr}')
			break
		}

		// 클라이언트 활동 시간 업데이트
		client.last_active_at = time.now()
		client.request_count += 1
		client.bytes_received += u64(4 + request_size)

		// 파이프라이닝을 위한 correlation_id와 api_key 파싱
		if request_buf.len >= 8 {
			api_key := i16(u16(request_buf[0]) << 8 | u16(request_buf[1]))
			api_version := i16(u16(request_buf[2]) << 8 | u16(request_buf[3]))
			correlation_id := i32(u32(request_buf[4]) << 24 | u32(request_buf[5]) << 16 | u32(request_buf[6]) << 8 | u32(request_buf[7]))

			println('[Request] api_key=${api_key}, version=${api_version}, correlation_id=${correlation_id}, size=${request_size}')

			// 요청 큐에 추가 (파이프라이닝 지원)
			pipeline.enqueue(correlation_id, api_key, api_version, request_buf) or {
				eprintln('[Connection] Pipeline full for ${client_addr}: ${err}')
				break
			}

			// 요청 처리
			mut response := s.handler.handle_request(request_buf) or {
				eprintln('[Connection] Error handling request from ${client_addr}: ${err}')
				// 클라이언트 타임아웃 방지를 위한 최소 에러 응답 생성
				// 유연한 응답인지 확인 (Fetch v12+, Metadata v9+ 등)
				is_flexible := (api_key == 1 && api_version >= 12) || // Fetch
				 (api_key == 0 && api_version >= 9) || // Produce
				 (api_key == 3 && api_version >= 9) || // Metadata
				 (api_key == 10 && api_version >= 6) // FindCoordinator

				if api_key == 1 && is_flexible {
					// Fetch v12+ 에러 응답은 적절한 본문 구조 필요
					// Body: throttle_time_ms(4) + error_code(2) + session_id(4) + topics(1) + tagged_fields(1)
					// 총 본문: 12바이트
					// 응답: size(4) + correlation_id(4) + header_tagged_fields(1) + body(12) = 21바이트
					mut error_resp := []u8{len: 21}
					error_resp[0] = 0
					error_resp[1] = 0
					error_resp[2] = 0
					error_resp[3] = 17 // size = 17 (correlation_id(4) + header_tagged(1) + body(12))
					error_resp[4] = u8(correlation_id >> 24)
					error_resp[5] = u8(correlation_id >> 16)
					error_resp[6] = u8(correlation_id >> 8)
					error_resp[7] = u8(correlation_id)
					error_resp[8] = 0 // header tagged_fields = 0
					// 본문 시작 (인덱스 9)
					error_resp[9] = 0 // throttle_time_ms 바이트 0
					error_resp[10] = 0 // throttle_time_ms 바이트 1
					error_resp[11] = 0 // throttle_time_ms 바이트 2
					error_resp[12] = 0 // throttle_time_ms 바이트 3
					error_resp[13] = 0 // error_code 바이트 0 (NONE = 0)
					error_resp[14] = 0 // error_code 바이트 1
					error_resp[15] = 0 // session_id 바이트 0
					error_resp[16] = 0 // session_id 바이트 1
					error_resp[17] = 0 // session_id 바이트 2
					error_resp[18] = 0 // session_id 바이트 3
					error_resp[19] = 1 // topics compact array length = 0 (1로 인코딩)
					error_resp[20] = 0 // body tagged_fields = 0
					error_resp
				} else if is_flexible {
					// 기타 유연한 응답: size(4) + correlation_id(4) + tagged_fields(1)
					mut error_resp := []u8{len: 9}
					error_resp[0] = 0
					error_resp[1] = 0
					error_resp[2] = 0
					error_resp[3] = 5 // size = 5 (correlation_id + tagged_fields)
					error_resp[4] = u8(correlation_id >> 24)
					error_resp[5] = u8(correlation_id >> 16)
					error_resp[6] = u8(correlation_id >> 8)
					error_resp[7] = u8(correlation_id)
					error_resp[8] = 0 // tagged_fields = 0 (태그 없음)
					error_resp
				} else {
					// 비유연 응답: size(4) + correlation_id(4)
					mut error_resp := []u8{len: 8}
					error_resp[0] = 0
					error_resp[1] = 0
					error_resp[2] = 0
					error_resp[3] = 4 // size = 4 (correlation_id만)
					error_resp[4] = u8(correlation_id >> 24)
					error_resp[5] = u8(correlation_id >> 16)
					error_resp[6] = u8(correlation_id >> 8)
					error_resp[7] = u8(correlation_id)
					error_resp
				}
			}

			println('[Response] api_key=${api_key}, response_size=${response.len}')

			// 요청 완료 표시
			pipeline.complete(correlation_id, response) or {}

			// 준비된 응답 전송 (순서대로)
			ready := pipeline.get_ready_responses()
			for req in ready {
				if req.error_msg.len > 0 {
					eprintln('[Response] Error for correlation_id=${req.correlation_id}: ${req.error_msg}')
					// 클라이언트 타임아웃 방지를 위해 에러가 있어도 응답 전송
				}

				// 디버그: Fetch 응답 로깅
				if api_key == 1 && req.response_data.len < 200 {
					eprintln('[Response] Fetch hex (${req.response_data.len} bytes): ${req.response_data.hex()}')
				}

				conn.write(req.response_data) or {
					eprintln('[Connection] Error sending response to ${client_addr}: ${err}')
					break
				}

				println('[Response] Sent ${req.response_data.len} bytes')
				client.bytes_sent += u64(req.response_data.len)
			}
		}
	}
}

/// cleanup_loop은 주기적으로 유휴 연결을 제거합니다.
/// 60초마다 실행되며, idle_timeout_ms를 초과한 연결을 정리합니다.
fn (mut s Server) cleanup_loop() {
	for s.is_running() {
		closed := s.conn_mgr.cleanup_idle()
		if closed > 0 {
			println('[Cleanup] Closed ${closed} idle connections')
		}
		time.sleep(60 * time.second)
	}
}

/// stats_loop은 주기적으로 서버 통계를 로깅합니다.
/// 5분마다 활성 연결 수, 총 연결 수, 거부된 연결 수를 출력합니다.
fn (mut s Server) stats_loop() {
	for s.is_running() {
		time.sleep(300 * time.second) // 5분(300초)마다 통계 출력

		if !s.is_running() {
			break
		}

		metrics := s.conn_mgr.get_metrics()
		println('[Stats] Active: ${metrics.active_connections}, Total: ${metrics.total_connections}, Rejected: ${metrics.rejected_connections}')
	}
}

/// format_bytes는 바이트를 읽기 쉬운 형식으로 변환합니다.
/// GB, MB, KB, B 단위로 자동 변환하여 문자열을 반환합니다.
fn format_bytes(bytes u64) string {
	if bytes >= 1073741824 { // 1GB = 1024^3
		return '${f64(bytes) / 1073741824.0:.2}GB'
	} else if bytes >= 1048576 { // 1MB = 1024^2
		return '${f64(bytes) / 1048576.0:.2}MB'
	} else if bytes >= 1024 { // 1KB = 1024
		return '${f64(bytes) / 1024.0:.2}KB'
	}
	return '${bytes}B'
}
