/// Interface Layer - io_uring based TCP Server
/// High-performance TCP server using io_uring on Linux 5.1+
///
/// This module handles network I/O using io_uring on Linux.
/// On non-Linux platforms, automatically falls back to the standard net module based server.
///
/// Key features:
/// - Asynchronous accept/recv/send via io_uring
/// - Compatible with the existing RequestHandler interface
/// - Automatic platform detection and fallback
module server

import domain
import infra.observability
import infra.performance.engines
import service.port
import sync
import time

const io_uring_recv_buf_size = 65536

// io_uring integrated server

/// IoUringTcpServer is an io_uring based TCP server.
/// Uses io_uring on Linux, falls back on other platforms.
pub struct IoUringTcpServer {
mut:
	config     ServerConfig
	state      ServerState
	handler    RequestHandler
	state_lock sync.Mutex
	logger     &observability.Logger
	// io_uring related (Linux only)
	uring_server ?&engines.IoUringServer
	// Connection information management
	conn_info map[int]&IoUringConnInfo
	metrics   IoUringServerMetrics
}

/// IoUringConnInfo holds connection information for the io_uring server.
struct IoUringConnInfo {
mut:
	fd             int
	remote_addr    string
	connected_at   time.Time
	last_active_at time.Time
	request_count  u64
	bytes_received u64
	bytes_sent     u64
	// Authentication state
	auth_state domain.AuthState
	principal  ?domain.Principal
	// Request parsing state
	recv_buf      []u8
	recv_offset   int
	expected_size int
}

/// is_authenticated checks whether the connection is authenticated.
pub fn (c &IoUringConnInfo) is_authenticated() bool {
	return c.auth_state == .authenticated
}

/// set_authenticated marks the connection as authenticated with the given principal.
pub fn (mut c IoUringConnInfo) set_authenticated(principal domain.Principal) {
	c.auth_state = .authenticated
	c.principal = principal
}

/// IoUringServerMetrics holds io_uring server metrics.
pub struct IoUringServerMetrics {
pub mut:
	active_connections   int
	total_connections    u64
	total_requests       u64
	total_bytes_received u64
	total_bytes_sent     u64
	io_uring_enabled     bool
}

/// new_io_uring_tcp_server creates an io_uring based TCP server.
pub fn new_io_uring_tcp_server(config ServerConfig, handler RequestHandler) &IoUringTcpServer {
	return &IoUringTcpServer{
		config:    config
		state:     .stopped
		handler:   handler
		logger:    observability.get_named_logger('tcp.io_uring')
		conn_info: map[int]&IoUringConnInfo{}
		metrics:   IoUringServerMetrics{}
	}
}

/// start starts the io_uring TCP server (blocking).
pub fn (mut s IoUringTcpServer) start() ! {
	s.state_lock.@lock()
	if s.state != .stopped {
		s.state_lock.unlock()
		return error('server is already running or stopping')
	}
	s.state = .starting
	s.state_lock.unlock()

	// Check io_uring availability
	$if linux {
		if s.config.use_io_uring && engines.is_io_uring_server_available() {
			s.start_io_uring_mode()!
			return
		}
	}

	// io_uring unavailable - return error (recommend using standard server)
	s.state_lock.@lock()
	s.state = .stopped
	s.state_lock.unlock()
	return error('io_uring not available, use standard Server instead')
}

/// start_io_uring_mode starts the server in io_uring mode.
fn (mut s IoUringTcpServer) start_io_uring_mode() ! {
	$if linux {
		// Configure io_uring server
		uring_config := engines.IoUringServerConfig{
			host:             s.config.host
			port:             s.config.port
			queue_depth:      s.config.io_uring_queue_depth
			backlog:          128
			max_connections:  s.config.max_connections
			recv_buffer_size: io_uring_recv_buf_size
			multi_accept:     8
			use_sqpoll:       s.config.io_uring_sqpoll
		}

		// Create and start io_uring server
		mut uring_server := engines.new_io_uring_server(uring_config)!
		uring_server.start()!

		s.uring_server = uring_server
		s.metrics.io_uring_enabled = true

		s.state_lock.@lock()
		s.state = .running
		s.state_lock.unlock()

		s.logger.info('DataCore Kafka-Compatible Broker started (io_uring)', port.field_string('host',
			s.config.host), port.field_int('port', s.config.port), port.field_int('broker_id',
			s.config.broker_id), port.field_string('mode', 'io_uring (Linux 5.1+)'), port.field_int('queue_depth',
			int(s.config.io_uring_queue_depth)))

		// Run event loop
		s.io_uring_event_loop()
	}
}

/// io_uring_event_loop is the io_uring event loop.
fn (mut s IoUringTcpServer) io_uring_event_loop() {
	$if linux {
		mut uring := s.uring_server or { return }

		for s.is_running() {
			// Wait for events
			events := uring.wait() or {
				if s.is_running() {
					s.logger.error('[io_uring] wait error', port.field_string('error',
						err.str()))
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
						// Send completion handling (no additional action needed)
					}
					.close {
						s.handle_io_uring_close(event.fd)
					}
				}
			}

			// Submit
			uring.submit() or {
				s.logger.error('[io_uring] submit failed', port.field_string('error',
					err.str()))
			}
		}
	}
}

/// handle_io_uring_accept handles a new connection.
fn (mut s IoUringTcpServer) handle_io_uring_accept(client_fd int) {
	if client_fd < 0 {
		return
	}

	now := time.now()
	conn := &IoUringConnInfo{
		fd:             client_fd
		remote_addr:    'unknown'
		connected_at:   now
		last_active_at: now
		recv_buf:       []u8{cap: io_uring_recv_buf_size}
		expected_size:  -1
	}

	s.conn_info[client_fd] = conn
	s.metrics.active_connections = s.conn_info.len
	s.metrics.total_connections++

	s.logger.info('[io_uring] New connection', port.field_int('fd', client_fd))
}

/// handle_io_uring_recv handles received data.
fn (mut s IoUringTcpServer) handle_io_uring_recv(fd int, data []u8) {
	$if linux {
		mut uring := s.uring_server or { return }

		if data.len == 0 {
			// Connection closed
			s.handle_io_uring_close(fd)
			return
		}

		mut conn := s.conn_info[fd] or {
			// Unknown connection
			uring.close_connection(fd)
			return
		}

		conn.last_active_at = time.now()
		conn.bytes_received += u64(data.len)
		s.metrics.total_bytes_received += u64(data.len)

		// Append data to buffer
		conn.recv_buf << data

		// Check for complete requests and process them
		s.process_recv_buffer(fd, mut conn, mut uring)

		// Prepare next recv
		uring.prepare_recv(fd)
	}
}

/// process_recv_buffer processes complete requests from the receive buffer.
fn (mut s IoUringTcpServer) process_recv_buffer(fd int, mut conn IoUringConnInfo, mut uring engines.IoUringServer) {
	for {
		// Request size not yet known
		if conn.expected_size < 0 {
			if conn.recv_buf.len < 4 {
				break
			}

			// Read request size in big-endian
			conn.expected_size = int(u32(conn.recv_buf[0]) << 24 | u32(conn.recv_buf[1]) << 16 | u32(conn.recv_buf[2]) << 8 | u32(conn.recv_buf[3]))

			if conn.expected_size <= 0 || conn.expected_size > s.config.max_request_size {
				s.logger.error('[io_uring] Invalid request size', port.field_int('size',
					conn.expected_size), port.field_int('fd', fd))
				uring.close_connection(fd)
				s.conn_info.delete(fd)
				s.metrics.active_connections = s.conn_info.len
				return
			}
		}

		// Check whether a complete request has arrived
		total_needed := 4 + conn.expected_size
		if conn.recv_buf.len < total_needed {
			break
		}

		// Extract request data
		request_data := conn.recv_buf[4..total_needed].clone()

		// Remove processed data from buffer
		conn.recv_buf = conn.recv_buf[total_needed..].clone()
		conn.expected_size = -1

		// Process request
		conn.request_count++
		s.metrics.total_requests++

		response := s.handler.handle_request(request_data, mut conn) or {
			s.logger.error('[io_uring] Error handling request', port.field_int('fd', fd),
				port.field_string('error', err.str()))
			// Generate minimal error response
			s.create_error_response(request_data)
		}

		// Send response
		conn.bytes_sent += u64(response.len)
		s.metrics.total_bytes_sent += u64(response.len)
		uring.prepare_send(fd, response)
	}
}

/// create_error_response creates an error response.
fn (s &IoUringTcpServer) create_error_response(request_data []u8) []u8 {
	if request_data.len < 8 {
		return []u8{}
	}

	correlation_id := i32(u32(request_data[4]) << 24 | u32(request_data[5]) << 16 | u32(request_data[6]) << 8 | u32(request_data[7]))

	mut error_resp := []u8{len: 8}
	error_resp[0] = 0
	error_resp[1] = 0
	error_resp[2] = 0
	error_resp[3] = 4
	error_resp[4] = u8(correlation_id >> 24)
	error_resp[5] = u8(correlation_id >> 16)
	error_resp[6] = u8(correlation_id >> 8)
	error_resp[7] = u8(correlation_id)
	return error_resp
}

/// handle_io_uring_close handles connection closure.
fn (mut s IoUringTcpServer) handle_io_uring_close(fd int) {
	$if linux {
		if mut uring := s.uring_server {
			uring.close_connection(fd)
		}
	}

	if _ := s.conn_info[fd] {
		s.logger.info('[io_uring] Connection closed', port.field_int('fd', fd))
		s.conn_info.delete(fd)
		s.metrics.active_connections = s.conn_info.len
	}
}

/// stop gracefully stops the io_uring TCP server.
pub fn (mut s IoUringTcpServer) stop() {
	s.state_lock.@lock()
	if s.state != .running {
		s.state_lock.unlock()
		return
	}
	s.state = .stopping
	s.state_lock.unlock()

	s.logger.info('[io_uring] Initiating graceful shutdown...')

	$if linux {
		if mut uring := s.uring_server {
			uring.stop()
		}
	}

	s.conn_info.clear()

	s.state_lock.@lock()
	s.state = .stopped
	s.state_lock.unlock()

	s.logger.info('[io_uring] Server stopped', port.field_string('total_connections',
		s.metrics.total_connections.str()), port.field_string('total_requests', s.metrics.total_requests.str()),
		port.field_string('total_bytes_received', format_bytes(s.metrics.total_bytes_received)),
		port.field_string('total_bytes_sent', format_bytes(s.metrics.total_bytes_sent)))
}

/// is_running returns true if the server is currently running.
pub fn (mut s IoUringTcpServer) is_running() bool {
	s.state_lock.@lock()
	defer { s.state_lock.unlock() }
	return s.state == .running
}

/// get_metrics returns a snapshot of the server's operational metrics.
pub fn (s &IoUringTcpServer) get_metrics() IoUringServerMetrics {
	return s.metrics
}

// io_uring availability check functions

/// is_io_uring_available returns true if io_uring is available on the current platform.
pub fn is_io_uring_available() bool {
	$if linux {
		return engines.is_io_uring_server_available()
	} $else {
		return false
	}
}

/// get_recommended_server_mode returns the recommended server mode string for the current platform.
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
