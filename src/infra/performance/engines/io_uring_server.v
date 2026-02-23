/// io_uring_server - io_uring-based network server
/// Provides high-performance async network I/O on Linux 5.1+
///
/// Features:
/// - Async accept/recv/send using io_uring
/// - Connection acceptance batching with multi-accept
/// - Zero-copy data transfer support
///
/// Note: Only available on Linux 5.1+, other platforms use fallback
module engines

import time

// io_uring server structures

/// IoUringServerConfig holds io_uring server configuration.
pub struct IoUringServerConfig {
pub:
	host             string = '0.0.0.0'
	port             int    = 9092
	queue_depth      u32    = 256
	backlog          int    = 128
	max_connections  int    = 10000
	recv_buffer_size int    = 65536
	multi_accept     int    = 8
	use_sqpoll       bool
}

/// IoUringServer is an io_uring-based network server.
pub struct IoUringServer {
mut:
	config    IoUringServerConfig
	ring      IoUring
	listen_fd int
	running   bool
	// Connection management
	connections map[int]&IoUringConnection
	stats       IoUringServerStats
}

/// IoUringConnection holds client connection information.
pub struct IoUringConnection {
pub mut:
	fd             int
	recv_buf       []u8
	send_buf       []u8
	connected_at   time.Time
	last_active_at time.Time
	bytes_received u64
	bytes_sent     u64
	recv_pending   bool
	send_pending   bool
}

/// IoUringServerStats holds server statistics.
pub struct IoUringServerStats {
pub mut:
	total_accepts     u64
	total_connections u64
	active_conns      int
	total_recv_bytes  u64
	total_send_bytes  u64
	total_recv_ops    u64
	total_send_ops    u64
}

/// IoUringEventType is the io_uring event type.
pub enum IoUringEventType {
	accept
	recv
	send
	close
}

/// IoUringEvent is an io_uring completion event.
pub struct IoUringEvent {
pub:
	event_type IoUringEventType
	fd         int
	result     i32
	data       []u8
}

// user_data encoding: upper 8 bits = event type, lower 56 bits = fd
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

// io_uring server implementation

/// new_io_uring_server creates a new io_uring server.
pub fn new_io_uring_server(config IoUringServerConfig) !&IoUringServer {
	$if linux {
		// io_uring configuration
		ring_config := IoUringConfig{
			queue_depth:    config.queue_depth
			flags:          if config.use_sqpoll { ioring_setup_sqpoll } else { 0 }
			sq_thread_idle: 1000
		}

		// Create io_uring
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

/// start starts the server (creates listening socket and prepares accept).
pub fn (mut s IoUringServer) start() ! {
	$if linux {
		if s.running {
			return error('server is already running')
		}

		// Create listening socket
		s.listen_fd = create_listen_socket(s.config.host, s.config.port, s.config.backlog)!

		// Prepare multi-accept
		for _ in 0 .. s.config.multi_accept {
			s.prepare_accept()
		}

		// Submit accept
		s.ring.submit(0)!

		s.running = true
	} $else {
		return error('io_uring server is only available on Linux')
	}
}

/// stop stops the server.
pub fn (mut s IoUringServer) stop() {
	$if linux {
		if !s.running {
			return
		}

		s.running = false

		// Close all connections
		for fd, _ in s.connections {
			close_socket(fd)
		}
		s.connections.clear()

		// Close listening socket
		if s.listen_fd >= 0 {
			close_socket(s.listen_fd)
			s.listen_fd = -1
		}

		// Cleanup io_uring
		s.ring.close()
	}
}

/// poll polls io_uring completion events.
/// Returns completed events, or an empty array if no events are available.
pub fn (mut s IoUringServer) poll() ![]IoUringEvent {
	$if linux {
		mut events := []IoUringEvent{cap: 16}

		// Check completions non-blocking
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
					// Connection close completed
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

/// wait waits until at least one completion event is available.
pub fn (mut s IoUringServer) wait() ![]IoUringEvent {
	$if linux {
		// Wait for at least 1 completion
		s.ring.submit(1)!
		return s.poll()
	} $else {
		return error('io_uring not available')
	}
}

/// prepare_recv prepares a recv operation for fd.
pub fn (mut s IoUringServer) prepare_recv(fd int) bool {
	$if linux {
		if mut conn := s.connections[fd] {
			if conn.recv_pending {
				return false
			}

			// Create buffer if not present
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

/// prepare_send prepares data transmission to fd.
pub fn (mut s IoUringServer) prepare_send(fd int, data []u8) bool {
	$if linux {
		if mut conn := s.connections[fd] {
			if conn.send_pending {
				// Already pending send - add to buffer
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

/// close_connection closes a connection.
pub fn (mut s IoUringServer) close_connection(fd int) {
	$if linux {
		if _ := s.connections[fd] {
			close_socket(fd)
			s.connections.delete(fd)
			s.stats.active_conns = s.connections.len
		}
	}
}

/// submit submits all pending operations.
pub fn (mut s IoUringServer) submit() !int {
	$if linux {
		return s.ring.submit(0)
	} $else {
		return error('io_uring not available')
	}
}

/// get_stats returns server statistics.
pub fn (s &IoUringServer) get_stats() IoUringServerStats {
	return s.stats
}

/// is_running returns whether the server is running.
pub fn (s &IoUringServer) is_running() bool {
	return s.running
}

// Internal helper methods

fn (mut s IoUringServer) prepare_accept() {
	$if linux {
		user_data := encode_user_data(.accept, s.listen_fd)
		s.ring.prep_accept(s.listen_fd, user_data)
	}
}

fn (mut s IoUringServer) handle_accept_completion(result i32) IoUringEvent {
	if result < 0 {
		// Accept failed - prepare again
		s.prepare_accept()
		return IoUringEvent{
			event_type: .accept
			fd:         -1
			result:     result
		}
	}

	// Register new connection
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

	// Prepare next accept
	s.prepare_accept()

	// Auto-prepare recv for new connection
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
			// Connection closed or error
			return IoUringEvent{
				event_type: .close
				fd:         fd
				result:     result
			}
		}

		// Data received successfully
		conn.bytes_received += u64(result)
		s.stats.total_recv_bytes += u64(result)
		s.stats.total_recv_ops++

		// Copy received data
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
			// Send error
			return IoUringEvent{
				event_type: .close
				fd:         fd
				result:     result
			}
		}

		conn.bytes_sent += u64(result)
		s.stats.total_send_bytes += u64(result)
		s.stats.total_send_ops++

		// Send pending data if any
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

// Non-Linux fallback implementation

/// IoUringServerFallback is a fallback for non-Linux systems.
/// Replaced by synchronous I/O using the net module.
pub struct IoUringServerFallback {
pub mut:
	available bool
}

/// new_io_uring_server_fallback creates a fallback server object.
pub fn new_io_uring_server_fallback() IoUringServerFallback {
	return IoUringServerFallback{
		available: true
	}
}

/// is_io_uring_server_available checks if the io_uring server is available.
pub fn is_io_uring_server_available() bool {
	$if linux {
		return is_io_uring_available()
	} $else {
		return false
	}
}
