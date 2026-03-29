/// Interface Layer - TCP Server
///
/// This module implements a Kafka-compatible TCP server.
/// Operates based on non-blocking I/O using V coroutines,
/// supporting high-performance concurrent connection handling.
///
/// Key features:
/// - Non-blocking I/O based TCP connection acceptance
/// - Concurrent connection handler limiting via worker pool
/// - Request pipelining support
/// - Automatic idle connection cleanup
/// - Graceful shutdown support
module server

import net
import sync
import sync.stdatomic
import time
import domain
import infra.observability

const initial_recv_buffer_cap = 65536
const cleanup_loop_interval = 60 * time.second
const stats_log_interval = 300 * time.second

/// ServerConfig is a struct holding server configuration.
/// Contains various options controlling TCP server behavior.
pub struct ServerConfig {
pub:
	host                   string = '0.0.0.0'
	port                   int    = 9092
	broker_id              int    = 1
	cluster_id             string = 'datacore-cluster'
	max_connections        int    = 10000
	max_connections_per_ip int    = 100
	idle_timeout_ms        int    = 600000
	request_timeout_ms     int    = 30000
	max_request_size       int    = 104857600
	max_pending_requests   int    = 100
	shutdown_timeout_ms    int    = 30000
	// Worker pool configuration (v0.28.0)
	max_concurrent_handlers int = 1000
	handler_acquire_timeout int = 5000
	// io_uring configuration (v0.32.0)
	use_io_uring         bool = true
	io_uring_queue_depth u32  = 256
	io_uring_sqpoll      bool
	// NUMA configuration (v0.33.0)
	numa_enabled      bool
	numa_bind_workers bool = true
	// TCP optimization settings (v0.42.0)
	tcp_nodelay       bool = true
	tcp_send_buf_size int  = 262144
	tcp_recv_buf_size int  = 262144
}

/// AuthConnection is an interface for connection types that support authentication.
/// NOTE: This interface is now defined in domain.auth for architectural compliance.
pub type AuthConnection = domain.AuthConnection

/// RequestHandler is the interface for handling protocol requests.
/// Receives Kafka protocol requests and generates responses.
pub interface RequestHandler {
mut:
	/// handle_request processes a request with connection context for auth checking.
	/// data: raw request bytes
	/// conn: client connection for authentication state (optional)
	handle_request(data []u8, mut conn ?&domain.AuthConnection) ![]u8
}

/// noop_handler is a no-op implementation for testing.
struct NoopHandler {}

pub fn (h NoopHandler) handle_request(data []u8, mut conn ?&AuthConnection) ![]u8 {
	return []u8{}
}

/// ServerState is an enum representing the current state of the server.
pub enum ServerState {
	stopped
	starting
	running
	stopping
}

/// Server is a non-blocking I/O based TCP server.
/// Handles Kafka-compatible protocol and controls concurrent
/// connections via a worker pool.
pub struct Server {
mut:
	config        ServerConfig
	state         ServerState
	conn_mgr      &ConnectionManager
	handler       RequestHandler
	shutdown_chan chan bool
	state_lock    sync.Mutex
	worker_pool   &WorkerPool
	// running_flag is an atomic bool (0=stopped, 1=running) for lock-free is_running() checks.
	running_flag i64
	// Rate limiter (optional, nil when disabled)
	rate_limiter ?&RateLimiter
}

/// new_server creates a new TCP server.
/// Accepts configuration and a request handler and returns a server instance.
pub fn new_server(config ServerConfig, handler RequestHandler) &Server {
	// Create worker pool configuration based on server settings
	pool_config := WorkerPoolConfig{
		max_workers:       config.max_concurrent_handlers
		acquire_timeout:   config.handler_acquire_timeout
		numa_aware:        config.numa_enabled
		numa_bind_workers: config.numa_bind_workers
	}

	return &Server{
		config:        config
		state:         .stopped
		conn_mgr:      new_connection_manager(config)
		handler:       handler
		shutdown_chan: chan bool{cap: 1}
		worker_pool:   new_worker_pool(pool_config)
	}
}

/// set_rate_limiter configures the rate limiter for the server.
/// Must be called before start(). Pass a limiter created by new_rate_limiter().
pub fn (mut s Server) set_rate_limiter(rl &RateLimiter) {
	s.rate_limiter = rl
}

/// start starts the TCP server.
/// Returns an error if the server is already running.
/// This method blocks and runs until stop() is called.
pub fn (mut s Server) start() ! {
	s.state_lock.@lock()
	if s.state != .stopped {
		s.state_lock.unlock()
		return error('server is already running or stopping')
	}
	s.state = .starting
	s.state_lock.unlock()

	// Create TCP listener
	mut listener := net.listen_tcp(.ip, '${s.config.host}:${s.config.port}')!

	s.state_lock.@lock()
	s.state = .running
	s.state_lock.unlock()
	stdatomic.store_i64(&s.running_flag, 1)

	println('╔═══════════════════════════════════════════════════════════╗')
	println('║             DataCore Kafka-Compatible Broker              ║')
	println('╠═══════════════════════════════════════════════════════════╣')
	println('║  Listening: ${s.config.host}:${s.config.port}                              ║')
	println('║  Broker ID: ${s.config.broker_id}                                          ║')
	println('║  Cluster:   ${s.config.cluster_id}                         ║')
	println('║  Max Connections: ${s.config.max_connections}                                  ║')
	println('║  Max Handlers: ${s.config.max_concurrent_handlers}                                     ║')
	println('╚═══════════════════════════════════════════════════════════╝')

	// Start background tasks
	spawn s.cleanup_loop()
	spawn s.stats_loop()

	// Connection accept loop (main loop)
	for s.is_running() {
		// Accept with timeout to periodically check for shutdown signal
		mut conn := listener.accept() or {
			// Check whether we should shut down
			if !s.is_running() {
				break
			}
			continue
		}

		// Attempt to acquire a worker slot (with timeout)
		// Prevents goroutine explosion under high load
		if !s.worker_pool.acquire() {
			// Failed to acquire slot - reject connection
			eprintln('[Connection] Rejected: worker pool exhausted (${s.worker_pool.active_count()}/${s.config.max_concurrent_handlers} active)')
			conn.close() or {}
			continue
		}

		// Handle each connection in a separate coroutine (non-blocking)
		// Worker slot is released when handle_connection returns
		spawn s.handle_connection_with_pool(mut conn)
	}

	listener.close() or {}
}

/// stop initiates graceful shutdown.
/// Waits for existing connections to complete and force-closes on timeout.
pub fn (mut s Server) stop() {
	s.state_lock.@lock()
	if s.state != .running {
		s.state_lock.unlock()
		return
	}
	s.state = .stopping
	s.state_lock.unlock()
	stdatomic.store_i64(&s.running_flag, 0)

	println('\n[DataCore] Initiating graceful shutdown...')

	// Send shutdown signal (state change terminates accept loop)
	select {
		s.shutdown_chan <- true {}
		else {}
	}

	// Shutdown worker pool
	s.worker_pool.shutdown()

	// Wait for existing connections to drain (with timeout)
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

	// Print final statistics
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

/// is_running checks whether the server is running.
/// Uses atomic load for lock-free access from hot-path loops.
pub fn (mut s Server) is_running() bool {
	return stdatomic.load_i64(&s.running_flag) == 1
}

/// get_state returns the current server state.
pub fn (mut s Server) get_state() ServerState {
	s.state_lock.@lock()
	defer { s.state_lock.unlock() }
	return s.state
}

/// get_metrics returns server metrics.
pub fn (mut s Server) get_metrics() ConnectionMetrics {
	return s.conn_mgr.get_metrics()
}

/// get_worker_pool_metrics returns worker pool metrics.
pub fn (mut s Server) get_worker_pool_metrics() WorkerPoolMetrics {
	return s.worker_pool.get_metrics()
}

// TCP optimization helper (set TCP_NODELAY)
fn set_tcp_nodelay(sock int) {
	$if linux {
		// Linux: disable Nagle algorithm with TCP_NODELAY
		// Send immediately to reduce socket latency
		flag := 1
		unsafe {
			C.setsockopt(sock, C.IPPROTO_TCP, C.TCP_NODELAY, &flag, sizeof(int))
		}
	}
}

// TCP buffer size optimization
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

/// handle_connection_with_pool handles a connection and releases the worker slot on completion.
/// Uses defer to guarantee the worker slot is always returned when the function exits.
fn (mut s Server) handle_connection_with_pool(mut conn net.TcpConn) {
	// Guarantee worker slot release on function exit (RAII pattern)
	defer {
		s.worker_pool.release()
	}

	// TCP optimization: disable Nagle algorithm to reduce latency
	// Get socket handle (using V net module internal structure)
	$if linux {
		unsafe {
			// net.TcpConn's sock field contains TcpSocket and handle is the file descriptor
			sock_ptr := &int(voidptr(usize(&conn.sock) + sizeof(voidptr)))
			set_tcp_nodelay(*sock_ptr)
			// Optimize TCP buffer sizes (256KB send, 256KB receive)
			set_tcp_buffers(*sock_ptr, 262144, 262144)
		}
	}

	// Bind worker to NUMA node (v0.33.0)
	// Distribute workers across NUMA nodes in round-robin fashion
	s.worker_pool.bind_worker_to_numa()

	s.handle_connection(mut conn)
}

/// RequestHeader holds parsed Kafka request header fields.
struct RequestHeader {
	api_key        i16
	api_version    i16
	correlation_id i32
}

/// read_request_size reads a 4-byte big-endian request size and validates it.
fn (s &Server) read_request_size(mut conn net.TcpConn, client_addr string) !int {
	mut size_buf := []u8{len: 4}
	bytes_read := conn.read(mut size_buf) or { return err }
	if bytes_read != 4 {
		return error('incomplete size header')
	}

	request_size := int(u32(size_buf[0]) << 24 | u32(size_buf[1]) << 16 | u32(size_buf[2]) << 8 | u32(size_buf[3]))

	if request_size <= 0 {
		eprintln('[Connection] Invalid request size: ${request_size} from ${client_addr}')
		return error('invalid request size: ${request_size} from ${client_addr}')
	}

	if request_size > s.config.max_request_size {
		eprintln('[Connection] Request too large: ${request_size} > ${s.config.max_request_size} from ${client_addr}')
		return error('request too large: ${request_size} (max: ${s.config.max_request_size}) from ${client_addr}')
	}

	return request_size
}

/// read_request_body reads the full request body into the pre-allocated buffer.
fn (s &Server) read_request_body(mut conn net.TcpConn, mut buf []u8, request_size int, client_addr string) ! {
	if request_size <= buf.cap {
		unsafe {
			buf.len = request_size
		}
	} else {
		buf = []u8{len: request_size}
	}
	mut total_read := 0
	for total_read < request_size {
		n := conn.read(mut buf[total_read..]) or { break }
		if n == 0 {
			break
		}
		total_read += n
	}

	if total_read != request_size {
		eprintln('[Connection] Incomplete request: expected ${request_size}, got ${total_read} from ${client_addr}')
		return error('incomplete request')
	}
}

/// parse_request_header extracts api_key, api_version, and correlation_id from the first 8 bytes.
fn (s &Server) parse_request_header(data []u8) ?RequestHeader {
	if data.len < 8 {
		return none
	}
	return RequestHeader{
		api_key:        i16(u16(data[0]) << 8 | u16(data[1]))
		api_version:    i16(u16(data[2]) << 8 | u16(data[3]))
		correlation_id: i32(u32(data[4]) << 24 | u32(data[5]) << 16 | u32(data[6]) << 8 | u32(data[7]))
	}
}

/// check_rate_limit checks the rate limiter and sends a throttle response if needed.
/// Returns true if the request was throttled (caller should continue to next request).
fn (mut s Server) check_rate_limit(mut conn net.TcpConn, client_addr string, correlation_id i32, request_size int) bool {
	if mut rl := s.rate_limiter {
		client_ip := extract_ip(client_addr)
		if !rl.allow_request_with_bytes(client_ip, i64(request_size)) {
			throttle_resp := build_throttle_response(correlation_id)
			conn.write(throttle_resp) or {
				observability.log_with_context('tcp', .warn, 'RateLimit', 'failed to write throttle response',
					{
					'client':         client_addr
					'correlation_id': correlation_id.str()
					'error':          err.str()
				})
			}
			return true
		}
	}
	return false
}

/// build_error_response builds a minimal error response for the given API key and version.
fn (s &Server) build_error_response(api_key i16, api_version i16, correlation_id i32) []u8 {
	is_flexible := (api_key == 1 && api_version >= 12)
		|| (api_key == 0 && api_version >= 9) || (api_key == 3 && api_version >= 9)
		|| (api_key == 10 && api_version >= 6)

	if api_key == 1 && is_flexible {
		// Fetch v12+ error response: size(4) + correlation_id(4) + tagged_fields(1) + body(12) = 21 bytes
		mut error_resp := []u8{len: 21}
		error_resp[0] = 0
		error_resp[1] = 0
		error_resp[2] = 0
		error_resp[3] = 17
		error_resp[4] = u8(correlation_id >> 24)
		error_resp[5] = u8(correlation_id >> 16)
		error_resp[6] = u8(correlation_id >> 8)
		error_resp[7] = u8(correlation_id)
		error_resp[8] = 0
		error_resp[9] = 0
		error_resp[10] = 0
		error_resp[11] = 0
		error_resp[12] = 0
		error_resp[13] = 0
		error_resp[14] = 0
		error_resp[15] = 0
		error_resp[16] = 0
		error_resp[17] = 0
		error_resp[18] = 0
		error_resp[19] = 1
		error_resp[20] = 0
		return error_resp
	} else if is_flexible {
		// Other flexible responses: size(4) + correlation_id(4) + tagged_fields(1)
		mut error_resp := []u8{len: 9}
		error_resp[0] = 0
		error_resp[1] = 0
		error_resp[2] = 0
		error_resp[3] = 5
		error_resp[4] = u8(correlation_id >> 24)
		error_resp[5] = u8(correlation_id >> 16)
		error_resp[6] = u8(correlation_id >> 8)
		error_resp[7] = u8(correlation_id)
		error_resp[8] = 0
		return error_resp
	} else {
		// Non-flexible response: size(4) + correlation_id(4)
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
}

/// send_ready_responses sends all ready pipeline responses to the connection in order.
fn (mut s Server) send_ready_responses(mut conn net.TcpConn, ready []PendingRequest, mut client ClientConnection, api_key i16, client_addr string) {
	for req in ready {
		if req.error_msg.len > 0 {
			observability.log_with_context('tcp', .warn, 'Response', 'Response error',
				{
				'correlation_id': req.correlation_id.str()
				'error':          req.error_msg
			})
		}

		$if debug {
			if api_key == 1 && req.response_data.len < 200 {
				observability.log_with_context('tcp', .debug, 'Response', 'Fetch response hex',
					{
					'size': req.response_data.len.str()
					'hex':  req.response_data.hex()
				})
			}
		}

		conn.write(req.response_data) or {
			observability.log_with_context('tcp', .error, 'Connection', 'Error sending response',
				{
				'client_addr': client_addr
				'error':       err.str()
			})
			break
		}

		observability.log_with_context('tcp', .debug, 'Response', 'Sent', {
			'bytes': req.response_data.len.str()
		})
		client.bytes_sent += u64(req.response_data.len)
	}
}

/// handle_connection handles a single client connection.
/// Reads requests and sends responses according to the Kafka protocol.
/// Supports persistent connections and maintains a request-response loop
/// until the connection is closed or an error occurs.
fn (mut s Server) handle_connection(mut conn net.TcpConn) {
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

	mut pipeline := new_pipeline(s.config.max_pending_requests)
	initial_buf_cap := initial_recv_buffer_cap
	mut request_buf := []u8{len: initial_buf_cap}

	for s.is_running() {
		if pipeline.has_timed_out(s.config.request_timeout_ms) {
			eprintln('[Connection] Request timeout for ${client_addr}')
			break
		}

		request_size := s.read_request_size(mut conn, client_addr) or { break }
		s.read_request_body(mut conn, mut request_buf, request_size, client_addr) or { break }

		client.last_active_at = time.now()
		client.request_count += 1
		client.bytes_received += u64(4 + request_size)

		header := s.parse_request_header(request_buf) or { continue }

		observability.log_with_context('tcp', .debug, 'Request', 'Incoming request', {
			'api_key':        header.api_key.str()
			'api_version':    header.api_version.str()
			'correlation_id': header.correlation_id.str()
			'size':           request_size.str()
		})

		if s.check_rate_limit(mut conn, client_addr, header.correlation_id, request_size) {
			continue
		}

		pipeline.enqueue(header.correlation_id, header.api_key, header.api_version, request_buf[..request_size].clone()) or {
			eprintln('[Connection] Pipeline full for ${client_addr}: ${err}')
			break
		}

		mut response := s.handler.handle_request(request_buf, mut client) or {
			eprintln('[Connection] Error handling request from ${client_addr}: ${err}')
			s.build_error_response(header.api_key, header.api_version, header.correlation_id)
		}

		observability.log_with_context('tcp', .debug, 'Response', 'Response ready', {
			'api_key': header.api_key.str()
			'size':    response.len.str()
		})

		pipeline.complete(header.correlation_id, response) or {
			observability.log_with_context('tcp', .error, 'Pipeline', 'failed to complete pipeline request',
				{
				'correlation_id': header.correlation_id.str()
				'client':         client_addr
				'error':          err.str()
			})
		}

		ready := pipeline.get_ready_responses()
		s.send_ready_responses(mut conn, ready, mut client, header.api_key, client_addr)
	}
}

/// cleanup_loop periodically removes idle connections.
/// Runs every 60 seconds and cleans up connections exceeding idle_timeout_ms.
fn (mut s Server) cleanup_loop() {
	for s.is_running() {
		closed := s.conn_mgr.cleanup_idle()
		if closed > 0 {
			println('[Cleanup] Closed ${closed} idle connections')
		}
		time.sleep(cleanup_loop_interval)
	}
}

/// stats_loop periodically logs server statistics.
/// Every 5 minutes, prints active connections, total connections, and rejected connections.
fn (mut s Server) stats_loop() {
	for s.is_running() {
		time.sleep(stats_log_interval)

		if !s.is_running() {
			break
		}

		metrics := s.conn_mgr.get_metrics()
		println('[Stats] Active: ${metrics.active_connections}, Total: ${metrics.total_connections}, Rejected: ${metrics.rejected_connections}')
	}
}

/// format_bytes converts bytes to a human-readable format.
/// Automatically converts to GB, MB, KB, or B and returns a string.
fn format_bytes(bytes u64) string {
	if bytes >= 1073741824 {
		return '${f64(bytes) / 1073741824.0:.2}GB'
	} else if bytes >= 1048576 {
		return '${f64(bytes) / 1048576.0:.2}MB'
	} else if bytes >= 1024 {
		return '${f64(bytes) / 1024.0:.2}KB'
	}
	return '${bytes}B'
}

/// build_throttle_response creates a Kafka error response with THROTTLING_QUOTA_EXCEEDED (55).
/// Returns a minimal response: size(4) + correlation_id(4) + error_code(2).
fn build_throttle_response(correlation_id i32) []u8 {
	// Response body: correlation_id(4) + error_code(2) = 6 bytes
	mut resp := []u8{len: 10}
	// Size prefix (6 bytes)
	resp[0] = 0
	resp[1] = 0
	resp[2] = 0
	resp[3] = 6
	// Correlation ID
	resp[4] = u8(correlation_id >> 24)
	resp[5] = u8(correlation_id >> 16)
	resp[6] = u8(correlation_id >> 8)
	resp[7] = u8(correlation_id)
	// Error code 55 = THROTTLING_QUOTA_EXCEEDED
	resp[8] = 0
	resp[9] = 55
	return resp
}
