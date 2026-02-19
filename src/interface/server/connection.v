/// Interface Layer - Connection Manager
///
/// This module manages client connections in a thread-safe manner.
/// Provides connection limiting, per-IP limits, idle connection cleanup, and more.
///
/// Key features:
/// - Thread-safe connection registration/deregistration
/// - Maximum total connections and per-IP connection limits
/// - Automatic idle connection cleanup
/// - Connection metrics collection
/// - Authentication state management
module server

import domain
import net
import sync
import time

/// ClientConnection is a struct holding client connection information.
/// Includes connection state, statistics, and authentication info.
pub struct ClientConnection {
pub mut:
	fd             int
	remote_addr    string
	connected_at   time.Time
	last_active_at time.Time
	request_count  u64
	bytes_received u64
	bytes_sent     u64
	client_id      string
	api_version    i16
	client_sw_name string
	client_sw_ver  string
	// Authentication state
	auth_state     domain.AuthState
	principal      ?domain.Principal
	sasl_mechanism ?string
}

/// ConnectionMetrics is a struct tracking connection statistics.
pub struct ConnectionMetrics {
pub mut:
	active_connections   int
	total_connections    u64
	rejected_connections u64
	rejected_max_total   u64
	rejected_max_per_ip  u64
	total_bytes_received u64
	total_bytes_sent     u64
	total_requests       u64
}

/// ConnectionManager manages client connections in a thread-safe manner.
/// Uses RwMutex to support concurrent reads and exclusive writes.
pub struct ConnectionManager {
mut:
	connections map[int]&ClientConnection
	config      ServerConfig
	ip_counts   map[string]int
	metrics     ConnectionMetrics
	lock        sync.RwMutex
}

/// new_connection_manager creates a new connection manager.
pub fn new_connection_manager(config ServerConfig) &ConnectionManager {
	return &ConnectionManager{
		config:      config
		connections: map[int]&ClientConnection{}
		ip_counts:   map[string]int{}
		metrics:     ConnectionMetrics{}
	}
}

/// accept accepts a new connection (thread-safe).
/// Returns an error if max connections or per-IP limit is reached.
pub fn (mut cm ConnectionManager) accept(mut conn net.TcpConn) !&ClientConnection {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	// Check max connections limit
	if cm.connections.len >= cm.config.max_connections {
		cm.metrics.rejected_connections += 1
		cm.metrics.rejected_max_total += 1
		conn.close() or {}
		return error('max connections reached: ${cm.connections.len}/${cm.config.max_connections}')
	}

	// Get peer address
	addr := conn.peer_addr() or { return error('cannot get peer address') }
	ip := extract_ip(addr.str())

	// Check per-IP connection limit
	ip_count := cm.ip_counts[ip] or { 0 }
	if ip_count >= cm.config.max_connections_per_ip {
		cm.metrics.rejected_connections += 1
		cm.metrics.rejected_max_per_ip += 1
		conn.close() or {}
		return error('max connections per IP reached: ${ip} has ${ip_count}/${cm.config.max_connections_per_ip}')
	}

	// Create client connection
	fd := conn.sock.handle
	now := time.now()
	client := &ClientConnection{
		fd:             fd
		remote_addr:    addr.str()
		connected_at:   now
		last_active_at: now
	}

	// Register connection
	cm.connections[fd] = client
	cm.ip_counts[ip] = ip_count + 1
	cm.metrics.active_connections = cm.connections.len
	cm.metrics.total_connections += 1

	return client
}

/// close closes a connection (thread-safe).
pub fn (mut cm ConnectionManager) close(fd int) {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	cm.close_internal(fd)
}

/// close_internal closes a connection without holding the lock.
/// Caution: this function must only be called while the lock is already held.
/// It is an internal helper and must not be called externally.
fn (mut cm ConnectionManager) close_internal(fd int) {
	if client := cm.connections[fd] {
		// Update totals before removing
		cm.metrics.total_bytes_received += client.bytes_received
		cm.metrics.total_bytes_sent += client.bytes_sent
		cm.metrics.total_requests += client.request_count

		// Decrement IP count
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

/// cleanup_idle removes idle connections (thread-safe).
/// Returns the number of connections removed.
pub fn (mut cm ConnectionManager) cleanup_idle() int {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	now := time.now()
	// Use small initial capacity since only a few connections typically time out at once
	mut to_close := []int{cap: 8}

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

/// get_connection returns a connection by fd (thread-safe, read-only).
pub fn (mut cm ConnectionManager) get_connection(fd int) ?&ClientConnection {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	return cm.connections[fd] or { return none }
}

/// update_client_info updates client metadata (thread-safe).
pub fn (mut cm ConnectionManager) update_client_info(fd int, client_id string, sw_name string, sw_version string) {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	if mut client := cm.connections[fd] {
		client.client_id = client_id
		client.client_sw_name = sw_name
		client.client_sw_ver = sw_version
	}
}

/// active_count returns the number of active connections (thread-safe).
pub fn (mut cm ConnectionManager) active_count() int {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	return cm.connections.len
}

/// get_metrics returns a snapshot of connection metrics (thread-safe).
pub fn (mut cm ConnectionManager) get_metrics() ConnectionMetrics {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	return cm.metrics
}

/// get_all_connections returns all active connections (thread-safe).
pub fn (mut cm ConnectionManager) get_all_connections() []ClientConnection {
	cm.lock.rlock()
	defer { cm.lock.runlock() }

	mut result := []ClientConnection{cap: cm.connections.len}
	for _, client in cm.connections {
		result << *client
	}
	return result
}

/// close_all closes all connections (used during shutdown).
/// Returns the number of connections closed.
pub fn (mut cm ConnectionManager) close_all() int {
	cm.lock.@lock()
	defer { cm.lock.unlock() }

	count := cm.connections.len

	for fd, _ in cm.connections {
		cm.close_internal(fd)
	}

	return count
}

/// extract_ip extracts the IP address from an address string.
/// Supports IPv4 (e.g. 127.0.0.1:9092) and IPv6 (e.g. [::1]:9092) formats.
fn extract_ip(addr string) string {
	// Handle IPv6 address (e.g. [::1]:9092 -> ::1)
	if addr.starts_with('[') {
		if end := addr.index(']:') {
			return addr[1..end]
		}
	}
	// Handle IPv4 address (e.g. 127.0.0.1:9092 -> 127.0.0.1)
	parts := addr.split(':')
	if parts.len >= 1 {
		return parts[0]
	}
	return addr
}

// Authentication helper methods

/// is_authenticated checks whether the connection is authenticated.
pub fn (c &ClientConnection) is_authenticated() bool {
	return c.auth_state == .authenticated
}

/// set_authenticated marks the connection as authenticated with the given principal.
pub fn (mut c ClientConnection) set_authenticated(principal domain.Principal) {
	c.auth_state = .authenticated
	c.principal = principal
}

/// set_handshake_complete marks the SASL handshake as complete.
pub fn (mut c ClientConnection) set_handshake_complete(mechanism string) {
	c.auth_state = .handshake_complete
	c.sasl_mechanism = mechanism
}

/// reset_auth resets the authentication state.
pub fn (mut c ClientConnection) reset_auth() {
	c.auth_state = .initial
	c.principal = none
	c.sasl_mechanism = none
}

/// get_principal returns the authenticated principal.
pub fn (c &ClientConnection) get_principal() ?domain.Principal {
	return c.principal
}

/// requires_auth checks whether the connection requires authentication.
/// Used to determine whether SASL is required before processing a request.
pub fn (c &ClientConnection) requires_auth(auth_required bool) bool {
	if !auth_required {
		return false
	}
	return c.auth_state != .authenticated
}
