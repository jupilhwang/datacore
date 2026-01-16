// Interface Layer - Connection Manager
// Thread-safe connection management with metrics
module server

import net
import sync
import time

// ClientConnection represents a client connection
pub struct ClientConnection {
pub mut:
    fd              int
    remote_addr     string
    connected_at    time.Time
    last_active_at  time.Time
    request_count   u64
    bytes_received  u64
    bytes_sent      u64
    client_id       string
    api_version     i16      // Client's preferred API version
    client_sw_name  string   // Client software name
    client_sw_ver   string   // Client software version
}

// ConnectionMetrics tracks connection statistics
pub struct ConnectionMetrics {
pub mut:
    active_connections      int   // Current active connections
    total_connections       u64   // Total connections accepted (cumulative)
    rejected_connections    u64   // Connections rejected
    rejected_max_total      u64   // Rejected: max connections reached
    rejected_max_per_ip     u64   // Rejected: max per IP reached
    total_bytes_received    u64   // Total bytes received
    total_bytes_sent        u64   // Total bytes sent
    total_requests          u64   // Total requests processed
}

// ConnectionManager manages client connections with thread safety
pub struct ConnectionManager {
mut:
    connections     map[int]&ClientConnection
    config          ServerConfig
    ip_counts       map[string]int
    metrics         ConnectionMetrics
    lock            sync.RwMutex  // Protects connections and ip_counts
}

// new_connection_manager creates a new connection manager
pub fn new_connection_manager(config ServerConfig) &ConnectionManager {
    return &ConnectionManager{
        config: config
        connections: map[int]&ClientConnection{}
        ip_counts: map[string]int{}
        metrics: ConnectionMetrics{}
    }
}

// accept accepts a new connection (thread-safe)
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
        fd: fd
        remote_addr: addr.str()
        connected_at: now
        last_active_at: now
    }
    
    // Register connection
    cm.connections[fd] = client
    cm.ip_counts[ip] = ip_count + 1
    cm.metrics.active_connections = cm.connections.len
    cm.metrics.total_connections += 1
    
    return client
}

// close closes a connection (thread-safe)
pub fn (mut cm ConnectionManager) close(fd int) {
    cm.lock.@lock()
    defer { cm.lock.unlock() }
    
    cm.close_internal(fd)
}

// close_internal closes without lock (must be called with lock held)
fn (mut cm ConnectionManager) close_internal(fd int) {
    if client := cm.connections[fd] {
        // Update total stats before removing
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

// cleanup_idle removes idle connections (thread-safe)
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

// get_connection returns a connection by fd (thread-safe, read-only)
pub fn (mut cm ConnectionManager) get_connection(fd int) ?&ClientConnection {
    cm.lock.rlock()
    defer { cm.lock.runlock() }
    
    return cm.connections[fd] or { return none }
}

// update_client_info updates client metadata (thread-safe)
pub fn (mut cm ConnectionManager) update_client_info(fd int, client_id string, sw_name string, sw_version string) {
    cm.lock.@lock()
    defer { cm.lock.unlock() }
    
    if mut client := cm.connections[fd] {
        client.client_id = client_id
        client.client_sw_name = sw_name
        client.client_sw_ver = sw_version
    }
}

// active_count returns the number of active connections (thread-safe)
pub fn (mut cm ConnectionManager) active_count() int {
    cm.lock.rlock()
    defer { cm.lock.runlock() }
    
    return cm.connections.len
}

// get_metrics returns a snapshot of connection metrics (thread-safe)
pub fn (mut cm ConnectionManager) get_metrics() ConnectionMetrics {
    cm.lock.rlock()
    defer { cm.lock.runlock() }
    
    return cm.metrics
}

// get_all_connections returns all active connections (thread-safe)
pub fn (mut cm ConnectionManager) get_all_connections() []ClientConnection {
    cm.lock.rlock()
    defer { cm.lock.runlock() }
    
    mut result := []ClientConnection{cap: cm.connections.len}
    for _, client in cm.connections {
        result << *client
    }
    return result
}

// close_all closes all connections (for shutdown)
pub fn (mut cm ConnectionManager) close_all() int {
    cm.lock.@lock()
    defer { cm.lock.unlock() }
    
    count := cm.connections.len
    
    for fd, _ in cm.connections {
        cm.close_internal(fd)
    }
    
    return count
}

// Helper function to extract IP from address string
fn extract_ip(addr string) string {
    // Handle IPv6 addresses like [::1]:9092
    if addr.starts_with('[') {
        if end := addr.index(']:') {
            return addr[1..end]
        }
    }
    // Handle IPv4 addresses like 127.0.0.1:9092
    parts := addr.split(':')
    if parts.len >= 1 {
        return parts[0]
    }
    return addr
}
