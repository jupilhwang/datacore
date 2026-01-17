// Interface Layer - TCP Server
// Non-blocking I/O based TCP server using V coroutines
module server

import net
import sync
import time

// ServerConfig holds server configuration
pub struct ServerConfig {
pub:
    host                    string = '0.0.0.0'
    port                    int    = 9092
    broker_id               int    = 1
    cluster_id              string = 'datacore-cluster'
    max_connections         int    = 10000
    max_connections_per_ip  int    = 100
    idle_timeout_ms         int    = 600000    // 10 minutes
    request_timeout_ms      int    = 30000     // 30 seconds
    max_request_size        int    = 104857600 // 100MB
    max_pending_requests    int    = 100       // Request pipelining limit
    shutdown_timeout_ms     int    = 30000     // Graceful shutdown timeout
}

// RequestHandler interface for handling protocol requests
pub interface RequestHandler {
mut:
    handle_request(data []u8) ![]u8
}

// ServerState represents the server's current state
pub enum ServerState {
    stopped
    starting
    running
    stopping
}

// Server is the TCP server with non-blocking I/O
pub struct Server {
mut:
    config          ServerConfig
    state           ServerState
    conn_mgr        &ConnectionManager
    handler         RequestHandler
    shutdown_chan   chan bool
    state_lock      sync.Mutex
}

// new_server creates a new TCP server
pub fn new_server(config ServerConfig, handler RequestHandler) &Server {
    return &Server{
        config: config
        state: .stopped
        conn_mgr: new_connection_manager(config)
        handler: handler
        shutdown_chan: chan bool{cap: 1}
    }
}

// start starts the TCP server
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
    
    println('╔═══════════════════════════════════════════════════════════╗')
    println('║             DataCore Kafka-Compatible Broker              ║')
    println('╠═══════════════════════════════════════════════════════════╣')
    println('║  Listening: ${s.config.host}:${s.config.port}                              ║')
    println('║  Broker ID: ${s.config.broker_id}                                          ║')
    println('║  Cluster:   ${s.config.cluster_id}                         ║')
    println('║  Max Connections: ${s.config.max_connections}                                  ║')
    println('╚═══════════════════════════════════════════════════════════╝')
    
    // Start background tasks
    spawn s.cleanup_loop()
    spawn s.stats_loop()
    
    // Accept loop (main loop)
    for s.is_running() {
        // Accept with timeout to check shutdown signal periodically
        mut conn := listener.accept() or { 
            // Check if we should stop
            if !s.is_running() {
                break
            }
            continue 
        }
        
        // Handle each connection in a separate coroutine (non-blocking)
        spawn s.handle_connection(mut conn)
    }
    
    listener.close() or {}
}

// stop initiates graceful shutdown
pub fn (mut s Server) stop() {
    s.state_lock.@lock()
    if s.state != .running {
        s.state_lock.unlock()
        return
    }
    s.state = .stopping
    s.state_lock.unlock()
    
    println('\n[DataCore] Initiating graceful shutdown...')
    
    // Signal shutdown (state change will cause accept loop to exit)
    select {
        s.shutdown_chan <- true {}
        else {}
    }
    
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
    
    // Print final stats
    metrics := s.conn_mgr.get_metrics()
    println('[DataCore] Server stopped')
    println('  Total connections: ${metrics.total_connections}')
    println('  Total requests: ${metrics.total_requests}')
    println('  Total bytes received: ${format_bytes(metrics.total_bytes_received)}')
    println('  Total bytes sent: ${format_bytes(metrics.total_bytes_sent)}')
}

// is_running checks if server is in running state
pub fn (mut s Server) is_running() bool {
    s.state_lock.@lock()
    defer { s.state_lock.unlock() }
    return s.state == .running
}

// get_state returns current server state
pub fn (mut s Server) get_state() ServerState {
    s.state_lock.@lock()
    defer { s.state_lock.unlock() }
    return s.state
}

// get_metrics returns server metrics
pub fn (mut s Server) get_metrics() ConnectionMetrics {
    return s.conn_mgr.get_metrics()
}

// handle_connection handles a single client connection
fn (mut s Server) handle_connection(mut conn net.TcpConn) {
    // Register connection with manager
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
    
    // Create request pipeline for this connection
    mut pipeline := new_pipeline(s.config.max_pending_requests)
    
    // Request processing loop (persistent connection)
    for s.is_running() {
        // Check for request timeout
        if pipeline.has_timed_out(s.config.request_timeout_ms) {
            eprintln('[Connection] Request timeout for ${client_addr}')
            break
        }
        
        // Read request size (4 bytes, big-endian)
        mut size_buf := []u8{len: 4}
        bytes_read := conn.read(mut size_buf) or { break }
        if bytes_read != 4 {
            break
        }
        
        request_size := int(u32(size_buf[0]) << 24 | u32(size_buf[1]) << 16 | 
                           u32(size_buf[2]) << 8 | u32(size_buf[3]))
        
        // Validate request size
        if request_size <= 0 {
            eprintln('[Connection] Invalid request size: ${request_size} from ${client_addr}')
            break
        }
        
        if request_size > s.config.max_request_size {
            eprintln('[Connection] Request too large: ${request_size} > ${s.config.max_request_size} from ${client_addr}')
            break
        }
        
        // Read request body
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
        
        // Update client activity time
        client.last_active_at = time.now()
        client.request_count += 1
        client.bytes_received += u64(4 + request_size)
        
        // Parse correlation_id and api_key for pipelining
        if request_buf.len >= 8 {
            api_key := i16(u16(request_buf[0]) << 8 | u16(request_buf[1]))
            api_version := i16(u16(request_buf[2]) << 8 | u16(request_buf[3]))
            correlation_id := i32(u32(request_buf[4]) << 24 | u32(request_buf[5]) << 16 |
                                  u32(request_buf[6]) << 8 | u32(request_buf[7]))
            
            println('[Request] api_key=${api_key}, version=${api_version}, correlation_id=${correlation_id}, size=${request_size}')
            
            // Enqueue request (for pipelining support)
            pipeline.enqueue(correlation_id, api_key, api_version, request_buf) or {
                eprintln('[Connection] Pipeline full for ${client_addr}: ${err}')
                break
            }
            
            // Process request
        mut response := s.handler.handle_request(request_buf) or {
                eprintln('[Connection] Error handling request from ${client_addr}: ${err}')
                // Build minimal error response to prevent client timeout
                mut error_resp := []u8{len: 8}
                error_resp[0] = 0
                error_resp[1] = 0
                error_resp[2] = 0
                error_resp[3] = 4  // size = 4 (just correlation_id)
                error_resp[4] = u8(correlation_id >> 24)
                error_resp[5] = u8(correlation_id >> 16)
                error_resp[6] = u8(correlation_id >> 8)
                error_resp[7] = u8(correlation_id)
                error_resp
            }
            
            println('[Response] api_key=${api_key}, response_size=${response.len}')
            
            // Mark request as complete
            pipeline.complete(correlation_id, response) or {}
            
            // Send ready responses (in order)
            ready := pipeline.get_ready_responses()
            for req in ready {
                if req.error_msg.len > 0 {
                    eprintln('[Response] Error for correlation_id=${req.correlation_id}: ${req.error_msg}')
                    // Still send response even with errors to prevent client timeout
                }
        
                // Debug: log Fetch responses
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

// cleanup_loop periodically removes idle connections
fn (mut s Server) cleanup_loop() {
    for s.is_running() {
        closed := s.conn_mgr.cleanup_idle()
        if closed > 0 {
            println('[Cleanup] Closed ${closed} idle connections')
        }
        time.sleep(60 * time.second)
    }
}

// stats_loop periodically logs server statistics
fn (mut s Server) stats_loop() {
    for s.is_running() {
        time.sleep(300 * time.second) // Every 5 minutes
        
        if !s.is_running() {
            break
        }
        
        metrics := s.conn_mgr.get_metrics()
        println('[Stats] Active: ${metrics.active_connections}, Total: ${metrics.total_connections}, Rejected: ${metrics.rejected_connections}')
    }
}

// Helper function to format bytes
fn format_bytes(bytes u64) string {
    if bytes >= 1073741824 { // 1GB
        return '${f64(bytes) / 1073741824.0:.2}GB'
    } else if bytes >= 1048576 { // 1MB
        return '${f64(bytes) / 1048576.0:.2}MB'
    } else if bytes >= 1024 { // 1KB
        return '${f64(bytes) / 1024.0:.2}KB'
    }
    return '${bytes}B'
}
