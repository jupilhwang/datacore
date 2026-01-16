// Infra Layer - Prometheus HTTP Exporter
// Exposes /metrics endpoint for Prometheus scraping
module observability

import net
import io

// MetricsServer serves metrics over HTTP
pub struct MetricsServer {
    host string
    port int
}

// new_metrics_server creates a new metrics server
pub fn new_metrics_server(host string, port int) MetricsServer {
    return MetricsServer{
        host: host
        port: port
    }
}

// start starts the metrics HTTP server (blocking)
pub fn (s MetricsServer) start() ! {
    mut listener := net.listen_tcp(.ip, '${s.host}:${s.port}')!
    eprintln('[Metrics] Starting Prometheus exporter on http://${s.host}:${s.port}/metrics')
    
    for {
        mut conn := listener.accept() or { continue }
        spawn s.handle_connection(mut conn)
    }
}

// start_background starts the metrics HTTP server in background
pub fn (s MetricsServer) start_background() {
    spawn fn [s] () {
        s.start() or {
            eprintln('[Metrics] Failed to start metrics server: ${err}')
        }
    }()
}

// handle_connection handles a single HTTP connection
fn (s MetricsServer) handle_connection(mut conn net.TcpConn) {
    defer { conn.close() or {} }
    
    // Read request (simple HTTP/1.1 parsing)
    mut reader := io.new_buffered_reader(reader: conn)
    line := reader.read_line() or { return }
    
    // Parse request line: GET /path HTTP/1.1
    parts := line.split(' ')
    if parts.len < 2 {
        return
    }
    
    path := parts[1]
    
    // Skip headers
    for {
        header_line := reader.read_line() or { break }
        if header_line.len == 0 || header_line == '\r' {
            break
        }
    }
    
    // Route request
    response := match path {
        '/metrics' { s.metrics_response() }
        '/health', '/healthz' { s.health_response() }
        '/ready', '/readyz' { s.ready_response() }
        else { s.not_found_response() }
    }
    
    conn.write_string(response) or {}
}

fn (s MetricsServer) metrics_response() string {
    body := get_registry().export_prometheus()
    return 'HTTP/1.1 200 OK\r\n' +
           'Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n' +
           'Content-Length: ${body.len}\r\n' +
           'Connection: close\r\n' +
           '\r\n' +
           body
}

fn (s MetricsServer) health_response() string {
    body := '{"status":"healthy"}'
    return 'HTTP/1.1 200 OK\r\n' +
           'Content-Type: application/json\r\n' +
           'Content-Length: ${body.len}\r\n' +
           'Connection: close\r\n' +
           '\r\n' +
           body
}

fn (s MetricsServer) ready_response() string {
    body := '{"status":"ready"}'
    return 'HTTP/1.1 200 OK\r\n' +
           'Content-Type: application/json\r\n' +
           'Content-Length: ${body.len}\r\n' +
           'Connection: close\r\n' +
           '\r\n' +
           body
}

fn (s MetricsServer) not_found_response() string {
    body := 'Not Found'
    return 'HTTP/1.1 404 Not Found\r\n' +
           'Content-Type: text/plain\r\n' +
           'Content-Length: ${body.len}\r\n' +
           'Connection: close\r\n' +
           '\r\n' +
           body
}
