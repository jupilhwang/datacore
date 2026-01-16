// Infra Layer - Prometheus HTTP Exporter
// Exposes /metrics endpoint for Prometheus scraping
module observability

import net.http

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
    mut server := http.Server{
        handler: handler
        addr: '${s.host}:${s.port}'
    }
    eprintln('[Metrics] Starting Prometheus exporter on http://${s.host}:${s.port}/metrics')
    server.listen_and_serve()
}

// start_background starts the metrics HTTP server in background
pub fn (s MetricsServer) start_background() {
    spawn fn [s] () {
        s.start() or {
            eprintln('[Metrics] Failed to start metrics server: ${err}')
        }
    }()
}

// HTTP handler for metrics endpoint
fn handler(req http.Request) http.Response {
    if req.url == '/metrics' {
        body := get_registry().export_prometheus()
        return http.Response{
            status: .ok
            header: http.new_header_from_map({
                .content_type: 'text/plain; version=0.0.4; charset=utf-8'
            })
            body: body
        }
    }
    
    if req.url == '/health' || req.url == '/healthz' {
        return http.Response{
            status: .ok
            header: http.new_header_from_map({
                .content_type: 'application/json'
            })
            body: '{"status":"healthy"}'
        }
    }
    
    if req.url == '/ready' || req.url == '/readyz' {
        return http.Response{
            status: .ok
            header: http.new_header_from_map({
                .content_type: 'application/json'
            })
            body: '{"status":"ready"}'
        }
    }
    
    return http.Response{
        status: .not_found
        body: 'Not Found'
    }
}
