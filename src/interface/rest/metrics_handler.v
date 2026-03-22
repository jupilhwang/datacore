// Metrics handler for REST API server
//
// Handles Prometheus-compatible metrics endpoint:
// - /metrics: Export metrics in Prometheus text format
module rest

import infra.observability
import net

// handle_metrics handles the /metrics endpoint (Prometheus format).
fn (s &RestServer) handle_metrics(mut conn net.TcpConn) {
	registry := observability.get_registry()
	metrics_output := registry.export_prometheus()

	response := 'HTTP/1.1 200 OK\r\n' +
		'Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n' +
		'Content-Length: ${metrics_output.len}\r\n' + 'Connection: close\r\n' + '\r\n' +
		metrics_output

	conn.write_string(response) or {}
	conn.close() or {}
}
