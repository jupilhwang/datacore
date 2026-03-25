// Protocol-level metrics interface following DIP.
// Abstracts protocol metrics recording from the infrastructure layer.
module port

/// ProtocolMetricsPort abstracts protocol-level request metrics recording.
/// Implemented by infra/observability ProtocolMetrics.
pub interface ProtocolMetricsPort {
mut:
	/// Records a protocol request with timing and size information.
	record_request(api_name string, latency_ms i64, success bool, bytes_received int, bytes_sent int)

	/// Returns a human-readable summary of recorded metrics.
	get_summary() string

	/// Resets all recorded metrics.
	reset()
}
