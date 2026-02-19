// Collects Kafka API request/response metrics
module observability

import sync

// Kafka protocol metrics

/// ProtocolMetrics tracks metrics for the Kafka protocol handler.
pub struct ProtocolMetrics {
mut:
	// API request metrics
	api_requests_total  map[string]i64
	api_requests_failed map[string]i64
	// Processing time metrics (milliseconds)
	api_latency_sum   map[string]i64
	api_latency_count map[string]i64
	// Error metrics
	errors_total i64
	// Byte metrics
	bytes_received_total i64
	bytes_sent_total     i64
	// Lock
	lock sync.Mutex
}

/// Creates a new ProtocolMetrics instance.
pub fn new_protocol_metrics() &ProtocolMetrics {
	return &ProtocolMetrics{
		api_requests_total:  map[string]i64{}
		api_requests_failed: map[string]i64{}
		api_latency_sum:     map[string]i64{}
		api_latency_count:   map[string]i64{}
	}
}

/// Records an API request.
pub fn (mut m ProtocolMetrics) record_request(api_name string, latency_ms i64, success bool, bytes_received int, bytes_sent int) {
	m.lock.lock()
	defer { m.lock.unlock() }

	// Increment total request count
	if api_name !in m.api_requests_total {
		m.api_requests_total[api_name] = 0
	}
	m.api_requests_total[api_name]++

	// Increment failed request count
	if !success {
		if api_name !in m.api_requests_failed {
			m.api_requests_failed[api_name] = 0
		}
		m.api_requests_failed[api_name]++
		m.errors_total++
	}

	// Record latency
	if api_name !in m.api_latency_sum {
		m.api_latency_sum[api_name] = 0
		m.api_latency_count[api_name] = 0
	}
	m.api_latency_sum[api_name] += latency_ms
	m.api_latency_count[api_name]++

	// Record byte count
	m.bytes_received_total += i64(bytes_received)
	m.bytes_sent_total += i64(bytes_sent)
}

/// Resets all metrics.
pub fn (mut m ProtocolMetrics) reset() {
	m.lock.lock()
	defer { m.lock.unlock() }

	m.api_requests_total.clear()
	m.api_requests_failed.clear()
	m.api_latency_sum.clear()
	m.api_latency_count.clear()
	m.errors_total = 0
	m.bytes_received_total = 0
	m.bytes_sent_total = 0
}

/// Returns a string summary of metrics.
pub fn (mut m ProtocolMetrics) get_summary() string {
	m.lock.lock()
	defer { m.lock.unlock() }

	mut result := '[Protocol Metrics]\n'
	result += '  Total Errors: ${m.errors_total}\n'
	result += '  Bytes: received=${m.bytes_received_total}, sent=${m.bytes_sent_total}\n'
	result += '  API Calls:\n'

	for api_name, count in m.api_requests_total {
		failed := m.api_requests_failed[api_name] or { 0 }
		success_rate := if count > 0 { (f64(count - failed) / f64(count)) * 100.0 } else { 0.0 }

		// Calculate average latency
		avg_latency := if api_name in m.api_latency_count && m.api_latency_count[api_name] > 0 {
			f64(m.api_latency_sum[api_name]) / f64(m.api_latency_count[api_name])
		} else {
			0.0
		}

		result += '    ${api_name}: ${count} requests, ${failed} failed (${success_rate:.1f}% success), avg_latency=${avg_latency:.2f}ms\n'
	}

	return result
}

/// Returns the average latency of a specific API (in milliseconds).
pub fn (mut m ProtocolMetrics) get_avg_latency(api_name string) f64 {
	m.lock.lock()
	defer { m.lock.unlock() }

	if api_name in m.api_latency_count && m.api_latency_count[api_name] > 0 {
		return f64(m.api_latency_sum[api_name]) / f64(m.api_latency_count[api_name])
	}
	return 0.0
}

/// Returns the success rate of a specific API (0.0 to 1.0).
pub fn (mut m ProtocolMetrics) get_success_rate(api_name string) f64 {
	m.lock.lock()
	defer { m.lock.unlock() }

	if api_name in m.api_requests_total {
		total := m.api_requests_total[api_name]
		failed := m.api_requests_failed[api_name] or { 0 }
		if total > 0 {
			return f64(total - failed) / f64(total)
		}
	}
	return 0.0
}
