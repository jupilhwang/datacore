// 인프라 레이어 - Kafka 프로토콜 메트릭
// Kafka API 요청/응답 메트릭 수집
module observability

import sync

// ============================================================
// Kafka 프로토콜 메트릭
// ============================================================

/// ProtocolMetrics는 Kafka 프로토콜 핸들러의 메트릭을 추적합니다.
pub struct ProtocolMetrics {
mut:
	// API 요청 메트릭
	api_requests_total  map[string]i64 // API별 총 요청 수
	api_requests_failed map[string]i64 // API별 실패 요청 수
	// 처리 시간 메트릭 (밀리초)
	api_latency_sum   map[string]i64 // API별 총 지연 시간
	api_latency_count map[string]i64 // API별 지연 시간 측정 횟수
	// 에러 메트릭
	errors_total i64 // 총 에러 수
	// 바이트 메트릭
	bytes_received_total i64 // 총 수신 바이트
	bytes_sent_total     i64 // 총 전송 바이트
	// 락
	lock sync.Mutex
}

/// 새로운 ProtocolMetrics를 생성합니다.
pub fn new_protocol_metrics() &ProtocolMetrics {
	return &ProtocolMetrics{
		api_requests_total:  map[string]i64{}
		api_requests_failed: map[string]i64{}
		api_latency_sum:     map[string]i64{}
		api_latency_count:   map[string]i64{}
	}
}

/// API 요청을 기록합니다.
pub fn (mut m ProtocolMetrics) record_request(api_name string, latency_ms i64, success bool, bytes_received int, bytes_sent int) {
	m.lock.lock()
	defer { m.lock.unlock() }

	// 총 요청 수 증가
	if api_name !in m.api_requests_total {
		m.api_requests_total[api_name] = 0
	}
	m.api_requests_total[api_name]++

	// 실패 요청 수 증가
	if !success {
		if api_name !in m.api_requests_failed {
			m.api_requests_failed[api_name] = 0
		}
		m.api_requests_failed[api_name]++
		m.errors_total++
	}

	// 지연 시간 기록
	if api_name !in m.api_latency_sum {
		m.api_latency_sum[api_name] = 0
		m.api_latency_count[api_name] = 0
	}
	m.api_latency_sum[api_name] += latency_ms
	m.api_latency_count[api_name]++

	// 바이트 수 기록
	m.bytes_received_total += i64(bytes_received)
	m.bytes_sent_total += i64(bytes_sent)
}

/// 메트릭을 초기화합니다.
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

/// 메트릭 요약을 문자열로 반환합니다.
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

		// 평균 지연 시간 계산
		avg_latency := if api_name in m.api_latency_count && m.api_latency_count[api_name] > 0 {
			f64(m.api_latency_sum[api_name]) / f64(m.api_latency_count[api_name])
		} else {
			0.0
		}

		result += '    ${api_name}: ${count} requests, ${failed} failed (${success_rate:.1f}% success), avg_latency=${avg_latency:.2f}ms\n'
	}

	return result
}

/// 특정 API의 평균 지연 시간을 반환합니다 (밀리초).
pub fn (mut m ProtocolMetrics) get_avg_latency(api_name string) f64 {
	m.lock.lock()
	defer { m.lock.unlock() }

	if api_name in m.api_latency_count && m.api_latency_count[api_name] > 0 {
		return f64(m.api_latency_sum[api_name]) / f64(m.api_latency_count[api_name])
	}
	return 0.0
}

/// 특정 API의 성공률을 반환합니다 (0.0 ~ 1.0).
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
