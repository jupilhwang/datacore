// Infra Layer - OTLP (OpenTelemetry Protocol) Exporter
// 로그, 메트릭, 트레이스를 OpenTelemetry Collector로 내보냄
module observability

import net.http
import sync
import time

// OTLP Exporter Configuration

// OTLPConfig holds OTLP exporter configuration
pub struct OTLPConfig {
pub:
	endpoint          string = 'http://localhost:4318'
	service_name      string = 'datacore'
	service_version   string = '0.28.0'
	instance_id       string
	environment       string = 'development'
	timeout_ms        int    = 5000
	batch_size        int    = 100
	flush_interval_ms int    = 1000
	retry_count       int    = 3
	retry_delay_ms    int    = 1000
	// Buffer limits (v0.28.0) - prevent unbounded memory growth
	max_log_buffer_size  int = 10000
	max_span_buffer_size int = 5000
}

// OTLP Exporter

// OTLPExporter exports telemetry data to OpenTelemetry Collector
pub struct OTLPExporter {
	config OTLPConfig
mut:
	log_buffer  []LogEntry
	span_buffer []&Span
	buffer_lock sync.Mutex
	running     bool
	flush_lock  sync.Mutex
	// Buffer overflow stats (v0.28.0)
	logs_dropped  u64
	spans_dropped u64
}

// new_otlp_exporter creates a new OTLP exporter
pub fn new_otlp_exporter(config OTLPConfig) &OTLPExporter {
	return &OTLPExporter{
		config:      config
		log_buffer:  []
		span_buffer: []
		running:     false
	}
}

// start starts the background flush loop
pub fn (mut e OTLPExporter) start() {
	if e.running {
		return
	}
	e.running = true
	spawn e.flush_loop()
}

// stop stops the exporter and flushes remaining data
pub fn (mut e OTLPExporter) stop() {
	e.running = false
	e.flush()
}

// flush_loop periodically flushes buffered data
fn (mut e OTLPExporter) flush_loop() {
	for e.running {
		time.sleep(time.millisecond * e.config.flush_interval_ms)
		if e.running {
			e.flush()
		}
	}
}

// flush sends all buffered data to OTLP endpoint
pub fn (mut e OTLPExporter) flush() {
	e.flush_lock.@lock()
	defer { e.flush_lock.unlock() }

	// Flush logs
	e.buffer_lock.@lock()
	if e.log_buffer.len > 0 {
		logs := e.log_buffer.clone()
		e.log_buffer.clear()
		e.buffer_lock.unlock()
		e.export_logs(logs)
	} else {
		e.buffer_lock.unlock()
	}

	// Flush spans
	e.buffer_lock.@lock()
	if e.span_buffer.len > 0 {
		spans := e.span_buffer.clone()
		e.span_buffer.clear()
		e.buffer_lock.unlock()
		e.export_spans(spans)
	} else {
		e.buffer_lock.unlock()
	}
}

// Log Export

// add_log adds a log entry to the buffer
pub fn (mut e OTLPExporter) add_log(entry LogEntry) {
	e.buffer_lock.@lock()

	// Check buffer limit (v0.28.0) - prevent unbounded memory growth
	if e.log_buffer.len >= e.config.max_log_buffer_size {
		// Drop oldest entries (keep most recent)
		drop_count := e.log_buffer.len / 10
		e.log_buffer = e.log_buffer[drop_count..]
		e.logs_dropped += u64(drop_count)
	}

	e.log_buffer << entry
	should_flush := e.log_buffer.len >= e.config.batch_size
	e.buffer_lock.unlock()

	if should_flush {
		spawn e.flush()
	}
}

// export_logs exports log entries to OTLP endpoint
fn (mut e OTLPExporter) export_logs(entries []LogEntry) {
	if entries.len == 0 || e.config.endpoint.len == 0 {
		return
	}

	payload := e.build_logs_payload(entries)
	endpoint := '${e.config.endpoint}/v1/logs'

	e.send_with_retry(endpoint, payload)
}

// build_logs_payload builds OTLP logs JSON payload
fn (e &OTLPExporter) build_logs_payload(entries []LogEntry) string {
	mut sb := []u8{cap: 2048}
	sb << '{"resourceLogs":[{"resource":{"attributes":['.bytes()
	sb << '{"key":"service.name","value":{"stringValue":"${e.config.service_name}"}}'.bytes()
	sb << ',{"key":"service.version","value":{"stringValue":"${e.config.service_version}"}}'.bytes()
	if e.config.instance_id.len > 0 {
		sb << ',{"key":"service.instance.id","value":{"stringValue":"${e.config.instance_id}"}}'.bytes()
	}
	sb << ',{"key":"deployment.environment","value":{"stringValue":"${e.config.environment}"}}'.bytes()
	sb << ']},"scopeLogs":[{"scope":{"name":"datacore"},"logRecords":['.bytes()

	for i, entry in entries {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << e.build_log_record(entry).bytes()
	}

	sb << ']}]}]}'.bytes()
	return sb.bytestr()
}

// build_log_record builds a single OTLP log record
fn (e &OTLPExporter) build_log_record(entry LogEntry) string {
	// Map LogLevel to OTLP severity number (1-24 scale)
	severity_number := match entry.level {
		.trace { 1 }
		.debug { 5 }
		.info { 9 }
		.warn { 13 }
		.error { 17 }
		.fatal { 21 }
	}

	mut sb := []u8{cap: 512}
	sb << '{"timeUnixNano":"${entry.timestamp.unix_nano()}"'.bytes()
	sb << ',"observedTimeUnixNano":"${entry.timestamp.unix_nano()}"'.bytes()
	sb << ',"severityNumber":${severity_number}'.bytes()
	sb << ',"severityText":"${entry.level.str()}"'.bytes()
	sb << ',"body":{"stringValue":"${escape_json_string(entry.message)}"}'.bytes()

	// Add trace context if present
	if entry.context.trace_id.len > 0 {
		sb << ',"traceId":"${entry.context.trace_id}"'.bytes()
	}
	if entry.context.span_id.len > 0 {
		sb << ',"spanId":"${entry.context.span_id}"'.bytes()
	}

	// Add attributes
	sb << ',"attributes":['.bytes()
	sb << '{"key":"logger.name","value":{"stringValue":"${escape_json_string(entry.logger_name)}"}}'.bytes()

	for f in entry.fields {
		sb << ',{"key":"${escape_json_string(f.key)}","value":{"stringValue":"${escape_json_string(f.value)}"}}'.bytes()
	}
	sb << ']}'.bytes()

	return sb.bytestr()
}

// Span/Trace Export

// add_span adds a span to the buffer
pub fn (mut e OTLPExporter) add_span(span &Span) {
	e.buffer_lock.@lock()

	// Check buffer limit (v0.28.0) - prevent unbounded memory growth
	if e.span_buffer.len >= e.config.max_span_buffer_size {
		// Drop oldest entries (keep most recent)
		drop_count := e.span_buffer.len / 10
		e.span_buffer = e.span_buffer[drop_count..]
		e.spans_dropped += u64(drop_count)
	}

	e.span_buffer << span
	should_flush := e.span_buffer.len >= e.config.batch_size
	e.buffer_lock.unlock()

	if should_flush {
		spawn e.flush()
	}
}

// export_spans exports spans to OTLP endpoint
fn (mut e OTLPExporter) export_spans(spans []&Span) {
	if spans.len == 0 || e.config.endpoint.len == 0 {
		return
	}

	payload := e.build_spans_payload(spans)
	endpoint := '${e.config.endpoint}/v1/traces'

	e.send_with_retry(endpoint, payload)
}

// build_spans_payload builds OTLP traces JSON payload
fn (e &OTLPExporter) build_spans_payload(spans []&Span) string {
	mut sb := []u8{cap: 2048}
	sb << '{"resourceSpans":[{"resource":{"attributes":['.bytes()
	sb << '{"key":"service.name","value":{"stringValue":"${e.config.service_name}"}}'.bytes()
	sb << ',{"key":"service.version","value":{"stringValue":"${e.config.service_version}"}}'.bytes()
	sb << ']},"scopeSpans":[{"scope":{"name":"datacore"},"spans":['.bytes()

	for i, span in spans {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << e.build_span_record(span).bytes()
	}

	sb << ']}]}]}'.bytes()
	return sb.bytestr()
}

// build_span_record builds a single OTLP span record
fn (e &OTLPExporter) build_span_record(span &Span) string {
	// Map SpanKind to OTLP (1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER)
	kind := match span.kind {
		.internal { 1 }
		.server { 2 }
		.client { 3 }
		.producer { 4 }
		.consumer { 5 }
	}

	// Map SpanStatus to OTLP (0=UNSET, 1=OK, 2=ERROR)
	status_code := match span.status {
		.unset { 0 }
		.ok { 1 }
		.error { 2 }
	}

	mut sb := []u8{cap: 512}
	sb << '{"traceId":"${span.context.trace_id}"'.bytes()
	sb << ',"spanId":"${span.context.span_id}"'.bytes()
	if span.context.parent_id.len > 0 {
		sb << ',"parentSpanId":"${span.context.parent_id}"'.bytes()
	}
	sb << ',"name":"${escape_json_string(span.name)}"'.bytes()
	sb << ',"kind":${kind}'.bytes()
	sb << ',"startTimeUnixNano":"${span.start_time.unix_nano()}"'.bytes()
	sb << ',"endTimeUnixNano":"${span.end_time.unix_nano()}"'.bytes()

	// Attributes
	sb << ',"attributes":['.bytes()
	for i, attr in span.attributes {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << e.build_attribute(attr).bytes()
	}
	sb << ']'.bytes()

	// Events
	if span.events.len > 0 {
		sb << ',"events":['.bytes()
		for i, event in span.events {
			if i > 0 {
				sb << ','.bytes()
			}
			sb << '{"name":"${escape_json_string(event.name)}"'.bytes()
			sb << ',"timeUnixNano":"${event.timestamp.unix_nano()}"}'.bytes()
		}
		sb << ']'.bytes()
	}

	// Status
	sb << ',"status":{"code":${status_code}'.bytes()
	if span.status_msg.len > 0 {
		sb << ',"message":"${escape_json_string(span.status_msg)}"'.bytes()
	}
	sb << '}'.bytes()

	sb << '}'.bytes()
	return sb.bytestr()
}

// build_attribute builds OTLP attribute JSON
fn (e &OTLPExporter) build_attribute(attr SpanAttribute) string {
	key := escape_json_string(attr.key)
	value := match attr.value {
		string {
			'{"stringValue":"${escape_json_string(attr.value as string)}"}'
		}
		i64 {
			'{"intValue":"${attr.value as i64}"}'
		}
		f64 {
			'{"doubleValue":${attr.value as f64}}'
		}
		bool {
			'{"boolValue":${attr.value as bool}}'
		}
		[]string {
			vals := (attr.value as []string).map(fn (s string) string {
				return '{"stringValue":"${escape_json_string(s)}"}'
			})
			'{"arrayValue":{"values":[${vals.join(',')}]}}'
		}
	}
	return '{"key":"${key}","value":${value}}'
}

// HTTP Transport

// send_with_retry sends HTTP request with retry logic
fn (e &OTLPExporter) send_with_retry(endpoint string, payload string) {
	mut last_err := ''

	for attempt in 0 .. e.config.retry_count {
		if attempt > 0 {
			time.sleep(time.millisecond * e.config.retry_delay_ms)
		}

		result := e.send_http(endpoint, payload)
		if result {
			return
		}
		last_err = 'attempt ${attempt + 1} failed'
	}

	// Log failure (avoid recursion by writing directly)
	eprint('{"level":"WARN","msg":"OTLP export failed","endpoint":"${endpoint}","error":"${last_err}"}\n')
}

// send_http sends HTTP POST request to OTLP endpoint
fn (e &OTLPExporter) send_http(endpoint string, payload string) bool {
	mut req := http.Request{
		method: .post
		url:    endpoint
		data:   payload
	}
	req.add_header(.content_type, 'application/json')
	req.add_header(.accept, 'application/json')

	// Set timeout
	// Note: V's http client doesn't have direct timeout support
	// In production, consider using a custom TCP client with timeout

	resp := req.do() or { return false }

	return resp.status_code >= 200 && resp.status_code < 300
}

// Global OTLP Exporter (Singleton)

// OTLPExporterHolder holds the singleton instance
struct OTLPExporterHolder {
mut:
	exporter &OTLPExporter = unsafe { nil }
	lock     sync.Mutex
}

const otlp_holder = &OTLPExporterHolder{}

// init_otlp_exporter initializes the global OTLP exporter
pub fn init_otlp_exporter(config OTLPConfig) {
	mut holder := unsafe { otlp_holder }
	holder.lock.@lock()
	defer { holder.lock.unlock() }

	if holder.exporter != unsafe { nil } {
		holder.exporter.stop()
	}

	holder.exporter = new_otlp_exporter(config)
	holder.exporter.start()
}

// get_otlp_exporter returns the global OTLP exporter
pub fn get_otlp_exporter() ?&OTLPExporter {
	holder := unsafe { otlp_holder }
	if holder.exporter == unsafe { nil } {
		return none
	}
	return holder.exporter
}

// shutdown_otlp_exporter stops and flushes the global exporter
pub fn shutdown_otlp_exporter() {
	mut holder := unsafe { otlp_holder }
	holder.lock.@lock()
	defer { holder.lock.unlock() }

	if holder.exporter != unsafe { nil } {
		holder.exporter.stop()
		holder.exporter = unsafe { nil }
	}
}
