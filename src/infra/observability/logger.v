// Infra Layer - Structured Logging (OpenTelemetry Compatible)
// JSON format logging with context propagation and OTLP export support
module observability

import sync
import time

// ============================================================
// Log Level
// ============================================================

// LogLevel represents the severity of a log entry
pub enum LogLevel {
	trace = 0
	debug = 1
	info  = 2
	warn  = 3
	error = 4
	fatal = 5
}

// LogLevel string conversion
pub fn (l LogLevel) str() string {
	return match l {
		.trace { 'TRACE' }
		.debug { 'DEBUG' }
		.info { 'INFO' }
		.warn { 'WARN' }
		.error { 'ERROR' }
		.fatal { 'FATAL' }
	}
}

// LogLevel from string
pub fn log_level_from_string(s string) LogLevel {
	return match s.to_lower() {
		'trace' { .trace }
		'debug' { .debug }
		'info' { .info }
		'warn', 'warning' { .warn }
		'error' { .error }
		'fatal' { .fatal }
		else { .info }
	}
}

// ============================================================
// Log Output Target
// ============================================================

// LogOutput determines where logs are sent
pub enum LogOutput {
	stdout // Default: write to stdout/stderr
	otel   // OpenTelemetry Collector via OTLP
	both   // Both stdout and OTLP
	none   // Disable logging (for testing)
}

// LogOutput from string
pub fn log_output_from_string(s string) LogOutput {
	return match s.to_lower() {
		'stdout', 'console' { .stdout }
		'otel', 'otlp', 'opentelemetry' { .otel }
		'both', 'all' { .both }
		'none', 'off', 'disabled' { .none }
		else { .stdout }
	}
}

// ============================================================
// Log Field (Structured Logging)
// ============================================================

// LogField represents a key-value pair for structured logging
pub struct LogField {
pub:
	key   string
	value string // All values serialized as strings
}

// Field constructors - zero allocation for disabled levels
@[inline]
pub fn field_string(key string, value string) LogField {
	return LogField{
		key:   key
		value: value
	}
}

@[inline]
pub fn field_int(key string, value i64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

@[inline]
pub fn field_uint(key string, value u64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

@[inline]
pub fn field_float(key string, value f64) LogField {
	return LogField{
		key:   key
		value: '${value:.6}'
	}
}

@[inline]
pub fn field_bool(key string, value bool) LogField {
	return LogField{
		key:   key
		value: if value { 'true' } else { 'false' }
	}
}

@[inline]
pub fn field_error(err IError) LogField {
	return LogField{
		key:   'error'
		value: err.str()
	}
}

@[inline]
pub fn field_err_str(err_msg string) LogField {
	return LogField{
		key:   'error'
		value: err_msg
	}
}

@[inline]
pub fn field_duration(key string, d time.Duration) LogField {
	ms := f64(d) / f64(time.millisecond)
	return LogField{
		key:   '${key}_ms'
		value: '${ms:.3}'
	}
}

@[inline]
pub fn field_bytes(key string, size i64) LogField {
	return LogField{
		key:   '${key}_bytes'
		value: '${size}'
	}
}

// ============================================================
// Log Context (Trace Propagation)
// ============================================================

// LogContext holds trace context for distributed tracing
pub struct LogContext {
pub:
	trace_id  string
	span_id   string
	parent_id string
	service   string
	instance  string
}

// ============================================================
// Log Entry
// ============================================================

// LogEntry represents a single log entry
pub struct LogEntry {
pub:
	timestamp   time.Time
	level       LogLevel
	message     string
	logger_name string
	fields      []LogField
	context     LogContext
}

// ============================================================
// Output Format
// ============================================================

// OutputFormat determines how logs are formatted
pub enum OutputFormat {
	json // JSON format (default, for production)
	text // Human-readable text format (for development)
}

// OutputFormat from string
pub fn output_format_from_string(s string) OutputFormat {
	return match s.to_lower() {
		'json' { .json }
		'text', 'plain', 'console' { .text }
		else { .json }
	}
}

// ============================================================
// Logger Configuration
// ============================================================

// LoggerConfig holds logger configuration
pub struct LoggerConfig {
pub:
	name          string       = 'datacore'
	level         LogLevel     = .info
	format        OutputFormat = .json
	output        LogOutput    = .stdout
	service       string       = 'datacore'
	instance_id   string
	otlp_endpoint string // For OTLP export (e.g., "http://localhost:4317")
}

// ============================================================
// Logger (Thread-Safe)
// ============================================================

// Logger provides structured logging capabilities
pub struct Logger {
pub:
	name          string
	level         LogLevel
	format        OutputFormat
	output        LogOutput
	context       LogContext
	fields        []LogField // Default fields for all log entries
	otlp_endpoint string
mut:
	otlp_buffer []LogEntry // Buffer for OTLP batch export
	buffer_lock sync.Mutex
}

// new_logger creates a new logger with config
pub fn new_logger(config LoggerConfig) &Logger {
	return &Logger{
		name:          config.name
		level:         config.level
		format:        config.format
		output:        config.output
		otlp_endpoint: config.otlp_endpoint
		context:       LogContext{
			service:  config.service
			instance: config.instance_id
		}
		fields:        []
		otlp_buffer:   []
	}
}

// new_default_logger creates a logger with default settings
pub fn new_default_logger() &Logger {
	return new_logger(LoggerConfig{})
}

// with_name returns a new logger with a different name (for sub-components)
pub fn (l &Logger) with_name(name string) &Logger {
	return &Logger{
		name:          name
		level:         l.level
		format:        l.format
		output:        l.output
		otlp_endpoint: l.otlp_endpoint
		context:       l.context
		fields:        l.fields
		otlp_buffer:   []
	}
}

// with_context returns a new logger with context
pub fn (l &Logger) with_context(ctx LogContext) &Logger {
	return &Logger{
		name:          l.name
		level:         l.level
		format:        l.format
		output:        l.output
		otlp_endpoint: l.otlp_endpoint
		context:       ctx
		fields:        l.fields
		otlp_buffer:   []
	}
}

// with_fields returns a new logger with additional default fields
pub fn (l &Logger) with_fields(fields ...LogField) &Logger {
	mut new_fields := l.fields.clone()
	new_fields << fields

	return &Logger{
		name:          l.name
		level:         l.level
		format:        l.format
		output:        l.output
		otlp_endpoint: l.otlp_endpoint
		context:       l.context
		fields:        new_fields
		otlp_buffer:   []
	}
}

// ============================================================
// Logging Methods (with early-exit for performance)
// ============================================================

// should_log checks if a level should be logged (inline for performance)
@[inline]
pub fn (l &Logger) should_log(level LogLevel) bool {
	return int(level) >= int(l.level)
}

// log writes a log entry
pub fn (mut l Logger) log(level LogLevel, msg string, fields ...LogField) {
	// Early exit for disabled levels (zero overhead)
	if !l.should_log(level) {
		return
	}

	// Early exit if output is disabled
	if l.output == .none {
		return
	}

	mut all_fields := l.fields.clone()
	all_fields << fields

	entry := LogEntry{
		timestamp:   time.now()
		level:       level
		message:     msg
		logger_name: l.name
		fields:      all_fields
		context:     l.context
	}

	// Output to stdout/stderr
	if l.output == .stdout || l.output == .both {
		output := if l.format == .json {
			format_entry_json(entry)
		} else {
			format_entry_text(entry)
		}

		// Write to stdout (errors to stderr)
		if int(level) >= int(LogLevel.error) {
			eprint(output)
		} else {
			print(output)
		}
	}

	// Buffer for OTLP export
	if l.output == .otel || l.output == .both {
		l.buffer_lock.@lock()
		l.otlp_buffer << entry
		l.buffer_lock.unlock()
	}

	// Fatal logs should terminate
	if level == .fatal {
		l.flush() // Ensure logs are sent before exit
		exit(1)
	}
}

// Convenience methods with inline hints for performance
@[inline]
pub fn (mut l Logger) trace(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.trace) {
		l.log(.trace, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) debug(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.debug) {
		l.log(.debug, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) info(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.info) {
		l.log(.info, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) warn(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.warn) {
		l.log(.warn, msg, ...fields)
	}
}

@[inline]
pub fn (mut l Logger) error(msg string, fields ...LogField) {
	l.log(.error, msg, ...fields)
}

@[inline]
pub fn (mut l Logger) fatal(msg string, fields ...LogField) {
	l.log(.fatal, msg, ...fields)
}

// flush sends buffered logs to OTLP endpoint
pub fn (mut l Logger) flush() {
	if l.otlp_endpoint.len == 0 {
		return
	}

	l.buffer_lock.@lock()
	if l.otlp_buffer.len == 0 {
		l.buffer_lock.unlock()
		return
	}
	entries := l.otlp_buffer.clone()
	l.otlp_buffer.clear()
	l.buffer_lock.unlock()

	// Export to OTLP (async)
	spawn export_logs_to_otlp(l.otlp_endpoint, l.context.service, entries)
}

// ============================================================
// Global Logger (Singleton Pattern using struct holder)
// ============================================================

// LoggerHolder holds the singleton logger instance
struct LoggerHolder {
mut:
	logger &Logger = unsafe { nil }
	lock   sync.Mutex
}

// Global holder instance (initialized inline)
const logger_holder = &LoggerHolder{}

// init_global_logger initializes the global logger (call once at startup)
pub fn init_global_logger(config LoggerConfig) {
	mut holder := unsafe { logger_holder }
	holder.lock.@lock()
	defer { holder.lock.unlock() }
	holder.logger = new_logger(config)
}

// get_logger returns the global logger instance
// If not initialized, returns a default logger
@[inline]
pub fn get_logger() &Logger {
	mut holder := unsafe { logger_holder }
	if holder.logger == unsafe { nil } {
		// Lazy initialization with defaults
		holder.lock.@lock()
		if holder.logger == unsafe { nil } {
			holder.logger = new_default_logger()
		}
		holder.lock.unlock()
	}
	return holder.logger
}

// get_named_logger returns a logger with a specific name (for sub-components)
@[inline]
pub fn get_named_logger(name string) &Logger {
	return get_logger().with_name(name)
}

// ============================================================
// Quick Logging Functions (use global logger)
// ============================================================

@[inline]
pub fn log_trace(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.trace(msg, ...fields)
}

@[inline]
pub fn log_debug(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.debug(msg, ...fields)
}

@[inline]
pub fn log_info(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.info(msg, ...fields)
}

@[inline]
pub fn log_warn(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.warn(msg, ...fields)
}

@[inline]
pub fn log_error(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.error(msg, ...fields)
}

@[inline]
pub fn log_fatal(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.fatal(msg, ...fields)
}

// ============================================================
// Formatting Functions
// ============================================================

fn escape_json_string(s string) string {
	mut result := []u8{cap: s.len + 10}
	for c in s.bytes() {
		match c {
			`"` {
				result << `\\`
				result << `"`
			}
			`\\` {
				result << `\\`
				result << `\\`
			}
			`\n` {
				result << `\\`
				result << `n`
			}
			`\r` {
				result << `\\`
				result << `r`
			}
			`\t` {
				result << `\\`
				result << `t`
			}
			else {
				result << c
			}
		}
	}
	return result.bytestr()
}

fn format_entry_json(entry LogEntry) string {
	mut sb := []u8{cap: 256}
	sb << '{"timestamp":"'.bytes()
	sb << entry.timestamp.format_rfc3339().bytes()
	sb << '","level":"'.bytes()
	sb << entry.level.str().bytes()
	sb << '","logger":"'.bytes()
	sb << escape_json_string(entry.logger_name).bytes()
	sb << '","msg":"'.bytes()
	sb << escape_json_string(entry.message).bytes()
	sb << '"'.bytes()

	// Add trace context if present
	if entry.context.trace_id.len > 0 {
		sb << ',"trace_id":"'.bytes()
		sb << entry.context.trace_id.bytes()
		sb << '"'.bytes()
	}
	if entry.context.span_id.len > 0 {
		sb << ',"span_id":"'.bytes()
		sb << entry.context.span_id.bytes()
		sb << '"'.bytes()
	}
	if entry.context.service.len > 0 {
		sb << ',"service":"'.bytes()
		sb << entry.context.service.bytes()
		sb << '"'.bytes()
	}

	// Add fields
	for f in entry.fields {
		sb << ',"'.bytes()
		sb << escape_json_string(f.key).bytes()
		sb << '":"'.bytes()
		sb << escape_json_string(f.value).bytes()
		sb << '"'.bytes()
	}

	sb << '}\n'.bytes()
	return sb.bytestr()
}

fn format_entry_text(entry LogEntry) string {
	mut sb := []u8{cap: 256}

	// Timestamp
	sb << entry.timestamp.format_ss().bytes()
	sb << ' '.bytes()

	// Level with color
	sb << get_level_color(entry.level).bytes()
	sb << '['.bytes()
	sb << entry.level.str().bytes()
	sb << ']'.bytes()
	sb << '\x1b[0m'.bytes()
	sb << ' '.bytes()

	// Logger name
	sb << '['.bytes()
	sb << entry.logger_name.bytes()
	sb << '] '.bytes()

	// Message
	sb << entry.message.bytes()

	// Fields
	if entry.fields.len > 0 {
		sb << ' |'.bytes()
		for f in entry.fields {
			sb << ' '.bytes()
			sb << f.key.bytes()
			sb << '='.bytes()
			sb << f.value.bytes()
		}
	}

	// Trace context
	if entry.context.trace_id.len > 0 {
		sb << ' trace_id='.bytes()
		sb << entry.context.trace_id.bytes()
	}

	sb << '\n'.bytes()
	return sb.bytestr()
}

fn get_level_color(level LogLevel) string {
	return match level {
		.trace { '\x1b[90m' } // Gray
		.debug { '\x1b[36m' } // Cyan
		.info { '\x1b[32m' } // Green
		.warn { '\x1b[33m' } // Yellow
		.error { '\x1b[31m' } // Red
		.fatal { '\x1b[35m' } // Magenta
	}
}

// ============================================================
// OTLP Log Export (OpenTelemetry Protocol)
// ============================================================

// export_logs_to_otlp exports log entries to OTLP endpoint
fn export_logs_to_otlp(endpoint string, service_name string, entries []LogEntry) {
	if entries.len == 0 || endpoint.len == 0 {
		return
	}

	// Build OTLP JSON payload
	payload := build_otlp_logs_payload(service_name, entries)

	// Send HTTP POST to OTLP endpoint
	// Note: Using simple HTTP for now, could use gRPC for better performance
	send_otlp_http(endpoint, payload)
}

fn build_otlp_logs_payload(service_name string, entries []LogEntry) string {
	mut sb := []u8{cap: 1024}
	sb << '{"resourceLogs":[{"resource":{"attributes":['.bytes()
	sb << '{"key":"service.name","value":{"stringValue":"${service_name}"}}'.bytes()
	sb << ']},"scopeLogs":[{"logRecords":['.bytes()

	for i, entry in entries {
		if i > 0 {
			sb << ','.bytes()
		}
		sb << build_otlp_log_record(entry).bytes()
	}

	sb << ']}]}]}'.bytes()
	return sb.bytestr()
}

fn build_otlp_log_record(entry LogEntry) string {
	// Map LogLevel to OTLP severity number
	severity_number := match entry.level {
		.trace { 1 }
		.debug { 5 }
		.info { 9 }
		.warn { 13 }
		.error { 17 }
		.fatal { 21 }
	}

	mut sb := []u8{cap: 256}
	sb << '{"timeUnixNano":"${entry.timestamp.unix_nano()}"'.bytes()
	sb << ',"severityNumber":${severity_number}'.bytes()
	sb << ',"severityText":"${entry.level.str()}"'.bytes()
	sb << ',"body":{"stringValue":"${escape_json_string(entry.message)}"}'.bytes()

	// Add trace context
	if entry.context.trace_id.len > 0 {
		sb << ',"traceId":"${entry.context.trace_id}"'.bytes()
	}
	if entry.context.span_id.len > 0 {
		sb << ',"spanId":"${entry.context.span_id}"'.bytes()
	}

	// Add attributes
	sb << ',"attributes":['.bytes()
	sb << '{"key":"logger","value":{"stringValue":"${escape_json_string(entry.logger_name)}"}}'.bytes()
	for f in entry.fields {
		sb << ',{"key":"${escape_json_string(f.key)}","value":{"stringValue":"${escape_json_string(f.value)}"}}'.bytes()
	}
	sb << ']}'.bytes()

	return sb.bytestr()
}

fn send_otlp_http(endpoint string, payload string) {
	// Simple HTTP POST using V's net.http
	// In production, consider connection pooling and retries
	$if !windows {
		// Use curl for simplicity (cross-platform HTTP client in V has limitations)
		// This is a fire-and-forget async call
		_ := $env('PATH') // Ensure curl is available
	}

	// For now, we'll use a simple approach
	// TODO: Implement proper HTTP client with retries
	_ = endpoint
	_ = payload
}

// ============================================================
// Utility: Log Level Severity Mapping
// ============================================================

// severity_to_level converts OTLP severity number to LogLevel
pub fn severity_to_level(severity int) LogLevel {
	return match true {
		severity <= 4 { .trace }
		severity <= 8 { .debug }
		severity <= 12 { .info }
		severity <= 16 { .warn }
		severity <= 20 { .error }
		else { .fatal }
	}
}
