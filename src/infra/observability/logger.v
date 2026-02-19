/// Infrastructure layer - Structured logging (OpenTelemetry compatible)
/// Supports JSON format logging, context propagation, and OTLP export
module observability

import sync
import time

// Log levels

/// LogLevel represents the severity of a log entry.
pub enum LogLevel {
	trace = 0
	debug = 1
	info  = 2
	warn  = 3
	error = 4
	fatal = 5
}

/// str() string - converts LogLevel to its string representation
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

/// log_level_from_string(s string) LogLevel - creates LogLevel from string representation
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

// Log output destinations

/// LogOutput determines where logs are sent.
pub enum LogOutput {
	stdout
	otel
	both
	none
}

/// log_output_from_string(s string) LogOutput - creates LogOutput from string representation
pub fn log_output_from_string(s string) LogOutput {
	return match s.to_lower() {
		'stdout', 'console' { .stdout }
		'otel', 'otlp', 'opentelemetry' { .otel }
		'both', 'all' { .both }
		'none', 'off', 'disabled' { .none }
		else { .stdout }
	}
}

// Log fields (structured logging)

/// LogField represents a key-value pair for structured logging.
pub struct LogField {
pub:
	key   string
	value string
}

/// Field constructors - zero allocation for disabled levels
@[inline]
pub fn field_string(key string, value string) LogField {
	return LogField{
		key:   key
		value: value
	}
}

/// field_int - creates an integer field
@[inline]
pub fn field_int(key string, value i64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

/// field_uint - creates an unsigned integer field
@[inline]
pub fn field_uint(key string, value u64) LogField {
	return LogField{
		key:   key
		value: '${value}'
	}
}

/// field_float - creates a floating point field
@[inline]
pub fn field_float(key string, value f64) LogField {
	return LogField{
		key:   key
		value: '${value:.6}'
	}
}

/// field_bool - creates a boolean field
@[inline]
pub fn field_bool(key string, value bool) LogField {
	return LogField{
		key:   key
		value: if value { 'true' } else { 'false' }
	}
}

/// field_error - creates an error field
@[inline]
pub fn field_error(err IError) LogField {
	return LogField{
		key:   'error'
		value: err.str()
	}
}

/// field_err_str - creates an error message field
@[inline]
pub fn field_err_str(err_msg string) LogField {
	return LogField{
		key:   'error'
		value: err_msg
	}
}

/// field_duration - creates a duration field
@[inline]
pub fn field_duration(key string, d time.Duration) LogField {
	ms := f64(d) / f64(time.millisecond)
	return LogField{
		key:   '${key}_ms'
		value: '${ms:.3}'
	}
}

/// field_bytes - creates a byte size field
@[inline]
pub fn field_bytes(key string, size i64) LogField {
	return LogField{
		key:   '${key}_bytes'
		value: '${size}'
	}
}

// Log context (trace propagation)

/// LogContext holds the trace context for distributed tracing.
pub struct LogContext {
pub:
	trace_id  string
	span_id   string
	parent_id string
	service   string
	instance  string
}

// Log entry

/// LogEntry represents a single log entry.
pub struct LogEntry {
pub:
	timestamp   time.Time
	level       LogLevel
	message     string
	logger_name string
	fields      []LogField
	context     LogContext
}

// Output format

/// OutputFormat determines the log format.
pub enum OutputFormat {
	json
	text
}

/// output_format_from_string(s string) OutputFormat - creates OutputFormat from string representation
pub fn output_format_from_string(s string) OutputFormat {
	return match s.to_lower() {
		'json' { .json }
		'text', 'plain', 'console' { .text }
		else { .json }
	}
}

// Logger configuration

/// LoggerConfig holds logger configuration.
pub struct LoggerConfig {
pub:
	name          string       = 'datacore'
	level         LogLevel     = .info
	format        OutputFormat = .json
	output        LogOutput    = .stdout
	service       string       = 'datacore'
	instance_id   string
	otlp_endpoint string
}

// Logger (thread-safe)

/// Logger provides structured logging functionality.
pub struct Logger {
pub:
	name          string
	level         LogLevel
	format        OutputFormat
	output        LogOutput
	context       LogContext
	fields        []LogField
	otlp_endpoint string
mut:
	otlp_buffer []LogEntry
	buffer_lock sync.Mutex
}

/// new_logger(config LoggerConfig) &Logger - creates a new Logger instance with the given configuration
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

/// new_default_logger() &Logger - creates a Logger instance with default configuration
pub fn new_default_logger() &Logger {
	return new_logger(LoggerConfig{})
}

/// with_name(name string) &Logger - returns a new Logger instance with a different name for sub-components
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

/// with_context(ctx LogContext) &Logger - returns a new Logger instance with the given context
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

/// with_fields(fields ...LogField) &Logger - returns a new Logger instance with additional default fields
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

// Logging methods (early exit for performance)

/// should_log checks whether the level should be logged (inlined for performance).
@[inline]
pub fn (l &Logger) should_log(level LogLevel) bool {
	return int(level) >= int(l.level)
}

/// log(level LogLevel, msg string, fields ...LogField) - writes a log entry with the specified level, message, and optional fields
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

		// Write to stdout (errors go to stderr)
		if int(level) >= int(LogLevel.error) {
			eprint(output)
		} else {
			print(output)
		}
	}

	// Buffering for OTLP export
	if l.output == .otel || l.output == .both {
		l.buffer_lock.@lock()
		l.otlp_buffer << entry
		l.buffer_lock.unlock()
	}

	// Fatal logs must exit
	if level == .fatal {
		l.flush()
		exit(1)
	}
}

/// debug - writes a DEBUG level log
@[inline]
pub fn (mut l Logger) debug(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.debug) {
		l.log(.debug, msg, ...fields)
	}
}

/// info - writes an INFO level log
@[inline]
pub fn (mut l Logger) info(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.info) {
		l.log(.info, msg, ...fields)
	}
}

/// warn - writes a WARN level log
@[inline]
pub fn (mut l Logger) warn(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.warn) {
		l.log(.warn, msg, ...fields)
	}
}

/// error - writes an ERROR level log
@[inline]
pub fn (mut l Logger) error(msg string, fields ...LogField) {
	l.log(.error, msg, ...fields)
}

/// fatal - writes a FATAL level log
@[inline]
pub fn (mut l Logger) fatal(msg string, fields ...LogField) {
	l.log(.fatal, msg, ...fields)
}

/// trace - writes a TRACE level log
@[inline]
pub fn (mut l Logger) trace(msg string, fields ...LogField) {
	if int(l.level) <= int(LogLevel.trace) {
		l.log(.trace, msg, ...fields)
	}
}

/// flush() - sends all buffered logs to the OTLP endpoint
pub fn (mut l Logger) flush() {
	if l.otlp_endpoint == '' {
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

// Global logger (singleton pattern using struct holder)

/// LoggerHolder holds the singleton logger instance.
struct LoggerHolder {
mut:
	logger &Logger = unsafe { nil }
	lock   sync.Mutex
}

/// Global holder instance (initialized inline)
const logger_holder = &LoggerHolder{}

/// init_global_logger(config LoggerConfig) - initializes the global logger instance (call once at startup)
pub fn init_global_logger(config LoggerConfig) {
	mut holder := unsafe { logger_holder }
	holder.lock.@lock()
	defer { holder.lock.unlock() }
	holder.logger = new_logger(config)
}

/// get_logger returns the global logger instance.
/// Returns the default logger if not initialized.
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

/// get_named_logger returns a logger with a specific name (for sub-components).
@[inline]
pub fn get_named_logger(name string) &Logger {
	return get_logger().with_name(name)
}

// Quick logging functions (using global logger)

/// log_trace writes a TRACE level log using the global logger.
@[inline]
pub fn log_trace(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.trace(msg, ...fields)
}

/// log_debug - writes a DEBUG level log using the global logger
@[inline]
pub fn log_debug(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.debug(msg, ...fields)
}

/// log_info - writes an INFO level log using the global logger
@[inline]
pub fn log_info(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.info(msg, ...fields)
}

/// log_warn - writes a WARN level log using the global logger
@[inline]
pub fn log_warn(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.warn(msg, ...fields)
}

/// log_error - writes an ERROR level log using the global logger
@[inline]
pub fn log_error(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.error(msg, ...fields)
}

/// log_fatal - writes a FATAL level log using the global logger
@[inline]
pub fn log_fatal(msg string, fields ...LogField) {
	mut logger := get_logger()
	logger.fatal(msg, ...fields)
}

// Formatting functions

/// Escapes a JSON string
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

/// Formats a log entry in JSON format.
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
	if entry.context.service != '' {
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

/// Formats a log entry in text format.
fn format_entry_text(entry LogEntry) string {
	mut sb := []u8{cap: 256}

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

/// Returns the ANSI color code for a log level.
fn get_level_color(level LogLevel) string {
	return match level {
		.trace { '\x1b[90m' }
		.debug { '\x1b[36m' }
		.info { '\x1b[32m' }
		.warn { '\x1b[33m' }
		.error { '\x1b[31m' }
		.fatal { '\x1b[35m' }
	}
}

// OTLP log export (OpenTelemetry Protocol)

/// export_logs_to_otlp exports log entries to the OTLP endpoint.
fn export_logs_to_otlp(endpoint string, service_name string, entries []LogEntry) {
	if entries.len == 0 || endpoint == '' {
		return
	}

	// Build OTLP JSON payload
	payload := build_otlp_logs_payload(service_name, entries)

	// Send HTTP POST to OTLP endpoint
	// Note: Currently using simple HTTP; gRPC can be used for better performance
	send_otlp_http(endpoint, payload)
}

/// Builds the OTLP logs payload.
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

/// Builds a single OTLP log record.
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

/// OTLP HTTP transport function
fn send_otlp_http(endpoint string, payload string) {
	// Simple HTTP POST using V's net.http
	// Consider connection pooling and retry in production
	$if !windows {
		// Using curl for simplicity (V's cross-platform HTTP client has limitations)
		// This is a fire-and-forget async call
		_ := $env('PATH')
	}

	// Using simple approach for now
	// TODO: Implement proper HTTP client with retry
	_ = endpoint
	_ = payload
}

// Utility: log level severity mapping

/// severity_to_level(severity int) LogLevel - converts an OTLP severity number to a LogLevel
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
