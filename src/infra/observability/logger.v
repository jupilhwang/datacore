// Infra Layer - Structured Logging (OpenTelemetry Compatible)
// JSON format logging with context propagation
module observability

import time
import os

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
        .info  { 'INFO' }
        .warn  { 'WARN' }
        .error { 'ERROR' }
        .fatal { 'FATAL' }
    }
}

// LogLevel from string
pub fn log_level_from_string(s string) LogLevel {
    return match s.to_lower() {
        'trace' { .trace }
        'debug' { .debug }
        'info'  { .info }
        'warn'  { .warn }
        'error' { .error }
        'fatal' { .fatal }
        else    { .info }
    }
}

// LogField represents a key-value pair for structured logging
pub struct LogField {
pub:
    key   string
    value string  // All values serialized as strings
}

// Field constructors
pub fn field_string(key string, value string) LogField {
    return LogField{ key: key, value: value }
}

pub fn field_int(key string, value i64) LogField {
    return LogField{ key: key, value: '${value}' }
}

pub fn field_float(key string, value f64) LogField {
    return LogField{ key: key, value: '${value}' }
}

pub fn field_bool(key string, value bool) LogField {
    return LogField{ key: key, value: if value { 'true' } else { 'false' } }
}

pub fn field_error(err IError) LogField {
    return LogField{ key: 'error', value: err.str() }
}

pub fn field_duration(key string, d time.Duration) LogField {
    ms := f64(d) / f64(time.millisecond)
    return LogField{ key: key, value: '${ms}' }
}

// LogContext holds trace context for distributed tracing
pub struct LogContext {
pub:
    trace_id   string
    span_id    string
    parent_id  string
    service    string
    instance   string
}

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

// OutputFormat determines how logs are formatted
pub enum OutputFormat {
    json
    text
}

// Logger provides structured logging capabilities
pub struct Logger {
pub:
    name        string
    level       LogLevel
    context     LogContext
    format      OutputFormat
    fields      []LogField  // Default fields for all log entries
}

// LoggerConfig holds logger configuration
pub struct LoggerConfig {
pub:
    name    string = 'datacore'
    level   LogLevel = .info
    format  OutputFormat = .json
    service string = 'datacore'
}

// new_logger creates a new logger with config
pub fn new_logger(config LoggerConfig) Logger {
    return Logger{
        name: config.name
        level: config.level
        format: config.format
        context: LogContext{
            service: config.service
        }
        fields: []
    }
}

// new_default_logger creates a logger with default settings
pub fn new_default_logger() Logger {
    return new_logger(LoggerConfig{})
}

// with_context returns a new logger with context
pub fn (l Logger) with_context(ctx LogContext) Logger {
    return Logger{
        name: l.name
        level: l.level
        format: l.format
        context: ctx
        fields: l.fields
    }
}

// with_fields returns a new logger with additional default fields
pub fn (l Logger) with_fields(fields ...LogField) Logger {
    mut new_fields := l.fields.clone()
    new_fields << fields
    
    return Logger{
        name: l.name
        level: l.level
        format: l.format
        context: l.context
        fields: new_fields
    }
}

// should_log checks if a level should be logged
fn (l Logger) should_log(level LogLevel) bool {
    return int(level) >= int(l.level)
}

// log writes a log entry
pub fn (l Logger) log(level LogLevel, msg string, fields ...LogField) {
    if !l.should_log(level) {
        return
    }
    
    mut all_fields := l.fields.clone()
    all_fields << fields
    
    entry := LogEntry{
        timestamp: time.now()
        level: level
        message: msg
        logger_name: l.name
        fields: all_fields
        context: l.context
    }
    
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
    
    // Fatal logs should terminate
    if level == .fatal {
        exit(1)
    }
}

pub fn (l Logger) trace(msg string, fields ...LogField) {
    l.log(.trace, msg, ...fields)
}

pub fn (l Logger) debug(msg string, fields ...LogField) {
    l.log(.debug, msg, ...fields)
}

pub fn (l Logger) info(msg string, fields ...LogField) {
    l.log(.info, msg, ...fields)
}

pub fn (l Logger) warn(msg string, fields ...LogField) {
    l.log(.warn, msg, ...fields)
}

pub fn (l Logger) error(msg string, fields ...LogField) {
    l.log(.error, msg, ...fields)
}

pub fn (l Logger) fatal(msg string, fields ...LogField) {
    l.log(.fatal, msg, ...fields)
}

// ============================================================
// Formatting Functions
// ============================================================

fn escape_json_string(s string) string {
    mut result := []u8{}
    for c in s.bytes() {
        match c {
            `"` { result << '\\'.bytes(); result << '"'.bytes() }
            `\\` { result << '\\'.bytes(); result << '\\'.bytes() }
            `\n` { result << '\\'.bytes(); result << 'n'.bytes() }
            `\r` { result << '\\'.bytes(); result << 'r'.bytes() }
            `\t` { result << '\\'.bytes(); result << 't'.bytes() }
            else { result << c }
        }
    }
    return result.bytestr()
}

fn format_entry_json(entry LogEntry) string {
    mut sb := []u8{}
    sb << '{"timestamp":"'.bytes()
    sb << entry.timestamp.format_rfc3339().bytes()
    sb << '","level":"'.bytes()
    sb << entry.level.str().bytes()
    sb << '","logger":"'.bytes()
    sb << escape_json_string(entry.logger_name).bytes()
    sb << '","message":"'.bytes()
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
    mut sb := []u8{}
    
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
        .trace { '\x1b[90m' }  // Gray
        .debug { '\x1b[36m' }  // Cyan
        .info  { '\x1b[32m' }  // Green
        .warn  { '\x1b[33m' }  // Yellow
        .error { '\x1b[31m' }  // Red
        .fatal { '\x1b[35m' }  // Magenta
    }
}

// ============================================================
// File Logger
// ============================================================

pub struct FileLogger {
pub:
    logger   Logger
    path     string
    max_size i64
mut:
    file     ?os.File
}

pub fn new_file_logger(config LoggerConfig, path string) FileLogger {
    mut fl := FileLogger{
        logger: new_logger(config)
        path: path
        max_size: 104857600  // 100MB
    }
    fl.open()
    return fl
}

fn (mut fl FileLogger) open() {
    fl.file = os.open_append(fl.path) or { return }
}

pub fn (mut fl FileLogger) log(level LogLevel, msg string, fields ...LogField) {
    if !fl.logger.should_log(level) {
        return
    }
    
    mut all_fields := fl.logger.fields.clone()
    all_fields << fields
    
    entry := LogEntry{
        timestamp: time.now()
        level: level
        message: msg
        logger_name: fl.logger.name
        fields: all_fields
        context: fl.logger.context
    }
    
    output := if fl.logger.format == .json {
        format_entry_json(entry)
    } else {
        format_entry_text(entry)
    }
    
    if mut file := fl.file {
        file.write_string(output) or {}
        file.flush()
    }
}

pub fn (mut fl FileLogger) close() {
    if mut file := fl.file {
        file.close()
    }
}
