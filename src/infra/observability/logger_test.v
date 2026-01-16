// Unit Tests - Structured Logger
module observability

import time

fn test_log_levels() {
    // Create logger with debug level
    logger := new_logger(LoggerConfig{
        name: 'test'
        level: .debug
        format: .text
    })
    
    // Should log returns true for levels >= debug
    assert logger.should_log(.trace) == false
    assert logger.should_log(.debug) == true
    assert logger.should_log(.info) == true
    assert logger.should_log(.warn) == true
    assert logger.should_log(.error) == true
    assert logger.should_log(.fatal) == true
}

fn test_log_level_info() {
    logger := new_logger(LoggerConfig{
        name: 'test'
        level: .info
    })
    
    assert logger.should_log(.trace) == false
    assert logger.should_log(.debug) == false
    assert logger.should_log(.info) == true
    assert logger.should_log(.warn) == true
}

fn test_logger_with_context() {
    ctx := LogContext{
        trace_id: 'trace-123'
        span_id: 'span-456'
        service: 'test-service'
    }
    
    base_logger := new_logger(LoggerConfig{
        name: 'test'
        level: .info
    })
    
    logger := base_logger.with_context(ctx)
    
    assert logger.context.trace_id == 'trace-123'
    assert logger.context.span_id == 'span-456'
}

fn test_logger_with_default_fields() {
    base_logger := new_logger(LoggerConfig{
        name: 'test'
        level: .info
    })
    
    logger := base_logger.with_fields(
        field_string('component', 'protocol'),
        field_string('version', '1.0')
    )
    
    assert logger.fields.len == 2
    assert logger.fields[0].key == 'component'
    assert logger.fields[0].value == 'protocol'
}

fn test_log_level_from_string() {
    assert log_level_from_string('trace') == .trace
    assert log_level_from_string('DEBUG') == .debug
    assert log_level_from_string('Info') == .info
    assert log_level_from_string('WARN') == .warn
    assert log_level_from_string('error') == .error
    assert log_level_from_string('FATAL') == .fatal
    assert log_level_from_string('unknown') == .info  // default
}

fn test_log_level_str() {
    assert LogLevel.trace.str() == 'TRACE'
    assert LogLevel.debug.str() == 'DEBUG'
    assert LogLevel.info.str() == 'INFO'
    assert LogLevel.warn.str() == 'WARN'
    assert LogLevel.error.str() == 'ERROR'
    assert LogLevel.fatal.str() == 'FATAL'
}

fn test_escape_json_string() {
    assert escape_json_string('hello') == 'hello'
    assert escape_json_string('hello"world') == 'hello\\"world'
    assert escape_json_string('line1\nline2') == 'line1\\nline2'
    assert escape_json_string('tab\there') == 'tab\\there'
}

fn test_field_constructors() {
    f1 := field_string('key', 'value')
    assert f1.key == 'key'
    assert f1.value == 'value'
    
    f2 := field_int('num', 100)
    assert f2.key == 'num'
    assert f2.value == '100'
    
    f3 := field_float('pi', 3.14159)
    assert f3.key == 'pi'
    
    f4 := field_bool('flag', true)
    assert f4.key == 'flag'
    assert f4.value == 'true'
    
    f5 := field_bool('flag2', false)
    assert f5.value == 'false'
}

fn test_field_duration() {
    f := field_duration('latency', 1500 * time.millisecond)
    assert f.key == 'latency'
    assert f.value.contains('1500')
}

fn test_json_output_format() {
    entry := LogEntry{
        timestamp: time.parse('2025-01-16 10:30:00') or { time.now() }
        level: .info
        message: 'test message'
        logger_name: 'test'
        fields: [
            field_string('key', 'value'),
        ]
        context: LogContext{
            trace_id: 'trace-id'
            service: 'service-name'
        }
    }
    
    json := format_entry_json(entry)
    assert json.contains('"level":"INFO"')
    assert json.contains('"message":"test message"')
    assert json.contains('"trace_id":"trace-id"')
    assert json.contains('"key":"value"')
}

fn test_text_output_format() {
    entry := LogEntry{
        timestamp: time.now()
        level: .warn
        message: 'warning message'
        logger_name: 'test-logger'
        fields: [
            field_string('component', 'server'),
        ]
        context: LogContext{}
    }
    
    text := format_entry_text(entry)
    assert text.contains('[WARN]')
    assert text.contains('[test-logger]')
    assert text.contains('warning message')
    assert text.contains('component=server')
}

fn test_logger_name() {
    logger := new_logger(LoggerConfig{
        name: 'my-service'
        level: .info
    })
    
    assert logger.name == 'my-service'
}

fn test_default_logger() {
    logger := new_default_logger()
    
    assert logger.name == 'datacore'
    assert logger.level == .info
    assert logger.format == .json
}

fn test_logger_format() {
    json_logger := new_logger(LoggerConfig{
        name: 'test'
        format: .json
    })
    assert json_logger.format == .json
    
    text_logger := new_logger(LoggerConfig{
        name: 'test'
        format: .text
    })
    assert text_logger.format == .text
}

fn test_log_entry_fields() {
    entry := LogEntry{
        timestamp: time.now()
        level: .debug
        message: 'debug msg'
        logger_name: 'test'
        fields: [
            field_string('a', '1'),
            field_int('b', 2),
            field_bool('c', true),
        ]
        context: LogContext{}
    }
    
    assert entry.fields.len == 3
    assert entry.fields[0].key == 'a'
    assert entry.fields[1].key == 'b'
    assert entry.fields[2].key == 'c'
}

fn test_log_context_empty() {
    ctx := LogContext{}
    assert ctx.trace_id == ''
    assert ctx.span_id == ''
    assert ctx.service == ''
}

fn test_special_characters_in_json() {
    // Test escaping of special characters
    entry := LogEntry{
        timestamp: time.now()
        level: .info
        message: 'test "quoted" message\nwith newline'
        logger_name: 'test'
        fields: []
        context: LogContext{}
    }
    
    json := format_entry_json(entry)
    assert json.contains('\\"quoted\\"')
    assert json.contains('\\n')
}
