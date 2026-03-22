module port

/// LoggerPort abstracts logging operations for the service layer.
/// This enables service-layer code to log without depending on
/// the infrastructure observability module directly.
pub interface LoggerPort {
mut:
	debug(msg string, fields ...LogField)
	info(msg string, fields ...LogField)
	warn(msg string, fields ...LogField)
	error(msg string, fields ...LogField)
}

/// NoopLogger is a silent logger for use when no logger is provided.
pub struct NoopLogger {}

pub fn new_noop_logger() &NoopLogger {
	return &NoopLogger{}
}

pub fn (mut l NoopLogger) debug(msg string, fields ...LogField) {}

pub fn (mut l NoopLogger) info(msg string, fields ...LogField) {}

pub fn (mut l NoopLogger) warn(msg string, fields ...LogField) {}

pub fn (mut l NoopLogger) error(msg string, fields ...LogField) {}
