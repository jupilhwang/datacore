module observability

import service.port

/// LoggerAdapter wraps observability.Logger to satisfy port.LoggerPort interface.
pub struct LoggerAdapter {
mut:
	logger &Logger
}

pub fn new_logger_adapter(logger &Logger) &LoggerAdapter {
	return &LoggerAdapter{
		logger: logger
	}
}

pub fn (mut la LoggerAdapter) debug(msg string, fields ...port.LogField) {
	la.logger.debug(msg, ...fields)
}

pub fn (mut la LoggerAdapter) info(msg string, fields ...port.LogField) {
	la.logger.info(msg, ...fields)
}

pub fn (mut la LoggerAdapter) warn(msg string, fields ...port.LogField) {
	la.logger.warn(msg, ...fields)
}

pub fn (mut la LoggerAdapter) error(msg string, fields ...port.LogField) {
	la.logger.error(msg, ...fields)
}

pub fn (mut la LoggerAdapter) trace(msg string, fields ...port.LogField) {
	la.logger.trace(msg, ...fields)
}
