/// Infrastructure layer - Shared log_with_context helper
/// Provides a unified log_message function for all modules to eliminate duplication.
module observability

/// log_with_context logs a message with structured context using the named logger.
/// prefix is the module prefix (e.g. 'tcp', 'grpc', 's3') for the logger name.
/// component is the sub-component name (e.g. 'Server', 'Handler').
/// context is a map of key-value pairs for structured logging.
pub fn log_with_context(prefix string, level LogLevel, component string, message string, context map[string]string) {
	mut logger := get_named_logger('${prefix}.${component}')
	match level {
		.trace { logger.debug_map(message, context) }
		.debug { logger.debug_map(message, context) }
		.info { logger.info_map(message, context) }
		.warn { logger.warn_map(message, context) }
		.error { logger.error_map(message, context) }
		.fatal { logger.fatal_map(message, context) }
	}
}
