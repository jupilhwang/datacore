// Audit logging interface following DIP.
// Abstracts audit event recording from the infrastructure auth layer.
module port

/// AuditLoggerPort abstracts authentication audit logging.
/// Implemented by infra/auth AuditLogger.
pub interface AuditLoggerPort {
mut:
	/// Logs a successful authentication event.
	log_auth_success(client_ip string, principal string, mechanism string)

	/// Logs a failed authentication event.
	log_auth_failure(client_ip string, reason string)
}
