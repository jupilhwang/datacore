// AuthSubHandler owns the dependencies specific to authentication and ACL operations.
//
// Holds a reference to HandlerContext for shared state, plus the auth manager,
// ACL manager, audit logger, and negotiated SASL mechanism.
module kafka

import domain
import service.port

/// AuthSubHandler groups the dependencies needed for SASL and ACL request handling.
/// The main Handler delegates auth-related work through this sub-handler.
pub struct AuthSubHandler {
pub mut:
	ctx                  &HandlerContext
	auth_manager         ?port.AuthManager
	acl_manager          ?port.AclManager
	audit_logger         ?port.AuditLoggerPort
	negotiated_mechanism ?domain.SaslMechanism
}
