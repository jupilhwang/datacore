// Defines authentication-related interfaces in the use-case layer, implemented in the adapter layer.
// Provides abstractions for SASL authentication and ACL management.
module port

import domain

/// UserStore is an interface for user storage and authentication.
/// Implemented in infra/auth.
pub interface UserStore {
mut:
	/// Retrieves a user by username.
	get_user(username string) !domain.User

	/// Creates a new user.
	create_user(username string, password string, mechanism domain.SaslMechanism) !domain.User

	/// Updates a user's password.
	update_password(username string, new_password string) !

	/// Deletes a user.
	delete_user(username string) !

	/// Returns a list of all users.
	list_users() ![]domain.User

	/// Validates a password for PLAIN authentication.
	/// Returns true if the password matches.
	validate_password(username string, password string) !bool
}

/// SaslAuthenticator is an interface for SASL authentication.
pub interface SaslAuthenticator {
	/// Returns the supported mechanism.
	mechanism() domain.SaslMechanism
mut:
	/// Performs authentication with the provided auth bytes.
	/// For PLAIN: auth_bytes format is [authzid]\0[authcid]\0[password]
	/// Returns AuthResult containing principal on success.
	authenticate(auth_bytes []u8) !domain.AuthResult

	/// For SCRAM: handles the next step of the challenge-response exchange.
	/// Returns AuthResult containing a challenge or final result.
	step(response []u8) !domain.AuthResult
}

/// AuthManager manages authentication for connections.
pub interface AuthManager {
	/// Returns the list of supported SASL mechanisms.
	supported_mechanisms() []domain.SaslMechanism

	/// Checks whether a specific mechanism is supported.
	is_mechanism_supported(mechanism string) bool
mut:
	/// Returns the authenticator for a specific mechanism.
	get_authenticator(mechanism domain.SaslMechanism) !SaslAuthenticator

	/// Performs authentication using the specified mechanism.
	authenticate(mechanism domain.SaslMechanism, auth_bytes []u8) !domain.AuthResult
}

/// AclManager manages access control lists (ACLs).
pub interface AclManager {
mut:
	/// Creates ACLs.
	create_acls(acls []domain.AclBinding) ![]domain.AclCreateResult

	/// Deletes ACLs matching the given filters.
	delete_acls(filters []domain.AclBindingFilter) ![]domain.AclDeleteResult

	/// Retrieves ACLs matching the given filter.
	describe_acls(filter domain.AclBindingFilter) ![]domain.AclBinding

	/// Checks authorization for an operation.
	/// Returns true if permission is granted.
	authorize(principal string, host string, operation domain.AclOperation, resource domain.ResourcePattern) !bool
}
