// UseCase Layer - Authentication Port Interfaces
// These interfaces are defined in UseCase layer and implemented by Adapter layer
module port

import domain

// UserStore defines the interface for user storage and authentication
// Implemented by infra/auth
pub interface UserStore {
mut:
	// Get user by username
	get_user(username string) !domain.User

	// Create a new user
	create_user(username string, password string, mechanism domain.SaslMechanism) !domain.User

	// Update user password
	update_password(username string, new_password string) !

	// Delete user
	delete_user(username string) !

	// List all users
	list_users() ![]domain.User

	// Validate password for PLAIN authentication
	// Returns true if password matches
	validate_password(username string, password string) !bool
}

// SaslAuthenticator defines the interface for SASL authentication
pub interface SaslAuthenticator {
	// Get the supported mechanism
	mechanism() domain.SaslMechanism
mut:
	// Authenticate with the provided auth bytes
	// For PLAIN: auth_bytes contains [authzid]\0[authcid]\0[password]
	// Returns AuthResult with principal on success
	authenticate(auth_bytes []u8) !domain.AuthResult

	// For SCRAM: Process next step in challenge-response
	// Returns AuthResult with challenge or final result
	step(response []u8) !domain.AuthResult
}

// AuthManager manages authentication for connections
pub interface AuthManager {
	// Get supported SASL mechanisms
	supported_mechanisms() []domain.SaslMechanism

	// Check if a mechanism is supported
	is_mechanism_supported(mechanism string) bool
mut:
	// Get authenticator for a specific mechanism
	get_authenticator(mechanism domain.SaslMechanism) !SaslAuthenticator

	// Authenticate using the specified mechanism
	authenticate(mechanism domain.SaslMechanism, auth_bytes []u8) !domain.AuthResult
}

// AclManager manages Access Control Lists
pub interface AclManager {
mut:
	// Create ACLs
	create_acls(acls []domain.AclBinding) ![]domain.AclCreateResult

	// Delete ACLs matching the filter
	delete_acls(filters []domain.AclBindingFilter) ![]domain.AclDeleteResult

	// Describe ACLs matching the filter
	describe_acls(filter domain.AclBindingFilter) ![]domain.AclBinding

	// Authorize an operation
	authorize(principal string, host string, operation domain.AclOperation, resource domain.ResourcePattern) !bool
}
