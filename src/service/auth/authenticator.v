// Manages SASL authentication mechanisms and handles user authentication.
// Supports various authentication methods including PLAIN and SCRAM.
module auth

import domain
import service.port
import infra.observability
import sync
import time

// Logging

/// log_message prints a structured log message.
fn log_message(level observability.LogLevel, component string, message string, context map[string]string) {
	mut logger := observability.get_named_logger('auth.${component}')
	match level {
		.trace { logger.debug_map(message, context) }
		.debug { logger.debug_map(message, context) }
		.info { logger.info_map(message, context) }
		.warn { logger.warn_map(message, context) }
		.error { logger.error_map(message, context) }
		.fatal { logger.fatal_map(message, context) }
	}
}

// Metrics

/// AuthMetrics tracks metrics for authentication operations.
pub struct AuthMetrics {
mut:
	// Authentication metrics
	auth_attempts i64
	auth_success  i64
	auth_failures i64
	// Per-mechanism metrics
	mechanism_attempts map[string]i64
	mechanism_success  map[string]i64
	// Latency metrics (milliseconds)
	auth_latency_sum   i64
	auth_latency_count i64
	// Lock
	lock sync.Mutex
}

/// Creates a new AuthMetrics instance.
pub fn new_auth_metrics() &AuthMetrics {
	return &AuthMetrics{
		mechanism_attempts: map[string]i64{}
		mechanism_success:  map[string]i64{}
	}
}

/// Records an authentication attempt.
pub fn (mut m AuthMetrics) record_auth_attempt(mechanism string, latency_ms i64, success bool) {
	m.lock.lock()
	defer { m.lock.unlock() }

	m.auth_attempts++
	m.auth_latency_sum += latency_ms
	m.auth_latency_count++

	// Per-mechanism count
	if mechanism !in m.mechanism_attempts {
		m.mechanism_attempts[mechanism] = 0
		m.mechanism_success[mechanism] = 0
	}
	m.mechanism_attempts[mechanism]++

	if success {
		m.auth_success++
		m.mechanism_success[mechanism]++
	} else {
		m.auth_failures++
	}
}

/// Resets all metrics.
pub fn (mut m AuthMetrics) reset() {
	m.lock.lock()
	defer { m.lock.unlock() }

	m.auth_attempts = 0
	m.auth_success = 0
	m.auth_failures = 0
	m.mechanism_attempts.clear()
	m.mechanism_success.clear()
	m.auth_latency_sum = 0
	m.auth_latency_count = 0
}

/// Returns a metrics summary as a string.
pub fn (mut m AuthMetrics) get_summary() string {
	m.lock.lock()
	defer { m.lock.unlock() }

	mut result := '[Auth Metrics]\n'
	result += '  Total: ${m.auth_attempts} attempts, ${m.auth_success} success, ${m.auth_failures} failures'

	if m.auth_attempts > 0 {
		success_rate := (f64(m.auth_success) / f64(m.auth_attempts)) * 100.0
		result += ' (${success_rate:.1f}% success)\n'
	} else {
		result += '\n'
	}

	// Average latency
	if m.auth_latency_count > 0 {
		avg_latency := f64(m.auth_latency_sum) / f64(m.auth_latency_count)
		result += '  Avg Latency: ${avg_latency:.2f}ms\n'
	}

	// Per-mechanism statistics
	if m.mechanism_attempts.len > 0 {
		result += '  By Mechanism:\n'
		for mech, count in m.mechanism_attempts {
			success := m.mechanism_success[mech] or { 0 }
			rate := if count > 0 { (f64(success) / f64(count)) * 100.0 } else { 0.0 }
			result += '    ${mech}: ${count} attempts, ${success} success (${rate:.1f}%)\n'
		}
	}

	return result
}

/// AuthService manages authentication for the broker.
/// Manages supported SASL mechanisms and authenticators.
pub struct AuthService {
mut:
	user_store     port.UserStore
	mechanisms     []domain.SaslMechanism
	authenticators map[string]port.SaslAuthenticator
	metrics        &AuthMetrics
}

/// new_auth_service creates a new authentication service.
/// Initializes authenticators for the specified mechanisms.
pub fn new_auth_service(user_store port.UserStore, mechanisms []domain.SaslMechanism) &AuthService {
	mut authenticators := map[string]port.SaslAuthenticator{}

	// Create authenticators for each mechanism
	for mech in mechanisms {
		match mech {
			.plain {
				authenticators[mech.str()] = new_plain_authenticator(user_store)
			}
			.scram_sha_256 {
				authenticators[mech.str()] = new_scram_sha256_authenticator(user_store)
			}
			else {
				// Other mechanisms to be implemented later (SCRAM-SHA-512, OAUTHBEARER)
			}
		}
	}

	metrics := new_auth_metrics()

	return &AuthService{
		user_store:     user_store
		mechanisms:     mechanisms
		authenticators: authenticators
		metrics:        metrics
	}
}

/// supported_mechanisms returns the list of supported SASL mechanisms.
pub fn (s &AuthService) supported_mechanisms() []domain.SaslMechanism {
	return s.mechanisms
}

/// supported_mechanism_strings returns mechanism names as a string array.
pub fn (s &AuthService) supported_mechanism_strings() []string {
	mut result := []string{}
	for m in s.mechanisms {
		result << m.str()
	}
	return result
}

/// is_mechanism_supported checks whether the specified mechanism is supported.
pub fn (s &AuthService) is_mechanism_supported(mechanism string) bool {
	return mechanism.to_upper() in s.authenticators
}

/// get_authenticator returns the authenticator for the specified mechanism.
pub fn (mut s AuthService) get_authenticator(mechanism domain.SaslMechanism) !port.SaslAuthenticator {
	if auth := s.authenticators[mechanism.str()] {
		return auth
	}
	return error('unsupported mechanism: ${mechanism.str()}')
}

/// authenticate performs authentication using the specified mechanism.
pub fn (mut s AuthService) authenticate(mechanism domain.SaslMechanism, auth_bytes []u8) !domain.AuthResult {
	start_time := time.now()
	mech_str := mechanism.str()

	mut auth := s.get_authenticator(mechanism) or {
		// Metrics: record failure
		elapsed_ms := time.since(start_time).milliseconds()
		s.metrics.record_auth_attempt(mech_str, elapsed_ms, false)
		log_message(.error, 'Auth', 'Unsupported mechanism', {
			'mechanism': mech_str
		})
		return err
	}

	result := auth.authenticate(auth_bytes) or {
		// Metrics: record failure
		elapsed_ms := time.since(start_time).milliseconds()
		s.metrics.record_auth_attempt(mech_str, elapsed_ms, false)
		log_message(.warn, 'Auth', 'Authentication failed', {
			'mechanism': mech_str
			'error':     err.msg()
		})
		return err
	}

	// Metrics: record success
	elapsed_ms := time.since(start_time).milliseconds()
	s.metrics.record_auth_attempt(mech_str, elapsed_ms, true)

	log_message(.info, 'Auth', 'Authentication successful', {
		'mechanism': mech_str
		'user':      if principal := result.principal { principal.name } else { 'unknown' }
	})

	return result
}

/// PlainAuthenticator implements SASL PLAIN authentication.
/// A simple authentication method that transmits username and password in plaintext.
pub struct PlainAuthenticator {
mut:
	user_store port.UserStore
}

/// new_plain_authenticator creates a new PLAIN authenticator.
pub fn new_plain_authenticator(user_store port.UserStore) &PlainAuthenticator {
	return &PlainAuthenticator{
		user_store: user_store
	}
}

/// mechanism returns the SASL mechanism type.
pub fn (a &PlainAuthenticator) mechanism() domain.SaslMechanism {
	return .plain
}

/// authenticate performs PLAIN authentication.
/// PLAIN format: [authzid]\0[authcid]\0[password]
/// authzid: authorization ID (optional, usually empty)
/// authcid: authentication ID (username)
/// password: password
pub fn (mut a PlainAuthenticator) authenticate(auth_bytes []u8) !domain.AuthResult {
	// Parse PLAIN authentication data
	parts := parse_plain_auth(auth_bytes) or {
		return domain.auth_failure(.sasl_authentication_failed, 'invalid PLAIN format')
	}

	username := parts.username
	password := parts.password

	// Validate credentials
	valid := a.user_store.validate_password(username, password) or {
		return domain.auth_failure(.sasl_authentication_failed, 'authentication failed')
	}

	if !valid {
		return domain.auth_failure(.sasl_authentication_failed, 'invalid credentials')
	}

	// Return with principal on success
	return domain.auth_success(domain.new_user_principal(username))
}

/// step is not used in PLAIN (single-step authentication).
pub fn (mut a PlainAuthenticator) step(response []u8) !domain.AuthResult {
	return error('PLAIN does not support multi-step authentication')
}

/// PlainAuthData represents parsed PLAIN authentication data.
struct PlainAuthData {
	authzid  string
	username string
	password string
}

/// parse_plain_auth parses SASL PLAIN authentication bytes.
/// Format: [authzid]\0[authcid]\0[password]
fn parse_plain_auth(data []u8) ?PlainAuthData {
	if data.len == 0 {
		return none
	}

	// Find null byte positions
	mut null_positions := []int{}
	for i, b in data {
		if b == 0 {
			null_positions << i
		}
	}

	// Exactly 2 null bytes required
	if null_positions.len != 2 {
		return none
	}

	// Extract each part
	authzid := data[0..null_positions[0]].bytestr()
	username := data[null_positions[0] + 1..null_positions[1]].bytestr()
	password := data[null_positions[1] + 1..].bytestr()

	// Username and password must not be empty
	if username.len == 0 || password.len == 0 {
		return none
	}

	return PlainAuthData{
		authzid:  authzid
		username: username
		password: password
	}
}

// Metrics Query

/// get_metrics_summary returns a summary of authentication metrics.
pub fn (mut s AuthService) get_metrics_summary() string {
	return s.metrics.get_summary()
}

/// get_metrics returns the authentication metrics struct.
pub fn (mut s AuthService) get_metrics() &AuthMetrics {
	return s.metrics
}

/// reset_metrics resets all authentication metrics.
pub fn (mut s AuthService) reset_metrics() {
	s.metrics.reset()
}
