// Infra Layer - Kafka Request Parsing - SASL Operations
// SaslHandshake, SaslAuthenticate request parsing
module kafka

// ============================================================================
// SaslHandshake Request (API Key 17)
// ============================================================================
// Used to negotiate SASL mechanism between client and broker
// v0: Basic mechanism negotiation
// v1: Adds mechanism enable/disable flags

pub struct SaslHandshakeRequest {
pub:
	mechanism string // The SASL mechanism chosen by the client
}

fn parse_sasl_handshake_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslHandshakeRequest {
	// SaslHandshake is never flexible (v0-v1 only)
	mechanism := reader.read_string()!

	return SaslHandshakeRequest{
		mechanism: mechanism
	}
}

// ============================================================================
// SaslAuthenticate Request (API Key 36)
// ============================================================================
// Used to perform SASL authentication after mechanism handshake
// v0: Basic authentication
// v1: Adds session lifetime
// v2: Flexible versions

pub struct SaslAuthenticateRequest {
pub:
	auth_bytes []u8 // The SASL authentication bytes from the client
}

fn parse_sasl_authenticate_request(mut reader BinaryReader, version i16, is_flexible bool) !SaslAuthenticateRequest {
	auth_bytes := if is_flexible {
		reader.read_compact_bytes()!
	} else {
		reader.read_bytes()!
	}

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return SaslAuthenticateRequest{
		auth_bytes: auth_bytes
	}
}
