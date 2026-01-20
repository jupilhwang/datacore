// Adapter Layer - Kafka SASL Response Building
// SaslHandshake, SaslAuthenticate responses
module kafka

// ============================================================================
// SaslHandshake Response (API Key 17)
// ============================================================================
// Returns the list of SASL mechanisms supported by the broker

pub struct SaslHandshakeResponse {
pub:
	error_code i16      // Error code (0 = no error, 33 = unsupported mechanism)
	mechanisms []string // List of SASL mechanisms enabled by the broker
}

pub fn (r SaslHandshakeResponse) encode(version i16) []u8 {
	// SaslHandshake is never flexible (v0-v1 only)
	mut writer := new_writer()

	// error_code: INT16
	writer.write_i16(r.error_code)

	// mechanisms: ARRAY[STRING]
	writer.write_array_len(r.mechanisms.len)
	for m in r.mechanisms {
		writer.write_string(m)
	}

	return writer.bytes()
}

// ============================================================================
// SaslAuthenticate Response (API Key 36)
// ============================================================================
// Returns the result of SASL authentication

pub struct SaslAuthenticateResponse {
pub:
	error_code          i16     // Error code (0 = success, 58 = SASL_AUTHENTICATION_FAILED)
	error_message       ?string // Error message if authentication failed
	auth_bytes          []u8    // SASL authentication bytes from server (for multi-step)
	session_lifetime_ms i64     // v1+: Session lifetime in milliseconds (0 = no lifetime)
}

pub fn (r SaslAuthenticateResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	// error_code: INT16
	writer.write_i16(r.error_code)

	// error_message: NULLABLE_STRING / COMPACT_NULLABLE_STRING
	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
	} else {
		writer.write_nullable_string(r.error_message)
	}

	// auth_bytes: BYTES / COMPACT_BYTES
	if is_flexible {
		writer.write_compact_bytes(r.auth_bytes)
	} else {
		writer.write_bytes(r.auth_bytes)
	}

	// session_lifetime_ms: INT64 (v1+)
	if version >= 1 {
		writer.write_i64(r.session_lifetime_ms)
	}

	// Tagged fields for flexible versions
	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}
