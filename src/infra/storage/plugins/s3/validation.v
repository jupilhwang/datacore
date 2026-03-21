// Input validation for S3 configuration.
// Prevents path traversal attacks in S3 key identifiers
// and SSRF attacks via endpoint URL validation.
module s3

/// max_identifier_length is the maximum allowed length for an identifier
/// used in S3 key construction. Consistent with max_topic_name_length.
const max_identifier_length = 255

/// validate_s3_endpoint validates that an S3 endpoint URL does not target
/// private, reserved, or loopback addresses (SSRF prevention).
/// Empty endpoint is allowed (means use default AWS endpoint).
pub fn validate_s3_endpoint(endpoint string) ! {
	if endpoint == '' {
		return
	}

	if !endpoint.starts_with('http://') && !endpoint.starts_with('https://') {
		return error('endpoint must start with http:// or https:// scheme')
	}

	host := extract_host(endpoint)
	if host == '' {
		return error('endpoint has empty hostname')
	}

	reject_forbidden_host(host)!
}

/// extract_host extracts the hostname from a URL string.
/// Strips scheme, port, path, and IPv6 bracket wrapping.
fn extract_host(url string) string {
	mut host := url
	// strip scheme
	if host.starts_with('https://') {
		host = host[8..]
	} else if host.starts_with('http://') {
		host = host[7..]
	}
	// strip path
	if slash_pos := host.index('/') {
		host = host[..slash_pos]
	}
	// handle IPv6 bracket notation [::1]:port
	if host.starts_with('[') {
		if bracket_end := host.index(']') {
			return host[1..bracket_end]
		}
		return host[1..]
	}
	// strip port
	if colon_pos := host.index(':') {
		host = host[..colon_pos]
	}
	return host
}

/// reject_forbidden_host checks the host against blocked address patterns.
fn reject_forbidden_host(host string) ! {
	if host == 'localhost' || host == '::1' {
		return error('endpoint resolves to loopback address: ${host}')
	}

	octets := parse_ipv4_octets(host) or { return }
	reject_private_ip(octets, host)!
}

/// parse_ipv4_octets tries to parse a host string as an IPv4 address.
/// Returns the four octets on success, or none if not a valid IPv4.
fn parse_ipv4_octets(host string) ![]int {
	parts := host.split('.')
	if parts.len != 4 {
		return error('not an IPv4 address')
	}
	mut octets := []int{cap: 4}
	for part in parts {
		if part == '' {
			return error('empty octet')
		}
		val := part.int()
		if val < 0 || val > 255 {
			return error('octet out of range')
		}
		// reject non-numeric strings that .int() returns 0 for
		if val == 0 && part != '0' {
			return error('non-numeric octet')
		}
		octets << val
	}
	return octets
}

/// reject_private_ip checks parsed IPv4 octets against private/reserved CIDR ranges.
fn reject_private_ip(octets []int, host string) ! {
	first := octets[0]
	second := octets[1]

	if first == 0 && second == 0 && octets[2] == 0 && octets[3] == 0 {
		return error('endpoint resolves to non-routable address: ${host}')
	}
	if first == 127 {
		return error('endpoint resolves to loopback address: ${host}')
	}
	if first == 10 {
		return error('endpoint resolves to private address (10.0.0.0/8): ${host}')
	}
	if first == 172 && second >= 16 && second <= 31 {
		return error('endpoint resolves to private address (172.16.0.0/12): ${host}')
	}
	if first == 192 && second == 168 {
		return error('endpoint resolves to private address (192.168.0.0/16): ${host}')
	}
	if first == 169 && second == 254 {
		return error('endpoint resolves to link-local address (169.254.0.0/16): ${host}')
	}
}

/// validate_identifier validates a string used in S3 key construction.
/// Rejects empty strings, path traversal sequences, null bytes,
/// non-printable characters, and characters outside the allowed set.
/// Allowed characters: alphanumeric, underscore, hyphen, dot (but not ".." substring).
fn validate_identifier(value string, field_name string) ! {
	if value.len == 0 {
		return error('${field_name} cannot be empty')
	}
	if value.len > max_identifier_length {
		return error('${field_name} too long: ${value.len} > ${max_identifier_length}')
	}
	if value.contains('..') {
		return error('${field_name} contains path traversal sequence: ".."')
	}
	for ch in value {
		if ch == 0 {
			return error('${field_name} contains null byte')
		}
		if ch < 0x20 {
			return error('${field_name} contains non-printable character: 0x${ch:02x}')
		}
		if !ch.is_alnum() && ch != `_` && ch != `-` && ch != `.` {
			return error('${field_name} contains invalid character: ${[ch].bytestr()}')
		}
	}
}
