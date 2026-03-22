// Endpoint validation for S3 configuration.
// Prevents SSRF attacks via endpoint URL validation.
// Moved from infra/storage/plugins/s3/validation.v to break
// the circular dependency between config and s3 modules.
module config

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
/// Validates IPv4, IPv6, IPv4-mapped IPv6, and special addresses.
fn reject_forbidden_host(host string) ! {
	if host == 'localhost' {
		return error('endpoint resolves to loopback address: ${host}')
	}

	lower := host.to_lower()

	// IPv4-mapped IPv6 (::ffff:x.x.x.x): extract and validate the IPv4 part
	if lower.starts_with('::ffff:') {
		ipv4_part := host[7..]
		octets := parse_ipv4_octets(ipv4_part) or { return }
		reject_private_ip(octets, host)!
		return
	}

	// Normalize IPv6 and check loopback (covers ::1, 0:0:0:0:0:0:0:1, etc.)
	normalized := normalize_ipv6(lower)
	if normalized == '::1' {
		return error('endpoint resolves to loopback address: ${host}')
	}

	// IPv6 unique local address (fc00::/7)
	if normalized.starts_with('fc') || normalized.starts_with('fd') {
		return error('endpoint resolves to private address (IPv6 unique local): ${host}')
	}

	// IPv6 link-local (fe80::/10)
	if normalized.starts_with('fe80') {
		return error('endpoint resolves to link-local address (IPv6 fe80::/10): ${host}')
	}

	// Standard IPv4 private range validation
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

/// normalize_ipv6 normalizes an IPv6 address string by stripping leading
/// zeros from each group and collapsing consecutive zero groups to `::`.
fn normalize_ipv6(host string) string {
	if !host.contains(':') {
		return host
	}

	// Detect IPv4 suffix (e.g., the 127.0.0.1 in ::ffff:127.0.0.1)
	all_parts := host.split(':')
	last := all_parts[all_parts.len - 1]
	mut ipv4_suffix := ''
	mut hex_part := host
	if last.contains('.') {
		ipv4_suffix = ':${last}'
		hex_part = host[..host.len - last.len - 1]
	}

	target := if ipv4_suffix != '' { 6 } else { 8 }
	mut groups := expand_ipv6_groups(hex_part, target)

	for i, g in groups {
		stripped := g.trim_left('0')
		groups[i] = if stripped == '' { '0' } else { stripped }
	}

	return collapse_ipv6_groups(groups, ipv4_suffix)
}

/// expand_ipv6_groups expands a `::` shorthand in an IPv6 hex part
/// into the full number of zero groups required by the target count.
fn expand_ipv6_groups(hex_part string, target int) []string {
	if !hex_part.contains('::') {
		return hex_part.split(':')
	}
	halves := hex_part.split('::')
	left := if halves[0] == '' { []string{} } else { halves[0].split(':') }
	right := if halves.len > 1 && halves[1] != '' {
		halves[1].split(':')
	} else {
		[]string{}
	}
	fill := target - left.len - right.len
	mut result := []string{cap: target}
	for g in left {
		result << g
	}
	for _ in 0 .. fill {
		result << '0'
	}
	for g in right {
		result << g
	}
	return result
}

/// collapse_ipv6_groups finds the longest consecutive run of '0' groups
/// and replaces it with `::` to produce the canonical short form.
fn collapse_ipv6_groups(groups []string, suffix string) string {
	mut best_start := -1
	mut best_len := 0
	mut cur_start := -1
	mut cur_len := 0
	for i, g in groups {
		if g == '0' {
			if cur_start < 0 {
				cur_start = i
				cur_len = 1
			} else {
				cur_len++
			}
			if cur_len > best_len {
				best_start = cur_start
				best_len = cur_len
			}
		} else {
			cur_start = -1
			cur_len = 0
		}
	}

	if best_len < 2 {
		return groups.join(':') + suffix
	}

	left := groups[..best_start].join(':')
	right := groups[best_start + best_len..].join(':')
	return '${left}::${right}${suffix}'
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
