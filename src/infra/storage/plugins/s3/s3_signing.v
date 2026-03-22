// Infra Layer - S3 SigV4 Signing
// AWS Signature Version 4 signing and URL encoding for S3 requests
module s3

import crypto.sha256
import crypto.hmac
import net.http
import time
import infra.observability

const hmac_block_size = 64

/// CachedSigningKey holds a cached SigV4 signing key for a specific UTC day.
/// AWS SigV4 signing keys depend on (secret_key, date, region, service) and
/// remain valid for an entire UTC day, eliminating 4 redundant HMAC-SHA256
/// computations per request.
struct CachedSigningKey {
mut:
	key      []u8
	date_day string
}

/// sign_request signs an HTTP request using AWS Signature V4.
/// Generates authentication headers required for AWS S3 API calls.
/// Signing process: Canonical Request -> String to Sign -> Signing Key -> Signature
fn (a &S3StorageAdapter) sign_request(method string, key string, query string, body []u8) http.Header {
	mut h := http.Header{}
	now := time.utc()

	// Manual formatting to ensure UTC time
	date_day := now.custom_format('YYYYMMDD')
	hours := now.hour
	minutes := now.minute
	seconds := now.second
	date_str := '${date_day}T${hours:02}${minutes:02}${seconds:02}Z'

	h.add_custom('x-amz-date', date_str) or {}
	host := a.get_host()
	h.add_custom('Host', host) or {}

	payload_hash := sha256.sum(body).hex()
	h.add_custom('x-amz-content-sha256', payload_hash) or {}

	if body.len > 0 {
		h.add_custom('Content-Length', body.len.str()) or {}
	}

	if a.config.access_key.len == 0 || a.config.secret_key.len == 0 {
		observability.log_with_context('s3', .warn, 'S3Client', 'S3 request signing skipped: credentials not configured. Request will be sent unsigned.',
			{
			'method': method
			'key':    key
		})
		return h
	}

	// Canonical Request
	canonical_uri := if key == '' {
		'/${a.config.bucket_name}'
	} else if key.starts_with('/') {
		'/${a.config.bucket_name}${key}'
	} else {
		'/${a.config.bucket_name}/${key}'
	}
	canonical_querystring := a.canonicalize_query(query)

	canonical_headers := 'host:${host}\nx-amz-content-sha256:${payload_hash}\nx-amz-date:${date_str}\n'
	signed_headers := 'host;x-amz-content-sha256;x-amz-date'

	canonical_request := '${method}\n${canonical_uri}\n${canonical_querystring}\n${canonical_headers}\n${signed_headers}\n${payload_hash}'

	// String to Sign
	algorithm := 'AWS4-HMAC-SHA256'
	credential_scope := '${date_day}/${a.config.region}/s3/aws4_request'
	canonical_request_hash := sha256.sum(canonical_request.bytes()).hex()

	string_to_sign := '${algorithm}\n${date_str}\n${credential_scope}\n${canonical_request_hash}'

	// Signing Key (cached per UTC day to avoid 4 redundant HMAC-SHA256 ops per request)
	k_signing := a.get_cached_signing_key(date_day)

	// Signature
	signature := hmac.new(k_signing, string_to_sign.bytes(), sha256.sum, hmac_block_size).hex()

	auth_header := '${algorithm} Credential=${a.config.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
	h.add_custom('Authorization', auth_header) or {}

	return h
}

/// get_cached_signing_key returns a cached SigV4 signing key for the given UTC day,
/// or computes and caches a new one when the date changes.
/// Uses interior mutability (unsafe) to update the cache from an immutable receiver,
/// preserving backward compatibility with callers that hold immutable references.
/// Thread-safe: protected by signing_key_lock mutex.
fn (a &S3StorageAdapter) get_cached_signing_key(date_day string) []u8 {
	// Check cache (lock scope: read only)
	unsafe {
		mut self := a
		self.signing_key_lock.@lock()
		if self.signing_key_cache.date_day == date_day && self.signing_key_cache.key.len > 0 {
			cached := self.signing_key_cache.key.clone()
			self.signing_key_lock.unlock()
			return cached
		}
		self.signing_key_lock.unlock()
	}
	// Cache miss: compute new signing key (4-step HMAC-SHA256 chain)
	k_date := hmac.new(('AWS4' + a.config.secret_key).bytes(), date_day.bytes(), sha256.sum,
		hmac_block_size)
	k_region := hmac.new(k_date, a.config.region.bytes(), sha256.sum, hmac_block_size)
	k_service := hmac.new(k_region, 's3'.bytes(), sha256.sum, hmac_block_size)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, hmac_block_size)

	// Update cache (lock scope: write only)
	unsafe {
		mut self := a
		self.signing_key_lock.@lock()
		self.signing_key_cache = CachedSigningKey{
			key:      k_signing.clone()
			date_day: date_day
		}
		self.signing_key_lock.unlock()
	}
	return k_signing
}

/// canonicalize_query sorts and encodes query parameters for AWS SigV4.
/// Sorts query parameters alphabetically and applies URL encoding.
fn (a &S3StorageAdapter) canonicalize_query(query string) string {
	if query == '' {
		return ''
	}

	// Parse query string into map
	mut params := map[string]string{}
	for pair in query.split('&') {
		parts := pair.split_nth('=', 2)
		if parts.len == 2 {
			// URL-decode then re-encode keys and values for AWS SigV4
			key := url_decode(parts[0])
			value := url_decode(parts[1])
			params[key] = value
		}
	}

	// Sort keys alphabetically
	mut keys := []string{}
	for k in params.keys() {
		keys << k
	}
	keys.sort()

	// Build canonical query string
	mut result := []string{}
	for key in keys {
		// AWS SigV4 requires specific encoding
		encoded_key := url_encode_for_sigv4(key)
		encoded_value := url_encode_for_sigv4(params[key])
		result << '${encoded_key}=${encoded_value}'
	}

	return result.join('&')
}

/// url_decode decodes a percent-encoded string in a single pass using a byte
/// accumulator, achieving O(n) time complexity.
/// Example: %20 -> space, %2F -> /
fn url_decode(s string) string {
	mut buf := []u8{cap: s.len}
	mut i := 0
	for i < s.len {
		if s[i] == u8(`%`) && i + 2 < s.len {
			hi := hex_digit_value(s[i + 1]) or {
				buf << s[i]
				i++
				continue
			}
			lo := hex_digit_value(s[i + 2]) or {
				buf << s[i]
				i++
				continue
			}
			buf << (hi << 4) | lo
			i += 3
		} else {
			buf << s[i]
			i++
		}
	}
	return buf.bytestr()
}

/// hex_digit_value returns the numeric value (0-15) of a hexadecimal ASCII digit.
/// Returns none for non-hex characters.
fn hex_digit_value(c u8) ?u8 {
	if c >= `0` && c <= `9` {
		return u8(c - `0`)
	} else if c >= `A` && c <= `F` {
		return u8(c - `A` + 10)
	} else if c >= `a` && c <= `f` {
		return u8(c - `a` + 10)
	}
	return none
}

/// is_hex_char checks whether a character is a valid hexadecimal digit.
/// Accepts characters in the range 0-9, A-F, a-f.
fn is_hex_char(c u8) bool {
	return (c >= `0` && c <= `9`) || (c >= `A` && c <= `F`) || (c >= `a` && c <= `f`)
}

/// hex_to_u8 converts a two-character hexadecimal string to a byte.
/// Example: "4A" -> 74
fn hex_to_u8(s string) u8 {
	mut result := u8(0)
	for c in s {
		result <<= 4
		if c >= `0` && c <= `9` {
			result += c - `0`
		} else if c >= `A` && c <= `F` {
			result += c - `A` + 10
		} else if c >= `a` && c <= `f` {
			result += c - `a` + 10
		}
	}
	return result
}

/// u8_to_hex converts a byte to a two-character uppercase hexadecimal string.
/// Example: 74 -> "4A"
fn u8_to_hex(c u8) string {
	high := (c >> 4) & 0x0F
	low := c & 0x0F
	mut high_hex := '0'
	mut low_hex := '0'

	if high < 10 {
		high_hex = (u8(`0`) + high).ascii_str()
	} else {
		high_hex = (u8(`A`) + high - 10).ascii_str()
	}

	if low < 10 {
		low_hex = (u8(`0`) + low).ascii_str()
	} else {
		low_hex = (u8(`A`) + low - 10).ascii_str()
	}

	return high_hex + low_hex
}

/// url_encode_for_sigv4 encodes a string according to AWS SigV4 requirements.
/// Does not encode A-Z, a-z, 0-9, -, ., _, ~ characters.
/// All other characters are percent-encoded in %XX format.
fn url_encode_for_sigv4(s string) string {
	mut result := []u8{}
	for c in s {
		match c {
			`A`...`Z`, `a`...`z`, `0`...`9`, `-`, `.`, `_`, `~` {
				result << c
			}
			else {
				result << u8(`%`)
				hex := u8_to_hex(c)
				result << hex[0]
				result << hex[1]
			}
		}
	}
	return result.bytestr()
}
