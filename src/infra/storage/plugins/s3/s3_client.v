// S3 Client - Low-level HTTP operations for S3 storage
// Provides AWS SigV4 signed requests and S3 API operations
module s3

import crypto.sha256
import crypto.hmac
import net.http
import time

// S3Object represents an object in S3 listing
pub struct S3Object {
pub:
	key          string
	size         i64
	last_modified time.Time
	etag         string
}

// ============================================================
// S3 HTTP Operations
// ============================================================

// get_object retrieves an object from S3
fn (mut a S3StorageAdapter) get_object(key string, start i64, end i64) !([]u8, string) {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	// Prepare headers
	mut headers := a.sign_request('GET', key, '', []u8{})

	// Add Range header if start is non-negative
	if start >= 0 {
		range_val := if end > start { 'bytes=${start}-${end}' } else { 'bytes=${start}-' }
		headers.add_custom('Range', range_val) or {}
	}

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .get
		header: headers
	}) or { return error('S3 GET failed: ${err}') }

	if resp.status_code == 404 {
		return error('Object not found: ${key}')
	}
	// 206 Partial Content is success for Range requests
	if resp.status_code != 200 && resp.status_code != 206 {
		return error('S3 GET failed with status ${resp.status_code}')
	}

	etag := resp.header.get(.etag) or { '' }
	return resp.body.bytes(), etag
}

// put_object writes an object to S3
fn (mut a S3StorageAdapter) put_object(key string, data []u8) ! {
	a.put_object_with_retry(key, data, 3)!
}

// put_object_with_retry attempts to put an object with exponential backoff retry
fn (mut a S3StorageAdapter) put_object_with_retry(key string, data []u8, max_retries int) ! {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut last_err := ''
	for attempt in 0 .. max_retries {
		headers := a.sign_request('PUT', key, '', data)

		resp := http.fetch(http.FetchConfig{
			url:    url
			method: .put
			header: headers
			data:   data.bytestr()
		}) or {
			last_err = 'S3 PUT failed: ${err}'
			if attempt < max_retries - 1 {
				// Exponential backoff: 100ms, 200ms, 400ms...
				time.sleep(time.Duration(100 * (1 << attempt)) * time.millisecond)
				continue
			}
			return error(last_err)
		}

		if resp.status_code in [200, 201, 204] {
			return
		}

		// Retry on 503 (Service Unavailable / Throttling) and 500 (Server Error)
		if resp.status_code in [500, 503] && attempt < max_retries - 1 {
			last_err = 'S3 PUT failed with status ${resp.status_code}'
			// Exponential backoff with jitter
			backoff_ms := 100 * (1 << attempt) + int(time.now().unix_milli() % 50)
			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		return error('S3 PUT failed with status ${resp.status_code}')
	}
	return error(last_err)
}

// put_object_if_not_exists writes an object only if it doesn't exist (conditional PUT)
fn (mut a S3StorageAdapter) put_object_if_not_exists(key string, data []u8) ! {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut headers := a.sign_request('PUT', key, '', data)
	headers.add_custom('If-None-Match', '*') or {}

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .put
		header: headers
		data:   data.bytestr()
	}) or { return error('S3 PUT failed: ${err}') }

	if resp.status_code == 412 {
		return error('Object already exists (precondition failed)')
	}
	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

// delete_object removes an object from S3
fn (mut a S3StorageAdapter) delete_object(key string) ! {
	endpoint := a.get_endpoint()
	url := '${endpoint}/${a.config.bucket_name}/${key}'

	headers := a.sign_request('DELETE', key, '', []u8{})

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .delete
		header: headers
	}) or { return error('S3 DELETE failed: ${err}') }

	if resp.status_code !in [200, 204] {
		return error('S3 DELETE failed with status ${resp.status_code}')
	}
}

// delete_objects_with_prefix removes all objects with a given prefix
fn (mut a S3StorageAdapter) delete_objects_with_prefix(prefix string) ! {
	objects := a.list_objects(prefix)!
	for obj in objects {
		a.delete_object(obj.key) or {}
	}
}

// list_objects lists objects in S3 with a given prefix
fn (mut a S3StorageAdapter) list_objects(prefix string) ![]S3Object {
	endpoint := a.get_endpoint()
	query := 'prefix=${prefix}&list-type=2'
	url := '${endpoint}/${a.config.bucket_name}?${query}'

	headers := a.sign_request('GET', '', query, []u8{})

	resp := http.fetch(http.FetchConfig{
		url:    url
		method: .get
		header: headers
	}) or { return error('S3 LIST failed: ${err}') }

	if resp.status_code != 200 {
		return error('S3 LIST failed with status ${resp.status_code}')
	}

	// Parse XML response (simplified)
	return parse_list_objects_response(resp.body)
}

// ============================================================
// AWS SigV4 Signing
// ============================================================

// sign_request signs an HTTP request using AWS Signature V4
fn (a &S3StorageAdapter) sign_request(method string, key string, query string, body []u8) http.Header {
	mut h := http.Header{}
	now := time.now().as_utc()

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

	// Signing Key
	k_date := hmac.new(('AWS4' + a.config.secret_key).bytes(), date_day.bytes(), sha256.sum,
		64)
	k_region := hmac.new(k_date, a.config.region.bytes(), sha256.sum, 64)
	k_service := hmac.new(k_region, 's3'.bytes(), sha256.sum, 64)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, 64)

	// Signature
	signature := hmac.new(k_signing, string_to_sign.bytes(), sha256.sum, 64).hex()

	auth_header := '${algorithm} Credential=${a.config.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
	h.add_custom('Authorization', auth_header) or {}

	return h
}

// get_endpoint returns the S3 endpoint URL
fn (a &S3StorageAdapter) get_endpoint() string {
	if a.config.endpoint.len > 0 {
		return a.config.endpoint
	}
	if a.config.use_path_style {
		return 'https://s3.${a.config.region}.amazonaws.com'
	} else {
		return 'https://${a.config.bucket_name}.s3.${a.config.region}.amazonaws.com'
	}
}

// get_host returns the host header value for S3 requests
fn (a &S3StorageAdapter) get_host() string {
	if a.config.endpoint.len > 0 {
		return a.config.endpoint.replace('http://', '').replace('https://', '').split('/')[0]
	}
	return 's3.${a.config.region}.amazonaws.com'
}

// canonicalize_query sorts and encodes query parameters for AWS SigV4
fn (a &S3StorageAdapter) canonicalize_query(query string) string {
	if query == '' {
		return ''
	}

	// Parse query string into map
	mut params := map[string]string{}
	for pair in query.split('&') {
		parts := pair.split_nth('=', 2)
		if parts.len == 2 {
			// URL decode key and value, then re-encode properly for AWS SigV4
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

// ============================================================
// URL Encoding Utilities
// ============================================================

// url_decode decodes a percent-encoded string
fn url_decode(s string) string {
	mut result := s
	mut i := 0
	for i < result.len {
		if result[i] == u8(`%`) && i + 2 < result.len {
			hex_str := result[i + 1..i + 3]
			if is_hex_char(hex_str[0]) && is_hex_char(hex_str[1]) {
				c := hex_to_u8(hex_str)
				result = result[0..i] + c.ascii_str() + result[i + 3..]
			} else {
				i++
			}
		} else {
			i++
		}
	}
	return result
}

// is_hex_char checks if a character is a valid hex digit
fn is_hex_char(c u8) bool {
	return (c >= `0` && c <= `9`) || (c >= `A` && c <= `F`) || (c >= `a` && c <= `f`)
}

// hex_to_u8 converts a two-character hex string to a byte
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

// u8_to_hex converts a byte to a two-character uppercase hex string
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

// url_encode_for_sigv4 encodes a string according to AWS SigV4 requirements
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

// ============================================================
// XML Response Parsing
// ============================================================

// parse_list_objects_response parses S3 ListObjectsV2 XML response
fn parse_list_objects_response(body string) []S3Object {
	// Simplified XML parsing - in production use proper XML parser
	mut objects := []S3Object{}

	mut remaining := body
	for {
		key_start := remaining.index('<Key>') or { break }
		key_end := remaining.index('</Key>') or { break }

		key := remaining[key_start + 5..key_end]
		objects << S3Object{
			key:  key
			size: 0
		}

		remaining = remaining[key_end + 6..]
	}

	return objects
}
