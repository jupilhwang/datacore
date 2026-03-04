// Infra Layer - S3 Client
// Low-level HTTP operations for S3 storage
// Provides AWS SigV4 signed requests and S3 API operations
module s3

import crypto.sha256
import crypto.hmac
import net.http
import time
import infra.observability

// S3 HTTP request retry configuration constants
const max_retries = 3
const initial_backoff_ms = 100
const dns_backoff_ms = 1000
const max_backoff_jitter_ms = 50
const max_delete_concurrent = 20
const hmac_block_size = 64
const s3_read_timeout_ms = 15000
const s3_write_timeout_ms = 15000

/// is_network_error detects network errors such as DNS resolution failures, connection refused, and timeouts.
/// Identifies transient errors that may occur when connecting to S3 endpoints in Docker container environments.
fn is_network_error(err_str string) bool {
	return err_str.contains('socket error') || err_str.contains('resolve')
		|| err_str.contains('connection refused') || err_str.contains('timed out')
		|| err_str.contains('Connection refused') || err_str.contains('ECONNREFUSED')
		|| err_str.contains('ETIMEDOUT') || err_str.contains('ECONNRESET')
		|| err_str.contains('network is unreachable') || err_str.contains('deadline exceeded')
		|| err_str.contains('read timeout') || err_str.contains('write timeout')
		|| err_str.contains('net.tcp: timed out')
}

/// S3Object represents an object from S3 list results.
/// Holds individual object information parsed from a ListObjectsV2 API response.
pub struct S3Object {
pub:
	key           string
	size          i64
	last_modified time.Time
	etag          string
}

/// get_object retrieves an object from S3.
/// Supports Range requests via start and end parameters.
/// Returns: (object data, ETag)
/// If start is negative, retrieves the entire object.
/// Retries with exponential backoff on DNS/network errors.
fn (mut a S3StorageAdapter) get_object(key string, start i64, end i64) !([]u8, string) {
	// Metric: S3 GET request
	a.metrics_lock.@lock()
	a.metrics.s3_get_count++
	a.metrics_lock.unlock()

	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut last_err := ''
	for attempt in 0 .. max_retries {
		// Prepare headers
		mut headers := a.sign_request('GET', key, '', []u8{})

		// Add Range header if start is non-negative
		if start >= 0 {
			range_val := if end > start { 'bytes=${start}-${end}' } else { 'bytes=${start}-' }
			headers.add_custom('Range', range_val) or {}
		}

		mut req := http.prepare(http.FetchConfig{
			url:    url
			method: .get
			header: headers
		}) or {
			last_err = 'S3 GET prepare failed: ${err}'
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error(last_err)
		}
		req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
		req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

		resp := req.do() or {
			last_err = 'S3 GET failed: ${err}'
			err_str := err.msg()

			// Retry on DNS/network errors
			if is_network_error(err_str) && attempt < max_retries - 1 {
				backoff_ms := dns_backoff_ms * (1 << attempt)
				observability.log_with_context('s3', .warn, 'S3Client', 'GET retry (network error)',
					{
					'attempt':    '${attempt + 1}/${max_retries}'
					'key':        key
					'error':      err_str
					'backoff_ms': backoff_ms.str()
				})
				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}

			// Non-network error or last retry: fail immediately
			// Metric: S3 error
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error(last_err)
		}

		if resp.status_code == 404 {
			// Metric: S3 error
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error('Object not found: ${key}')
		}
		// 206 Partial Content is also a success for Range requests
		if resp.status_code != 200 && resp.status_code != 206 {
			// Metric: S3 error
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error('S3 GET failed with status ${resp.status_code}')
		}

		etag := resp.header.get(.etag) or { '' }
		return resp.body.bytes(), etag
	}

	// Metric: S3 error
	a.metrics_lock.@lock()
	a.metrics.s3_error_count++
	a.metrics_lock.unlock()
	return error(last_err)
}

/// put_object writes an object to S3.
/// Internally calls put_object_with_retry which includes retry logic.
fn (mut a S3StorageAdapter) put_object(key string, data []u8) ! {
	a.put_object_with_retry(key, data, max_retries)!
}

/// put_object_with_retry attempts to write an object with exponential backoff retries.
/// Retries with exponential backoff (100ms, 200ms, 400ms...) on 500 and 503 errors.
/// Uses longer backoff (1s, 2s, 4s) on DNS/network errors.
/// Adds jitter to prevent thundering herd on concurrent retries.
fn (mut a S3StorageAdapter) put_object_with_retry(key string, data []u8, max_retries_ int) ! {
	// Metric: S3 PUT request (counted once, including retries)
	a.metrics_lock.@lock()
	a.metrics.s3_put_count++
	a.metrics_lock.unlock()

	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut last_err := ''
	for attempt in 0 .. max_retries_ {
		headers := a.sign_request('PUT', key, '', data)

		mut req := http.prepare(http.FetchConfig{
			url:    url
			method: .put
			header: headers
			data:   data.bytestr()
		}) or {
			last_err = 'S3 PUT prepare failed: ${err}'
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error(last_err)
		}
		req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
		req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

		resp := req.do() or {
			last_err = 'S3 PUT failed: ${err}'
			err_str := err.msg()

			if attempt < max_retries_ - 1 {
				// Apply longer backoff for DNS/network errors (1s, 2s, 4s)
				backoff_ms := if is_network_error(err_str) {
					dns_backoff_ms * (1 << attempt)
				} else {
					initial_backoff_ms * (1 << attempt)
				}

				observability.log_with_context('s3', .warn, 'S3Client', 'PUT retry', {
					'attempt':    '${attempt + 1}/${max_retries_}'
					'key':        key
					'error':      err_str
					'backoff_ms': backoff_ms.str()
					'endpoint':   endpoint
					'bucket':     a.config.bucket_name
				})

				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}
			// Final failure: log detailed error
			observability.log_with_context('s3', .error, 'S3Client', 'PUT failed after all retries',
				{
				'key':      key
				'error':    err_str
				'endpoint': endpoint
				'bucket':   a.config.bucket_name
				'retries':  max_retries_.str()
			})
			// Metric: S3 error
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error(last_err)
		}

		if resp.status_code in [200, 201, 204] {
			return
		}

		// Retry on 503 (Service Unavailable / throttling) and 500 (Server Error)
		if resp.status_code in [500, 503] && attempt < max_retries_ - 1 {
			last_err = 'S3 PUT failed with status ${resp.status_code}'
			// Exponential backoff with jitter
			backoff_ms := initial_backoff_ms * (1 << attempt) +
				int(time.now().unix_milli() % max_backoff_jitter_ms)

			observability.log_with_context('s3', .warn, 'S3Client', 'PUT status error, retrying',
				{
				'attempt':     '${attempt + 1}/${max_retries_}'
				'key':         key
				'status_code': resp.status_code.str()
				'backoff_ms':  backoff_ms.str()
			})

			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		// Metric: S3 error
		a.metrics_lock.@lock()
		a.metrics.s3_error_count++
		a.metrics_lock.unlock()
		return error('S3 PUT failed with status ${resp.status_code}')
	}
	// Metric: S3 error
	a.metrics_lock.@lock()
	a.metrics.s3_error_count++
	a.metrics_lock.unlock()
	return error(last_err)
}

/// put_object_if_not_exists writes an object only if it does not already exist (conditional PUT).
/// Uses If-None-Match: * header to prevent concurrent creation.
/// Returns an error if the object already exists (412 Precondition Failed).
fn (mut a S3StorageAdapter) put_object_if_not_exists(key string, data []u8) ! {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut headers := a.sign_request('PUT', key, '', data)
	headers.add_custom('If-None-Match', '*') or {}

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .put
		header: headers
		data:   data.bytestr()
	}) or { return error('S3 PUT prepare failed: ${err}') }
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or { return error('S3 PUT failed: ${err}') }

	if resp.status_code == 412 {
		return error('Object already exists (precondition failed)')
	}
	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

/// put_object_if_match overwrites an object only when the ETag matches (conditional PUT).
/// Uses If-Match header to implement optimistic locking.
/// Returns an 'etag_mismatch' error on 412 Precondition Failed when ETag does not match.
fn (mut a S3StorageAdapter) put_object_if_match(key string, data []u8, etag string) ! {
	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	mut headers := a.sign_request('PUT', key, '', data)
	headers.add_custom('If-Match', etag) or {}

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .put
		header: headers
		data:   data.bytestr()
	}) or { return error('S3 PUT prepare failed: ${err}') }
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or { return error('S3 PUT failed: ${err}') }

	if resp.status_code == 412 {
		return error('etag_mismatch')
	}
	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

/// delete_object deletes an object from S3.
/// Returns 200 or 204 status code on successful deletion.
fn (mut a S3StorageAdapter) delete_object(key string) ! {
	// Metric: S3 DELETE request
	a.metrics_lock.@lock()
	a.metrics.s3_delete_count++
	a.metrics_lock.unlock()

	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	headers := a.sign_request('DELETE', key, '', []u8{})

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .delete
		header: headers
	}) or {
		a.metrics_lock.@lock()
		a.metrics.s3_error_count++
		a.metrics_lock.unlock()
		return error('S3 DELETE prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		// Metric: S3 error
		a.metrics_lock.@lock()
		a.metrics.s3_error_count++
		a.metrics_lock.unlock()
		return error('S3 DELETE failed: ${err}')
	}

	if resp.status_code !in [200, 204] {
		// Metric: S3 error
		a.metrics_lock.@lock()
		a.metrics.s3_error_count++
		a.metrics_lock.unlock()
		return error('S3 DELETE failed with status ${resp.status_code}')
	}
}

/// delete_objects_with_prefix deletes all objects with the specified prefix.
/// First lists objects with the prefix, then deletes each in parallel.
fn (mut a S3StorageAdapter) delete_objects_with_prefix(prefix string) ! {
	objects := a.list_objects(prefix)!

	if objects.len == 0 {
		return
	}

	// Parallel deletion (up to 20 concurrent)
	// Limit channel buffer size to max_concurrent to control memory usage
	max_concurrent := max_delete_concurrent
	ch := chan bool{cap: max_concurrent}
	mut active := 0

	for obj in objects {
		// Limit concurrent execution
		for active >= max_concurrent {
			_ = <-ch
			active--
		}

		active++
		spawn fn [mut a, obj, ch] () {
			a.delete_object(obj.key) or {
				observability.log_with_context('s3', .error, 'S3Client', 'Failed to delete object',
					{
					'object_key': obj.key
					'error':      err.msg()
				})
			}
			ch <- true
		}()
	}

	// Wait for all deletions to complete
	for _ in 0 .. active {
		_ = <-ch
	}
}

/// list_objects retrieves a list of S3 objects with the specified prefix.
/// Uses the ListObjectsV2 API to fetch the object list.
/// Includes retry logic to handle network issues such as OpenSSL errors.
fn (mut a S3StorageAdapter) list_objects(prefix string) ![]S3Object {
	// Metric: S3 LIST request
	a.metrics_lock.@lock()
	a.metrics.s3_list_count++
	a.metrics_lock.unlock()

	endpoint := a.get_endpoint()
	query := 'prefix=${prefix}&list-type=2'
	url := '${endpoint}/${a.config.bucket_name}?${query}'

	mut last_err := ''

	for attempt in 0 .. max_retries {
		if attempt > 0 {
			observability.log_with_context('s3', .debug, 'S3Client', 'LIST retry', {
				'attempt': (attempt + 1).str()
				'max':     max_retries.str()
				'prefix':  prefix
			})
		}

		headers := a.sign_request('GET', '', query, []u8{})

		mut req := http.prepare(http.FetchConfig{
			url:    url
			method: .get
			header: headers
		}) or {
			last_err = 'S3 LIST prepare failed: ${err}'
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error(last_err)
		}
		req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
		req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

		resp := req.do() or {
			last_err = 'S3 LIST failed: ${err}'
			err_str := err.msg()
			observability.log_with_context('s3', .error, 'S3Client', 'LIST error', {
				'attempt':  (attempt + 1).str()
				'error':    err_str
				'prefix':   prefix
				'endpoint': endpoint
				'bucket':   a.config.bucket_name
			})

			if attempt < max_retries - 1 {
				// Apply longer backoff for DNS/network errors (1s, 2s, 4s)
				backoff_ms := if is_network_error(err_str) {
					dns_backoff_ms * (1 << attempt)
				} else {
					initial_backoff_ms * (1 << attempt)
				}
				observability.log_with_context('s3', .warn, 'S3Client', 'LIST retry',
					{
					'attempt':    '${attempt + 1}/${max_retries}'
					'backoff_ms': backoff_ms.str()
				})
				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}
			// Metric: S3 error
			a.metrics_lock.@lock()
			a.metrics.s3_error_count++
			a.metrics_lock.unlock()
			return error(last_err)
		}

		if resp.status_code == 200 {
			// Success - parse XML response
			return parse_list_objects_response(resp.body)
		}

		// Retry on 503 (Service Unavailable) and 500 (Server Error)
		if resp.status_code in [500, 503] && attempt < max_retries - 1 {
			last_err = 'S3 LIST failed with status ${resp.status_code}'
			observability.log_with_context('s3', .warn, 'S3Client', 'LIST status error, retrying',
				{
				'status_code': resp.status_code.str()
			})

			// Exponential backoff with jitter
			backoff_ms := initial_backoff_ms * (1 << attempt) +
				int(time.now().unix_milli() % max_backoff_jitter_ms)
			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		// Metric: S3 error
		a.metrics_lock.@lock()
		a.metrics.s3_error_count++
		a.metrics_lock.unlock()
		return error('S3 LIST failed with status ${resp.status_code}')
	}

	// Metric: S3 error
	a.metrics_lock.@lock()
	a.metrics.s3_error_count++
	a.metrics_lock.unlock()
	return error(last_err)
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
		hmac_block_size)
	k_region := hmac.new(k_date, a.config.region.bytes(), sha256.sum, hmac_block_size)
	k_service := hmac.new(k_region, 's3'.bytes(), sha256.sum, hmac_block_size)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, hmac_block_size)

	// Signature
	signature := hmac.new(k_signing, string_to_sign.bytes(), sha256.sum, hmac_block_size).hex()

	auth_header := '${algorithm} Credential=${a.config.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
	h.add_custom('Authorization', auth_header) or {}

	return h
}

/// get_endpoint returns the S3 endpoint URL.
/// Uses the custom endpoint (MinIO/LocalStack) if configured.
/// Otherwise returns the AWS S3 endpoint in path-style or virtual-hosted style.
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

/// get_host returns the Host header value for S3 requests.
/// Host header is a required signed header in SigV4 signing.
/// Includes bucket name in host when using virtual-hosted style.
fn (a &S3StorageAdapter) get_host() string {
	if a.config.endpoint.len > 0 {
		// For custom endpoints (MinIO/LocalStack, etc.)
		// Include bucket name in host if using virtual-hosted style
		if !a.config.use_path_style {
			return '${a.config.bucket_name}.${a.config.endpoint.replace('http://', '').replace('https://',
				'').split('/')[0]}'
		}
		return a.config.endpoint.replace('http://', '').replace('https://', '').split('/')[0]
	}
	// For AWS S3
	if a.config.use_path_style {
		return 's3.${a.config.region}.amazonaws.com'
	} else {
		return '${a.config.bucket_name}.s3.${a.config.region}.amazonaws.com'
	}
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

/// url_decode decodes a percent-encoded string.
/// Example: %20 -> space, %2F -> /
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

/// parse_list_objects_response parses an S3 ListObjectsV2 XML response.
/// Simplified XML parsing that extracts only <Key> tags.
/// A proper XML parser is recommended for production use.
fn parse_list_objects_response(body string) []S3Object {
	// Simplified XML parsing - use a proper XML parser in production
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
