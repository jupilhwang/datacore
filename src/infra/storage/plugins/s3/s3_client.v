// Infra Layer - S3 Client
// Low-level HTTP operations for S3 storage
// Provides AWS SigV4 signed requests and S3 API operations
module s3

import encoding.xml
import net.http
import time
import sync.stdatomic
import infra.observability

// S3 HTTP request retry configuration constants
// NOTE: max_retries and initial_backoff_ms are now sourced from S3Config fields
// (S3Config.max_retries and S3Config.retry_delay_ms) to allow runtime configuration.
// The constants below are only used as fallbacks where adapter config is not accessible.
const dns_backoff_ms = 1000
const max_backoff_jitter_ms = 50
const max_delete_concurrent = 20
const s3_read_timeout_ms = 15000
const s3_write_timeout_ms = 15000

/// S3NetworkError represents transient network connectivity errors.
/// Used for retry decisions instead of fragile string matching.
struct S3NetworkError {
	Error
	detail string
}

fn (e S3NetworkError) msg() string {
	return 'S3 network error: ${e.detail}'
}

/// S3ETagMismatchError indicates a conditional PUT failed due to ETag mismatch.
/// Returned on HTTP 412 Precondition Failed.
struct S3ETagMismatchError {
	Error
}

fn (e S3ETagMismatchError) msg() string {
	return 'etag_mismatch'
}

/// is_network_error detects network errors such as DNS resolution failures, connection refused, and timeouts.
/// Identifies transient errors that may occur when connecting to S3 endpoints in Docker container environments.
/// Checks for S3NetworkError type first, then falls back to string matching for V stdlib errors.
fn is_network_error(err IError) bool {
	if err is S3NetworkError {
		return true
	}
	// Fallback for errors from V's stdlib (net.http) which are still string-based
	err_str := err.msg()
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
	stdatomic.add_i64(&a.metrics_collector.data.s3_get_count, 1)

	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}'
	} else {
		'${endpoint}/${key}'
	}

	cfg_max_retries := a.config.max_retries

	mut last_err := ''
	for attempt in 0 .. cfg_max_retries {
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
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error(last_err)
		}
		req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
		req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

		resp := req.do() or {
			last_err = 'S3 GET failed: ${err}'
			err_str := err.msg()

			// Retry on DNS/network errors
			if is_network_error(err) && attempt < cfg_max_retries - 1 {
				backoff_ms := dns_backoff_ms * (1 << attempt)
				observability.log_with_context('s3', .warn, 'S3Client', 'GET retry (network error)',
					{
					'attempt':    '${attempt + 1}/${cfg_max_retries}'
					'key':        key
					'error':      err_str
					'backoff_ms': backoff_ms.str()
				})
				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}

			// Non-network error or last retry: fail immediately
			// Metric: S3 error
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error(last_err)
		}

		if resp.status_code == 404 {
			// Metric: S3 error
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error('Object not found: ${key}')
		}
		// 206 Partial Content is also a success for Range requests
		if resp.status_code != 200 && resp.status_code != 206 {
			// Metric: S3 error
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error('S3 GET failed with status ${resp.status_code}')
		}

		etag := resp.header.get(.etag) or { '' }
		return resp.body.bytes(), etag
	}

	// Metric: S3 error
	stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
	return error(last_err)
}

/// put_object writes an object to S3.
/// Internally calls put_object_with_retry which includes retry logic.
fn (mut a S3StorageAdapter) put_object(key string, data []u8) ! {
	a.put_object_with_retry(key, data, a.config.max_retries)!
}

/// put_object_with_retry attempts to write an object with exponential backoff retries.
/// Retries with exponential backoff using config.retry_delay_ms on 500 and 503 errors.
/// Uses longer backoff (1s, 2s, 4s) on DNS/network errors.
/// Adds jitter to prevent thundering herd on concurrent retries.
fn (mut a S3StorageAdapter) put_object_with_retry(key string, data []u8, max_retries_ int) ! {
	// Metric: S3 PUT request (counted once, including retries)
	stdatomic.add_i64(&a.metrics_collector.data.s3_put_count, 1)

	cfg_retry_delay_ms := a.config.retry_delay_ms
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
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error(last_err)
		}
		req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
		req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

		resp := req.do() or {
			last_err = 'S3 PUT failed: ${err}'
			err_str := err.msg()

			if attempt < max_retries_ - 1 {
				// Apply longer backoff for DNS/network errors (1s, 2s, 4s)
				backoff_ms := if is_network_error(err) {
					dns_backoff_ms * (1 << attempt)
				} else {
					cfg_retry_delay_ms * (1 << attempt)
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
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error(last_err)
		}

		if resp.status_code in [200, 201, 204] {
			return
		}

		// Retry on 503 (Service Unavailable / throttling) and 500 (Server Error)
		if resp.status_code in [500, 503] && attempt < max_retries_ - 1 {
			last_err = 'S3 PUT failed with status ${resp.status_code}'
			// Exponential backoff with jitter
			backoff_ms := cfg_retry_delay_ms * (1 << attempt) +
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
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 PUT failed with status ${resp.status_code}')
	}
	// Metric: S3 error
	stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
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
		return S3ETagMismatchError{}
	}
	if resp.status_code !in [200, 201, 204] {
		return error('S3 PUT failed with status ${resp.status_code}')
	}
}

/// delete_object deletes an object from S3.
/// Returns 200 or 204 status code on successful deletion.
fn (mut a S3StorageAdapter) delete_object(key string) ! {
	// Metric: S3 DELETE request
	stdatomic.add_i64(&a.metrics_collector.data.s3_delete_count, 1)

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
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 DELETE prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		// Metric: S3 error
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 DELETE failed: ${err}')
	}

	if resp.status_code !in [200, 204] {
		// Metric: S3 error
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
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

/// list_objects retrieves a complete list of S3 objects with the specified prefix.
/// Uses the ListObjectsV2 API with automatic pagination via IsTruncated and
/// NextContinuationToken to handle result sets larger than MaxKeys=1000.
/// Includes retry logic to handle network issues such as OpenSSL errors.
fn (mut a S3StorageAdapter) list_objects(prefix string) ![]S3Object {
	mut all_objects := []S3Object{}
	mut continuation_token := ''

	for {
		page := a.list_objects_page(prefix, continuation_token)!
		all_objects << page.objects

		if !page.is_truncated {
			break
		}
		continuation_token = page.next_continuation_token
	}

	return all_objects
}

/// list_objects_page fetches a single page of S3 objects with the specified prefix.
/// Uses continuation_token for pagination; pass empty string for the first page.
/// Includes retry logic for transient network and server errors.
fn (mut a S3StorageAdapter) list_objects_page(prefix string, continuation_token string) !ListObjectsPage {
	// Metric: S3 LIST request
	stdatomic.add_i64(&a.metrics_collector.data.s3_list_count, 1)

	cfg_max_retries := a.config.max_retries
	cfg_retry_delay_ms := a.config.retry_delay_ms
	endpoint := a.get_endpoint()

	mut query := 'list-type=2&prefix=${prefix}'
	if continuation_token.len > 0 {
		query += '&continuation-token=${continuation_token}'
	}
	url := '${endpoint}/${a.config.bucket_name}?${query}'

	mut last_err := ''

	for attempt in 0 .. cfg_max_retries {
		if attempt > 0 {
			observability.log_with_context('s3', .debug, 'S3Client', 'LIST retry', {
				'attempt': (attempt + 1).str()
				'max':     cfg_max_retries.str()
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
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
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

			if attempt < cfg_max_retries - 1 {
				// Apply longer backoff for DNS/network errors (1s, 2s, 4s)
				backoff_ms := if is_network_error(err) {
					dns_backoff_ms * (1 << attempt)
				} else {
					cfg_retry_delay_ms * (1 << attempt)
				}
				observability.log_with_context('s3', .warn, 'S3Client', 'LIST retry',
					{
					'attempt':    '${attempt + 1}/${cfg_max_retries}'
					'backoff_ms': backoff_ms.str()
				})
				time.sleep(time.Duration(backoff_ms) * time.millisecond)
				continue
			}
			// Metric: S3 error
			stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
			return error(last_err)
		}

		if resp.status_code == 200 {
			// Success - parse XML response with pagination info
			return parse_list_objects_page(resp.body)
		}

		// Retry on 503 (Service Unavailable) and 500 (Server Error)
		if resp.status_code in [500, 503] && attempt < cfg_max_retries - 1 {
			last_err = 'S3 LIST failed with status ${resp.status_code}'
			observability.log_with_context('s3', .warn, 'S3Client', 'LIST status error, retrying',
				{
				'status_code': resp.status_code.str()
			})

			// Exponential backoff with jitter
			backoff_ms := cfg_retry_delay_ms * (1 << attempt) +
				int(time.now().unix_milli() % max_backoff_jitter_ms)
			time.sleep(time.Duration(backoff_ms) * time.millisecond)
			continue
		}

		// Metric: S3 error
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 LIST failed with status ${resp.status_code}')
	}

	// Metric: S3 error
	stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
	return error(last_err)
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

/// ListObjectsPage holds a single page of S3 ListObjectsV2 results
/// including pagination state for iterating beyond MaxKeys=1000.
struct ListObjectsPage {
	objects                 []S3Object
	is_truncated            bool
	next_continuation_token string
}

/// parse_list_objects_page parses an S3 ListObjectsV2 XML response into a
/// ListObjectsPage, extracting IsTruncated and NextContinuationToken for
/// pagination support.
fn parse_list_objects_page(body string) ListObjectsPage {
	doc := xml.XMLDocument.from_string(body) or { return ListObjectsPage{} }

	is_truncated_nodes := doc.root.get_elements_by_tag('IsTruncated')
	is_truncated := if is_truncated_nodes.len > 0 {
		xml_node_text(is_truncated_nodes[0]) == 'true'
	} else {
		false
	}

	token_nodes := doc.root.get_elements_by_tag('NextContinuationToken')
	token := if token_nodes.len > 0 {
		xml_node_text(token_nodes[0])
	} else {
		''
	}

	return ListObjectsPage{
		objects:                 parse_contents_to_objects(doc.root.get_elements_by_tag('Contents'))
		is_truncated:            is_truncated
		next_continuation_token: token
	}
}

/// parse_list_objects_response parses an S3 ListObjectsV2 XML response,
/// extracting Key, Size, LastModified, and ETag from each Contents element.
fn parse_list_objects_response(body string) []S3Object {
	doc := xml.XMLDocument.from_string(body) or { return [] }
	return parse_contents_to_objects(doc.root.get_elements_by_tag('Contents'))
}

/// xml_node_text extracts the text content from an XML node's children.
fn xml_node_text(node xml.XMLNode) string {
	for child in node.children {
		if child is string {
			return child
		}
	}
	return ''
}

/// parse_contents_to_objects converts a list of XML Contents nodes into S3Objects.
fn parse_contents_to_objects(contents_nodes []xml.XMLNode) []S3Object {
	mut objects := []S3Object{cap: contents_nodes.len}
	for node in contents_nodes {
		mut key := ''
		mut size := i64(0)
		mut etag := ''
		mut last_modified := time.Time{}
		for child in node.children {
			if child is xml.XMLNode {
				match child.name {
					'Key' {
						key = xml_node_text(child)
					}
					'Size' {
						size = xml_node_text(child).i64()
					}
					'ETag' {
						etag = xml_node_text(child).trim('"')
					}
					'LastModified' {
						last_modified = time.parse_iso8601(xml_node_text(child)) or { time.Time{} }
					}
					else {}
				}
			}
		}
		objects << S3Object{
			key:           key
			size:          size
			last_modified: last_modified
			etag:          etag
		}
	}
	return objects
}
