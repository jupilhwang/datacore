// Infra Layer - S3 Batch Delete Operations
// Implements S3 Multi-Object Delete API (POST /?delete)
// Reduces per-key DELETE requests by batching up to 1000 keys per request.
module s3

import crypto.md5
import encoding.base64
import net.http
import time
import sync.stdatomic
import infra.observability

// Maximum number of keys per S3 Multi-Object Delete request (AWS limit)
const max_delete_batch_size = 1000

/// build_delete_objects_xml constructs the XML body for S3 Multi-Object Delete API.
/// Each key is wrapped in an <Object><Key>...</Key></Object> element.
fn build_delete_objects_xml(keys []string) string {
	mut sb := []string{cap: keys.len + 3}
	sb << '<?xml version="1.0" encoding="UTF-8"?>'
	sb << '<Delete>'
	for key in keys {
		sb << '<Object><Key>${key}</Key></Object>'
	}
	sb << '</Delete>'
	return sb.join('')
}

/// chunk_keys splits a list of keys into chunks of the given size.
/// Returns an empty array if keys is empty.
fn chunk_keys(keys []string, chunk_size int) [][]string {
	if keys.len == 0 {
		return []
	}
	mut chunks := [][]string{}
	mut i := 0
	for i < keys.len {
		end := if i + chunk_size > keys.len { keys.len } else { i + chunk_size }
		chunks << keys[i..end]
		i += chunk_size
	}
	return chunks
}

/// extract_xml_tag_value extracts the text content between an XML open and close tag.
/// Returns empty string if the tag is not found.
fn extract_xml_tag_value(xml string, tag string) string {
	open_tag := '<${tag}>'
	close_tag := '</${tag}>'
	start := xml.index(open_tag) or { return '' }
	end := xml.index(close_tag) or { return '' }
	if end <= start {
		return ''
	}
	return xml[start + open_tag.len..end]
}

/// parse_delete_objects_errors extracts error entries from the DeleteResult XML response.
/// Returns a list of human-readable error strings for logging.
fn parse_delete_objects_errors(body string) []string {
	mut errors := []string{}
	mut remaining := body
	for {
		err_start := remaining.index('<Error>') or { break }
		err_end := remaining.index('</Error>') or { break }
		err_block := remaining[err_start..err_end + 8]

		key := extract_xml_tag_value(err_block, 'Key')
		code := extract_xml_tag_value(err_block, 'Code')
		message := extract_xml_tag_value(err_block, 'Message')

		errors << 'key=${key} code=${code} message=${message}'
		remaining = remaining[err_end + 8..]
	}
	return errors
}

/// build_delete_request prepares an HTTP request for S3 Multi-Object Delete API.
/// Sets Content-MD5 (required by AWS) and Content-Type headers.
fn (mut a S3StorageAdapter) build_delete_request(url string, chunk []string) !http.Request {
	xml_body := build_delete_objects_xml(chunk)
	body_bytes := xml_body.bytes()

	md5_hash := md5.sum(body_bytes)
	content_md5 := base64.encode(md5_hash)

	mut headers := a.sign_request('POST', '', 'delete', body_bytes)
	headers.add_custom('Content-MD5', content_md5) or {}
	headers.add_custom('Content-Type', 'application/xml') or {}

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .post
		header: headers
		data:   xml_body
	}) or { return error('S3 batch DELETE prepare failed: ${err}') }
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)
	return req
}

/// parse_delete_response validates the HTTP response and logs per-object errors.
/// Returns an error for non-200 status codes.
fn (a &S3StorageAdapter) parse_delete_response(resp http.Response) ! {
	if resp.status_code != 200 {
		return error('S3 batch DELETE failed with status ${resp.status_code}')
	}

	errors := parse_delete_objects_errors(resp.body)
	for err_msg in errors {
		observability.log_with_context('s3', .warn, 'S3Client', 'Batch delete partial error',
			{
			'error': err_msg
		})
	}
}

/// delete_objects_batch deletes multiple S3 objects using the Multi-Object Delete API.
/// Automatically chunks requests at 1000 keys per batch (S3 API limit).
/// Individual object errors in the response are logged as warnings (non-fatal).
fn (mut a S3StorageAdapter) delete_objects_batch(keys []string) ! {
	if keys.len == 0 {
		return
	}

	stdatomic.add_i64(&a.metrics.s3_delete_count, keys.len)

	chunks := chunk_keys(keys, max_delete_batch_size)
	endpoint := a.get_endpoint()
	url := '${endpoint}/${a.config.bucket_name}?delete'

	for chunk in chunks {
		mut req := a.build_delete_request(url, chunk) or {
			stdatomic.add_i64(&a.metrics.s3_error_count, 1)
			return error(err.msg())
		}

		resp := req.do() or {
			stdatomic.add_i64(&a.metrics.s3_error_count, 1)
			return error('S3 batch DELETE failed: ${err}')
		}

		a.parse_delete_response(resp) or {
			stdatomic.add_i64(&a.metrics.s3_error_count, 1)
			return err
		}
	}
}
