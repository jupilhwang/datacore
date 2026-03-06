// Infra Layer - S3 Server-Side Copy for Compaction
// Implements S3 Multipart Upload + UploadPartCopy API to eliminate data transfer
// during compaction. Falls back to download-reupload when format is incompatible.
module s3

import net.http
import time
import sync.stdatomic
import infra.observability

// S3 Multipart Upload constants
const multipart_min_part_size = i64(5 * 1024 * 1024)
const max_single_copy_size = i64(5) * i64(1024) * i64(1024) * i64(1024)
const max_multipart_parts = 10000

/// MultipartPart holds the part number and ETag for a completed upload part.
struct MultipartPart {
	part_number int
	etag        string
}

/// segments_support_server_side_copy checks whether the segment binary format
/// supports server-side copy via simple concatenation.
/// DataCore segments use a record-count-prefixed binary format (4-byte count header)
/// which means concatenating two segments produces invalid data.
/// Returns false because server-side copy requires simple binary concat compatibility.
fn segments_support_server_side_copy() bool {
	return false
}

/// build_copy_source constructs the x-amz-copy-source header value.
/// Format: /bucket/key
fn (a &S3StorageAdapter) build_copy_source(source_key string) string {
	return '/${a.config.bucket_name}/${source_key}'
}

/// build_complete_multipart_xml constructs the XML body for CompleteMultipartUpload.
fn build_complete_multipart_xml(parts []MultipartPart) string {
	mut sb := []string{cap: parts.len + 3}
	sb << '<CompleteMultipartUpload>'
	for part in parts {
		sb << '<Part><PartNumber>${part.part_number}</PartNumber><ETag>${part.etag}</ETag></Part>'
	}
	sb << '</CompleteMultipartUpload>'
	return sb.join('')
}

/// parse_upload_id_from_xml extracts the UploadId from InitiateMultipartUploadResult XML.
fn parse_upload_id_from_xml(body string) !string {
	start_tag := '<UploadId>'
	end_tag := '</UploadId>'
	start_idx := body.index(start_tag) or { return error('UploadId not found in response') }
	end_idx := body.index(end_tag) or { return error('UploadId end tag not found in response') }
	return body[start_idx + start_tag.len..end_idx]
}

/// parse_copy_part_etag_from_xml extracts the ETag from CopyPartResult XML.
fn parse_copy_part_etag_from_xml(body string) !string {
	start_tag := '<ETag>'
	end_tag := '</ETag>'
	start_idx := body.index(start_tag) or { return error('ETag not found in copy part response') }
	end_idx := body.index(end_tag) or {
		return error('ETag end tag not found in copy part response')
	}
	return body[start_idx + start_tag.len..end_idx]
}

/// create_multipart_upload initiates a multipart upload and returns the UploadId.
/// S3 API: POST /{key}?uploads
fn (mut a S3StorageAdapter) create_multipart_upload(key string) !string {
	stdatomic.add_i64(&a.metrics.s3_put_count, 1)

	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}?uploads'
	} else {
		'${endpoint}/${key}?uploads'
	}

	headers := a.sign_request('POST', key, 'uploads', []u8{})

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .post
		header: headers
	}) or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CreateMultipartUpload prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CreateMultipartUpload failed: ${err}')
	}

	if resp.status_code != 200 {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CreateMultipartUpload failed with status ${resp.status_code}')
	}

	upload_id := parse_upload_id_from_xml(resp.body)!
	return upload_id
}

/// upload_part_copy copies a part from a source object using server-side copy.
/// S3 API: PUT /{key}?partNumber=N&uploadId=ID
///   Header: x-amz-copy-source: /bucket/source-key
///   Header: x-amz-copy-source-range: bytes=start-end (optional)
fn (mut a S3StorageAdapter) upload_part_copy(dest_key string, upload_id string, part_number int, source_key string, byte_range string) !string {
	stdatomic.add_i64(&a.metrics.s3_put_count, 1)

	endpoint := a.get_endpoint()
	query := 'partNumber=${part_number}&uploadId=${upload_id}'
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${dest_key}?${query}'
	} else {
		'${endpoint}/${dest_key}?${query}'
	}

	mut headers := a.sign_request('PUT', dest_key, query, []u8{})
	copy_source := a.build_copy_source(source_key)
	headers.add_custom('x-amz-copy-source', copy_source) or {}

	if byte_range != '' {
		headers.add_custom('x-amz-copy-source-range', byte_range) or {}
	}

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .put
		header: headers
	}) or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 UploadPartCopy prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 UploadPartCopy failed: ${err}')
	}

	if resp.status_code != 200 {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 UploadPartCopy failed with status ${resp.status_code}')
	}

	etag := parse_copy_part_etag_from_xml(resp.body)!
	return etag
}

/// complete_multipart_upload finalizes a multipart upload.
/// S3 API: POST /{key}?uploadId=ID with XML body listing parts.
fn (mut a S3StorageAdapter) complete_multipart_upload(key string, upload_id string, parts []MultipartPart) ! {
	endpoint := a.get_endpoint()
	query := 'uploadId=${upload_id}'
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}?${query}'
	} else {
		'${endpoint}/${key}?${query}'
	}

	body := build_complete_multipart_xml(parts)
	body_bytes := body.bytes()

	headers := a.sign_request('POST', key, query, body_bytes)

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .post
		header: headers
		data:   body
	}) or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CompleteMultipartUpload prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CompleteMultipartUpload failed: ${err}')
	}

	if resp.status_code != 200 {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CompleteMultipartUpload failed with status ${resp.status_code}')
	}
}

/// abort_multipart_upload cancels an in-progress multipart upload to free resources.
/// S3 API: DELETE /{key}?uploadId=ID
fn (mut a S3StorageAdapter) abort_multipart_upload(key string, upload_id string) ! {
	endpoint := a.get_endpoint()
	query := 'uploadId=${upload_id}'
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${key}?${query}'
	} else {
		'${endpoint}/${key}?${query}'
	}

	headers := a.sign_request('DELETE', key, query, []u8{})

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .delete
		header: headers
	}) or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 AbortMultipartUpload prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 AbortMultipartUpload failed: ${err}')
	}

	if resp.status_code !in [200, 204] {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 AbortMultipartUpload failed with status ${resp.status_code}')
	}
}

/// merge_segments_server_side attempts to merge segments using S3 server-side copy.
/// Returns a specific error 'server_side_copy_unsupported' when the segment format
/// does not support simple binary concatenation, signaling the caller to fall back
/// to the traditional download-reupload merge path.
fn (mut a S3StorageAdapter) merge_segments_server_side(topic string, partition int, mut index PartitionIndex, segments []LogSegment) ! {
	if segments.len == 0 {
		return
	}

	// Check segment format compatibility
	if !segments_support_server_side_copy() {
		observability.log_with_context('s3', .debug, 'Compaction', 'Segment format does not support server-side copy, falling back',
			{
			'topic':     topic
			'partition': partition.str()
		})
		return error('server_side_copy_unsupported')
	}

	// Calculate total size for deciding single copy vs multipart
	mut total_size := i64(0)
	for seg in segments {
		total_size += seg.size_bytes
	}

	new_start_offset := segments[0].start_offset
	new_end_offset := segments[segments.len - 1].end_offset
	dest_key := a.log_segment_key(topic, partition, new_start_offset, new_end_offset)

	if segments.len == 1 && total_size < max_single_copy_size {
		// Single source: use simple CopyObject (PUT with x-amz-copy-source)
		a.copy_object(dest_key, segments[0].key)!
	} else {
		// Multiple sources or large: use Multipart Upload + UploadPartCopy
		a.multipart_copy_segments(dest_key, segments)!
	}

	// Update index with merged segment
	new_segment := LogSegment{
		start_offset: new_start_offset
		end_offset:   new_end_offset
		key:          dest_key
		size_bytes:   total_size
		created_at:   time.now()
	}

	a.update_index_with_merged_segment(topic, partition, mut index, segments, new_segment)!

	// Delete old segments
	a.delete_segments_parallel(segments)
}

/// copy_object performs a simple S3 CopyObject (PUT with x-amz-copy-source).
/// Used for single-source copies under 5GB.
fn (mut a S3StorageAdapter) copy_object(dest_key string, source_key string) ! {
	stdatomic.add_i64(&a.metrics.s3_put_count, 1)

	endpoint := a.get_endpoint()
	url := if a.config.use_path_style {
		'${endpoint}/${a.config.bucket_name}/${dest_key}'
	} else {
		'${endpoint}/${dest_key}'
	}

	mut headers := a.sign_request('PUT', dest_key, '', []u8{})
	copy_source := a.build_copy_source(source_key)
	headers.add_custom('x-amz-copy-source', copy_source) or {}

	mut req := http.prepare(http.FetchConfig{
		url:    url
		method: .put
		header: headers
	}) or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CopyObject prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CopyObject failed: ${err}')
	}

	if resp.status_code != 200 {
		stdatomic.add_i64(&a.metrics.s3_error_count, 1)
		return error('S3 CopyObject failed with status ${resp.status_code}')
	}
}

/// multipart_copy_segments performs a multipart upload using server-side copy parts.
/// Each source segment becomes one UploadPartCopy. Aborts on failure.
fn (mut a S3StorageAdapter) multipart_copy_segments(dest_key string, segments []LogSegment) ! {
	upload_id := a.create_multipart_upload(dest_key)!

	// Use defer-like error handling: abort on any failure
	mut completed_parts := []MultipartPart{cap: segments.len}
	mut copy_failed := false
	mut copy_error := ''

	for i, seg in segments {
		part_number := i + 1
		if part_number > max_multipart_parts {
			copy_failed = true
			copy_error = 'exceeded maximum multipart parts (${max_multipart_parts})'
			break
		}

		// Validate minimum part size (last part is exempt)
		if seg.size_bytes < multipart_min_part_size && i < segments.len - 1 {
			copy_failed = true
			copy_error = 'part ${part_number} size ${seg.size_bytes} bytes is below minimum ${multipart_min_part_size} bytes'
			break
		}

		etag := a.upload_part_copy(dest_key, upload_id, part_number, seg.key, '') or {
			copy_failed = true
			copy_error = 'UploadPartCopy failed for part ${part_number}: ${err}'
			break
		}

		completed_parts << MultipartPart{
			part_number: part_number
			etag:        etag
		}
	}

	if copy_failed {
		// Abort the multipart upload to prevent resource leaks
		a.abort_multipart_upload(dest_key, upload_id) or {
			observability.log_with_context('s3', .error, 'Compaction', 'Failed to abort multipart upload after copy failure',
				{
				'dest_key':  dest_key
				'upload_id': upload_id
				'error':     err.msg()
			})
		}
		return error(copy_error)
	}

	a.complete_multipart_upload(dest_key, upload_id, completed_parts)!
}
