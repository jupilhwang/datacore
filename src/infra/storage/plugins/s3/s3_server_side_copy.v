// Infra Layer - S3 Server-Side Copy for Compaction
// Orchestrates S3 CopyObject and Multipart Upload for segment merging.
// Low-level multipart operations are in s3_multipart.v.
module s3

import net.http
import time
import sync.stdatomic
import infra.observability

/// segments_support_server_side_copy returns true because DataCore's binary segment
/// format supports multi-segment concatenation. The decoder in s3_record_codec.v
/// reads (count, records...) tuples in a loop until EOF, so concatenated segments
/// are decoded correctly as a single logical stream.
///
/// IMPORTANT: If the segment format changes to include a file-level header or
/// other non-repeatable prefix, this function MUST be updated to return false
/// (or implement actual format detection) to prevent data corruption.
fn segments_support_server_side_copy() bool {
	return true
}

/// build_copy_source constructs the x-amz-copy-source header value.
/// Format: /bucket/key
fn (a &S3StorageAdapter) build_copy_source(source_key string) string {
	return '/${a.config.bucket_name}/${source_key}'
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
	stdatomic.add_i64(&a.metrics_collector.data.s3_put_count, 1)

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
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 CopyObject prepare failed: ${err}')
	}
	req.read_timeout = i64(s3_read_timeout_ms) * i64(time.millisecond)
	req.write_timeout = i64(s3_write_timeout_ms) * i64(time.millisecond)

	resp := req.do() or {
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 CopyObject failed: ${err}')
	}

	if resp.status_code != 200 {
		stdatomic.add_i64(&a.metrics_collector.data.s3_error_count, 1)
		return error('S3 CopyObject failed with status ${resp.status_code}')
	}
}
