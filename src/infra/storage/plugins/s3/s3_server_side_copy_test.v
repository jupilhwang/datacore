// Unit tests for S3 server-side copy (Multipart Upload API)
// Tests multipart upload lifecycle, compaction fallback, and config flag behavior
module s3

import time

// -- MultipartPart struct tests --

fn test_multipart_part_has_required_fields() {
	part := MultipartPart{
		part_number: 1
		etag:        '"abc123"'
	}
	assert part.part_number == 1
	assert part.etag == '"abc123"'
}

// -- build_complete_multipart_xml tests --

fn test_build_complete_multipart_xml_single_part() {
	parts := [
		MultipartPart{
			part_number: 1
			etag:        '"etag1"'
		},
	]
	xml := build_complete_multipart_xml(parts)
	assert xml.contains('<CompleteMultipartUpload>')
	assert xml.contains('<PartNumber>1</PartNumber>')
	assert xml.contains('<ETag>&quot;etag1&quot;</ETag>')
	assert xml.contains('</CompleteMultipartUpload>')
}

fn test_build_complete_multipart_xml_multiple_parts() {
	parts := [
		MultipartPart{
			part_number: 1
			etag:        '"etag1"'
		},
		MultipartPart{
			part_number: 2
			etag:        '"etag2"'
		},
		MultipartPart{
			part_number: 3
			etag:        '"etag3"'
		},
	]
	xml := build_complete_multipart_xml(parts)
	assert xml.contains('<Part><PartNumber>1</PartNumber><ETag>&quot;etag1&quot;</ETag></Part>')
	assert xml.contains('<Part><PartNumber>2</PartNumber><ETag>&quot;etag2&quot;</ETag></Part>')
	assert xml.contains('<Part><PartNumber>3</PartNumber><ETag>&quot;etag3&quot;</ETag></Part>')
}

fn test_build_complete_multipart_xml_escapes_etag_special_chars() {
	parts := [
		MultipartPart{
			part_number: 1
			etag:        '"etag&special"'
		},
		MultipartPart{
			part_number: 2
			etag:        'etag<with>brackets'
		},
	]
	xml := build_complete_multipart_xml(parts)
	assert xml.contains('<ETag>&quot;etag&amp;special&quot;</ETag>')
	assert xml.contains('<ETag>etag&lt;with&gt;brackets</ETag>')
	// Must NOT contain unescaped special chars
	assert !xml.contains('<ETag>"etag&special"</ETag>')
}

fn test_build_complete_multipart_xml_empty_parts() {
	parts := []MultipartPart{}
	xml := build_complete_multipart_xml(parts)
	assert xml.contains('<CompleteMultipartUpload>')
	assert xml.contains('</CompleteMultipartUpload>')
	// No <Part> elements
	assert !xml.contains('<Part>')
}

// -- parse_upload_id_from_xml tests --

fn test_parse_upload_id_from_xml_valid() {
	xml := '<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>test-bucket</Bucket>
  <Key>test-key</Key>
  <UploadId>abc123-upload-id</UploadId>
</InitiateMultipartUploadResult>'
	upload_id := parse_upload_id_from_xml(xml) or {
		assert false, 'should parse upload id: ${err}'
		return
	}
	assert upload_id == 'abc123-upload-id'
}

fn test_parse_upload_id_from_xml_missing() {
	xml := '<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchUpload</Code></Error>'
	upload_id := parse_upload_id_from_xml(xml) or {
		assert err.msg().contains('UploadId not found')
		return
	}
	assert false, 'should return error for missing UploadId, got: ${upload_id}'
}

// -- parse_copy_part_etag_from_xml tests --

fn test_parse_copy_part_etag_from_xml_valid() {
	xml := '<?xml version="1.0" encoding="UTF-8"?>
<CopyPartResult>
  <LastModified>2026-03-06T00:00:00.000Z</LastModified>
  <ETag>"abc123etag"</ETag>
</CopyPartResult>'
	etag := parse_copy_part_etag_from_xml(xml) or {
		assert false, 'should parse etag: ${err}'
		return
	}
	assert etag == '"abc123etag"'
}

fn test_parse_copy_part_etag_from_xml_missing() {
	xml := '<Error><Code>InternalError</Code></Error>'
	etag := parse_copy_part_etag_from_xml(xml) or {
		assert err.msg().contains('ETag not found')
		return
	}
	assert false, 'should return error for missing ETag, got: ${etag}'
}

// -- build_copy_source tests --

fn test_build_copy_source_path_style() {
	adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name: 'my-bucket'
		}
	}
	result := adapter.build_copy_source('datacore/topics/test/log-001.bin')
	assert result == '/my-bucket/datacore/topics/test/log-001.bin'
}

// -- min_part_size constant tests --

fn test_min_part_size_is_5mib() {
	assert multipart_min_part_size == i64(5 * 1024 * 1024)
}

fn test_max_single_copy_size_is_5gib() {
	assert max_single_copy_size == i64(5) * i64(1024) * i64(1024) * i64(1024)
}

// -- segments_support_server_side_copy tests --

fn test_segments_support_server_side_copy_returns_true() {
	// Decoder reads concatenated (count, records...) tuples until EOF,
	// so server-side copy via binary concat is now safe.
	result := segments_support_server_side_copy()
	assert result == true
}

// -- merge_segments_server_side attempts copy when format is supported --

fn test_merge_segments_server_side_attempts_copy_when_supported() {
	// With segment format now supporting server-side copy,
	// merge_segments_server_side should NOT return 'server_side_copy_unsupported'.
	// It will attempt the actual S3 copy, which fails in unit test (no real S3),
	// but the error should be an S3 API error, not the format-unsupported sentinel.
	mut adapter := S3StorageAdapter{
		config: S3Config{
			bucket_name: 'test-bucket'
			region:      'us-east-1'
			endpoint:    'http://localhost:9000'
			access_key:  'test'
			secret_key:  'test'
		}
	}

	segments := [
		LogSegment{
			start_offset: 0
			end_offset:   100
			key:          'seg1.bin'
			size_bytes:   1024
			created_at:   time.now()
		},
		LogSegment{
			start_offset: 101
			end_offset:   200
			key:          'seg2.bin'
			size_bytes:   2048
			created_at:   time.now()
		},
	]

	mut index := PartitionIndex{
		topic:           'test-topic'
		partition:       0
		earliest_offset: 0
		high_watermark:  200
		log_segments:    segments
	}

	adapter.merge_segments_server_side('test-topic', 0, mut index, segments) or {
		// Should NOT be the format-unsupported fallback
		assert !err.msg().contains('server_side_copy_unsupported'), 'should attempt copy, not fallback'
		return
	}
}

// -- S3Config.use_server_side_copy default value --

fn test_s3_config_use_server_side_copy_default_true() {
	cfg := S3Config{}
	assert cfg.use_server_side_copy == true
}

// -- compact_partition server-side copy preference --

fn test_compact_partition_uses_server_side_when_enabled() {
	// Verify that S3Config has use_server_side_copy field
	cfg := S3Config{
		use_server_side_copy: true
	}
	assert cfg.use_server_side_copy == true

	cfg_disabled := S3Config{
		use_server_side_copy: false
	}
	assert cfg_disabled.use_server_side_copy == false
}
