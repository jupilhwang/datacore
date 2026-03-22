// Tests for S3 ListObjectsV2 pagination parsing
module s3

// test_parse_list_objects_response_basic verifies basic key extraction.
fn test_parse_list_objects_response_basic() {
	body := '<ListBucketResult><Contents><Key>file1.parquet</Key></Contents><Contents><Key>file2.parquet</Key></Contents></ListBucketResult>'
	result := parse_list_objects_response(body)
	assert result.len == 2
	assert result[0].key == 'file1.parquet'
	assert result[1].key == 'file2.parquet'
}

// test_parse_list_objects_page_not_truncated verifies that IsTruncated=false
// is correctly detected (no continuation token returned).
fn test_parse_list_objects_page_not_truncated() {
	body := '<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>file1.parquet</Key></Contents></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.objects.len == 1
	assert page.objects[0].key == 'file1.parquet'
	assert page.is_truncated == false
	assert page.next_continuation_token == ''
}

// test_parse_list_objects_page_truncated verifies that IsTruncated=true
// and NextContinuationToken are correctly extracted.
fn test_parse_list_objects_page_truncated() {
	body := '<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>token-abc-123</NextContinuationToken><Contents><Key>file1.parquet</Key></Contents></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.is_truncated == true
	assert page.next_continuation_token == 'token-abc-123'
	assert page.objects.len == 1
}

// test_parse_list_objects_page_empty_response verifies empty result handling.
fn test_parse_list_objects_page_empty_response() {
	body := '<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.objects.len == 0
	assert page.is_truncated == false
	assert page.next_continuation_token == ''
}

// test_parse_list_objects_response_full_fields verifies Size, ETag extraction
// from Contents elements beyond just the Key tag.
fn test_parse_list_objects_response_full_fields() {
	body := '<ListBucketResult><Contents><Key>file1.parquet</Key><Size>12345</Size><LastModified>2024-01-15T10:30:00.000Z</LastModified><ETag>"abc123"</ETag></Contents></ListBucketResult>'
	result := parse_list_objects_response(body)
	assert result.len == 1
	assert result[0].key == 'file1.parquet'
	assert result[0].size == 12345
	assert result[0].etag == 'abc123'
}

// test_parse_list_objects_response_with_xml_prolog verifies parsing works
// with an XML declaration header, as real S3 responses include one.
fn test_parse_list_objects_response_with_xml_prolog() {
	body := '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Contents><Key>data/file.parquet</Key><Size>999</Size></Contents></ListBucketResult>'
	result := parse_list_objects_response(body)
	assert result.len == 1
	assert result[0].key == 'data/file.parquet'
	assert result[0].size == 999
}

// test_parse_list_objects_response_empty_body verifies graceful handling
// of empty or malformed XML input.
fn test_parse_list_objects_response_empty_body() {
	assert parse_list_objects_response('').len == 0
	assert parse_list_objects_response('not xml').len == 0
}

// test_parse_list_objects_page_with_full_xml verifies end-to-end parsing
// of a realistic S3 ListObjectsV2 response with all fields.
fn test_parse_list_objects_page_with_full_xml() {
	body := '<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IsTruncated>true</IsTruncated><NextContinuationToken>tok-xyz</NextContinuationToken><Contents><Key>a.parquet</Key><Size>100</Size><ETag>"e1"</ETag></Contents><Contents><Key>b.parquet</Key><Size>200</Size><ETag>"e2"</ETag></Contents></ListBucketResult>'
	page := parse_list_objects_page(body)
	assert page.is_truncated == true
	assert page.next_continuation_token == 'tok-xyz'
	assert page.objects.len == 2
	assert page.objects[0].key == 'a.parquet'
	assert page.objects[0].size == 100
	assert page.objects[1].key == 'b.parquet'
	assert page.objects[1].size == 200
}
