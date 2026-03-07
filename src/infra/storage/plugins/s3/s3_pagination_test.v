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
