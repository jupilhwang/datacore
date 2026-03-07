// Tests for Parquet file path normalization in flush_all_partitions.
// Problem: flush_all_partitions uses w.table_metadata.location which may be
// an s3:// URL. The adapter.put_object expects a relative S3 key (no s3://bucket/ prefix).
// These tests verify that normalize_s3_location correctly strips the bucket prefix.
module s3

// test_normalize_s3_location_strips_s3_url:
// 's3://datacore/iceberg/warehouse/test/partition_0' -> 'iceberg/warehouse/test/partition_0'
fn test_normalize_s3_location_strips_s3_url() {
	result := normalize_s3_location('s3://datacore/iceberg/warehouse/test/partition_0')
	assert result == 'iceberg/warehouse/test/partition_0', 'expected iceberg/warehouse/test/partition_0, got ${result}'
}

// test_normalize_s3_location_keeps_relative_path:
// 'iceberg/warehouse' -> 'iceberg/warehouse' (no prefix to strip)
fn test_normalize_s3_location_keeps_relative_path() {
	result := normalize_s3_location('iceberg/warehouse')
	assert result == 'iceberg/warehouse', 'expected iceberg/warehouse, got ${result}'
}

// test_normalize_s3_location_empty:
// '' -> ''
fn test_normalize_s3_location_empty() {
	result := normalize_s3_location('')
	assert result == '', 'expected empty string, got ${result}'
}

// test_normalize_s3_location_bucket_only:
// 's3://datacore' -> ''
fn test_normalize_s3_location_bucket_only() {
	result := normalize_s3_location('s3://datacore')
	assert result == '', 'expected empty string, got ${result}'
}

// test_normalize_s3_location_with_trailing_slash:
// 's3://datacore/iceberg/warehouse/' -> 'iceberg/warehouse'
fn test_normalize_s3_location_with_trailing_slash() {
	result := normalize_s3_location('s3://datacore/iceberg/warehouse/')
	assert result == 'iceberg/warehouse', 'expected iceberg/warehouse, got ${result}'
}
