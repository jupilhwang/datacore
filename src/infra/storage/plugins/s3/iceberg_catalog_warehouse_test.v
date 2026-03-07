// Tests for HadoopCatalog warehouse URL normalization.
// Verifies that new_hadoop_catalog strips the s3://bucket/ prefix
// so that path-style S3 keys do not embed the full URL.
module s3

// test_new_hadoop_catalog_strips_s3_url_with_bucket:
// 's3://datacore/iceberg/warehouse' -> 'iceberg/warehouse'
fn test_new_hadoop_catalog_strips_s3_url_with_bucket() {
	catalog := new_hadoop_catalog(&S3StorageAdapter{}, 's3://datacore/iceberg/warehouse')
	assert catalog.warehouse == 'iceberg/warehouse', 'expected iceberg/warehouse, got ${catalog.warehouse}'
}

// test_new_hadoop_catalog_strips_s3_url_bucket_only:
// 's3://datacore/' -> ''
fn test_new_hadoop_catalog_strips_s3_url_bucket_only() {
	catalog := new_hadoop_catalog(&S3StorageAdapter{}, 's3://datacore/')
	assert catalog.warehouse == '', 'expected empty string, got ${catalog.warehouse}'
}

// test_new_hadoop_catalog_strips_s3_url_bucket_no_trailing_slash:
// 's3://datacore' -> ''
fn test_new_hadoop_catalog_strips_s3_url_bucket_no_trailing_slash() {
	catalog := new_hadoop_catalog(&S3StorageAdapter{}, 's3://datacore')
	assert catalog.warehouse == '', 'expected empty string, got ${catalog.warehouse}'
}

// test_new_hadoop_catalog_keeps_relative_path:
// 'iceberg/warehouse' -> 'iceberg/warehouse' (이미 relative)
fn test_new_hadoop_catalog_keeps_relative_path() {
	catalog := new_hadoop_catalog(&S3StorageAdapter{}, 'iceberg/warehouse')
	assert catalog.warehouse == 'iceberg/warehouse', 'expected iceberg/warehouse, got ${catalog.warehouse}'
}

// test_new_hadoop_catalog_empty_warehouse:
// '' -> ''
fn test_new_hadoop_catalog_empty_warehouse() {
	catalog := new_hadoop_catalog(&S3StorageAdapter{}, '')
	assert catalog.warehouse == '', 'expected empty string, got ${catalog.warehouse}'
}
