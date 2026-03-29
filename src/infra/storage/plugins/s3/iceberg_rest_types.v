// v3 table format internal types for S3 storage plugin.
module s3

// v3 table format additional types (S3-internal)

/// IcebergDeleteFile represents a delete file (v3: Binary Deletion Vectors support).
/// ContentType: delete file type (position_deletes, equality_deletes, dv)
/// FilePath: file path
/// FileFormat: file format
/// RecordCount: number of deleted records
/// FileSizeInBytes: file size
/// ReferencedDataFile: referenced data file (used in dv type)
pub struct IcebergDeleteFile {
pub mut:
	content_type         string
	file_path            string
	file_format          string
	record_count         i64
	file_size_in_bytes   i64
	referenced_data_file string
	equality_ids         []int
	partition            map[string]string
}

/// IcebergDeletionVector represents a binary deletion vector (new in v3).
/// StorageType: storage type ('inline', 'path')
/// Data: inline data (base64 encoded)
/// Path: external file path
/// Offset: offset within the file (used in path type)
/// Length: data length
/// Cardinality: number of deleted rows
pub struct IcebergDeletionVector {
pub mut:
	storage_type string
	data         string
	path         string
	offset       i64
	length       i64
	cardinality  i64
}
