module s3

import time
import service.port

/// IcebergManifest represents a manifest containing a list of data files.
/// ManifestPath: manifest file path
/// ManifestLength: byte size of the manifest file
/// SnapshotId: owning snapshot ID
/// AddedFiles: number of added files
/// DeletedFiles: number of deleted files
pub struct IcebergManifest {
pub mut:
	manifest_path   string
	manifest_length i64
	snapshot_id     i64
	added_files     int
	deleted_files   int
	added_rows      i64
	deleted_rows    i64
}

/// IcebergDataFile represents an actual data file.
/// FilePath: S3 file path
/// FileFormat: file format (PARQUET, ORC, AVRO)
/// RecordCount: number of records
/// FileSizeInBytes: file size in bytes
/// ColumnSizes: per-column size information (key = field_id)
/// ValueCounts: per-column value counts (key = field_id)
/// NullValueCounts: per-column NULL counts (key = field_id)
/// LowerBounds: per-column minimum values serialized as Iceberg binary (key = field_id)
/// UpperBounds: per-column maximum values serialized as Iceberg binary (key = field_id)
/// Partition: partition values
pub struct IcebergDataFile {
pub mut:
	file_path          string
	file_format        string
	record_count       i64
	file_size_in_bytes i64
	column_sizes       map[int]i64
	value_counts       map[int]i64
	null_value_counts  map[int]i64
	lower_bounds       map[int][]u8
	upper_bounds       map[int][]u8
	partition          map[string]string
	// v3 Row Lineage support
	row_lineage_first i64
	row_lineage_last  i64
}

/// IcebergCatalogConfig represents catalog configuration.
/// CatalogType: catalog type (glue, hive, rest, hadoop)
/// Warehouse: default S3 location
/// Region: AWS region
/// Endpoint: REST catalog endpoint (optional)
/// FormatVersion: table format version (1, 2, 3)
pub struct IcebergCatalogConfig {
pub:
	catalog_type   string = 'hadoop'
	warehouse      string
	region         string = 'us-east-1'
	endpoint       string
	format_version int = 3
}

/// generate_snapshot_id generates a new snapshot ID.
fn generate_snapshot_id() i64 {
	return time.now().unix_milli()
}

/// IcebergConfig represents Iceberg Writer configuration.
/// format_version: Iceberg format version (default 2 - stable spec, v3 not yet implemented)
pub struct IcebergConfig {
pub mut:
	enabled           bool
	format            string   = 'parquet'
	compression       string   = 'zstd'
	write_mode        string   = 'append'
	partition_by      []string = ['timestamp', 'topic']
	max_rows_per_file int      = 1000000
	max_file_size_mb  int      = 128
	schema_evolution  bool     = true
	format_version    int      = 2
}

/// create_partition_spec_from_config creates a partition specification from config.
fn create_partition_spec_from_config(partition_by []string) port.IcebergPartitionSpec {
	mut fields := []port.IcebergPartitionField{}
	mut field_id := 1000

	for column in partition_by {
		source_id := match column {
			'offset' { 1 }
			'timestamp' { 2 }
			'topic' { 3 }
			'partition' { 4 }
			'key' { 5 }
			'value' { 6 }
			'headers' { 7 }
			else { 2 }
		}

		fields << port.IcebergPartitionField{
			source_id: source_id
			field_id:  field_id
			name:      column
			transform: if column == 'timestamp' { 'day' } else { 'identity' }
		}

		field_id++
	}

	return port.IcebergPartitionSpec{
		spec_id: 0
		fields:  fields
	}
}
