module s3

import time
import crypto.md5

/// IcebergSchema represents an Iceberg table schema.
/// Fields: list of schema fields
/// SchemaId: unique schema ID
/// IdentifierFieldIds: list of identifier field IDs
pub struct IcebergSchema {
pub mut:
	schema_id            int
	fields               []IcebergField
	identifier_field_ids []int
}

/// IcebergField represents an Iceberg schema field.
/// Id: field ID
/// Name: field name
/// Type: field type (string, int, long, double, boolean, binary, timestamp, timestamp_ns, unknown, variant, geometry, geography)
/// Required: whether the field is required
/// DefaultValue: default value (v3 support)
pub struct IcebergField {
pub mut:
	id            int
	name          string
	typ           string
	required      bool
	default_value string
}

/// IcebergPartitionSpec represents a partition specification.
/// SpecId: specification ID
/// Fields: list of partition fields
pub struct IcebergPartitionSpec {
pub mut:
	spec_id int
	fields  []IcebergPartitionField
}

/// IcebergPartitionField represents a partition field.
/// SourceId: source column ID
/// FieldId: partition field ID
/// Name: partition name
/// Transform: transform function (identity, bucket[N], truncate[N], year, month, day, hour)
/// TransformArgs: list of transform arguments (v3: multi-argument transforms support)
pub struct IcebergPartitionField {
pub mut:
	source_id      int
	field_id       int
	name           string
	transform      string
	transform_args []string
}

/// IcebergSnapshot represents a table snapshot.
/// SnapshotId: unique snapshot ID
/// TimestampMs: snapshot creation time (milliseconds)
/// ManifestList: manifest list file path
/// SchemaId: schema ID
/// Summary: snapshot summary information
pub struct IcebergSnapshot {
pub mut:
	snapshot_id   i64
	timestamp_ms  i64
	manifest_list string
	schema_id     int
	summary       map[string]string
}

/// IcebergManifest represents a manifest containing a list of data files.
/// ManifestPath: manifest file path
/// SnapshotId: owning snapshot ID
/// AddedFiles: number of added files
/// DeletedFiles: number of deleted files
pub struct IcebergManifest {
pub mut:
	manifest_path string
	snapshot_id   i64
	added_files   int
	deleted_files int
	added_rows    i64
	deleted_rows  i64
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

/// IcebergMetadata represents table metadata.
/// FormatVersion: Iceberg format version (1, 2, or 3)
/// TableUuid: table UUID
/// Location: table location (S3 path)
/// LastUpdatedMs: last update time (milliseconds)
/// Schemas: list of schemas
/// CurrentSchemaId: current schema ID
/// PartitionSpecs: list of partition specifications
/// DefaultSpecId: default partition specification ID
/// Snapshots: list of snapshots
/// CurrentSnapshotId: current snapshot ID
/// Properties: table properties
pub struct IcebergMetadata {
pub mut:
	// 기본값 2: v3 기능(row lineage 등) 미구현으로 안정 스펙인 v2 사용
	format_version      int = 2
	table_uuid          string
	location            string
	last_updated_ms     i64
	schemas             []IcebergSchema
	current_schema_id   int
	partition_specs     []IcebergPartitionSpec
	default_spec_id     int
	snapshots           []IcebergSnapshot
	current_snapshot_id i64
	properties          map[string]string
}

/// IcebergTableIdentifier represents a table identifier.
/// Namespace: namespace (database)
/// Name: table name
pub struct IcebergTableIdentifier {
pub:
	namespace []string
	name      string
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

/// generate_table_uuid generates a UUID for a new table.
/// seed: deterministic input (e.g. table location or name) for reproducible UUIDs.
///       Pass '' to fall back to a timestamp-based value.
pub fn generate_table_uuid(seed string) string {
	input := if seed != '' { seed } else { time.now().str() }
	hash := md5.sum(input.bytes())
	return '${hash[0..4].hex()}-${hash[4..6].hex()}-${hash[6..8].hex()}-${hash[8..10].hex()}-${hash[10..16].hex()}'
}

/// generate_snapshot_id generates a new snapshot ID.
pub fn generate_snapshot_id() i64 {
	return time.now().unix_milli()
}

/// create_default_schema creates the default Kafka record schema.
pub fn create_default_schema() IcebergSchema {
	return IcebergSchema{
		schema_id:            0
		fields:               [
			IcebergField{
				id:       1
				name:     'offset'
				typ:      'long'
				required: true
			},
			IcebergField{
				id:       2
				name:     'timestamp'
				typ:      'timestamp'
				required: true
			},
			IcebergField{
				id:       3
				name:     'topic'
				typ:      'string'
				required: true
			},
			IcebergField{
				id:       4
				name:     'partition'
				typ:      'int'
				required: true
			},
			IcebergField{
				id:       5
				name:     'key'
				typ:      'binary'
				required: false
			},
			IcebergField{
				id:       6
				name:     'value'
				typ:      'binary'
				required: false
			},
			IcebergField{
				id:       7
				name:     'headers'
				typ:      'string'
				required: false
			},
		]
		identifier_field_ids: [1]
	}
}

/// create_default_partition_spec creates the default partition specification.
pub fn create_default_partition_spec() IcebergPartitionSpec {
	return IcebergPartitionSpec{
		spec_id: 0
		fields:  [
			IcebergPartitionField{
				source_id: 2
				field_id:  1000
				name:      'timestamp_day'
				transform: 'day'
			},
			IcebergPartitionField{
				source_id: 3
				field_id:  1001
				name:      'topic'
				transform: 'identity'
			},
		]
	}
}

/// IcebergConfig represents Iceberg Writer configuration.
/// format_version: Iceberg 포맷 버전 (기본값 2 - 안정 스펙, v3 기능 미구현)
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
pub fn create_partition_spec_from_config(partition_by []string) IcebergPartitionSpec {
	mut fields := []IcebergPartitionField{}
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

		fields << IcebergPartitionField{
			source_id: source_id
			field_id:  field_id
			name:      column
			transform: if column == 'timestamp' { 'day' } else { 'identity' }
		}

		field_id++
	}

	return IcebergPartitionSpec{
		spec_id: 0
		fields:  fields
	}
}
