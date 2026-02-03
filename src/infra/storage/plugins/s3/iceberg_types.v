// 인프라 레이어 - Iceberg 테이블 형식 타입 정의
// Iceberg 메타데이터, 스냅샷, 매니페스트 관련 타입을 정의합니다.
module s3

import time
import crypto.md5

/// IcebergSchema는 Iceberg 테이블 스키마를 나타냅니다.
/// Fields: 스키마 필드 목록
/// SchemaId: 스키마 고유 ID
/// IdentifierFieldIds: 식별자 필드 ID 목록
pub struct IcebergSchema {
pub mut:
	schema_id            int
	fields               []IcebergField
	identifier_field_ids []int
}

/// IcebergField는 Iceberg 스키마 필드를 나타냅니다.
/// Id: 필드 ID
/// Name: 필드 이름
/// Type: 필드 타입 (string, int, long, double, boolean, binary, timestamp, timestamp_ns, unknown, variant, geometry, geography)
/// Required: 필수 여부
/// DefaultValue: 기본값 (v3 지원)
pub struct IcebergField {
pub mut:
	id            int
	name          string
	typ           string
	required      bool
	default_value string // v3: 기본 컬럼 값 지원
}

/// IcebergPartitionSpec은 파티션 사양을 나타냅니다.
/// SpecId: 사양 ID
/// Fields: 파티션 필드 목록
pub struct IcebergPartitionSpec {
pub mut:
	spec_id int
	fields  []IcebergPartitionField
}

/// IcebergPartitionField는 파티션 필드를 나타냅니다.
/// SourceId: 소스 컬럼 ID
/// FieldId: 파티션 필드 ID
/// Name: 파티션 이름
/// Transform: 변환 함수 (identity, bucket[N], truncate[N], year, month, day, hour)
/// TransformArgs: 변환 인자 목록 (v3: multi-argument transforms 지원)
pub struct IcebergPartitionField {
pub mut:
	source_id      int
	field_id       int
	name           string
	transform      string
	transform_args []string // v3: 멀티 인자 변환 지원
}

/// IcebergSnapshot은 테이블 스냅샷을 나타냅니다.
/// SnapshotId: 스냅샷 고유 ID
/// TimestampMs: 스냅샷 생성 시간 (밀리초)
/// ManifestList: 매니페스트 목록 파일 경로
/// SchemaId: 스키마 ID
/// Summary: 스냅샷 요약 정보
pub struct IcebergSnapshot {
pub mut:
	snapshot_id   i64
	timestamp_ms  i64
	manifest_list string
	schema_id     int
	summary       map[string]string
}

/// IcebergManifest는 데이터 파일 목록을 포함하는 매니페스트를 나타냅니다.
/// ManifestPath: 매니페스트 파일 경로
/// SnapshotId: 소속 스냅샷 ID
/// AddedFiles: 추가된 파일 수
/// DeletedFiles: 삭제된 파일 수
pub struct IcebergManifest {
pub mut:
	manifest_path string
	snapshot_id   i64
	added_files   int
	deleted_files int
	added_rows    i64
	deleted_rows  i64
}

/// IcebergDataFile은 실제 데이터 파일을 나타냅니다.
/// FilePath: S3 파일 경로
/// FileFormat: 파일 형식 (PARQUET, ORC, AVRO)
/// RecordCount: 레코드 수
/// FileSizeInBytes: 파일 크기 (바이트)
/// ColumnSizes: 컬럼별 크기 정보
/// ValueCounts: 컬럼별 값 개수
/// NullValueCounts: 컬럼별 NULL 개수
/// LowerBounds: 컬럼별 최소값
/// UpperBounds: 컬럼별 최대값
/// Partition: 파티션 값
pub struct IcebergDataFile {
pub mut:
	file_path          string
	file_format        string
	record_count       i64
	file_size_in_bytes i64
	column_sizes       map[string]i64
	value_counts       map[string]i64
	null_value_counts  map[string]i64
	lower_bounds       map[string]string
	upper_bounds       map[string]string
	partition          map[string]string
	// v3 Row Lineage 지원
	row_lineage_first  i64 // v3: 행 리니지 시작 ID
	row_lineage_last   i64 // v3: 행 리니지 종료 ID
}

/// IcebergMetadata는 테이블 메타데이터를 나타냅니다.
/// FormatVersion: Iceberg 포맷 버전 (1, 2 또는 3)
/// TableUuid: 테이블 UUID
/// Location: 테이블 위치 (S3 경로)
/// LastUpdatedMs: 마지막 업데이트 시간 (밀리초)
/// Schemas: 스키마 목록
/// CurrentSchemaId: 현재 스키마 ID
/// PartitionSpecs: 파티션 사양 목록
/// DefaultSpecId: 기본 파티션 사양 ID
/// Snapshots: 스냅샷 목록
/// CurrentSnapshotId: 현재 스냅샷 ID
/// Properties: 테이블 속성
pub struct IcebergMetadata {
pub mut:
	format_version      int = 3 // v3 기본값으로 변경
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

/// IcebergTableIdentifier는 테이블 식별자를 나타냅니다.
/// Namespace: 네임스페이스 (데이터베이스)
/// Name: 테이블 이름
pub struct IcebergTableIdentifier {
pub:
	namespace []string
	name      string
}

/// IcebergCatalogConfig는 카탈로그 설정을 나타냅니다.
/// CatalogType: 카탈로그 유형 (glue, hive, rest, hadoop)
/// Warehouse: 기본 S3 위치
/// Region: AWS 리전
/// Endpoint: REST 카탈로그 엔드포인트 (선택사항)
/// FormatVersion: 테이블 포맷 버전 (1, 2, 3)
pub struct IcebergCatalogConfig {
pub:
	catalog_type   string = 'hadoop'
	warehouse      string
	region         string = 'us-east-1'
	endpoint       string
	format_version int    = 3 // v3 기본값
}

/// generate_table_uuid는 새로운 테이블 UUID를 생성합니다.
pub fn generate_table_uuid() string {
	now := time.now()
	hash := md5.sum(now.str().bytes())
	return '${hash[0..8].hex()}-${hash[8..12].hex()}-${hash[12..16].hex()}-${hash[16..20].hex()}-${hash[20..32].hex()}'
}

/// generate_snapshot_id는 새로운 스냅샷 ID를 생성합니다.
pub fn generate_snapshot_id() i64 {
	return time.now().unix_milli()
}

/// create_default_schema는 기본 Kafka 레코드 스키마를 생성합니다.
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

/// create_default_partition_spec은 기본 파티션 사양을 생성합니다.
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

/// IcebergConfig는 Iceberg Writer 설정을 나타냅니다.
pub struct IcebergConfig {
pub mut:
	enabled           bool
	format            string = 'parquet'
	compression       string = 'zstd'
	write_mode        string = 'append'
	partition_by      []string
	max_rows_per_file int = 1000000
	max_file_size_mb  int = 128
	schema_evolution  bool
}

/// create_partition_spec_from_config는 설정에서 파티션 사양을 생성합니다.
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
