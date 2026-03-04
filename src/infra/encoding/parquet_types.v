// Shared Parquet type definitions, enums, and constants.
// Used by parquet_encoder.v, parquet_decoder.v, and parquet_metadata.v.
module encoding

// Parquet physical types (parquet.thrift Type enum).
const parquet_type_boolean = i32(0)
const parquet_type_int32 = i32(1)
const parquet_type_int64 = i32(2)
const parquet_type_int96 = i32(3)
const parquet_type_float = i32(4)
const parquet_type_double = i32(5)
const parquet_type_byte_array = i32(6)
const parquet_type_fixed_len_byte_array = i32(7)

// Parquet encoding types (parquet.thrift Encoding enum).
const parquet_encoding_plain = i32(0)
const parquet_encoding_rle = i32(3)
const parquet_encoding_bit_packed = i32(4)

// Parquet compression codecs (parquet.thrift CompressionCodec enum).
const parquet_compression_uncompressed = i32(0)
const parquet_compression_snappy = i32(1)
const parquet_compression_gzip = i32(2)
const parquet_compression_lzo = i32(3)
const parquet_compression_brotli = i32(4)
const parquet_compression_lz4 = i32(5)
const parquet_compression_zstd = i32(6)

// Parquet page types (parquet.thrift PageType enum).
const parquet_page_data = i32(0)
const parquet_page_index = i32(1)
const parquet_page_dictionary = i32(2)
const parquet_page_data_v2 = i32(3)

// Parquet field repetition types (parquet.thrift FieldRepetitionType enum).
const parquet_required = i32(0)
const parquet_optional = i32(1)
const parquet_repeated = i32(2)

// Parquet converted types for logical annotations.
const parquet_converted_type_utf8 = i32(0)
const parquet_converted_type_timestamp_millis = i32(9)

// Parquet magic bytes written at start and end of file.
const parquet_magic = [u8(`P`), u8(`A`), u8(`R`), u8(`1`)]!

/// ParquetCompression represents the compression method for Parquet files.
pub enum ParquetCompression {
	uncompressed
	snappy
	gzip
	lzo
	brotli
	lz4
	zstd
}

/// Converts ParquetCompression to a string.
pub fn (pc ParquetCompression) str() string {
	return match pc {
		.uncompressed { 'UNCOMPRESSED' }
		.snappy { 'SNAPPY' }
		.gzip { 'GZIP' }
		.lzo { 'LZO' }
		.brotli { 'BROTLI' }
		.lz4 { 'LZ4' }
		.zstd { 'ZSTD' }
	}
}

// to_thrift_codec returns the Thrift codec integer for the given compression.
fn (pc ParquetCompression) to_thrift_codec() i32 {
	return match pc {
		.uncompressed { parquet_compression_uncompressed }
		.snappy { parquet_compression_snappy }
		.gzip { parquet_compression_gzip }
		.lzo { parquet_compression_lzo }
		.brotli { parquet_compression_brotli }
		.lz4 { parquet_compression_lz4 }
		.zstd { parquet_compression_zstd }
	}
}

/// Parses ParquetCompression from a string.
pub fn parquet_compression_from_string(s string) !ParquetCompression {
	return match s.to_lower() {
		'uncompressed', 'none' { ParquetCompression.uncompressed }
		'snappy' { ParquetCompression.snappy }
		'gzip' { ParquetCompression.gzip }
		'lzo' { ParquetCompression.lzo }
		'brotli' { ParquetCompression.brotli }
		'lz4' { ParquetCompression.lz4 }
		'zstd', 'zstandard' { ParquetCompression.zstd }
		else { return error('unknown parquet compression: ${s}') }
	}
}

/// ParquetSchema represents the schema of a Parquet file.
pub struct ParquetSchema {
pub mut:
	columns []ParquetColumn
}

/// ParquetColumn represents a Parquet column definition.
pub struct ParquetColumn {
pub mut:
	name     string
	typ      ParquetDataType
	required bool
}

/// ParquetDataType represents a Parquet data type.
pub enum ParquetDataType {
	boolean
	int32
	int64
	float
	double
	binary
	string
	timestamp_millis
	timestamp_micros
}

// to_physical_type returns the Parquet physical type integer for metadata encoding.
fn (dt ParquetDataType) to_physical_type() i32 {
	return match dt {
		.boolean { parquet_type_boolean }
		.int32 { parquet_type_int32 }
		.int64 { parquet_type_int64 }
		.float { parquet_type_float }
		.double { parquet_type_double }
		.binary { parquet_type_byte_array }
		.string { parquet_type_byte_array }
		.timestamp_millis { parquet_type_int64 }
		.timestamp_micros { parquet_type_int64 }
	}
}

// has_converted_type returns whether this type requires a ConvertedType annotation.
fn (dt ParquetDataType) has_converted_type() bool {
	return dt == .string || dt == .timestamp_millis || dt == .timestamp_micros
}

// converted_type returns the Parquet ConvertedType integer if applicable.
fn (dt ParquetDataType) converted_type() i32 {
	return match dt {
		.string { parquet_converted_type_utf8 }
		.timestamp_millis { parquet_converted_type_timestamp_millis }
		else { 0 }
	}
}

/// ParquetRowGroup represents a Row Group in a Parquet file.
pub struct ParquetRowGroup {
pub mut:
	row_count int
	columns   []ParquetColumnChunk
}

/// ParquetColumnChunk represents a column chunk.
pub struct ParquetColumnChunk {
pub mut:
	column_name string
	data_offset i64
	data_size   i64
	value_count i64
	null_count  i64
	min_value   string
	max_value   string
	min_bytes   []u8
	max_bytes   []u8
	compression ParquetCompression
}

/// ParquetMetadata represents Parquet file metadata.
pub struct ParquetMetadata {
pub mut:
	schema      ParquetSchema
	row_groups  []ParquetRowGroup
	created_by  string
	num_rows    i64
	compression ParquetCompression
	file_size   i64
}

/// ParquetRecord represents a record to be written to a Parquet file.
pub struct ParquetRecord {
pub mut:
	offset    i64
	timestamp i64
	topic     string
	partition int
	key       []u8
	value     []u8
	headers   string
}

/// ParquetFileInfo represents Parquet file information.
pub struct ParquetFileInfo {
pub:
	file_path     string
	record_count  i64
	file_size     i64
	min_offset    i64
	max_offset    i64
	min_timestamp i64
	max_timestamp i64
	compression   string
}
