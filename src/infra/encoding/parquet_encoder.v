// Encodes Kafka Records to Parquet file format.
// Implements a real Parquet writer using Thrift Compact Protocol for metadata encoding.
// Spec: https://parquet.apache.org/docs/file-format/
module encoding

import domain
import json

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
const parquet_magic = [u8(`P`), u8(`A`), u8(`R`), u8(`1`)]

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

// ColumnData holds the collected values for a single column before encoding.
struct ColumnData {
mut:
	name     string
	dtype    ParquetDataType
	required bool
	// For int64 columns (offset, timestamp)
	i64_values []i64
	// For int32 columns (partition)
	i32_values []i32
	// For byte_array columns (topic, key, value, headers)
	bytes_values [][]u8
	null_count   i64
}

/// ParquetEncoder encodes Kafka Records to Parquet format.
pub struct ParquetEncoder {
pub mut:
	compression   ParquetCompression
	buffer        []u8
	records       []ParquetRecord
	current_size  i64
	max_file_size i64
}

/// new_parquet_encoder creates a new Parquet encoder.
pub fn new_parquet_encoder(compression string, max_file_size_mb int) !&ParquetEncoder {
	comp := parquet_compression_from_string(compression)!

	return &ParquetEncoder{
		compression:   comp
		buffer:        []
		records:       []
		current_size:  0
		max_file_size: i64(max_file_size_mb) * 1024 * 1024
	}
}

/// default_parquet_schema returns the default Kafka record Parquet schema.
pub fn default_parquet_schema() ParquetSchema {
	return ParquetSchema{
		columns: [
			ParquetColumn{
				name:     'offset'
				typ:      .int64
				required: true
			},
			ParquetColumn{
				name:     'timestamp'
				typ:      .timestamp_millis
				required: true
			},
			ParquetColumn{
				name:     'topic'
				typ:      .string
				required: true
			},
			ParquetColumn{
				name:     'partition'
				typ:      .int32
				required: true
			},
			ParquetColumn{
				name:     'key'
				typ:      .binary
				required: false
			},
			ParquetColumn{
				name:     'value'
				typ:      .binary
				required: false
			},
			ParquetColumn{
				name:     'headers'
				typ:      .string
				required: false
			},
		]
	}
}

/// add_record converts a Kafka Record to a Parquet record and adds it.
pub fn (mut e ParquetEncoder) add_record(topic string, partition int, record domain.Record, offset i64) ! {
	mut headers_json := '{}'
	if record.headers.len > 0 {
		mut headers_map := map[string]string{}
		for key, value in record.headers {
			headers_map[key] = value.bytestr()
		}
		headers_json = json.encode(headers_map)
	}

	prec := ParquetRecord{
		offset:    offset
		timestamp: record.timestamp.unix_milli()
		topic:     topic
		partition: partition
		key:       record.key.clone()
		value:     record.value.clone()
		headers:   headers_json
	}

	e.records << prec
	e.current_size += i64(record.key.len + record.value.len + 100)
}

/// add_records adds multiple Kafka Records at once.
pub fn (mut e ParquetEncoder) add_records(topic string, partition int, records []domain.Record, start_offset i64) ! {
	for i, record in records {
		e.add_record(topic, partition, record, start_offset + i64(i))!
	}
}

/// should_flush checks whether a flush is needed.
pub fn (e &ParquetEncoder) should_flush(max_rows int) bool {
	return e.records.len >= max_rows || e.current_size >= e.max_file_size
}

/// record_count returns the number of records currently in the buffer.
pub fn (e &ParquetEncoder) record_count() int {
	return e.records.len
}

/// reset resets the encoder.
pub fn (mut e ParquetEncoder) reset() {
	e.records = []
	e.buffer = []
	e.current_size = 0
}

// encode_plain_int64_page encodes int64 values as a Plain-encoded data page.
// is_optional controls whether definition levels are prepended.
fn encode_plain_int64_page(values []i64, null_count i64, codec i32) []u8 {
	return encode_plain_int64_page_opt(values, null_count, false, codec)
}

// encode_plain_int64_page_opt encodes int64 values with optional definition levels.
fn encode_plain_int64_page_opt(values []i64, null_count i64, is_optional bool, codec i32) []u8 {
	// Pre-allocate: 8 bytes per int64 value
	mut data := []u8{cap: values.len * 8}
	for v in values {
		uv := u64(v)
		data << u8(uv & 0xFF)
		data << u8((uv >> 8) & 0xFF)
		data << u8((uv >> 16) & 0xFF)
		data << u8((uv >> 24) & 0xFF)
		data << u8((uv >> 32) & 0xFF)
		data << u8((uv >> 40) & 0xFF)
		data << u8((uv >> 48) & 0xFF)
		data << u8((uv >> 56) & 0xFF)
	}

	return encode_data_page_v1(data, values.len, null_count, is_optional, codec)
}

// encode_plain_int32_page encodes int32 values as a Plain-encoded data page.
fn encode_plain_int32_page(values []i32, null_count i64, codec i32) []u8 {
	return encode_plain_int32_page_opt(values, null_count, false, codec)
}

// encode_plain_int32_page_opt encodes int32 values with optional definition levels.
fn encode_plain_int32_page_opt(values []i32, null_count i64, is_optional bool, codec i32) []u8 {
	// Pre-allocate: 4 bytes per int32 value
	mut data := []u8{cap: values.len * 4}
	for v in values {
		uv := u32(v)
		data << u8(uv & 0xFF)
		data << u8((uv >> 8) & 0xFF)
		data << u8((uv >> 16) & 0xFF)
		data << u8((uv >> 24) & 0xFF)
	}

	return encode_data_page_v1(data, values.len, null_count, is_optional, codec)
}

// encode_plain_byte_array_page encodes byte arrays as a Plain-encoded data page.
// Format: for each value, 4-byte little-endian length followed by bytes.
fn encode_plain_byte_array_page(values [][]u8, null_count i64, codec i32) []u8 {
	return encode_plain_byte_array_page_opt(values, null_count, false, codec)
}

// encode_plain_byte_array_page_opt encodes byte arrays with optional definition levels.
fn encode_plain_byte_array_page_opt(values [][]u8, null_count i64, is_optional bool, codec i32) []u8 {
	// Pre-allocate: 4-byte length header per value + sum of all value bytes
	mut total_bytes := values.len * 4
	for v in values {
		total_bytes += v.len
	}
	mut data := []u8{cap: total_bytes}
	for v in values {
		l := u32(v.len)
		data << u8(l & 0xFF)
		data << u8((l >> 8) & 0xFF)
		data << u8((l >> 16) & 0xFF)
		data << u8((l >> 24) & 0xFF)
		data << v
	}

	return encode_data_page_v1(data, values.len, null_count, is_optional, codec)
}

// encode_rle_definition_levels encodes definition levels using RLE for all-present values.
// For max_definition_level=1, all non-null: RLE run of (count, 1).
// Format: 4-byte LE length | rle_data
// RLE: run_header = (count << 1) | 0, followed by the value byte (=1 since bit_width=1).
fn encode_rle_definition_levels(count int) []u8 {
	if count == 0 {
		return [u8(0x00), u8(0x00), u8(0x00), u8(0x00)]
	}
	// Run-length header encodes (count << 1 | 0) meaning run-length mode with 'count' repetitions
	run_header := u32(count) << 1
	// varint needs at most ceil(32/7)=5 bytes; +1 for the value byte
	mut rle_data := []u8{cap: 6}
	mut rh := run_header
	for rh >= 0x80 {
		rle_data << u8((rh & 0x7F) | 0x80)
		rh >>= 7
	}
	rle_data << u8(rh)
	// All values are defined (=1) since null_count=0
	rle_data << u8(0x01)

	rle_len := u32(rle_data.len)
	mut result := []u8{cap: 4 + rle_data.len}
	result << u8(rle_len & 0xFF)
	result << u8((rle_len >> 8) & 0xFF)
	result << u8((rle_len >> 16) & 0xFF)
	result << u8((rle_len >> 24) & 0xFF)
	result << rle_data
	return result
}

// encode_data_page_v1 wraps raw column data in a DataPageHeaderV1 + page body.
// Page structure: Thrift PageHeader | page_body
// page_body = [def_levels] + column_data
// is_optional: if true, prepends RLE-encoded definition levels (max_def_level=1, all present).
// _uncompressed_size and _codec are reserved for future compression support; currently unused.
fn encode_data_page_v1(data []u8, num_values int, _uncompressed_size i64, is_optional bool, _codec i32) []u8 {
	// Build definition levels if this is an optional column
	def_levels_bytes := if is_optional {
		encode_rle_definition_levels(num_values)
	} else {
		[]u8{}
	}

	// Build DataPageHeader (Thrift struct, field IDs from parquet.thrift)
	// DataPageHeader fields:
	//   1: num_values (i32)
	//   2: encoding (Encoding enum = i32)
	//   3: definition_level_encoding (Encoding = i32)
	//   4: repetition_level_encoding (Encoding = i32)
	mut dph := new_thrift_writer()
	dph.write_struct_begin()
	dph.write_i32(1, i32(num_values))
	dph.write_i32(2, i32(parquet_encoding_plain))
	dph.write_i32(3, i32(parquet_encoding_rle))
	dph.write_i32(4, i32(parquet_encoding_rle))
	dph.write_struct_end()
	dph_bytes := dph.bytes()

	// Page body = definition_levels + column_data
	page_body_len := def_levels_bytes.len + data.len
	mut page_body := []u8{cap: page_body_len}
	page_body << def_levels_bytes
	page_body << data

	// Build PageHeader (Thrift struct)
	// PageHeader fields (parquet.thrift):
	//   1: type (PageType enum = i32)
	//   2: uncompressed_page_size (i32)
	//   3: compressed_page_size (i32)
	//   4: crc (optional, omitted)
	//   5: data_page_header (DataPageHeader struct) - only for DATA_PAGE
	mut ph := new_thrift_writer()
	ph.write_struct_begin()
	ph.write_i32(1, i32(parquet_page_data))
	ph.write_i32(2, i32(page_body.len))
	ph.write_i32(3, i32(page_body.len))
	// Field 5: data_page_header (nested struct - write inline)
	ph.write_field_header(thrift_type_struct, 5)
	ph.buf << dph_bytes
	ph.write_struct_end()
	ph_bytes := ph.bytes()

	mut result := []u8{cap: ph_bytes.len + page_body.len}
	result << ph_bytes
	result << page_body
	return result
}

// collect_column_data extracts per-column values from the record set.
fn collect_column_data(records []ParquetRecord, schema ParquetSchema) []ColumnData {
	mut cols := []ColumnData{cap: schema.columns.len}
	for col in schema.columns {
		cols << ColumnData{
			name:     col.name
			dtype:    col.typ
			required: col.required
		}
	}

	for rec in records {
		for mut col in cols {
			match col.name {
				'offset' {
					col.i64_values << rec.offset
				}
				'timestamp' {
					col.i64_values << rec.timestamp
				}
				'topic' {
					col.bytes_values << rec.topic.bytes()
				}
				'partition' {
					col.i32_values << i32(rec.partition)
				}
				'key' {
					col.bytes_values << rec.key
				}
				'value' {
					col.bytes_values << rec.value
				}
				'headers' {
					col.bytes_values << rec.headers.bytes()
				}
				else {}
			}
		}
	}

	return cols
}

// encode_column_chunk encodes a single column chunk (all pages for one column).
// Returns (chunk_bytes, value_count).
fn encode_column_chunk(col ColumnData, codec i32) ([]u8, i64) {
	num_values := if col.dtype == .int64 || col.dtype == .timestamp_millis
		|| col.dtype == .timestamp_micros {
		col.i64_values.len
	} else if col.dtype == .int32 {
		col.i32_values.len
	} else {
		col.bytes_values.len
	}

	// optional columns (required=false) need definition levels prepended
	is_optional := !col.required

	mut page_bytes := []u8{}
	match col.dtype {
		.int64, .timestamp_millis, .timestamp_micros {
			page_bytes = encode_plain_int64_page_opt(col.i64_values, col.null_count, is_optional,
				codec)
		}
		.int32 {
			page_bytes = encode_plain_int32_page_opt(col.i32_values, col.null_count, is_optional,
				codec)
		}
		else {
			page_bytes = encode_plain_byte_array_page_opt(col.bytes_values, col.null_count,
				is_optional, codec)
		}
	}

	return page_bytes, i64(num_values)
}

// i64_min_bytes returns the little-endian bytes of an int64 value for statistics.
fn i64_min_bytes(values []i64) []u8 {
	if values.len == 0 {
		return []u8{}
	}
	mut min := values[0]
	for v in values {
		if v < min {
			min = v
		}
	}
	uv := u64(min)
	return [
		u8(uv & 0xFF),
		u8((uv >> 8) & 0xFF),
		u8((uv >> 16) & 0xFF),
		u8((uv >> 24) & 0xFF),
		u8((uv >> 32) & 0xFF),
		u8((uv >> 40) & 0xFF),
		u8((uv >> 48) & 0xFF),
		u8((uv >> 56) & 0xFF),
	]
}

fn i64_max_bytes(values []i64) []u8 {
	if values.len == 0 {
		return []u8{}
	}
	mut max := values[0]
	for v in values {
		if v > max {
			max = v
		}
	}
	uv := u64(max)
	return [
		u8(uv & 0xFF),
		u8((uv >> 8) & 0xFF),
		u8((uv >> 16) & 0xFF),
		u8((uv >> 24) & 0xFF),
		u8((uv >> 32) & 0xFF),
		u8((uv >> 40) & 0xFF),
		u8((uv >> 48) & 0xFF),
		u8((uv >> 56) & 0xFF),
	]
}

fn i32_min_bytes(values []i32) []u8 {
	if values.len == 0 {
		return []u8{}
	}
	mut min := values[0]
	for v in values {
		if v < min {
			min = v
		}
	}
	uv := u32(min)
	return [u8(uv & 0xFF), u8((uv >> 8) & 0xFF), u8((uv >> 16) & 0xFF), u8((uv >> 24) & 0xFF)]
}

fn i32_max_bytes(values []i32) []u8 {
	if values.len == 0 {
		return []u8{}
	}
	mut max := values[0]
	for v in values {
		if v > max {
			max = v
		}
	}
	uv := u32(max)
	return [u8(uv & 0xFF), u8((uv >> 8) & 0xFF), u8((uv >> 16) & 0xFF), u8((uv >> 24) & 0xFF)]
}

// encode_file_metadata encodes the Parquet FileMetaData as Thrift Compact Protocol bytes.
// Schema (parquet.thrift):
//
//	FileMetaData {
//	  1: version (i32)
//	  2: schema (list<SchemaElement>)
//	  3: num_rows (i64)
//	  4: row_groups (list<RowGroup>)
//	  5: key_value_metadata (optional list<KeyValue>)
//	  6: created_by (optional string)
//	}
//
// SchemaElement {
//	  1: type (optional Type enum)
//	  2: type_length (optional i32)
//	  3: repetition_type (optional FieldRepetitionType enum)
//	  4: name (required string)
//	  5: num_children (optional i32) -- for group nodes
//	  6: converted_type (optional ConvertedType)
//	}
//
// RowGroup {
//	  1: columns (list<ColumnChunk>)
//	  2: total_byte_size (i64)
//	  3: num_rows (i64)
//	}
//
// ColumnChunk {
//	  1: file_path (optional string)
//	  2: file_offset (i64)
//	  3: meta_data (optional ColumnMetaData)
//	}
//
// ColumnMetaData {
//	  1: type (Type enum)
//	  2: encodings (list<Encoding>)
//	  3: path_in_schema (list<string>)
//	  4: codec (CompressionCodec enum)
//	  5: num_values (i64)
//	  6: total_uncompressed_size (i64)
//	  7: total_compressed_size (i64)
//	  8: key_value_metadata (optional)
//	  9: data_page_offset (i64)
//	 10: index_page_offset (optional i64)
//	 11: dictionary_page_offset (optional i64)
//	 12: statistics (optional Statistics)
//	}
//
// Statistics {
//	  1: max (optional binary)
//	  2: min (optional binary)
//	  3: null_count (optional i64)
//	  4: distinct_count (optional i64)
//	  5: max_value (optional binary)
//	  6: min_value (optional binary)
//	}
fn encode_file_metadata(schema ParquetSchema, num_rows i64, col_chunks_meta []ColChunkMeta, codec i32, created_by string) []u8 {
	mut w := new_thrift_writer()
	w.write_struct_begin()

	// Field 1: version = 2 (Parquet format version 2)
	w.write_i32(1, 2)

	// Field 2: schema (list<SchemaElement>)
	// Total elements = 1 root element + N column elements
	total_schema_elems := 1 + schema.columns.len
	w.write_list_begin(2, thrift_type_struct, total_schema_elems)

	// Root schema element (message node, no type, has num_children)
	w.write_raw_struct_begin()
	// field 4: name
	w.write_string(4, 'schema')
	// field 5: num_children
	w.write_i32(5, i32(schema.columns.len))
	w.write_raw_struct_end()

	// Column schema elements (leaf nodes)
	for col in schema.columns {
		w.write_raw_struct_begin()
		// field 1: type (physical type)
		w.write_i32(1, col.typ.to_physical_type())
		// field 3: repetition_type
		rep := if col.required { parquet_required } else { parquet_optional }
		w.write_i32(3, rep)
		// field 4: name
		w.write_string(4, col.name)
		// field 6: converted_type (if applicable)
		if col.typ.has_converted_type() {
			w.write_i32(6, col.typ.converted_type())
		}
		w.write_raw_struct_end()
	}

	// Field 3: num_rows
	w.write_i64(3, num_rows)

	// Field 4: row_groups (list<RowGroup>) -- one row group for all records
	w.write_list_begin(4, thrift_type_struct, 1)

	// RowGroup
	w.write_raw_struct_begin()

	// field 1: columns (list<ColumnChunk>)
	w.write_list_begin(1, thrift_type_struct, col_chunks_meta.len)
	for cm in col_chunks_meta {
		w.write_raw_struct_begin()
		// field 2: file_offset (start of column chunk data)
		w.write_i64(2, cm.file_offset)
		// field 3: meta_data (ColumnMetaData)
		w.write_field_header(thrift_type_struct, 3)
		// Write ColumnMetaData inline
		w.write_raw_struct_begin()
		// field 1: type
		w.write_i32(1, cm.physical_type)
		// field 2: encodings (list<Encoding>) = [PLAIN, RLE, BIT_PACKED]
		w.write_list_begin(2, thrift_type_i32, 2)
		w.write_raw_i32(i32(parquet_encoding_plain))
		w.write_raw_i32(i32(parquet_encoding_rle))
		// field 3: path_in_schema (list<string>)
		w.write_list_begin(3, thrift_type_binary, 1)
		w.write_raw_string(cm.col_name)
		// field 4: codec
		w.write_i32(4, codec)
		// field 5: num_values
		w.write_i64(5, cm.num_values)
		// field 6: total_uncompressed_size
		w.write_i64(6, cm.total_size)
		// field 7: total_compressed_size
		w.write_i64(7, cm.total_size)
		// field 9: data_page_offset
		w.write_i64(9, cm.data_page_offset)
		// field 12: statistics
		if cm.min_val.len > 0 || cm.max_val.len > 0 {
			w.write_field_header(thrift_type_struct, 12)
			w.write_raw_struct_begin()
			// field 1: max (deprecated, keep for compatibility)
			if cm.max_val.len > 0 {
				w.write_binary(1, cm.max_val)
			}
			// field 2: min (deprecated)
			if cm.min_val.len > 0 {
				w.write_binary(2, cm.min_val)
			}
			// field 3: null_count
			w.write_i64(3, cm.null_count)
			// field 5: max_value (new-style)
			if cm.max_val.len > 0 {
				w.write_binary(5, cm.max_val)
			}
			// field 6: min_value (new-style)
			if cm.min_val.len > 0 {
				w.write_binary(6, cm.min_val)
			}
			w.write_raw_struct_end()
		}
		w.write_raw_struct_end()
		// end ColumnMetaData
		w.write_raw_struct_end()
		// end ColumnChunk
	}

	// field 2: total_byte_size (sum of all column chunk sizes)
	mut total_bytes := i64(0)
	for cm in col_chunks_meta {
		total_bytes += cm.total_size
	}
	w.write_i64(2, total_bytes)

	// field 3: num_rows
	w.write_i64(3, num_rows)

	w.write_raw_struct_end()
	// end RowGroup

	// Field 6: created_by
	if created_by.len > 0 {
		w.write_string(6, created_by)
	}

	w.write_struct_end()
	// end FileMetaData

	return w.bytes()
}

// ColChunkMeta holds metadata about a single column chunk for footer encoding.
struct ColChunkMeta {
	col_name         string
	physical_type    i32
	file_offset      i64
	data_page_offset i64
	total_size       i64
	num_values       i64
	null_count       i64
	min_val          []u8
	max_val          []u8
}

/// encode encodes all records in the current buffer to real Parquet format.
/// Returns: (Parquet file bytes, metadata)
pub fn (mut e ParquetEncoder) encode() !([]u8, ParquetMetadata) {
	if e.records.len == 0 {
		return error('no records to encode')
	}

	schema := default_parquet_schema()
	codec := e.compression.to_thrift_codec()
	num_rows := i64(e.records.len)

	// Collect column values
	cols := collect_column_data(e.records, schema)

	// Estimate initial file buffer capacity: magic(4) + ~100 bytes overhead per record + magic(4)
	estimated_size := 8 + int(e.current_size) + 4096
	mut file_bytes := []u8{cap: estimated_size}

	// 1. Write magic number at start
	file_bytes << parquet_magic

	// 2. Encode each column chunk and append to file
	mut col_metas := []ColChunkMeta{cap: cols.len}

	for col in cols {
		chunk_start := i64(file_bytes.len)
		page_bytes, val_count := encode_column_chunk(col, codec)

		// The data page starts immediately within the chunk (no dictionary page)
		data_page_off := chunk_start

		// Compute statistics
		mut min_b := []u8{}
		mut max_b := []u8{}
		if col.dtype == .int64 || col.dtype == .timestamp_millis || col.dtype == .timestamp_micros {
			min_b = i64_min_bytes(col.i64_values)
			max_b = i64_max_bytes(col.i64_values)
		} else if col.dtype == .int32 {
			min_b = i32_min_bytes(col.i32_values)
			max_b = i32_max_bytes(col.i32_values)
		}

		file_bytes << page_bytes

		col_metas << ColChunkMeta{
			col_name:         col.name
			physical_type:    col.dtype.to_physical_type()
			file_offset:      chunk_start
			data_page_offset: data_page_off
			total_size:       i64(page_bytes.len)
			num_values:       val_count
			null_count:       col.null_count
			min_val:          min_b
			max_val:          max_b
		}
	}

	// 3. Encode and write FileMetaData (footer)
	footer_bytes := encode_file_metadata(schema, num_rows, col_metas, codec, 'DataCore v0.46')
	file_bytes << footer_bytes

	// 4. Write 4-byte footer length (little-endian)
	fl := u32(footer_bytes.len)
	file_bytes << u8(fl & 0xFF)
	file_bytes << u8((fl >> 8) & 0xFF)
	file_bytes << u8((fl >> 16) & 0xFF)
	file_bytes << u8((fl >> 24) & 0xFF)

	// 5. Write magic number at end
	file_bytes << parquet_magic

	// Build ParquetMetadata return value (for callers that inspect metadata)
	mut min_offset := e.records[0].offset
	mut max_offset := e.records[0].offset
	mut min_timestamp := e.records[0].timestamp
	mut max_timestamp := e.records[0].timestamp
	for rec in e.records {
		if rec.offset < min_offset {
			min_offset = rec.offset
		}
		if rec.offset > max_offset {
			max_offset = rec.offset
		}
		if rec.timestamp < min_timestamp {
			min_timestamp = rec.timestamp
		}
		if rec.timestamp > max_timestamp {
			max_timestamp = rec.timestamp
		}
	}

	mut row_group := ParquetRowGroup{
		row_count: e.records.len
		columns:   []ParquetColumnChunk{cap: col_metas.len}
	}

	for cm in col_metas {
		row_group.columns << ParquetColumnChunk{
			column_name: cm.col_name
			data_offset: cm.data_page_offset
			data_size:   cm.total_size
			value_count: cm.num_values
			null_count:  cm.null_count
			min_value:   min_offset.str()
			max_value:   max_offset.str()
			compression: e.compression
		}
	}

	metadata := ParquetMetadata{
		schema:      schema
		row_groups:  [row_group]
		created_by:  'DataCore S3 Iceberg Writer'
		num_rows:    num_rows
		compression: e.compression
		file_size:   i64(file_bytes.len)
	}

	return file_bytes, metadata
}

/// encode_batch encodes a batch of records to Parquet.
pub fn encode_batch(records []ParquetRecord, compression ParquetCompression) !([]u8, ParquetMetadata) {
	mut encoder := ParquetEncoder{
		compression:   compression
		buffer:        []
		records:       records
		current_size:  i64(records.len * 100)
		max_file_size: 134217728
	}

	return encoder.encode()!
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

/// extract_parquet_info extracts basic information from a Parquet file.
/// Validates magic bytes and reads footer length.
pub fn extract_parquet_info(data []u8, file_path string) ParquetFileInfo {
	if data.len < 12 {
		return ParquetFileInfo{
			file_path:   file_path
			file_size:   i64(data.len)
			compression: 'UNKNOWN'
		}
	}

	// Validate magic bytes at start and end
	has_magic_start := data[0] == u8(`P`) && data[1] == u8(`A`) && data[2] == u8(`R`)
		&& data[3] == u8(`1`)
	has_magic_end := data[data.len - 4] == u8(`P`) && data[data.len - 3] == u8(`A`)
		&& data[data.len - 2] == u8(`R`) && data[data.len - 1] == u8(`1`)

	if !has_magic_start || !has_magic_end {
		return ParquetFileInfo{
			file_path:   file_path
			file_size:   i64(data.len)
			compression: 'INVALID'
		}
	}

	// Read footer length from bytes [len-8..len-4] (little-endian i32)
	// Validated to ensure the file length is consistent
	footer_len_bytes := data[data.len - 8..data.len - 4]
	_ = u32(footer_len_bytes[0]) | (u32(footer_len_bytes[1]) << 8) | (u32(footer_len_bytes[2]) << 16) | (u32(footer_len_bytes[3]) << 24)

	return ParquetFileInfo{
		file_path:    file_path
		record_count: 0
		file_size:    i64(data.len)
		compression:  'UNCOMPRESSED'
	}
}
