// ParquetDecoder provides a high-level API for reading Parquet files.
// Integrates metadata parser and page decoders.
module encoding

// DecodedColumns holds all column values decoded from one row group.
struct DecodedColumns {
mut:
	offsets    []i64
	timestamps []i64
	topics     [][]u8
	partitions []i32
	keys       [][]u8
	values     [][]u8
	headers    [][]u8
}

// ParquetDecoder reads Parquet files and returns records.
pub struct ParquetDecoder {
mut:
	data     []u8
	metadata ParsedMetadata
}

// new_parquet_decoder creates a new decoder from raw Parquet file bytes.
fn new_parquet_decoder(data []u8) !&ParquetDecoder {
	mut parser := new_parquet_metadata_parser()
	metadata := parser.parse(data)!

	return &ParquetDecoder{
		data:     data
		metadata: metadata
	}
}

// row_count returns the number of rows in the file.
fn (d &ParquetDecoder) row_count() i64 {
	return d.metadata.num_rows
}

// schema returns the schema as a ParquetSchema with named columns.
fn (d &ParquetDecoder) schema() ParquetSchema {
	mut cols := []ParquetColumn{cap: d.metadata.schema.len}
	for elem in d.metadata.schema {
		cols << ParquetColumn{
			name:     elem.name
			typ:      elem.typ
			required: elem.repetition == .required
		}
	}
	return ParquetSchema{
		columns: cols
	}
}

// read_all reads all records from the file.
fn (mut d ParquetDecoder) read_all() ![]ParquetRecord {
	if d.metadata.row_groups.len == 0 {
		return []
	}

	mut all_records := []ParquetRecord{}
	for rg in d.metadata.row_groups {
		records := d.read_row_group(rg)!
		all_records << records
	}
	return all_records
}

// read_row_group reads all records from a single row group.
fn (mut d ParquetDecoder) read_row_group(group ParsedRowGroup) ![]ParquetRecord {
	row_count := int(group.row_count)
	if row_count == 0 || group.columns.len == 0 {
		return []
	}

	cols := d.decode_columns(group)!
	return build_records(row_count, cols)
}

// decode_columns decodes all column data from a row group.
fn (mut d ParquetDecoder) decode_columns(group ParsedRowGroup) !DecodedColumns {
	schema_names := d.metadata.schema.map(it.name)

	mut col_by_name := map[string]ParsedColumnMetadata{}
	for col in group.columns {
		name := if col.path.len > 0 { col.path[0] } else { '' }
		col_by_name[name] = col
	}

	mut result := DecodedColumns{}
	for name in schema_names {
		col := col_by_name[name] or { continue }
		offset := int(col.data_offset)
		if offset >= d.data.len {
			continue
		}
		col_data := d.data[offset..]
		decode_column_by_name(name, col_data, mut result)
	}
	return result
}

// decode_column_by_name decodes a single named column and stores result in cols.
fn decode_column_by_name(name string, col_data []u8, mut cols DecodedColumns) {
	match name {
		'offset' {
			cols.offsets = decode_plain_int64_page(col_data) or { []i64{} }
		}
		'timestamp' {
			cols.timestamps = decode_plain_int64_page(col_data) or { []i64{} }
		}
		'topic' {
			cols.topics = decode_plain_byte_array_page(col_data) or { [][]u8{} }
		}
		'partition' {
			cols.partitions = decode_plain_int32_page(col_data) or { []i32{} }
		}
		'key' {
			cols.keys = decode_optional_byte_array_page(col_data) or { [][]u8{} }
		}
		'value' {
			cols.values = decode_optional_byte_array_page(col_data) or { [][]u8{} }
		}
		'headers' {
			cols.headers = decode_optional_byte_array_page(col_data) or { [][]u8{} }
		}
		else {}
	}
}

// build_records constructs ParquetRecord slice from decoded columns.
fn build_records(row_count int, cols DecodedColumns) []ParquetRecord {
	mut records := []ParquetRecord{cap: row_count}
	for i in 0 .. row_count {
		records << ParquetRecord{
			offset:    if i < cols.offsets.len { cols.offsets[i] } else { 0 }
			timestamp: if i < cols.timestamps.len { cols.timestamps[i] } else { 0 }
			topic:     if i < cols.topics.len { cols.topics[i].bytestr() } else { '' }
			partition: if i < cols.partitions.len { int(cols.partitions[i]) } else { 0 }
			key:       if i < cols.keys.len { cols.keys[i].clone() } else { []u8{} }
			value:     if i < cols.values.len { cols.values[i].clone() } else { []u8{} }
			headers:   if i < cols.headers.len { cols.headers[i].bytestr() } else { '' }
		}
	}
	return records
}

// decode_optional_byte_array_page decodes a byte array page that may have RLE definition levels.
// Handles both required (no def levels) and optional (RLE def levels prepended) columns.
fn decode_optional_byte_array_page(data []u8) ![][]u8 {
	num_values, data_start := parse_page_header_num_values(data)!

	// Skip RLE definition levels if present (4-byte LE length prefix).
	mut pos := data_start
	pos = skip_definition_levels_if_present(data, pos)

	mut result := [][]u8{cap: num_values}
	for _ in 0 .. num_values {
		if pos + 4 > data.len {
			break
		}
		arr_len := int(read_le_u32(data, pos))
		pos += 4
		if pos + arr_len > data.len {
			break
		}
		result << data[pos..pos + arr_len].clone()
		pos += arr_len
	}
	return result
}

// skip_definition_levels_if_present detects and skips RLE definition levels.
// Returns the new position after the definition levels block, or the original pos if none found.
fn skip_definition_levels_if_present(data []u8, pos int) int {
	if pos + 4 > data.len {
		return pos
	}
	def_len := int(read_le_u32(data, pos))
	remaining := data.len - pos
	// Heuristic: def_len must be positive, fit in remaining data, and leave room for values.
	if def_len > 0 && def_len < remaining && pos + 4 + def_len < data.len {
		return pos + 4 + def_len
	}
	return pos
}
