// Parquet metadata parser - parses FileMetaData from the Parquet file footer.
// Uses Thrift Compact Protocol (ThriftReader) to decode the serialized footer.
module encoding

// ParsedSchemaElement represents a parsed Parquet schema element (leaf column node).
pub struct ParsedSchemaElement {
pub:
	name           string
	typ            ParquetDataType
	repetition     ParquetRepetitionType
	num_children   int
	converted_type i32
}

// ParquetRepetitionType represents the repetition type of a schema element.
pub enum ParquetRepetitionType {
	required
	optional
	repeated
}

// ParsedColumnMetadata holds parsed column chunk metadata.
pub struct ParsedColumnMetadata {
pub:
	path          []string
	physical_type i32
	codec         i32
	num_values    i64
	data_offset   i64
	min_value     []u8
	max_value     []u8
	null_count    i64
}

// ParsedRowGroup represents a parsed row group.
pub struct ParsedRowGroup {
pub:
	columns     []ParsedColumnMetadata
	total_bytes i64
	row_count   i64
}

// ParsedMetadata represents the complete parsed Parquet file metadata.
pub struct ParsedMetadata {
pub mut:
	schema     []ParsedSchemaElement
	num_rows   i64
	row_groups []ParsedRowGroup
	created_by string
	version    i32
}

// StatisticsResult is a local helper to return statistics fields.
struct StatisticsResult {
	min_value  []u8
	max_value  []u8
	null_count i64
}

// ParquetMetadataParser parses Parquet FileMetaData from the file footer.
pub struct ParquetMetadataParser {
mut:
	reader ThriftReader
}

pub fn new_parquet_metadata_parser() ParquetMetadataParser {
	return ParquetMetadataParser{}
}

// parse parses Parquet metadata from a complete file byte slice.
pub fn (mut p ParquetMetadataParser) parse(data []u8) !ParsedMetadata {
	if data.len < 12 {
		return error('file too small: missing magic bytes')
	}
	if data[0] != u8(`P`) || data[1] != u8(`A`) || data[2] != u8(`R`) || data[3] != u8(`1`) {
		return error('invalid parquet magic at start')
	}
	if data[data.len - 4] != u8(`P`) || data[data.len - 3] != u8(`A`)
		|| data[data.len - 2] != u8(`R`) || data[data.len - 1] != u8(`1`) {
		return error('invalid parquet magic at end')
	}
	footer_len := u32(data[data.len - 8]) | (u32(data[data.len - 7]) << 8) | (u32(data[data.len - 6]) << 16) | (u32(data[data.len - 5]) << 24)
	footer_start := data.len - 8 - int(footer_len)
	if footer_start < 4 {
		return error('invalid footer: footer extends before start magic')
	}
	p.reader = new_thrift_reader(data[footer_start..data.len - 8])
	return p.parse_file_metadata()!
}

fn (mut p ParquetMetadataParser) parse_file_metadata() !ParsedMetadata {
	mut metadata := ParsedMetadata{}
	mut last_fid := i16(0)
	p.reader.read_struct_begin()
	for {
		field_type, delta := p.reader.read_field_header() or {
			if err.msg() == 'stop byte encountered' {
				break
			}
			return err
		}
		last_fid += delta
		match last_fid {
			1 {
				metadata.version = p.reader.read_i32()!
			}
			2 {
				_, count := p.reader.read_list_begin()!
				all_elems := p.parse_schema_elements(count)!
				// Filter out root group node (num_children > 0); keep leaf columns only.
				metadata.schema = all_elems.filter(it.num_children == 0)
			}
			3 {
				metadata.num_rows = p.reader.read_i64()!
			}
			4 {
				_, count := p.reader.read_list_begin()!
				metadata.row_groups = p.parse_row_groups(count)!
			}
			6 {
				metadata.created_by = p.reader.read_string()!
			}
			else {
				p.skip_field(field_type)!
			}
		}
	}
	return metadata
}

fn (mut p ParquetMetadataParser) parse_schema_elements(count int) ![]ParsedSchemaElement {
	mut elements := []ParsedSchemaElement{cap: count}
	for _ in 0 .. count {
		p.reader.read_struct_begin()
		mut last_fid := i16(0)
		mut name := ''
		mut typ := ParquetDataType.binary
		mut repetition := ParquetRepetitionType.required
		mut num_children := 0
		mut converted_type := i32(0)
		for {
			field_type, delta := p.reader.read_field_header() or {
				if err.msg() == 'stop byte encountered' {
					break
				}
				return err
			}
			last_fid += delta
			match last_fid {
				1 {
					typ = physical_type_to_data_type(p.reader.read_i32()!)
				}
				3 {
					rep := p.reader.read_i32()!
					repetition = match rep {
						0 { ParquetRepetitionType.required }
						1 { ParquetRepetitionType.optional }
						else { ParquetRepetitionType.repeated }
					}
				}
				4 {
					name = p.reader.read_string()!
				}
				5 {
					num_children = int(p.reader.read_i32()!)
				}
				6 {
					converted_type = p.reader.read_i32()!
				}
				else {
					p.skip_field(field_type)!
				}
			}
		}
		elements << ParsedSchemaElement{
			name:           name
			typ:            typ
			repetition:     repetition
			num_children:   num_children
			converted_type: converted_type
		}
	}
	return elements
}

fn (mut p ParquetMetadataParser) parse_row_groups(count int) ![]ParsedRowGroup {
	mut groups := []ParsedRowGroup{cap: count}
	for _ in 0 .. count {
		p.reader.read_struct_begin()
		mut last_fid := i16(0)
		mut columns := []ParsedColumnMetadata{}
		mut total_bytes := i64(0)
		mut row_count := i64(0)
		for {
			field_type, delta := p.reader.read_field_header() or {
				if err.msg() == 'stop byte encountered' {
					break
				}
				return err
			}
			last_fid += delta
			match last_fid {
				1 {
					_, col_count := p.reader.read_list_begin()!
					columns = p.parse_column_chunks(col_count)!
				}
				2 {
					total_bytes = p.reader.read_i64()!
				}
				3 {
					row_count = p.reader.read_i64()!
				}
				else {
					p.skip_field(field_type)!
				}
			}
		}
		groups << ParsedRowGroup{
			columns:     columns
			total_bytes: total_bytes
			row_count:   row_count
		}
	}
	return groups
}

fn (mut p ParquetMetadataParser) parse_column_chunks(count int) ![]ParsedColumnMetadata {
	mut chunks := []ParsedColumnMetadata{cap: count}
	for _ in 0 .. count {
		p.reader.read_struct_begin()
		mut last_fid := i16(0)
		mut chunk := ParsedColumnMetadata{
			path: []
		}
		for {
			field_type, delta := p.reader.read_field_header() or {
				if err.msg() == 'stop byte encountered' {
					break
				}
				return err
			}
			last_fid += delta
			match last_fid {
				2 { _ = p.reader.read_i64()! }
				3 { chunk = p.parse_column_metadata()! }
				else { p.skip_field(field_type)! }
			}
		}
		chunks << chunk
	}
	return chunks
}

fn (mut p ParquetMetadataParser) parse_column_metadata() !ParsedColumnMetadata {
	p.reader.read_struct_begin()
	mut last_fid := i16(0)
	mut physical_type := i32(0)
	mut codec := i32(0)
	mut num_values := i64(0)
	mut data_offset := i64(0)
	mut path := []string{}
	mut min_value := []u8{}
	mut max_value := []u8{}
	mut null_count := i64(0)
	for {
		field_type, delta := p.reader.read_field_header() or {
			if err.msg() == 'stop byte encountered' {
				break
			}
			return err
		}
		last_fid += delta
		match last_fid {
			1 {
				physical_type = p.reader.read_i32()!
			}
			2 {
				_, enc_count := p.reader.read_list_begin()!
				for _ in 0 .. enc_count {
					_ = p.reader.read_raw_i32()!
				}
			}
			3 {
				_, path_count := p.reader.read_list_begin()!
				mut paths := []string{cap: path_count}
				for _ in 0 .. path_count {
					paths << p.reader.read_string()!
				}
				path = paths.clone()
			}
			4 {
				codec = p.reader.read_i32()!
			}
			5 {
				num_values = p.reader.read_i64()!
			}
			6 {
				_ = p.reader.read_i64()!
			}
			7 {
				_ = p.reader.read_i64()!
			}
			9 {
				data_offset = p.reader.read_i64()!
			}
			12 {
				stats := p.parse_statistics()!
				min_value = stats.min_value.clone()
				max_value = stats.max_value.clone()
				null_count = stats.null_count
			}
			else {
				p.skip_field(field_type)!
			}
		}
	}
	return ParsedColumnMetadata{
		path:          path
		physical_type: physical_type
		codec:         codec
		num_values:    num_values
		data_offset:   data_offset
		min_value:     min_value
		max_value:     max_value
		null_count:    null_count
	}
}

fn (mut p ParquetMetadataParser) parse_statistics() !StatisticsResult {
	p.reader.read_struct_begin()
	mut last_fid := i16(0)
	mut min_value := []u8{}
	mut max_value := []u8{}
	mut null_count := i64(0)
	for {
		field_type, delta := p.reader.read_field_header() or {
			if err.msg() == 'stop byte encountered' {
				break
			}
			return err
		}
		last_fid += delta
		match last_fid {
			1 { max_value = p.reader.read_binary()! } // max (deprecated)
			2 { min_value = p.reader.read_binary()! } // min (deprecated)
			3 { null_count = p.reader.read_i64()! }
			4 { _ = p.reader.read_i64()! } // distinct_count
			5 { max_value = p.reader.read_binary()! } // max_value (new-style)
			6 { min_value = p.reader.read_binary()! } // min_value (new-style)
			else { p.skip_field(field_type)! }
		}
	}
	return StatisticsResult{
		min_value:  min_value
		max_value:  max_value
		null_count: null_count
	}
}

// skip_field skips the value of a Thrift field by type — used for unknown fields.
fn (mut p ParquetMetadataParser) skip_field(field_type u8) ! {
	match field_type {
		thrift_type_boolean_true, thrift_type_boolean_false {}
		thrift_type_byte {
			if p.reader.pos < p.reader.buf.len {
				p.reader.pos++
			}
		}
		thrift_type_i16, thrift_type_i32 {
			_ = p.reader.read_varint32()!
		}
		thrift_type_i64, thrift_type_double {
			_ = p.reader.read_varint64()!
		}
		thrift_type_binary {
			_ = p.reader.read_binary()!
		}
		thrift_type_list, thrift_type_set {
			elem_type, count := p.reader.read_list_begin()!
			for _ in 0 .. count {
				p.skip_field(elem_type)!
			}
		}
		thrift_type_struct {
			p.reader.read_struct_begin()
			mut last_fid := i16(0)
			for {
				ft, delta := p.reader.read_field_header() or {
					if err.msg() == 'stop byte encountered' {
						break
					}
					return err
				}
				last_fid += delta
				p.skip_field(ft)!
			}
		}
		else {}
	}
}

// physical_type_to_data_type maps Parquet physical type integer to ParquetDataType.
fn physical_type_to_data_type(typ i32) ParquetDataType {
	return match typ {
		0 { ParquetDataType.boolean }
		1 { ParquetDataType.int32 }
		2 { ParquetDataType.int64 }
		4 { ParquetDataType.float }
		5 { ParquetDataType.double }
		else { ParquetDataType.binary }
	}
}
