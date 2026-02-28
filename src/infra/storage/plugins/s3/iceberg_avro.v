// Avro encoding functions for Iceberg manifest files.
// Implements the Avro Object Container File format per the Iceberg spec.
module s3

/// encode_manifest encodes manifest file content in Avro Object Container File format.
/// Implements the Iceberg Manifest File Avro schema per the Iceberg spec.
fn (w &IcebergWriter) encode_manifest(data_files []IcebergDataFile) ![]u8 {
	// Avro Object Container File format:
	// - 4-byte magic: 'O', 'b', 'j', 0x01
	// - file-level metadata (map<bytes>), including schema and codec
	// - 16-byte sync marker
	// - blocks: [count varint][size varint][data bytes][sync marker]

	// Iceberg manifest file Avro schema (manifest_entry records)
	manifest_schema := '{"type":"record","name":"manifest_entry","fields":[' +
		'{"name":"status","type":"int"},' +
		'{"name":"snapshot_id","type":["null","long"],"default":null},' +
		'{"name":"sequence_number","type":["null","long"],"default":null},' +
		'{"name":"file_sequence_number","type":["null","long"],"default":null},' +
		'{"name":"data_file","type":{"type":"record","name":"r2","fields":[' +
		'{"name":"content","type":"int"},' + '{"name":"file_path","type":"string"},' +
		'{"name":"file_format","type":"string"},' +
		'{"name":"partition","type":{"type":"record","name":"r102","fields":[]}},' +
		'{"name":"record_count","type":"long"},' + '{"name":"file_size_in_bytes","type":"long"},' +
		'{"name":"column_sizes","type":["null",{"type":"array","items":{"type":"record","name":"r104","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}],"default":null},' +
		'{"name":"value_counts","type":["null",{"type":"array","items":{"type":"record","name":"r105","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}],"default":null},' +
		'{"name":"null_value_counts","type":["null",{"type":"array","items":{"type":"record","name":"r106","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]}}],"default":null},' +
		'{"name":"lower_bounds","type":["null",{"type":"array","items":{"type":"record","name":"r108","fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}],"default":null},' +
		'{"name":"upper_bounds","type":["null",{"type":"array","items":{"type":"record","name":"r109","fields":[{"name":"key","type":"int"},{"name":"value","type":"bytes"}]}}],"default":null}' +
		']}}}]}'

	// Generate 16-byte sync marker from snapshot/schema metadata
	sync_source := '${w.table_metadata.table_uuid}-${w.table_metadata.current_snapshot_id}'
	sync_marker := avro_generate_sync_marker(sync_source)

	// Build Avro file-level metadata
	mut meta_map := map[string]string{}
	meta_map['avro.schema'] = manifest_schema
	meta_map['avro.codec'] = 'null'
	meta_map['iceberg.schema'] = manifest_schema
	meta_map['format-version'] = w.table_metadata.format_version.str()
	meta_map['partition-spec-id'] = w.table_metadata.default_spec_id.str()
	meta_map['schema-id'] = w.table_metadata.current_schema_id.str()
	meta_map['content'] = 'data'

	mut buf := []u8{}

	// Magic bytes: Obj\x01
	buf << u8(`O`)
	buf << u8(`b`)
	buf << u8(`j`)
	buf << u8(0x01)

	// File-level metadata as Avro map<bytes>
	avro_write_meta_map(mut buf, meta_map)

	// Sync marker (16 bytes)
	buf << sync_marker

	// Encode each data file as a manifest_entry record
	mut records_buf := []u8{}
	for file in data_files {
		avro_write_manifest_entry(mut records_buf, file, w.table_metadata.current_snapshot_id)
	}

	if records_buf.len > 0 {
		// Block: [count][byte_count][data][sync_marker]
		avro_write_varint(mut buf, i64(data_files.len))
		avro_write_varint(mut buf, i64(records_buf.len))
		buf << records_buf
		buf << sync_marker
	}

	// End-of-file block: count=0
	buf << u8(0)

	return buf
}

/// avro_generate_sync_marker generates a 16-byte sync marker from a string.
fn avro_generate_sync_marker(source string) []u8 {
	mut result := []u8{len: 16}
	src_bytes := source.bytes()
	for i in 0 .. 16 {
		result[i] = if i < src_bytes.len { src_bytes[i] } else { u8(i * 13 + 7) }
	}
	// XOR mixing for better distribution
	for i in 1 .. 16 {
		result[i] ^= result[i - 1]
	}
	return result
}

/// avro_write_varint writes a zigzag-encoded varint to the buffer.
fn avro_write_varint(mut buf []u8, n i64) {
	// Zigzag encoding: (n << 1) ^ (n >> 63)
	mut v := u64((n << 1) ^ (n >> 63))
	for {
		if v <= 0x7F {
			buf << u8(v)
			break
		}
		buf << u8((v & 0x7F) | 0x80)
		v >>= 7
	}
}

/// avro_write_string writes an Avro string (length-prefixed bytes).
fn avro_write_string(mut buf []u8, s string) {
	avro_write_varint(mut buf, i64(s.len))
	buf << s.bytes()
}

/// avro_write_bytes writes Avro bytes (length-prefixed).
fn avro_write_bytes(mut buf []u8, data []u8) {
	avro_write_varint(mut buf, i64(data.len))
	buf << data
}

/// avro_write_meta_map writes the Avro file-level metadata map<bytes>.
fn avro_write_meta_map(mut buf []u8, meta map[string]string) {
	if meta.len == 0 {
		buf << u8(0)
		return
	}

	// Map block: [count varint] followed by [key string][value bytes] pairs, then 0
	avro_write_varint(mut buf, i64(meta.len))
	for key, value in meta {
		avro_write_string(mut buf, key)
		avro_write_bytes(mut buf, value.bytes())
	}
	// End of map
	buf << u8(0)
}

/// avro_write_nullable_long writes an Avro union [null, long] with non-null value.
fn avro_write_nullable_long(mut buf []u8, value i64) {
	// Union index 1 = long
	avro_write_varint(mut buf, 1)
	avro_write_varint(mut buf, value)
}

/// avro_write_manifest_entry writes a single manifest_entry Avro record.
fn avro_write_manifest_entry(mut buf []u8, file IcebergDataFile, snapshot_id i64) {
	// status: 1 = ADDED (int)
	avro_write_varint(mut buf, 1)

	// snapshot_id: union [null, long] - use snapshot_id
	avro_write_nullable_long(mut buf, snapshot_id)

	// sequence_number: union [null, long]
	avro_write_nullable_long(mut buf, 0)

	// file_sequence_number: union [null, long]
	avro_write_nullable_long(mut buf, 0)

	// data_file record:
	// content: 0 = DATA
	avro_write_varint(mut buf, 0)

	// file_path: string
	avro_write_string(mut buf, file.file_path)

	// file_format: string
	avro_write_string(mut buf, file.file_format)

	// partition: empty record (no partition fields in schema for simplicity)
	// (already encoded as an empty struct - nothing to write)

	// record_count: long
	avro_write_varint(mut buf, file.record_count)

	// file_size_in_bytes: long
	avro_write_varint(mut buf, file.file_size_in_bytes)

	// column_sizes: union [null, array<{key:int,value:long}>]
	avro_write_nullable_int_long_map(mut buf, file.column_sizes)

	// value_counts: union [null, array<{key:int,value:long}>]
	avro_write_nullable_int_long_map(mut buf, file.value_counts)

	// null_value_counts: union [null, array<{key:int,value:long}>]
	avro_write_nullable_int_long_map(mut buf, file.null_value_counts)

	// lower_bounds: union [null, array<{key:int,value:bytes}>]
	avro_write_nullable_int_bytes_map(mut buf, file.lower_bounds)

	// upper_bounds: union [null, array<{key:int,value:bytes}>]
	avro_write_nullable_int_bytes_map(mut buf, file.upper_bounds)
}

/// avro_write_nullable_int_long_map writes Avro union [null, array<{key:int,value:long}>].
/// map이 비어있으면 null(0)을, 아니면 non-null(2=union index 1)로 인코딩
fn avro_write_nullable_int_long_map(mut buf []u8, m map[int]i64) {
	if m.len == 0 {
		// union index 0 = null
		buf << u8(0)
		return
	}
	// union index 1 = array (zigzag(1) = 2)
	avro_write_varint(mut buf, 1)
	avro_write_int_long_map(mut buf, m)
}

/// avro_write_nullable_int_bytes_map writes Avro union [null, array<{key:int,value:bytes}>].
fn avro_write_nullable_int_bytes_map(mut buf []u8, m map[int][]u8) {
	if m.len == 0 {
		// union index 0 = null
		buf << u8(0)
		return
	}
	// union index 1 = array
	avro_write_varint(mut buf, 1)
	avro_write_int_bytes_map(mut buf, m)
}

/// avro_write_int_long_map writes Avro array<{key:int,value:long}> (Iceberg map entry style).
/// Iceberg spec: map은 array of {key, value} record로 인코딩
pub fn avro_write_int_long_map(mut buf []u8, m map[int]i64) {
	if m.len == 0 {
		buf << u8(0)
		return
	}
	// array block: count 값 (zigzag encoded)
	avro_write_varint(mut buf, i64(m.len))
	for key, value in m {
		// key: int (zigzag encoded)
		avro_write_varint(mut buf, i64(key))
		// value: long (zigzag encoded)
		avro_write_varint(mut buf, value)
	}
	// array 종료: count = 0
	buf << u8(0)
}

/// avro_write_int_bytes_map writes Avro array<{key:int,value:bytes}> (Iceberg map entry style).
pub fn avro_write_int_bytes_map(mut buf []u8, m map[int][]u8) {
	if m.len == 0 {
		buf << u8(0)
		return
	}
	avro_write_varint(mut buf, i64(m.len))
	for key, value in m {
		// key: int
		avro_write_varint(mut buf, i64(key))
		// value: bytes (length-prefixed)
		avro_write_bytes(mut buf, value)
	}
	buf << u8(0)
}

/// iceberg_serialize_long serializes an int64 value to Iceberg binary format (little-endian 8 bytes).
pub fn iceberg_serialize_long(v i64) []u8 {
	uv := u64(v)
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

/// iceberg_serialize_int serializes an int32 value to Iceberg binary format (little-endian 4 bytes).
pub fn iceberg_serialize_int(v i32) []u8 {
	uv := u32(v)
	return [
		u8(uv & 0xFF),
		u8((uv >> 8) & 0xFF),
		u8((uv >> 16) & 0xFF),
		u8((uv >> 24) & 0xFF),
	]
}

/// iceberg_serialize_string serializes a string value to Iceberg binary format (UTF-8 bytes).
pub fn iceberg_serialize_string(s string) []u8 {
	return s.bytes()
}
