// JSON parsing helpers for Iceberg catalog metadata.
// Provides lightweight string-based JSON extraction without reflection,
// used by HadoopCatalog and GlueCatalog to decode metadata files.
module s3

// json_extract_string_dual tries kebab key first, then camelCase key.
fn json_extract_string_dual(json_str string, kebab string, camel string) ?string {
	if v := json_extract_string(json_str, kebab) {
		return v
	}
	return json_extract_string(json_str, camel)
}

// json_extract_int_dual tries kebab key first, then camelCase key.
fn json_extract_int_dual(json_str string, kebab string, camel string) ?int {
	if v := json_extract_int(json_str, kebab) {
		return v
	}
	return json_extract_int(json_str, camel)
}

// json_extract_i64_dual tries kebab key first, then camelCase key.
fn json_extract_i64_dual(json_str string, kebab string, camel string) ?i64 {
	if v := json_extract_i64(json_str, kebab) {
		return v
	}
	return json_extract_i64(json_str, camel)
}

// json_extract_array_dual tries kebab key first, then camelCase key.
fn json_extract_array_dual(json_str string, kebab string, camel string) ?string {
	if v := json_extract_array(json_str, kebab) {
		return v
	}
	return json_extract_array(json_str, camel)
}

// json_find_value_start finds the start index of the value after "key": in json_str,
// returning the offset into json_str where the value begins (skipping whitespace).
// Returns -1 if not found.
fn json_find_value_start(json_str string, key string) int {
	needle := '"${key}"'
	key_idx := json_str.index(needle) or { return -1 }
	mut pos := key_idx + needle.len

	// Skip to ':'
	for pos < json_str.len && json_str[pos] != `:` {
		pos++
	}
	if pos >= json_str.len {
		return -1
	}
	pos++ // skip ':'

	// Skip whitespace
	for pos < json_str.len && (json_str[pos] == ` ` || json_str[pos] == `\t`
		|| json_str[pos] == `\n` || json_str[pos] == `\r`) {
		pos++
	}
	if pos >= json_str.len {
		return -1
	}
	return pos
}

/// json_extract_string extracts a string value by key from a flat JSON object.
fn json_extract_string(json_str string, key string) ?string {
	pos := json_find_value_start(json_str, key)
	if pos < 0 || json_str[pos] != `"` {
		return none
	}
	// pos points to opening quote; find closing quote
	start := pos + 1
	mut end := start
	for end < json_str.len && json_str[end] != `"` {
		end++
	}
	if end >= json_str.len {
		return none
	}
	return json_str[start..end]
}

/// json_extract_int extracts an integer value by key from a flat JSON object.
fn json_extract_int(json_str string, key string) ?int {
	pos := json_find_value_start(json_str, key)
	if pos < 0 {
		return none
	}

	// Read until non-numeric
	mut end := pos
	for end < json_str.len && ((json_str[end] >= `0` && json_str[end] <= `9`)
		|| (end == pos && json_str[end] == `-`)) {
		end++
	}
	if end == pos {
		return none
	}
	return json_str[pos..end].int()
}

/// json_extract_i64 extracts an i64 value by key from a flat JSON object.
fn json_extract_i64(json_str string, key string) ?i64 {
	pos := json_find_value_start(json_str, key)
	if pos < 0 {
		return none
	}

	mut end := pos
	for end < json_str.len && ((json_str[end] >= `0` && json_str[end] <= `9`)
		|| (end == pos && json_str[end] == `-`)) {
		end++
	}
	if end == pos {
		return none
	}
	return json_str[pos..end].i64()
}

/// json_extract_object extracts a JSON object {...} by key.
fn json_extract_object(json_str string, key string) ?string {
	pos := json_find_value_start(json_str, key)
	if pos < 0 || json_str[pos] != `{` {
		return none
	}
	return json_find_matching_brace(json_str[pos..], `{`, `}`)
}

/// json_extract_array extracts a JSON array [...] by key.
fn json_extract_array(json_str string, key string) ?string {
	pos := json_find_value_start(json_str, key)
	if pos < 0 || json_str[pos] != `[` {
		return none
	}
	return json_find_matching_brace(json_str[pos..], `[`, `]`)
}

/// json_find_matching_brace finds the matching closing brace/bracket.
fn json_find_matching_brace(s string, open u8, close u8) ?string {
	if s == '' || s[0] != open {
		return none
	}
	mut depth := 0
	mut in_string := false
	mut escape := false

	for i in 0 .. s.len {
		ch := s[i]
		if escape {
			escape = false
			continue
		}
		if ch == `\\` && in_string {
			escape = true
			continue
		}
		if ch == `"` {
			in_string = !in_string
			continue
		}
		if in_string {
			continue
		}
		if ch == open {
			depth++
		} else if ch == close {
			depth--
			if depth == 0 {
				return s[0..i + 1]
			}
		}
	}
	return none
}

/// json_parse_string_map parses a JSON object into map[string]string.
fn json_parse_string_map(obj_str string) map[string]string {
	mut result := map[string]string{}
	if obj_str.len < 2 {
		return result
	}
	// Strip outer braces
	inner := obj_str[1..obj_str.len - 1].trim_space()
	if inner.len == 0 {
		return result
	}

	mut pos := 0
	for pos < inner.len {
		// Skip whitespace and commas
		for pos < inner.len
			&& (inner[pos] == ` ` || inner[pos] == `\t` || inner[pos] == `\n` || inner[pos] == `,`) {
			pos++
		}
		if pos >= inner.len {
			break
		}

		// Expect key string
		if inner[pos] != `"` {
			break
		}
		pos++ // skip opening quote
		mut key_end := pos
		for key_end < inner.len && inner[key_end] != `"` {
			key_end++
		}
		key := inner[pos..key_end]
		pos = key_end + 1 // skip closing quote

		// Skip colon
		for pos < inner.len && inner[pos] != `:` {
			pos++
		}
		pos++ // skip colon

		// Skip whitespace
		for pos < inner.len && (inner[pos] == ` ` || inner[pos] == `\t`) {
			pos++
		}

		if pos >= inner.len {
			break
		}

		// Read value
		if inner[pos] == `"` {
			pos++ // skip opening quote
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `"` {
				val_end++
			}
			result[key] = inner[pos..val_end]
			pos = val_end + 1
		} else {
			// Non-string value (number/bool)
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `,` && inner[val_end] != `}`
				&& inner[val_end] != `\n` {
				val_end++
			}
			result[key] = inner[pos..val_end].trim_space()
			pos = val_end
		}
	}
	return result
}

/// json_split_array_items splits a JSON array into individual item strings.
fn json_split_array_items(arr_str string) []string {
	mut items := []string{}
	if arr_str.len < 2 {
		return items
	}
	inner := arr_str[1..arr_str.len - 1].trim_space()
	if inner.len == 0 {
		return items
	}

	mut pos := 0
	for pos < inner.len {
		// Skip commas and whitespace
		for pos < inner.len
			&& (inner[pos] == `,` || inner[pos] == ` ` || inner[pos] == `\n` || inner[pos] == `\t`) {
			pos++
		}
		if pos >= inner.len {
			break
		}

		if inner[pos] == `{` {
			end := json_find_matching_brace(inner[pos..], `{`, `}`) or { break }
			items << end
			pos += end.len
		} else if inner[pos] == `[` {
			end := json_find_matching_brace(inner[pos..], `[`, `]`) or { break }
			items << end
			pos += end.len
		} else if inner[pos] == `"` {
			pos++
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `"` {
				val_end++
			}
			items << '"${inner[pos..val_end]}"'
			pos = val_end + 1
		} else {
			mut val_end := pos
			for val_end < inner.len && inner[val_end] != `,` {
				val_end++
			}
			items << inner[pos..val_end].trim_space()
			pos = val_end
		}
	}
	return items
}

/// json_parse_schemas parses a JSON array of schema objects.
fn json_parse_schemas(arr_str string) []IcebergSchema {
	mut schemas := []IcebergSchema{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut schema := IcebergSchema{}
		schema.schema_id = json_extract_int_dual(item, 'schema-id', 'schemaId') or { 0 }
		if fields_str := json_extract_array(item, 'fields') {
			schema.fields = json_parse_fields(fields_str)
		}
		schemas << schema
	}
	return schemas
}

/// json_parse_fields parses a JSON array of field objects.
fn json_parse_fields(arr_str string) []IcebergField {
	mut fields := []IcebergField{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut field := IcebergField{}
		if v := json_extract_int(item, 'id') {
			field.id = v
		}
		if v := json_extract_string(item, 'name') {
			field.name = v
		}
		if v := json_extract_string(item, 'type') {
			field.typ = v
		}
		// required: boolean
		if item.contains('"required":true') || item.contains('"required": true') {
			field.required = true
		}
		if v := json_extract_string(item, 'initial-default') {
			field.default_value = v
		}
		fields << field
	}
	return fields
}

/// json_parse_partition_specs parses a JSON array of partition spec objects.
fn json_parse_partition_specs(arr_str string) []IcebergPartitionSpec {
	mut specs := []IcebergPartitionSpec{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut spec := IcebergPartitionSpec{}
		spec.spec_id = json_extract_int_dual(item, 'spec-id', 'specId') or { 0 }
		if fields_str := json_extract_array(item, 'fields') {
			spec.fields = json_parse_partition_fields(fields_str)
		}
		specs << spec
	}
	return specs
}

/// json_parse_partition_fields parses a JSON array of partition field objects.
fn json_parse_partition_fields(arr_str string) []IcebergPartitionField {
	mut pfields := []IcebergPartitionField{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut pfield := IcebergPartitionField{}
		pfield.source_id = json_extract_int_dual(item, 'source-id', 'sourceId') or { 0 }
		pfield.field_id = json_extract_int_dual(item, 'field-id', 'fieldId') or { 0 }
		if v := json_extract_string(item, 'name') {
			pfield.name = v
		}
		if v := json_extract_string(item, 'transform') {
			pfield.transform = v
		}
		pfields << pfield
	}
	return pfields
}

/// json_parse_snapshots parses a JSON array of snapshot objects.
fn json_parse_snapshots(arr_str string) []IcebergSnapshot {
	mut snapshots := []IcebergSnapshot{}
	items := json_split_array_items(arr_str)
	for item in items {
		mut snap := IcebergSnapshot{}
		snap.snapshot_id = json_extract_i64_dual(item, 'snapshot-id', 'snapshotId') or { i64(0) }
		snap.timestamp_ms = json_extract_i64_dual(item, 'timestamp-ms', 'timestampMs') or { i64(0) }
		snap.manifest_list = json_extract_string_dual(item, 'manifest-list', 'manifestList') or {
			''
		}
		snap.schema_id = json_extract_int_dual(item, 'schema-id', 'schemaId') or { 0 }
		if summary_str := json_extract_object(item, 'summary') {
			snap.summary = json_parse_string_map(summary_str)
		}
		snapshots << snap
	}
	return snapshots
}
