// JSON parsing helpers for Iceberg catalog metadata.
// Lightweight string-based extraction for Glue API responses and
// key normalization for standard json.decode-based metadata parsing.
module s3

/// normalize_metadata_json_keys converts camelCase Iceberg metadata keys to kebab-case
/// for compatibility with V's json.decode using @[json: 'kebab-case'] attributes.
/// WARNING: This uses string replacement which could incorrectly modify JSON values
/// that happen to contain key-like patterns (e.g., a description containing "formatVersion:").
/// This is acceptable for Iceberg metadata where such collisions are extremely unlikely.
/// For a more robust solution, use a key-only matching approach.
fn normalize_metadata_json_keys(json_str string) string {
	return json_str
		.replace('"formatVersion":', '"format-version":')
		.replace('"tableUuid":', '"table-uuid":')
		.replace('"lastUpdatedMs":', '"last-updated-ms":')
		.replace('"currentSchemaId":', '"current-schema-id":')
		.replace('"defaultSpecId":', '"default-spec-id":')
		.replace('"currentSnapshotId":', '"current-snapshot-id":')
		.replace('"partitionSpecs":', '"partition-specs":')
		.replace('"identifierFieldIds":', '"identifier-field-ids":')
		.replace('"schemaId":', '"schema-id":')
		.replace('"specId":', '"spec-id":')
		.replace('"sourceId":', '"source-id":')
		.replace('"fieldId":', '"field-id":')
		.replace('"snapshotId":', '"snapshot-id":')
		.replace('"timestampMs":', '"timestamp-ms":')
		.replace('"manifestList":', '"manifest-list":')
		.replace('"initialDefault":', '"initial-default":')
		.replace('"transformArgs":', '"transform-args":')
}

// --- Low-level JSON extraction helpers (used by GlueCatalog for AWS responses) ---

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
