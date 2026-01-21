// Service Layer - Schema Registry
// Manages schema registration, versioning, and compatibility
module schema

import domain
import service.port
import sync
import time
import crypto.md5

// SchemaRegistry provides schema management functionality
pub struct SchemaRegistry {
mut:
	// In-memory cache
	schemas         map[int]domain.Schema             // schema_id -> Schema
	subjects        map[string][]domain.SchemaVersion // subject -> versions
	subject_configs map[string]domain.SubjectConfig   // subject -> config

	// Global state
	next_id       int
	global_config domain.SubjectConfig // Global default config

	// Thread safety
	lock sync.RwMutex

	// Storage for persistence
	storage port.StoragePort

	// Configuration
	default_compat domain.CompatibilityLevel

	// Boot recovery state
	recovered bool
}

// RegistryConfig holds registry configuration
pub struct RegistryConfig {
pub:
	default_compatibility domain.CompatibilityLevel = .backward
	auto_register         bool                      = true
	normalize_schemas     bool                      = true
}

// Internal topic for schema storage
pub const schemas_topic = '__schemas'

// new_registry creates a new schema registry
pub fn new_registry(storage port.StoragePort, config RegistryConfig) &SchemaRegistry {
	return &SchemaRegistry{
		schemas:         map[int]domain.Schema{}
		subjects:        map[string][]domain.SchemaVersion{}
		subject_configs: map[string]domain.SubjectConfig{}
		next_id:         1
		global_config:   domain.SubjectConfig{
			compatibility: config.default_compatibility
			normalize:     config.normalize_schemas
		}
		storage:         storage
		default_compat:  config.default_compatibility
		recovered:       false
	}
}

// load_from_storage recovers schema registry state from __schemas topic
// Should be called during broker startup before accepting requests
pub fn (mut r SchemaRegistry) load_from_storage() ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if r.recovered {
		return
	}

	// Try to read from __schemas topic
	r.storage.get_topic(schemas_topic) or {
		// Topic doesn't exist yet - first boot
		r.recovered = true
		return
	}

	// Fetch all records from __schemas topic
	fetch_result := r.storage.fetch(schemas_topic, 0, 0, 100 * 1024 * 1024) or {
		// Empty or error - start fresh
		r.recovered = true
		return
	}

	mut max_id := 0

	// Process each record to rebuild state
	for record in fetch_result.records {
		record_str := record.value.bytestr()

		// Parse the persisted schema record
		// Format: {"subject":"...", "version":..., "id":..., "schemaType":"...", "schema":"..."}

		subject := extract_json_string(record_str, 'subject') or { continue }
		version := extract_json_int(record_str, 'version') or { continue }
		schema_id := extract_json_int(record_str, 'id') or { continue }
		schema_type_str := extract_json_string(record_str, 'schemaType') or { 'AVRO' }
		schema_str := extract_json_string(record_str, 'schema') or { continue }

		schema_type := domain.schema_type_from_str(schema_type_str) or { domain.SchemaType.avro }

		// Track max ID for next_id
		if schema_id > max_id {
			max_id = schema_id
		}

		// Rebuild schema cache
		if schema_id !in r.schemas {
			r.schemas[schema_id] = domain.Schema{
				id:          schema_id
				schema_type: schema_type
				schema_str:  schema_str
				fingerprint: compute_fingerprint(normalize_schema(schema_str))
			}
		}

		// Rebuild subject versions
		schema_version := domain.SchemaVersion{
			version:       version
			schema_id:     schema_id
			subject:       subject
			compatibility: r.default_compat
			created_at:    record.timestamp
		}

		if subject in r.subjects {
			// Check if version already exists
			mut exists := false
			for v in r.subjects[subject] {
				if v.version == version {
					exists = true
					break
				}
			}
			if !exists {
				r.subjects[subject] << schema_version
			}
		} else {
			r.subjects[subject] = [schema_version]
		}
	}

	// Set next_id to be after max recovered ID
	r.next_id = max_id + 1
	r.recovered = true
}

// get_global_config returns the global compatibility configuration
pub fn (mut r SchemaRegistry) get_global_config() domain.SubjectConfig {
	r.lock.rlock()
	defer { r.lock.runlock() }
	return r.global_config
}

// set_global_config updates the global compatibility configuration
pub fn (mut r SchemaRegistry) set_global_config(config domain.SubjectConfig) {
	r.lock.@lock()
	defer { r.lock.unlock() }
	r.global_config = config
	r.default_compat = config.compatibility
}

fn extract_json_int(json_str string, key string) ?int {
	// Find "key": number pattern
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip : and whitespace
	for pos < json_str.len && (json_str[pos] == `:` || json_str[pos] == ` `
		|| json_str[pos] == `\t` || json_str[pos] == `\n`) {
		pos += 1
	}

	if pos >= json_str.len {
		return none
	}

	// Read number
	mut end := pos
	for end < json_str.len && (json_str[end] >= `0` && json_str[end] <= `9`) {
		end += 1
	}

	if end == pos {
		return none
	}

	return json_str[pos..end].int()
}

// register registers a new schema under a subject
// Returns the schema ID (existing or new)
pub fn (mut r SchemaRegistry) register(subject string, schema_str string, schema_type domain.SchemaType) !int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	// 1. Normalize and fingerprint the schema
	normalized := normalize_schema(schema_str)
	fingerprint := compute_fingerprint(normalized)

	// 2. Check if this exact schema already exists for this subject
	if existing_id := r.find_schema_by_fingerprint(subject, fingerprint) {
		return existing_id
	}

	// 3. Validate schema syntax
	r.validate_schema(schema_type, schema_str)!

	// 4. Check compatibility with existing versions
	versions := r.subjects[subject] or { []domain.SchemaVersion{} }
	if versions.len > 0 {
		config := r.subject_configs[subject] or {
			domain.SubjectConfig{
				compatibility: r.default_compat
			}
		}
		r.check_compatibility(subject, schema_str, schema_type, config.compatibility)!
	}

	// 5. Allocate new schema ID
	schema_id := r.next_id
	r.next_id += 1

	// 6. Create schema and version
	schema := domain.Schema{
		id:          schema_id
		schema_type: schema_type
		schema_str:  schema_str
		fingerprint: fingerprint
	}

	version := domain.SchemaVersion{
		version:       versions.len + 1
		schema_id:     schema_id
		subject:       subject
		compatibility: r.subject_configs[subject] or { domain.SubjectConfig{} }.compatibility
		created_at:    time.now()
	}

	// 7. Store in cache
	r.schemas[schema_id] = schema
	if subject in r.subjects {
		r.subjects[subject] << version
	} else {
		r.subjects[subject] = [version]
	}

	// 8. Persist to storage (async-safe, already locked)
	r.persist_schema(subject, schema, version) or {
		// Rollback on persistence failure
		r.schemas.delete(schema_id)
		r.subjects[subject] = versions
		return err
	}

	return schema_id
}

// get_schema retrieves a schema by ID
pub fn (mut r SchemaRegistry) get_schema(schema_id int) !domain.Schema {
	r.lock.rlock()
	defer { r.lock.runlock() }

	return r.schemas[schema_id] or { return error('schema not found: ${schema_id}') }
}

// get_schema_by_subject retrieves a schema by subject and version
pub fn (mut r SchemaRegistry) get_schema_by_subject(subject string, version int) !domain.Schema {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	// Handle special version values
	actual_version := if version == -1 {
		versions.len // Latest version
	} else {
		version
	}

	if actual_version < 1 || actual_version > versions.len {
		return error('version not found: ${subject}/${actual_version}')
	}

	schema_version := versions[actual_version - 1]
	return r.schemas[schema_version.schema_id] or { return error('schema not found') }
}

// get_latest_version gets the latest version of a schema for a subject
pub fn (mut r SchemaRegistry) get_latest_version(subject string) !domain.SchemaVersion {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }
	if versions.len == 0 {
		return error('no versions for subject: ${subject}')
	}

	return versions[versions.len - 1]
}

// list_subjects returns all registered subjects
pub fn (mut r SchemaRegistry) list_subjects() []string {
	r.lock.rlock()
	defer { r.lock.runlock() }

	mut result := []string{cap: r.subjects.len}
	for subject, _ in r.subjects {
		result << subject
	}
	return result
}

// list_versions returns all versions for a subject
pub fn (mut r SchemaRegistry) list_versions(subject string) ![]int {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	mut result := []int{cap: versions.len}
	for v in versions {
		result << v.version
	}
	return result
}

// delete_subject deletes all versions of a subject
pub fn (mut r SchemaRegistry) delete_subject(subject string) ![]int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	mut deleted := []int{cap: versions.len}
	for v in versions {
		deleted << v.version
	}

	// Remove from cache (schemas may still be referenced by other subjects)
	r.subjects.delete(subject)
	r.subject_configs.delete(subject)

	return deleted
}

// delete_version deletes a specific version of a subject
pub fn (mut r SchemaRegistry) delete_version(subject string, version int) !int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	mut versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	if version < 1 || version > versions.len {
		return error('version not found: ${subject}/${version}')
	}

	schema_id := versions[version - 1].schema_id

	// Remove version (soft delete - keep schema in cache for other subjects)
	mut new_versions := []domain.SchemaVersion{}
	for v in versions {
		if v.version != version {
			new_versions << v
		}
	}
	r.subjects[subject] = new_versions

	return schema_id
}

// get_compatibility returns the compatibility level for a subject
pub fn (mut r SchemaRegistry) get_compatibility(subject string) domain.CompatibilityLevel {
	r.lock.rlock()
	defer { r.lock.runlock() }

	config := r.subject_configs[subject] or { return r.default_compat }
	return config.compatibility
}

// set_compatibility sets the compatibility level for a subject
pub fn (mut r SchemaRegistry) set_compatibility(subject string, level domain.CompatibilityLevel) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if config := r.subject_configs[subject] {
		r.subject_configs[subject] = domain.SubjectConfig{
			...config
			compatibility: level
		}
	} else {
		r.subject_configs[subject] = domain.SubjectConfig{
			compatibility: level
		}
	}
}

// test_compatibility tests if a schema is compatible with the subject
pub fn (mut r SchemaRegistry) test_compatibility(subject string, schema_str string, schema_type domain.SchemaType) !bool {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return true } // No versions = compatible
	if versions.len == 0 {
		return true
	}

	config := r.subject_configs[subject] or {
		domain.SubjectConfig{
			compatibility: r.default_compat
		}
	}

	// Use check_compatibility_internal without acquiring lock again
	return r.check_compatibility_internal(subject, schema_str, schema_type, config.compatibility)
}

// Private methods

fn (r &SchemaRegistry) find_schema_by_fingerprint(subject string, fingerprint string) ?int {
	versions := r.subjects[subject] or { return none }

	for v in versions {
		if schema := r.schemas[v.schema_id] {
			if schema.fingerprint == fingerprint {
				return v.schema_id
			}
		}
	}
	return none
}

fn (r &SchemaRegistry) validate_schema(schema_type domain.SchemaType, schema_str string) ! {
	match schema_type {
		.avro {
			validate_avro_schema_syntax(schema_str)!
		}
		.json {
			validate_json_schema_syntax(schema_str)!
		}
		.protobuf {
			validate_protobuf_schema_syntax(schema_str)!
		}
	}
}

// ============================================================================
// Enhanced Schema Validation
// ============================================================================

// validate_avro_schema_syntax validates Avro schema syntax
fn validate_avro_schema_syntax(schema_str string) ! {
	// Check if it's valid JSON
	if !is_valid_json(schema_str) {
		return error('invalid Avro schema: not valid JSON')
	}

	trimmed := schema_str.trim_space()

	// Handle primitive type strings like "string", "int", etc.
	if trimmed.starts_with('"') && trimmed.ends_with('"') {
		type_name := trimmed[1..trimmed.len - 1]
		valid_primitives := ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string']
		if type_name !in valid_primitives {
			return error('invalid Avro schema: unknown primitive type "${type_name}"')
		}
		return
	}

	// Handle array (union type) like ["null", "string"]
	if trimmed.starts_with('[') && trimmed.ends_with(']') {
		// Union type - validate each element
		return
	}

	// Handle object schema
	if !trimmed.starts_with('{') {
		return error('invalid Avro schema: expected JSON object, array, or primitive type string')
	}

	// Must have "type" field for complex types
	if !schema_str.contains('"type"') {
		return error('invalid Avro schema: missing "type" field')
	}

	// Extract and validate type
	schema_type := extract_json_string(schema_str, 'type') or {
		return error('invalid Avro schema: cannot parse "type" field')
	}

	valid_types := ['record', 'enum', 'array', 'map', 'fixed', 'null', 'boolean', 'int', 'long',
		'float', 'double', 'bytes', 'string']
	if schema_type !in valid_types {
		return error('invalid Avro schema: unknown type "${schema_type}"')
	}

	// Validate type-specific requirements
	match schema_type {
		'record' {
			// Record must have "name" and "fields"
			if !schema_str.contains('"name"') {
				return error('invalid Avro schema: record type requires "name" field')
			}
			if !schema_str.contains('"fields"') {
				return error('invalid Avro schema: record type requires "fields" field')
			}
			// Validate fields array
			fields := parse_avro_fields(schema_str) or { []AvroField{} }
			for field in fields {
				if field.name.len == 0 {
					return error('invalid Avro schema: field missing "name"')
				}
			}
		}
		'enum' {
			// Enum must have "name" and "symbols"
			if !schema_str.contains('"name"') {
				return error('invalid Avro schema: enum type requires "name" field')
			}
			if !schema_str.contains('"symbols"') {
				return error('invalid Avro schema: enum type requires "symbols" field')
			}
			// Validate symbols is non-empty array
			symbols := parse_json_string_array(schema_str, 'symbols') or { []string{} }
			if symbols.len == 0 {
				return error('invalid Avro schema: enum "symbols" cannot be empty')
			}
		}
		'array' {
			// Array must have "items"
			if !schema_str.contains('"items"') {
				return error('invalid Avro schema: array type requires "items" field')
			}
		}
		'map' {
			// Map must have "values"
			if !schema_str.contains('"values"') {
				return error('invalid Avro schema: map type requires "values" field')
			}
		}
		'fixed' {
			// Fixed must have "name" and "size"
			if !schema_str.contains('"name"') {
				return error('invalid Avro schema: fixed type requires "name" field')
			}
			if !schema_str.contains('"size"') {
				return error('invalid Avro schema: fixed type requires "size" field')
			}
		}
		else {
			// Primitive types are valid
		}
	}
}

// validate_json_schema_syntax validates JSON Schema syntax (Draft-07 compatible)
fn validate_json_schema_syntax(schema_str string) ! {
	// Check if it's valid JSON
	if !is_valid_json(schema_str) {
		return error('invalid JSON Schema: not valid JSON')
	}

	trimmed := schema_str.trim_space()

	// JSON Schema can be boolean (true/false)
	if trimmed == 'true' || trimmed == 'false' {
		return
	}

	// Must be an object
	if !trimmed.starts_with('{') {
		return error('invalid JSON Schema: expected JSON object or boolean')
	}

	// Optional: check $schema field for draft version
	if schema_version := extract_json_string(schema_str, r'$schema') {
		// Validate supported drafts
		supported_drafts := [
			'http://json-schema.org/draft-04/schema#',
			'http://json-schema.org/draft-06/schema#',
			'http://json-schema.org/draft-07/schema#',
			'https://json-schema.org/draft/2019-09/schema',
			'https://json-schema.org/draft/2020-12/schema',
		]
		mut supported := false
		for draft in supported_drafts {
			if schema_version.contains('draft') {
				supported = true
				break
			}
		}
		// Don't fail on unknown drafts, just warn
		_ = supported
	}

	// Validate type field if present
	if schema_type := extract_json_string(schema_str, 'type') {
		valid_types := ['string', 'number', 'integer', 'boolean', 'array', 'object', 'null']
		if schema_type !in valid_types {
			return error('invalid JSON Schema: unknown type "${schema_type}"')
		}
	}

	// Validate properties structure
	if schema_str.contains('"properties"') {
		props_start := schema_str.index('"properties"') or { return }
		props_json := extract_json_object_value(schema_str, props_start + 12)
		if props_json.len > 0 && !is_valid_json(props_json) {
			return error('invalid JSON Schema: malformed "properties" object')
		}
	}

	// Validate items structure for arrays
	if schema_str.contains('"items"') {
		items_start := schema_str.index('"items"') or { return }
		// Items can be object or array (tuple validation)
		mut pos := items_start + 7
		for pos < schema_str.len
			&& (schema_str[pos] == `:` || schema_str[pos] == ` ` || schema_str[pos] == `\t`) {
			pos += 1
		}
		if pos < schema_str.len {
			if schema_str[pos] != `{` && schema_str[pos] != `[` && schema_str[pos] != `t`
				&& schema_str[pos] != `f` {
				return error('invalid JSON Schema: "items" must be object, array, or boolean')
			}
		}
	}

	// Validate required is an array of strings
	if schema_str.contains('"required"') {
		required := parse_json_string_array(schema_str, 'required') or {
			return error('invalid JSON Schema: "required" must be an array of strings')
		}
		// Check for duplicates
		mut seen := map[string]bool{}
		for prop in required {
			if prop in seen {
				return error('invalid JSON Schema: duplicate in "required" array: "${prop}"')
			}
			seen[prop] = true
		}
	}

	// Validate enum is a non-empty array with unique values
	if schema_str.contains('"enum"') {
		// enum should be an array
		enum_start := schema_str.index('"enum"') or { return }
		mut pos := enum_start + 6
		for pos < schema_str.len && (schema_str[pos] == `:` || schema_str[pos] == ` `) {
			pos += 1
		}
		if pos < schema_str.len && schema_str[pos] != `[` {
			return error('invalid JSON Schema: "enum" must be an array')
		}
	}

	// Validate numeric constraints are consistent
	if min_val := extract_json_float(schema_str, 'minimum') {
		if max_val := extract_json_float(schema_str, 'maximum') {
			if min_val > max_val {
				return error('invalid JSON Schema: minimum (${min_val}) > maximum (${max_val})')
			}
		}
	}

	// Validate minLength/maxLength
	if min_len := extract_json_number(schema_str, 'minLength') {
		if min_len < 0 {
			return error('invalid JSON Schema: minLength must be non-negative')
		}
		if max_len := extract_json_number(schema_str, 'maxLength') {
			if min_len > max_len {
				return error('invalid JSON Schema: minLength (${min_len}) > maxLength (${max_len})')
			}
		}
	}

	// Validate minItems/maxItems
	if min_items := extract_json_number(schema_str, 'minItems') {
		if min_items < 0 {
			return error('invalid JSON Schema: minItems must be non-negative')
		}
		if max_items := extract_json_number(schema_str, 'maxItems') {
			if min_items > max_items {
				return error('invalid JSON Schema: minItems (${min_items}) > maxItems (${max_items})')
			}
		}
	}
}

// validate_protobuf_schema_syntax validates Protobuf schema syntax
fn validate_protobuf_schema_syntax(schema_str string) ! {
	trimmed := schema_str.trim_space()

	// Must contain message or enum definition
	has_message := trimmed.contains('message ')
	has_enum := trimmed.contains('enum ')

	if !has_message && !has_enum {
		return error('invalid Protobuf schema: must contain "message" or "enum" definition')
	}

	// Check for basic syntax errors
	mut open_braces := 0
	mut in_string := false
	mut in_comment := false

	for i, c in trimmed {
		// Track comments
		if i > 0 && trimmed[i - 1] == `/` && c == `/` {
			in_comment = true
		}
		if in_comment && c == `\n` {
			in_comment = false
			continue
		}
		if in_comment {
			continue
		}

		// Track strings
		if c == `"` && (i == 0 || trimmed[i - 1] != `\\`) {
			in_string = !in_string
		}
		if in_string {
			continue
		}

		if c == `{` {
			open_braces += 1
		} else if c == `}` {
			open_braces -= 1
			if open_braces < 0 {
				return error('invalid Protobuf schema: unmatched closing brace')
			}
		}
	}

	if open_braces != 0 {
		return error('invalid Protobuf schema: unmatched braces')
	}

	// Validate message structure
	if has_message {
		// Extract message name
		msg_idx := trimmed.index('message ') or { return }
		mut pos := msg_idx + 8
		for pos < trimmed.len && (trimmed[pos] == ` ` || trimmed[pos] == `\t`) {
			pos += 1
		}

		// Read message name
		mut name_end := pos
		for name_end < trimmed.len && trimmed[name_end] != ` ` && trimmed[name_end] != `{`
			&& trimmed[name_end] != `\n` {
			name_end += 1
		}

		if name_end == pos {
			return error('invalid Protobuf schema: message must have a name')
		}

		msg_name := trimmed[pos..name_end].trim_space()

		// Message name must start with uppercase letter
		if msg_name.len > 0 && (msg_name[0] < `A` || msg_name[0] > `Z`) {
			// This is a warning, not an error - proto3 allows lowercase but convention is uppercase
		}

		// Find message body and validate fields
		brace_start := trimmed.index_after('{', msg_idx) or {
			return error('invalid Protobuf schema: message missing opening brace')
		}

		// Extract message body
		mut brace_depth := 1
		mut brace_end := brace_start + 1
		for brace_end < trimmed.len && brace_depth > 0 {
			if trimmed[brace_end] == `{` {
				brace_depth += 1
			} else if trimmed[brace_end] == `}` {
				brace_depth -= 1
			}
			brace_end += 1
		}

		if brace_depth != 0 {
			return error('invalid Protobuf schema: unclosed message body')
		}

		body := trimmed[brace_start + 1..brace_end - 1]

		// Validate field definitions
		validate_protobuf_fields(body)!
	}

	// Validate enum structure
	if has_enum {
		enum_idx := trimmed.index('enum ') or { return }
		mut pos := enum_idx + 5
		for pos < trimmed.len && (trimmed[pos] == ` ` || trimmed[pos] == `\t`) {
			pos += 1
		}

		mut name_end := pos
		for name_end < trimmed.len && trimmed[name_end] != ` ` && trimmed[name_end] != `{`
			&& trimmed[name_end] != `\n` {
			name_end += 1
		}

		if name_end == pos {
			return error('invalid Protobuf schema: enum must have a name')
		}
	}
}

// validate_protobuf_fields validates protobuf field definitions
fn validate_protobuf_fields(body string) ! {
	lines := body.split('\n')
	mut field_numbers := map[int]string{} // Track used field numbers
	mut reserved_numbers := []int{}

	for line in lines {
		trimmed := line.trim_space()

		// Skip empty lines, comments
		if trimmed.len == 0 || trimmed.starts_with('//') {
			continue
		}

		// Handle reserved statement
		if trimmed.starts_with('reserved ') {
			// Parse reserved numbers
			reserved_part := trimmed[9..].trim_right(';')
			parts := reserved_part.split(',')
			for part in parts {
				p := part.trim_space()
				if p.contains(' to ') {
					// Range like "1 to 10"
					range_parts := p.split(' to ')
					if range_parts.len == 2 {
						start := range_parts[0].trim_space().int()
						end := range_parts[1].trim_space().int()
						for n := start; n <= end; n++ {
							reserved_numbers << n
						}
					}
				} else if !p.starts_with('"') {
					// Single number
					reserved_numbers << p.int()
				}
			}
			continue
		}

		// Skip nested message/enum
		if trimmed.starts_with('message ') || trimmed.starts_with('enum ') {
			continue
		}

		// Parse field definition
		if trimmed.contains('=') && !trimmed.starts_with('option') {
			parts := trimmed.split('=')
			if parts.len >= 2 {
				// Extract field number
				num_part := parts[1].trim_space().split(' ')[0].trim_right(';').trim_right('[')
				field_num := num_part.int()

				if field_num <= 0 {
					return error('invalid Protobuf schema: field number must be positive')
				}

				if field_num >= 19000 && field_num <= 19999 {
					return error('invalid Protobuf schema: field numbers 19000-19999 are reserved for protobuf implementation')
				}

				// Check reserved
				if field_num in reserved_numbers {
					return error('invalid Protobuf schema: field number ${field_num} is reserved')
				}

				// Check duplicates
				if existing := field_numbers[field_num] {
					return error('invalid Protobuf schema: duplicate field number ${field_num}')
				}

				// Extract field name for tracking
				type_name_part := parts[0].trim_space()
				tokens := type_name_part.split(' ').filter(fn (s string) bool {
					return s.len > 0
				})
				if tokens.len >= 2 {
					field_name := tokens[tokens.len - 1]
					field_numbers[field_num] = field_name
				}
			}
		}
	}
}

fn (r &SchemaRegistry) check_compatibility(subject string, schema_str string, schema_type domain.SchemaType, level domain.CompatibilityLevel) ! {
	r.check_compatibility_internal(subject, schema_str, schema_type, level) or { return err }
}

fn (r &SchemaRegistry) check_compatibility_internal(subject string, schema_str string, schema_type domain.SchemaType, level domain.CompatibilityLevel) !bool {
	if level == .none {
		return true
	}

	versions := r.subjects[subject] or { return true }
	if versions.len == 0 {
		return true
	}

	// Get schemas to check against based on compatibility level
	schemas_to_check := match level {
		.backward, .forward, .full {
			// Check against latest only
			[versions[versions.len - 1]]
		}
		.backward_transitive, .forward_transitive, .full_transitive {
			// Check against all versions
			versions
		}
		.none {
			[]domain.SchemaVersion{}
		}
	}

	for v in schemas_to_check {
		existing := r.schemas[v.schema_id] or { continue }

		compatible := match level {
			.backward, .backward_transitive {
				// New schema can read data written with old schema
				check_backward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.forward, .forward_transitive {
				// Old schema can read data written with new schema
				check_forward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.full, .full_transitive {
				// Both directions

				check_backward_compatible(existing.schema_str, schema_str, schema_type)
					&& check_forward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.none {
				true
			}
		}

		if !compatible {
			return error('schema is not ${level.str()} compatible with version ${v.version}')
		}
	}

	return true
}

fn (mut r SchemaRegistry) persist_schema(subject string, schema domain.Schema, version domain.SchemaVersion) ! {
	// Create a record to store in __schemas topic
	// Format: JSON with schema details
	record_data := '{"subject":"${subject}","version":${version.version},"id":${schema.id},"schemaType":"${schema.schema_type.str()}","schema":${escape_json_string(schema.schema_str)}}'

	record := domain.Record{
		key:       subject.bytes()
		value:     record_data.bytes()
		timestamp: time.now()
	}

	// Ensure __schemas topic exists (create if not)
	r.storage.get_topic(schemas_topic) or {
		// Create internal topic with single partition
		r.storage.create_topic(schemas_topic, 1, domain.TopicConfig{
			retention_ms:   -1 // Keep forever
			cleanup_policy: 'compact'
		}) or {
			// Topic might already exist from another thread
		}
	}

	// Append to __schemas topic
	r.storage.append(schemas_topic, 0, [record]) or {
		return error('failed to persist schema: ${err}')
	}
}

// Helper functions

fn normalize_schema(schema_str string) string {
	// Remove whitespace for consistent fingerprinting
	// This is a simplified normalization
	return schema_str.replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
		'')
}

fn compute_fingerprint(schema_str string) string {
	hash := md5.sum(schema_str.bytes())
	return hash.hex()
}

fn is_valid_json(s string) bool {
	// Basic JSON validation
	trimmed := s.trim_space()
	if trimmed.len == 0 {
		return false
	}

	// Valid JSON can be: object, array, string, number, boolean, or null
	first := trimmed[0]

	// Boolean or null
	if trimmed == 'true' || trimmed == 'false' || trimmed == 'null' {
		return true
	}

	// String
	if first == `"` && trimmed.len >= 2 && trimmed[trimmed.len - 1] == `"` {
		// Basic string validation - check for proper escaping
		mut i := 1
		for i < trimmed.len - 1 {
			if trimmed[i] == `\\` {
				i += 2 // Skip escaped character
			} else if trimmed[i] == `"` {
				return false // Unescaped quote in middle
			} else {
				i += 1
			}
		}
		return true
	}

	// Number (simplified check)
	if first == `-` || (first >= `0` && first <= `9`) {
		mut valid := true
		for c in trimmed {
			if !((c >= `0` && c <= `9`) || c == `-` || c == `.` || c == `e` || c == `E` || c == `+`) {
				valid = false
				break
			}
		}
		return valid
	}

	// Object or Array
	if (trimmed.starts_with('{') && trimmed.ends_with('}'))
		|| (trimmed.starts_with('[') && trimmed.ends_with(']')) {
		// Basic bracket matching
		mut depth := 0
		mut in_string := false
		mut escape := false

		for c in trimmed {
			if escape {
				escape = false
				continue
			}

			if c == `\\` && in_string {
				escape = true
				continue
			}

			if c == `"` {
				in_string = !in_string
				continue
			}

			if in_string {
				continue
			}

			if c == `{` || c == `[` {
				depth += 1
			} else if c == `}` || c == `]` {
				depth -= 1
				if depth < 0 {
					return false
				}
			}
		}

		return depth == 0 && !in_string
	}

	return false
}

fn escape_json_string(s string) string {
	// Escape special characters for JSON embedding
	return '"${s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r',
		'\\r').replace('\t', '\\t')}"'
}

// Compatibility checking functions

fn check_backward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	// Backward compatible: new schema can read data written with old schema
	match schema_type {
		.avro {
			return check_avro_backward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_backward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_backward_compatible(old_schema, new_schema)
		}
	}
}

fn check_forward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
	// Forward compatible: old schema can read data written with new schema
	match schema_type {
		.avro {
			return check_avro_forward_compatible(old_schema, new_schema)
		}
		.json {
			return check_json_forward_compatible(old_schema, new_schema)
		}
		.protobuf {
			return check_protobuf_forward_compatible(old_schema, new_schema)
		}
	}
}

// ============================================================================
// Avro Schema Compatibility Checking
// ============================================================================

// AvroField represents a parsed Avro record field
struct AvroField {
mut:
	name        string
	field_type  string // primitive type or complex type name
	has_default bool
	is_nullable bool // union with null
	is_array    bool
	is_map      bool
	is_union    bool
	union_types []string // types in union
}

// AvroSchema represents a parsed Avro schema
pub struct AvroSchema {
pub mut:
	schema_type string // record, enum, array, map, union, fixed, primitive
	name        string // schema name (for record/enum)
	namespace   string
	fields      []AvroField // for record type
	symbols     []string    // for enum type
	items_type  string      // for array type
	values_type string      // for map type
	union_types []string    // for union type
	fixed_size  int         // for fixed type
}

fn check_avro_backward_compatible(old_schema string, new_schema string) bool {
	// Parse both schemas
	old_parsed := parse_avro_schema(old_schema) or { return false }
	new_parsed := parse_avro_schema(new_schema) or { return false }

	// Check schema type compatibility
	if old_parsed.schema_type != new_parsed.schema_type {
		return false
	}

	match old_parsed.schema_type {
		'record' {
			return check_avro_record_backward_compatible(old_parsed, new_parsed)
		}
		'enum' {
			return check_avro_enum_backward_compatible(old_parsed, new_parsed)
		}
		else {
			// Primitives and other types must match exactly
			return old_parsed.schema_type == new_parsed.schema_type
		}
	}
}

fn check_avro_forward_compatible(old_schema string, new_schema string) bool {
	// Parse both schemas
	old_parsed := parse_avro_schema(old_schema) or { return false }
	new_parsed := parse_avro_schema(new_schema) or { return false }

	// Check schema type compatibility
	if old_parsed.schema_type != new_parsed.schema_type {
		return false
	}

	match old_parsed.schema_type {
		'record' {
			return check_avro_record_forward_compatible(old_parsed, new_parsed)
		}
		'enum' {
			return check_avro_enum_forward_compatible(old_parsed, new_parsed)
		}
		else {
			return old_parsed.schema_type == new_parsed.schema_type
		}
	}
}

fn check_avro_record_backward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Rule 1: All fields in the OLD schema must exist in NEW schema OR have defaults in NEW
	// Rule 2: New fields added to NEW schema must have defaults (so old data can be read)

	// Create map of new schema fields for quick lookup
	mut new_fields := map[string]AvroField{}
	for f in new_schema.fields {
		new_fields[f.name] = f
	}

	// Check each old field exists in new schema with compatible type
	for old_field in old_schema.fields {
		if new_field := new_fields[old_field.name] {
			// Field exists - check type compatibility
			if !is_avro_type_compatible(old_field, new_field) {
				return false
			}
		} else {
			// Field removed in new schema - NOT backward compatible
			// (new schema cannot read old data that has this field)
			// Actually, this IS backward compatible if the reader ignores unknown fields
			// Avro readers typically ignore unknown fields, so this is allowed
			continue
		}
	}

	// Check new fields have defaults (so old data without these fields can be read)
	mut old_field_names := map[string]bool{}
	for f in old_schema.fields {
		old_field_names[f.name] = true
	}

	for new_field in new_schema.fields {
		if new_field.name !in old_field_names {
			// New field - must have default or be nullable
			if !new_field.has_default && !new_field.is_nullable {
				return false
			}
		}
	}

	return true
}

fn check_avro_record_forward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Rule 1: All fields in NEW schema must exist in OLD schema OR have defaults in OLD
	// Rule 2: Fields removed from NEW schema must have had defaults in OLD

	mut old_fields := map[string]AvroField{}
	for f in old_schema.fields {
		old_fields[f.name] = f
	}

	// Check each new field exists in old schema with compatible type
	for new_field in new_schema.fields {
		if old_field := old_fields[new_field.name] {
			if !is_avro_type_compatible(old_field, new_field) {
				return false
			}
		} else {
			// Field added in new schema - old reader won't know about it
			// Old reader will ignore unknown fields, so this is allowed
			continue
		}
	}

	// Check removed fields have defaults (so new data without these fields can be read by old)
	mut new_field_names := map[string]bool{}
	for f in new_schema.fields {
		new_field_names[f.name] = true
	}

	for old_field in old_schema.fields {
		if old_field.name !in new_field_names {
			// Field removed in new schema - old reader needs default
			if !old_field.has_default && !old_field.is_nullable {
				return false
			}
		}
	}

	return true
}

fn check_avro_enum_backward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Backward compatible: new enum can add symbols (old data still valid)
	// All old symbols must exist in new enum
	for symbol in old_schema.symbols {
		if symbol !in new_schema.symbols {
			return false
		}
	}
	return true
}

fn check_avro_enum_forward_compatible(old_schema AvroSchema, new_schema AvroSchema) bool {
	// Forward compatible: new enum can remove symbols (new data still valid for old reader)
	// All new symbols must exist in old enum
	for symbol in new_schema.symbols {
		if symbol !in old_schema.symbols {
			return false
		}
	}
	return true
}

fn is_avro_type_compatible(old_field AvroField, new_field AvroField) bool {
	// Same type is always compatible
	if old_field.field_type == new_field.field_type {
		return true
	}

	// Check type promotion rules (Avro allows certain promotions)
	// int -> long, float, double
	// long -> float, double
	// float -> double
	// string -> bytes (and vice versa)

	promotable := {
		'int':    ['long', 'float', 'double']
		'long':   ['float', 'double']
		'float':  ['double']
		'string': ['bytes']
		'bytes':  ['string']
	}

	if allowed := promotable[old_field.field_type] {
		if new_field.field_type in allowed {
			return true
		}
	}

	// Union compatibility - if new field is union containing old type
	if new_field.is_union {
		if old_field.field_type in new_field.union_types {
			return true
		}
	}

	// If old field is union, new field can be any of the union types
	if old_field.is_union {
		if new_field.field_type in old_field.union_types {
			return true
		}
	}

	return false
}

fn parse_avro_schema(schema_str string) !AvroSchema {
	// Simple JSON-based Avro schema parser
	trimmed := schema_str.trim_space()

	// Handle primitive types as strings
	if trimmed.starts_with('"') {
		type_name := trimmed.trim('"')
		return AvroSchema{
			schema_type: type_name
		}
	}

	// Handle complex types (object)
	if !trimmed.starts_with('{') {
		return error('invalid Avro schema: expected object')
	}

	// Extract type field
	schema_type := extract_json_string(trimmed, 'type') or { 'unknown' }

	mut result := AvroSchema{
		schema_type: schema_type
		name:        extract_json_string(trimmed, 'name') or { '' }
		namespace:   extract_json_string(trimmed, 'namespace') or { '' }
	}

	if schema_type == 'record' {
		// Parse fields array
		result.fields = parse_avro_fields(trimmed) or { []AvroField{} }
	} else if schema_type == 'enum' {
		// Parse symbols array
		result.symbols = parse_json_string_array(trimmed, 'symbols') or { []string{} }
	} else if schema_type == 'array' {
		result.items_type = extract_json_string(trimmed, 'items') or { '' }
	} else if schema_type == 'map' {
		result.values_type = extract_json_string(trimmed, 'values') or { '' }
	}

	return result
}

fn parse_avro_fields(schema_str string) ![]AvroField {
	// Find fields array in JSON
	fields_start := schema_str.index('"fields"') or { return []AvroField{} }

	// Find the array start after "fields":
	mut pos := fields_start + 8 // length of '"fields"'
	for pos < schema_str.len && schema_str[pos] != `[` {
		pos += 1
	}
	if pos >= schema_str.len {
		return []AvroField{}
	}

	// Find matching bracket
	array_start := pos
	mut depth := 1
	pos += 1
	for pos < schema_str.len && depth > 0 {
		if schema_str[pos] == `[` {
			depth += 1
		} else if schema_str[pos] == `]` {
			depth -= 1
		}
		pos += 1
	}

	fields_json := schema_str[array_start..pos]

	// Parse individual field objects
	mut fields := []AvroField{}
	mut field_start := 1 // skip opening [
	mut brace_depth := 0

	for i := 1; i < fields_json.len - 1; i++ {
		c := fields_json[i]
		if c == `{` {
			if brace_depth == 0 {
				field_start = i
			}
			brace_depth += 1
		} else if c == `}` {
			brace_depth -= 1
			if brace_depth == 0 {
				field_json := fields_json[field_start..i + 1]
				if field := parse_single_avro_field(field_json) {
					fields << field
				}
			}
		}
	}

	return fields
}

fn parse_single_avro_field(field_json string) ?AvroField {
	name := extract_json_string(field_json, 'name') or { return none }

	mut field := AvroField{
		name:        name
		has_default: field_json.contains('"default"')
	}

	// Parse type - can be string, array (union), or object
	type_start := field_json.index('"type"') or { return field }
	mut pos := type_start + 6

	// Skip to value
	for pos < field_json.len
		&& (field_json[pos] == `:` || field_json[pos] == ` ` || field_json[pos] == `\t`) {
		pos += 1
	}

	if pos >= field_json.len {
		return field
	}

	if field_json[pos] == `"` {
		// Simple type string
		end_quote := field_json.index_after('"', pos + 1) or { return field }
		field.field_type = field_json[pos + 1..end_quote]
		field.is_nullable = field.field_type == 'null'
	} else if field_json[pos] == `[` {
		// Union type
		field.is_union = true
		// Find matching bracket
		mut bracket_depth := 1
		mut union_end := pos + 1
		for union_end < field_json.len && bracket_depth > 0 {
			if field_json[union_end] == `[` {
				bracket_depth += 1
			} else if field_json[union_end] == `]` {
				bracket_depth -= 1
			}
			union_end += 1
		}
		union_str := field_json[pos..union_end]
		field.union_types = parse_union_types(union_str)
		field.is_nullable = 'null' in field.union_types
		// Set primary type (first non-null type)
		for t in field.union_types {
			if t != 'null' {
				field.field_type = t
				break
			}
		}
	} else if field_json[pos] == `{` {
		// Complex type (nested record, array, map, etc.)
		field.field_type = 'complex'
	}

	return field
}

fn parse_union_types(union_str string) []string {
	// Parse ["null", "string"] style union
	mut types := []string{}
	mut in_string := false
	mut start := 0

	for i, c in union_str {
		if c == `"` && !in_string {
			in_string = true
			start = i + 1
		} else if c == `"` && in_string {
			in_string = false
			types << union_str[start..i]
		}
	}

	return types
}

fn extract_json_string(json_str string, key string) ?string {
	// Find "key": "value" pattern
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip : and whitespace
	for pos < json_str.len && (json_str[pos] == `:` || json_str[pos] == ` `
		|| json_str[pos] == `\t` || json_str[pos] == `\n`) {
		pos += 1
	}

	if pos >= json_str.len || json_str[pos] != `"` {
		return none
	}

	// Find end quote
	start := pos + 1
	mut end := start
	mut escaped := false
	for end < json_str.len {
		if escaped {
			escaped = false
			end += 1
			continue
		}
		if json_str[end] == `\\` {
			escaped = true
			end += 1
			continue
		}
		if json_str[end] == `"` {
			break
		}
		end += 1
	}

	return json_str[start..end]
}

fn parse_json_string_array(json_str string, key string) ?[]string {
	// Find "key": [...] pattern
	key_pattern := '"${key}"'
	key_pos := json_str.index(key_pattern) or { return none }

	mut pos := key_pos + key_pattern.len

	// Skip to [
	for pos < json_str.len && json_str[pos] != `[` {
		pos += 1
	}

	if pos >= json_str.len {
		return none
	}

	// Find matching ]
	mut depth := 1
	mut end := pos + 1
	for end < json_str.len && depth > 0 {
		if json_str[end] == `[` {
			depth += 1
		} else if json_str[end] == `]` {
			depth -= 1
		}
		end += 1
	}

	array_str := json_str[pos..end]

	// Extract strings from array
	mut result := []string{}
	mut in_string := false
	mut start := 0

	for i, c in array_str {
		if c == `"` && !in_string {
			in_string = true
			start = i + 1
		} else if c == `"` && in_string {
			in_string = false
			result << array_str[start..i]
		}
	}

	return result
}

// ============================================================================
// JSON Schema Compatibility Checking (Enhanced)
// ============================================================================

// JsonSchemaInfo represents parsed JSON Schema information for compatibility checking
struct JsonSchemaInfo {
mut:
	schema_type           string
	properties            map[string]JsonPropertyInfo
	required              []string
	additional_properties bool = true
	has_default           bool
	enum_values           []string
	// Numeric constraints
	minimum     f64
	maximum     f64
	has_minimum bool
	has_maximum bool
	// String constraints
	min_length int
	max_length int
	pattern    string
	// Array constraints
	min_items int
	max_items int
}

struct JsonPropertyInfo {
mut:
	prop_type   string
	has_default bool
	nullable    bool
	enum_values []string
}

fn check_json_backward_compatible(old_schema string, new_schema string) bool {
	// JSON Schema backward compatibility:
	// - New schema can read old data
	// - All old required properties must exist in new schema
	// - New required properties must have been optional or have defaults
	// - Property types must be compatible (new type can read old data)

	old_info := parse_json_schema_info(old_schema)
	new_info := parse_json_schema_info(new_schema)

	// Type compatibility check
	if old_info.schema_type.len > 0 && new_info.schema_type.len > 0 {
		if !is_json_type_backward_compatible(old_info.schema_type, new_info.schema_type) {
			return false
		}
	}

	// All old required properties must exist in new schema with compatible types
	for prop in old_info.required {
		if new_prop := new_info.properties[prop] {
			// Property exists - check type compatibility
			if old_prop := old_info.properties[prop] {
				if !is_json_property_backward_compatible(old_prop, new_prop) {
					return false
				}
			}
		} else if !new_info.additional_properties {
			// Property removed and additionalProperties is false
			return false
		}
		// If additionalProperties is true, removed properties are OK (ignored)
	}

	// New required properties must have existed before (optional or required)
	// or have a default value
	for prop in new_info.required {
		if prop !in old_info.required {
			// New required property
			if old_prop := old_info.properties[prop] {
				// Property existed as optional - OK
				continue
			} else {
				// Completely new required property - check for default
				if new_prop := new_info.properties[prop] {
					if !new_prop.has_default && !new_prop.nullable {
						return false // Old data won't have this property
					}
				} else {
					return false
				}
			}
		}
	}

	// Check property type compatibility for all shared properties
	for name, old_prop in old_info.properties {
		if new_prop := new_info.properties[name] {
			if !is_json_property_backward_compatible(old_prop, new_prop) {
				return false
			}
		}
	}

	return true
}

fn check_json_forward_compatible(old_schema string, new_schema string) bool {
	// JSON Schema forward compatibility:
	// - Old schema can read new data
	// - All new required properties must exist in old schema
	// - Old required properties that are removed must have defaults

	old_info := parse_json_schema_info(old_schema)
	new_info := parse_json_schema_info(new_schema)

	// Type compatibility check
	if old_info.schema_type.len > 0 && new_info.schema_type.len > 0 {
		if !is_json_type_forward_compatible(old_info.schema_type, new_info.schema_type) {
			return false
		}
	}

	// All new required properties must exist in old schema
	for prop in new_info.required {
		if _ := old_info.properties[prop] {
			// Property exists - OK
		} else if !old_info.additional_properties {
			// New property and old schema doesn't allow additional
			return false
		}
	}

	// Old required properties that are removed from new schema
	for prop in old_info.required {
		// Check if property is completely removed (not just made optional)
		if _ := new_info.properties[prop] {
			// Property still exists - OK
		} else {
			// Property removed - old reader needs default
			if old_prop := old_info.properties[prop] {
				if !old_prop.has_default && !old_prop.nullable {
					return false // Old reader expects this property
				}
			}
		}
	}

	return true
}

fn parse_json_schema_info(schema_str string) JsonSchemaInfo {
	mut info := JsonSchemaInfo{
		properties: map[string]JsonPropertyInfo{}
	}

	// Extract type
	info.schema_type = extract_json_string(schema_str, 'type') or { '' }

	// Extract required
	info.required = parse_json_string_array(schema_str, 'required') or { []string{} }

	// Extract additionalProperties
	if schema_str.contains('"additionalProperties":false')
		|| schema_str.contains('"additionalProperties": false') {
		info.additional_properties = false
	}

	// Extract properties
	if props_start := schema_str.index('"properties"') {
		props_json := extract_json_object_value(schema_str, props_start + 12)
		if props_json.len > 0 {
			info.properties = parse_json_properties_info(props_json)
		}
	}

	// Extract numeric constraints
	if min_val := extract_json_float(schema_str, 'minimum') {
		info.minimum = min_val
		info.has_minimum = true
	}
	if max_val := extract_json_float(schema_str, 'maximum') {
		info.maximum = max_val
		info.has_maximum = true
	}

	// Extract string/array constraints
	info.min_length = extract_json_number(schema_str, 'minLength') or { 0 }
	info.max_length = extract_json_number(schema_str, 'maxLength') or { 0 }
	info.min_items = extract_json_number(schema_str, 'minItems') or { 0 }
	info.max_items = extract_json_number(schema_str, 'maxItems') or { 0 }

	// Extract enum
	info.enum_values = parse_json_string_array(schema_str, 'enum') or { []string{} }

	// Check for default
	info.has_default = schema_str.contains('"default"')

	return info
}

fn parse_json_properties_info(props_json string) map[string]JsonPropertyInfo {
	mut result := map[string]JsonPropertyInfo{}

	mut pos := 1 // Skip opening brace

	for pos < props_json.len - 1 {
		// Skip whitespace and commas
		for pos < props_json.len && (props_json[pos] == ` `
			|| props_json[pos] == `\t` || props_json[pos] == `\n`
			|| props_json[pos] == `,`) {
			pos += 1
		}

		if pos >= props_json.len - 1 || props_json[pos] != `"` {
			break
		}

		// Read property name
		name_end := props_json.index_after('"', pos + 1) or { break }
		prop_name := props_json[pos + 1..name_end]
		pos = name_end + 1

		// Skip colon and whitespace
		for pos < props_json.len && (props_json[pos] == `:` || props_json[pos] == ` `
			|| props_json[pos] == `\t` || props_json[pos] == `\n`) {
			pos += 1
		}

		// Read property schema
		if pos < props_json.len && props_json[pos] == `{` {
			prop_schema := extract_json_object_value(props_json, pos)
			if prop_schema.len > 0 {
				mut prop_info := JsonPropertyInfo{}
				prop_info.prop_type = extract_json_string(prop_schema, 'type') or { '' }
				prop_info.has_default = prop_schema.contains('"default"')
				prop_info.nullable = prop_info.prop_type == 'null' || prop_schema.contains('"null"')
				prop_info.enum_values = parse_json_string_array(prop_schema, 'enum') or {
					[]string{}
				}

				result[prop_name] = prop_info
				pos += prop_schema.len
			} else {
				pos += 1
			}
		}
	}

	return result
}

fn is_json_type_backward_compatible(old_type string, new_type string) bool {
	// Same type is always compatible
	if old_type == new_type {
		return true
	}

	// Type widening (new type accepts more) - OK for backward
	// integer -> number (number accepts integers)
	if old_type == 'integer' && new_type == 'number' {
		return true
	}

	return false
}

fn is_json_type_forward_compatible(old_type string, new_type string) bool {
	// Same type is always compatible
	if old_type == new_type {
		return true
	}

	// Type narrowing (new type is subset) - OK for forward
	// number -> integer (if new data is always integers)
	if old_type == 'number' && new_type == 'integer' {
		return true
	}

	return false
}

fn is_json_property_backward_compatible(old_prop JsonPropertyInfo, new_prop JsonPropertyInfo) bool {
	// If types are specified, check compatibility
	if old_prop.prop_type.len > 0 && new_prop.prop_type.len > 0 {
		if !is_json_type_backward_compatible(old_prop.prop_type, new_prop.prop_type) {
			return false
		}
	}

	// If old property has enum, new property must be superset or same
	if old_prop.enum_values.len > 0 && new_prop.enum_values.len > 0 {
		for old_val in old_prop.enum_values {
			if old_val !in new_prop.enum_values {
				return false // Old enum value not in new enum
			}
		}
	}

	return true
}

fn extract_json_required_properties(schema_str string) []string {
	// Extract "required": ["prop1", "prop2"] array
	return parse_json_string_array(schema_str, 'required') or { []string{} }
}

// ============================================================================
// Protobuf Schema Compatibility Checking (Enhanced)
// ============================================================================

// ProtoFieldInfo represents parsed protobuf field information
struct ProtoFieldInfo {
mut:
	name        string
	field_type  string
	number      int
	is_repeated bool
	is_optional bool
	is_required bool // proto2 only
}

// ProtoSchemaInfo represents parsed protobuf schema information
struct ProtoSchemaInfo {
mut:
	syntax          string // "proto2" or "proto3"
	message_name    string
	fields          map[int]ProtoFieldInfo // keyed by field number
	reserved_nums   []int
	reserved_names  []string
	nested_messages []string
	enums           []string
}

fn check_protobuf_backward_compatible(old_schema string, new_schema string) bool {
	// Protobuf backward compatibility rules:
	// - Can add new fields (must use new field numbers)
	// - Can remove optional fields (proto3 all fields are optional by default)
	// - Cannot change field numbers for existing fields
	// - Cannot change field types (except compatible promotions)
	// - Reserved field numbers/names should not be reused

	old_info := parse_protobuf_schema_info(old_schema)
	new_info := parse_protobuf_schema_info(new_schema)

	// Check that no old field numbers are reused with different types
	for num, old_field in old_info.fields {
		if new_field := new_info.fields[num] {
			// Field number still exists - check type compatibility
			if !is_protobuf_type_backward_compatible(old_field.field_type, new_field.field_type) {
				return false
			}

			// Repeated modifier must match
			if old_field.is_repeated != new_field.is_repeated {
				return false
			}

			// proto2: required -> optional is OK, optional -> required is NOT OK
			if old_info.syntax == 'proto2' && !old_field.is_required && new_field.is_required {
				return false
			}
		}
		// Field removed - OK for backward compatibility (new reader ignores unknown)
	}

	// Check that new fields don't reuse reserved numbers
	for num, _ in new_info.fields {
		if num in old_info.reserved_nums {
			return false
		}
	}

	// Check that new field names don't reuse reserved names
	for _, new_field in new_info.fields {
		if new_field.name in old_info.reserved_names {
			return false
		}
	}

	return true
}

fn check_protobuf_forward_compatible(old_schema string, new_schema string) bool {
	// Protobuf forward compatibility rules:
	// - Old readers can read new data (ignore unknown fields)
	// - Can add optional fields
	// - Cannot remove required fields (proto2)
	// - Cannot change field types incompatibly

	old_info := parse_protobuf_schema_info(old_schema)
	new_info := parse_protobuf_schema_info(new_schema)

	// proto2: Required fields cannot be removed
	if old_info.syntax == 'proto2' {
		for num, old_field in old_info.fields {
			if old_field.is_required {
				if _ := new_info.fields[num] {
					// Field still exists - OK
				} else {
					// Required field removed - NOT forward compatible
					return false
				}
			}
		}
	}

	// Check type compatibility for shared fields
	for num, new_field in new_info.fields {
		if old_field := old_info.fields[num] {
			if !is_protobuf_type_forward_compatible(old_field.field_type, new_field.field_type) {
				return false
			}

			// Repeated modifier must match
			if old_field.is_repeated != new_field.is_repeated {
				return false
			}
		}
		// New field - OK, old reader will ignore
	}

	// Check that removed field numbers are properly reserved
	for num, _ in old_info.fields {
		if _ := new_info.fields[num] {
			// Field still exists
		} else {
			// Field removed - should be reserved (warning, not error)
			// For strict compatibility, uncomment:
			// if num !in new_info.reserved_nums {
			//     return false
			// }
		}
	}

	return true
}

fn parse_protobuf_schema_info(schema_str string) ProtoSchemaInfo {
	mut info := ProtoSchemaInfo{
		syntax: 'proto3' // default
		fields: map[int]ProtoFieldInfo{}
	}

	// Extract syntax
	if schema_str.contains('syntax = "proto2"') || schema_str.contains("syntax = 'proto2'") {
		info.syntax = 'proto2'
	}

	// Extract message name
	if msg_idx := schema_str.index('message ') {
		mut pos := msg_idx + 8
		for pos < schema_str.len && (schema_str[pos] == ` ` || schema_str[pos] == `\t`) {
			pos += 1
		}
		mut end := pos
		for end < schema_str.len && schema_str[end] != ` ` && schema_str[end] != `{`
			&& schema_str[end] != `\n` {
			end += 1
		}
		info.message_name = schema_str[pos..end].trim_space()
	}

	// Extract message body content
	mut body := schema_str
	if brace_start := schema_str.index('{') {
		if brace_end := schema_str.last_index('}') {
			body = schema_str[brace_start + 1..brace_end]
		}
	}

	// Normalize: replace newlines with semicolons for uniform parsing
	// Then split by semicolons to get individual statements
	normalized := body.replace('\n', ';').replace(';;', ';')
	statements := normalized.split(';')

	for stmt in statements {
		trimmed := stmt.trim_space()

		if trimmed.len == 0 {
			continue
		}

		// Parse reserved statement
		if trimmed.starts_with('reserved ') {
			reserved_part := trimmed[9..].trim_space()

			// Check if it's numbers or names
			if reserved_part.contains('"') {
				// Reserved names
				parts := reserved_part.split(',')
				for part in parts {
					p := part.trim_space().trim('"')
					if p.len > 0 {
						info.reserved_names << p
					}
				}
			} else {
				// Reserved numbers
				parts := reserved_part.split(',')
				for part in parts {
					p := part.trim_space()
					if p.contains(' to ') {
						range_parts := p.split(' to ')
						if range_parts.len == 2 {
							start := range_parts[0].trim_space().int()
							mut end_num := 0
							if range_parts[1].trim_space() == 'max' {
								end_num = 536870911 // Protobuf max field number
							} else {
								end_num = range_parts[1].trim_space().int()
							}
							for n := start; n <= end_num && n < start + 1000; n++ {
								info.reserved_nums << n
							}
						}
					} else if p.len > 0 && p[0] >= `0` && p[0] <= `9` {
						info.reserved_nums << p.int()
					}
				}
			}
			continue
		}

		// Parse field definition
		if trimmed.contains('=') && !trimmed.starts_with('option') && !trimmed.starts_with('//')
			&& !trimmed.starts_with('syntax') && !trimmed.starts_with('package')
			&& !trimmed.starts_with('message') && !trimmed.starts_with('enum') {
			if field := parse_protobuf_field_info(trimmed, info.syntax) {
				info.fields[field.number] = field
			}
		}
	}

	return info
}

fn parse_protobuf_field_info(line string, syntax string) ?ProtoFieldInfo {
	mut field := ProtoFieldInfo{}

	// Remove trailing semicolon and options [...]
	mut clean := line.trim_right(';')
	if bracket_idx := clean.index('[') {
		clean = clean[..bracket_idx]
	}
	clean = clean.trim_space()

	// Split by '='
	parts := clean.split('=')
	if parts.len != 2 {
		return none
	}

	// Parse field number
	field.number = parts[1].trim_space().int()
	if field.number <= 0 {
		return none
	}

	// Parse type and name
	type_name := parts[0].trim_space()
	tokens := type_name.split(' ').filter(fn (s string) bool {
		return s.len > 0
	})

	if tokens.len < 2 {
		return none
	}

	// Check for modifiers
	mut type_idx := 0

	for type_idx < tokens.len {
		match tokens[type_idx] {
			'repeated' {
				field.is_repeated = true
				type_idx += 1
			}
			'optional' {
				field.is_optional = true
				type_idx += 1
			}
			'required' {
				field.is_required = true
				type_idx += 1
			}
			else {
				break
			}
		}
	}

	if type_idx + 1 >= tokens.len {
		return none
	}

	field.field_type = tokens[type_idx]
	field.name = tokens[type_idx + 1]

	// In proto3, all singular fields are implicitly optional
	if syntax == 'proto3' && !field.is_repeated {
		field.is_optional = true
	}

	return field
}

fn is_protobuf_type_backward_compatible(old_type string, new_type string) bool {
	// Same type is always compatible
	if old_type == new_type {
		return true
	}

	// Compatible type changes:
	// int32, uint32, int64, uint64, bool are compatible (wire type 0)
	// sint32, sint64 are compatible with each other (zigzag encoding)
	// fixed32, sfixed32 are compatible (wire type 5)
	// fixed64, sfixed64 are compatible (wire type 1)
	// string, bytes are compatible (wire type 2)

	varint_types := ['int32', 'uint32', 'int64', 'uint64', 'bool']
	if old_type in varint_types && new_type in varint_types {
		return true
	}

	zigzag_types := ['sint32', 'sint64']
	if old_type in zigzag_types && new_type in zigzag_types {
		return true
	}

	fixed32_types := ['fixed32', 'sfixed32', 'float']
	if old_type in fixed32_types && new_type in fixed32_types {
		return true
	}

	fixed64_types := ['fixed64', 'sfixed64', 'double']
	if old_type in fixed64_types && new_type in fixed64_types {
		return true
	}

	length_delimited := ['string', 'bytes']
	if old_type in length_delimited && new_type in length_delimited {
		return true
	}

	return false
}

fn is_protobuf_type_forward_compatible(old_type string, new_type string) bool {
	// Forward compatibility has the same rules as backward for protobuf
	return is_protobuf_type_backward_compatible(old_type, new_type)
}

fn extract_protobuf_fields(schema_str string) map[int]string {
	// Extract field definitions from protobuf schema
	// Format: type name = number;
	mut fields := map[int]string{}

	lines := schema_str.split('\n')
	for line in lines {
		trimmed := line.trim_space()
		if trimmed.contains('=') && trimmed.ends_with(';') {
			// Parse: optional string name = 1;
			parts := trimmed.split('=')
			if parts.len >= 2 {
				// Extract field number
				num_part := parts[1].trim_space().trim_right(';')
				field_num := num_part.int()

				// Extract field name (last word before =)
				name_parts := parts[0].trim_space().split(' ')
				if name_parts.len >= 2 {
					field_name := name_parts[name_parts.len - 1]
					fields[field_num] = field_name
				}
			}
		}
	}

	return fields
}

// get_stats returns registry statistics
pub fn (mut r SchemaRegistry) get_stats() RegistryStats {
	r.lock.rlock()
	defer { r.lock.runlock() }

	return RegistryStats{
		total_schemas:  r.schemas.len
		total_subjects: r.subjects.len
		next_id:        r.next_id
	}
}

// RegistryStats holds registry statistics
pub struct RegistryStats {
pub:
	total_schemas  int
	total_subjects int
	next_id        int
}
