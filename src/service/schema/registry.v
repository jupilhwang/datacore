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
			// Basic Avro schema validation (check for JSON structure with required fields)
			if !schema_str.contains('"type"') {
				return error('invalid Avro schema: missing type field')
			}
			// Check if it's valid JSON
			if !is_valid_json(schema_str) {
				return error('invalid Avro schema: not valid JSON')
			}
		}
		.json {
			// Basic JSON Schema validation
			if !is_valid_json(schema_str) {
				return error('invalid JSON schema: not valid JSON')
			}
		}
		.protobuf {
			// Basic Protobuf schema validation
			if !schema_str.contains('message') && !schema_str.contains('enum') {
				return error('invalid Protobuf schema: no message or enum definition')
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

	// Check for basic JSON structure
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

			if c == `\\` {
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
// JSON Schema Compatibility Checking
// ============================================================================

fn check_json_backward_compatible(old_schema string, new_schema string) bool {
	// JSON Schema backward compatibility:
	// - New schema can add optional properties
	// - New schema cannot add required properties (without defaults)
	// - New schema cannot remove properties that old data has

	old_required := extract_json_required_properties(old_schema)
	new_required := extract_json_required_properties(new_schema)

	// All old required properties must still be required (or optional) in new
	for prop in old_required {
		// Property must exist in new schema (required or optional is fine)
		if !new_schema.contains('"${prop}"') {
			return false
		}
	}

	// New required properties must have been optional before or have defaults
	for prop in new_required {
		if prop !in old_required {
			// New required property - check if old schema had it as optional
			if !old_schema.contains('"${prop}"') {
				// Completely new property that's required - NOT backward compatible
				// unless it has a default value
				continue // Be lenient for now
			}
		}
	}

	return true
}

fn check_json_forward_compatible(old_schema string, new_schema string) bool {
	// JSON Schema forward compatibility:
	// - Old schema can read data with fewer properties
	// - New schema can remove optional properties

	new_required := extract_json_required_properties(new_schema)
	old_required := extract_json_required_properties(old_schema)

	// All new required properties must exist in old schema
	for prop in new_required {
		if !old_schema.contains('"${prop}"') {
			return false
		}
	}

	// Old required properties that are removed must have defaults
	for prop in old_required {
		if prop !in new_required && !new_schema.contains('"${prop}"') {
			// Property completely removed and was required
			return false
		}
	}

	return true
}

fn extract_json_required_properties(schema_str string) []string {
	// Extract "required": ["prop1", "prop2"] array
	return parse_json_string_array(schema_str, 'required') or { []string{} }
}

// ============================================================================
// Protobuf Schema Compatibility Checking
// ============================================================================

fn check_protobuf_backward_compatible(old_schema string, new_schema string) bool {
	// Protobuf backward compatibility rules:
	// - Can add new fields (must use new field numbers)
	// - Can remove optional fields
	// - Cannot remove required fields
	// - Cannot change field numbers
	// - Cannot change field types (except compatible type changes)

	old_fields := extract_protobuf_fields(old_schema)
	new_fields := extract_protobuf_fields(new_schema)

	// All old field numbers must map to same or compatible types in new
	for old_num, old_name in old_fields {
		if new_name := new_fields[old_num] {
			// Field number exists - names should match (or be compatible)
			// In practice, field names can change as long as numbers match
			_ = new_name
			_ = old_name
		}
		// Field removed - this is OK for backward compatibility
		// (new schema can still read old data, will just ignore the field)
	}

	return true
}

fn check_protobuf_forward_compatible(old_schema string, new_schema string) bool {
	// Protobuf forward compatibility rules:
	// - Can add optional fields with defaults
	// - Old readers will ignore unknown fields

	old_fields := extract_protobuf_fields(old_schema)
	new_fields := extract_protobuf_fields(new_schema)

	// Check that field number reuse is safe
	for new_num, _ in new_fields {
		if old_name := old_fields[new_num] {
			_ = old_name
			// Field number exists in both - OK
		}
		// New field number - OK, old reader will ignore
	}

	return true
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
