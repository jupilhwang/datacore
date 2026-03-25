// Manages schema registration, versioning, and compatibility checks.
// Provides an API compatible with Confluent Schema Registry.
module schema

import domain
import service.port
import sync
import time
import crypto.md5
import common

/// SchemaRegistry provides schema management functionality.
/// Responsible for schema registration, retrieval, versioning, and compatibility checks.
pub struct SchemaRegistry {
mut:
	// In-memory cache
	schemas         map[int]domain.Schema
	subjects        map[string][]domain.SchemaVersion
	subject_configs map[string]domain.SubjectConfig
	// Version lookup cache (v0.28.0 optimization)
	version_map map[string]map[int]int

	// Global state
	next_id       int
	global_config domain.SubjectConfig

	// Thread safety
	lock sync.RwMutex

	// Storage for persistence
	storage port.StoragePort

	default_compat domain.CompatibilityLevel

	// Boot recovery state
	recovered bool
}

/// RegistryConfig holds the registry configuration.
pub struct RegistryConfig {
pub:
	default_compatibility domain.CompatibilityLevel = .backward
	auto_register         bool                      = true
	normalize_schemas     bool                      = true
}

/// RegistryStats holds registry statistics.
pub struct RegistryStats {
pub:
	total_schemas  int
	total_subjects int
	next_id        int
}

// Internal topic for schema storage
/// schemas_topic constant.
pub const schemas_topic = '__schemas'

/// new_registry creates a new schema registry.
pub fn new_registry(storage port.StoragePort, config RegistryConfig) &SchemaRegistry {
	return &SchemaRegistry{
		schemas:         map[int]domain.Schema{}
		subjects:        map[string][]domain.SchemaVersion{}
		subject_configs: map[string]domain.SubjectConfig{}
		version_map:     map[string]map[int]int{}
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

/// load_from_storage recovers schema registry state from the __schemas topic.
/// Should be called at broker startup before accepting requests.
pub fn (mut r SchemaRegistry) load_from_storage() ! {
	r.lock.@lock()
	defer { r.lock.unlock() }

	if r.recovered {
		return
	}

	// Try reading from the __schemas topic
	r.storage.get_topic(schemas_topic) or {
		// Topic does not exist yet - first boot
		r.recovered = true
		return
	}

	// Fetch all records from the __schemas topic
	fetch_result := r.storage.fetch(schemas_topic, 0, 0, 100 * 1024 * 1024) or {
		// Empty or error - start fresh
		r.recovered = true
		return
	}

	mut max_id := 0

	// Process each record to reconstruct state
	for record in fetch_result.records {
		record_str := record.value.bytestr()

		// Parse stored schema record
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

		// Reconstruct schema cache
		if schema_id !in r.schemas {
			r.schemas[schema_id] = domain.Schema{
				id:          schema_id
				schema_type: schema_type
				schema_str:  schema_str
				fingerprint: compute_fingerprint(normalize_schema(schema_str))
			}
		}

		// Reconstruct subject versions
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

	// Set next_id to after the max recovered ID
	r.next_id = max_id + 1
	r.recovered = true
}

/// get_global_config returns the global compatibility configuration.
pub fn (mut r SchemaRegistry) get_global_config() domain.SubjectConfig {
	r.lock.rlock()
	defer { r.lock.runlock() }
	return r.global_config
}

/// set_global_config updates the global compatibility configuration.
pub fn (mut r SchemaRegistry) set_global_config(config domain.SubjectConfig) {
	r.lock.@lock()
	defer { r.lock.unlock() }
	r.global_config = config
	r.default_compat = config.compatibility
}

/// register registers a new schema under a subject.
/// Returns: schema ID (existing ID if duplicate, new ID if new)
pub fn (mut r SchemaRegistry) register(subject string, schema_str string, schema_type domain.SchemaType) !int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	// Validate schema
	validate_schema(schema_type, schema_str)!

	// Normalize and compute fingerprint
	normalized := normalize_schema(schema_str)
	fingerprint := compute_fingerprint(normalized)

	// Check if an identical schema already exists for this subject
	if existing_id := r.find_schema_by_fingerprint(subject, fingerprint) {
		return existing_id
	}

	// Get compatibility level
	config := r.subject_configs[subject] or { r.global_config }

	// Check compatibility against existing versions
	r.check_compatibility_internal(subject, schema_str, schema_type, config.compatibility)!

	// Create new schema
	schema_id := r.next_id
	r.next_id += 1

	schema := domain.Schema{
		id:          schema_id
		schema_type: schema_type
		schema_str:  schema_str
		fingerprint: fingerprint
	}

	// Determine version number
	versions := r.subjects[subject] or { []domain.SchemaVersion{} }
	version_num := versions.len + 1

	version := domain.SchemaVersion{
		version:       version_num
		schema_id:     schema_id
		subject:       subject
		compatibility: config.compatibility
		created_at:    time.now()
	}

	// Store schema and version
	r.schemas[schema_id] = schema

	if subject in r.subjects {
		r.subjects[subject] << version
	} else {
		r.subjects[subject] = [version]
	}

	// Update version_map for O(1) lookup (v0.28.0 optimization)
	if subject !in r.version_map {
		r.version_map[subject] = map[int]int{}
	}
	r.version_map[subject][version_num] = versions.len

	// Persist to storage (unlocked operation)
	r.persist_schema(subject, schema, version) or {
		// Log error but do not fail - in-memory state is updated
	}

	return schema_id
}

/// get_schema retrieves a schema by ID.
pub fn (mut r SchemaRegistry) get_schema(schema_id int) !domain.Schema {
	r.lock.rlock()
	defer { r.lock.runlock() }

	return r.schemas[schema_id] or { return error('schema not found: ${schema_id}') }
}

/// get_schema_by_subject retrieves a schema by subject and version.
/// Use version -1 to get the latest version.
pub fn (mut r SchemaRegistry) get_schema_by_subject(subject string, version int) !domain.Schema {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	// version -1 means latest
	if version == -1 {
		if versions.len == 0 {
			return error('no versions for subject: ${subject}')
		}
		latest := versions[versions.len - 1]
		return r.schemas[latest.schema_id] or { return error('schema not found') }
	}

	// O(1) lookup using version_map (v0.28.0 optimization)
	if vm := r.version_map[subject] {
		if idx := vm[version] {
			if idx < versions.len {
				return r.schemas[versions[idx].schema_id] or { return error('schema not found') }
			}
		}
	}

	// Linear search fallback for backwards compatibility
	for v in versions {
		if v.version == version {
			return r.schemas[v.schema_id] or { return error('schema not found') }
		}
	}

	return error('version ${version} not found for subject ${subject}')
}

/// get_latest_version retrieves the latest schema version for a subject.
fn (mut r SchemaRegistry) get_latest_version(subject string) !domain.SchemaVersion {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }
	if versions.len == 0 {
		return error('no versions for subject: ${subject}')
	}

	return versions[versions.len - 1]
}

/// list_subjects returns all registered subjects.
pub fn (mut r SchemaRegistry) list_subjects() []string {
	r.lock.rlock()
	defer { r.lock.runlock() }

	mut result := []string{}
	for subject, _ in r.subjects {
		result << subject
	}
	return result
}

/// list_versions returns all versions for a subject.
pub fn (mut r SchemaRegistry) list_versions(subject string) ![]int {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	mut result := []int{}
	for v in versions {
		result << v.version
	}
	return result
}

/// delete_subject deletes a subject and all its versions.
pub fn (mut r SchemaRegistry) delete_subject(subject string) ![]int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	mut deleted := []int{}
	for v in versions {
		deleted << v.version
	}

	r.subjects.delete(subject)
	r.subject_configs.delete(subject)
	r.version_map.delete(subject)

	return deleted
}

/// delete_version deletes a specific version of a subject.
pub fn (mut r SchemaRegistry) delete_version(subject string, version int) !int {
	r.lock.@lock()
	defer { r.lock.unlock() }

	mut versions := r.subjects[subject] or { return error('subject not found: ${subject}') }

	for i, v in versions {
		if v.version == version {
			versions.delete(i)
			r.subjects[subject] = versions

			// Rebuild version_map for this subject (v0.28.0)
			if subject in r.version_map {
				r.version_map[subject].delete(version)
				// Update indices for versions after the deleted one
				for j := i; j < versions.len; j++ {
					r.version_map[subject][versions[j].version] = j
				}
			}

			return version
		}
	}

	return error('version ${version} not found for subject ${subject}')
}

/// get_compatibility returns the compatibility level for a subject.
pub fn (mut r SchemaRegistry) get_compatibility(subject string) domain.CompatibilityLevel {
	r.lock.rlock()
	defer { r.lock.runlock() }

	config := r.subject_configs[subject] or { return r.default_compat }
	return config.compatibility
}

/// set_compatibility sets the compatibility level for a subject.
pub fn (mut r SchemaRegistry) set_compatibility(subject string, level domain.CompatibilityLevel) {
	r.lock.@lock()
	defer { r.lock.unlock() }

	existing := r.subject_configs[subject] or { domain.SubjectConfig{} }
	// Create new config with updated compatibility (SubjectConfig fields are immutable)
	r.subject_configs[subject] = domain.SubjectConfig{
		compatibility: level
		alias:         existing.alias
		normalize:     existing.normalize
	}
}

/// test_compatibility tests whether a schema is compatible with a subject.
pub fn (mut r SchemaRegistry) test_compatibility(subject string, schema_str string, schema_type domain.SchemaType) !bool {
	r.lock.rlock()
	defer { r.lock.runlock() }

	versions := r.subjects[subject] or { return true }
	if versions.len == 0 {
		return true
	}

	config := r.subject_configs[subject] or {
		domain.SubjectConfig{
			compatibility: r.default_compat
		}
	}

	// Use check_compatibility_internal without re-acquiring lock
	return r.check_compatibility_internal(subject, schema_str, schema_type, config.compatibility)
}

/// get_stats returns registry statistics.
fn (mut r SchemaRegistry) get_stats() RegistryStats {
	r.lock.rlock()
	defer { r.lock.runlock() }

	return RegistryStats{
		total_schemas:  r.schemas.len
		total_subjects: r.subjects.len
		next_id:        r.next_id
	}
}

// Private helper methods

/// find_schema_by_fingerprint finds a schema by its fingerprint.
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

/// check_compatibility_internal performs an internal compatibility check.
fn (r &SchemaRegistry) check_compatibility_internal(subject string, schema_str string, schema_type domain.SchemaType, level domain.CompatibilityLevel) !bool {
	if level == .none {
		return true
	}

	versions := r.subjects[subject] or { return true }
	if versions.len == 0 {
		return true
	}

	// Get schemas to check based on compatibility level
	schemas_to_check := match level {
		.backward, .forward, .full {
			// Check latest version only
			[versions[versions.len - 1]]
		}
		.backward_transitive, .forward_transitive, .full_transitive {
			// Check all versions
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
				// New schema can read data written with the old schema
				check_backward_compatible(existing.schema_str, schema_str, schema_type)
			}
			.forward, .forward_transitive {
				// Old schema can read data written with the new schema
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

/// persist_schema saves a schema to the __schemas topic.
fn (mut r SchemaRegistry) persist_schema(subject string, schema domain.Schema, version domain.SchemaVersion) ! {
	// Create record to store in the __schemas topic
	// Format: JSON with schema details
	record_data := '{"subject":"${subject}","version":${version.version},"id":${schema.id},"schemaType":"${schema.schema_type.str()}","schema":"${common.escape_json_string(schema.schema_str)}"}'

	record := domain.Record{
		key:       subject.bytes()
		value:     record_data.bytes()
		timestamp: time.now()
	}

	// Ensure __schemas topic exists (create if missing)
	r.storage.get_topic(schemas_topic) or {
		// Create internal topic with a single partition
		r.storage.create_topic(schemas_topic, 1, domain.TopicConfig{
			retention_ms:   -1
			cleanup_policy: 'compact'
		}) or {
			// May have already been created by another thread
		}
	}

	// Append to the __schemas topic
	r.storage.append(schemas_topic, 0, [record], i16(1)) or {
		return error('failed to persist schema: ${err}')
	}
}

// Utility functions

/// normalize_schema strips whitespace for consistent fingerprint generation.
fn normalize_schema(schema_str string) string {
	return schema_str.replace(' ', '').replace('\n', '').replace('\t', '').replace('\r',
		'')
}

/// compute_fingerprint calculates the MD5 hash of a schema string.
fn compute_fingerprint(schema_str string) string {
	hash := md5.sum(schema_str.bytes())
	return hash.hex()
}
