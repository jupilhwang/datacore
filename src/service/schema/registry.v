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
    schemas         map[int]domain.Schema           // schema_id -> Schema
    subjects        map[string][]domain.SchemaVersion  // subject -> versions
    subject_configs map[string]domain.SubjectConfig   // subject -> config
    
    // Global state
    next_id         int
    
    // Thread safety
    lock            sync.RwMutex
    
    // Storage for persistence
    storage         port.StoragePort
    
    // Configuration
    default_compat  domain.CompatibilityLevel
}

// RegistryConfig holds registry configuration
pub struct RegistryConfig {
pub:
    default_compatibility domain.CompatibilityLevel = .backward
    auto_register         bool = true
    normalize_schemas     bool = true
}

// Internal topic for schema storage
pub const schemas_topic = '__schemas'

// new_registry creates a new schema registry
pub fn new_registry(storage port.StoragePort, config RegistryConfig) &SchemaRegistry {
    return &SchemaRegistry{
        schemas: map[int]domain.Schema{}
        subjects: map[string][]domain.SchemaVersion{}
        subject_configs: map[string]domain.SubjectConfig{}
        next_id: 1
        storage: storage
        default_compat: config.default_compatibility
    }
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
        config := r.subject_configs[subject] or { domain.SubjectConfig{ compatibility: r.default_compat } }
        r.check_compatibility(subject, schema_str, schema_type, config.compatibility)!
    }
    
    // 5. Allocate new schema ID
    schema_id := r.next_id
    r.next_id += 1
    
    // 6. Create schema and version
    schema := domain.Schema{
        id: schema_id
        schema_type: schema_type
        schema_str: schema_str
        fingerprint: fingerprint
    }
    
    version := domain.SchemaVersion{
        version: versions.len + 1
        schema_id: schema_id
        subject: subject
        compatibility: r.subject_configs[subject] or { domain.SubjectConfig{} }.compatibility
        created_at: time.now()
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
        versions.len  // Latest version
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
    
    versions := r.subjects[subject] or { return true }  // No versions = compatible
    if versions.len == 0 {
        return true
    }
    
    config := r.subject_configs[subject] or { domain.SubjectConfig{ compatibility: r.default_compat } }
    
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
                check_backward_compatible(existing.schema_str, schema_str, schema_type) &&
                check_forward_compatible(existing.schema_str, schema_str, schema_type)
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
        key: subject.bytes()
        value: record_data.bytes()
        timestamp: time.now()
    }
    
    // Ensure __schemas topic exists (create if not)
    r.storage.get_topic(schemas_topic) or {
        // Create internal topic with single partition
        r.storage.create_topic(schemas_topic, 1, domain.TopicConfig{
            retention_ms: -1  // Keep forever
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
    return schema_str.replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '')
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
    if (trimmed.starts_with('{') && trimmed.ends_with('}')) ||
       (trimmed.starts_with('[') && trimmed.ends_with(']')) {
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
    return '"${s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')}"'
}

// Compatibility checking functions (simplified for P0)

fn check_backward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
    // Simplified compatibility check for P0
    // In a full implementation, this would do proper schema analysis
    
    match schema_type {
        .avro {
            // For Avro, new schema can add fields with defaults
            // New schema should have all required fields from old schema
            return true  // Simplified - always compatible for P0
        }
        .json {
            return true
        }
        .protobuf {
            return true
        }
    }
}

fn check_forward_compatible(old_schema string, new_schema string, schema_type domain.SchemaType) bool {
    // Simplified compatibility check for P0
    match schema_type {
        .avro {
            return true
        }
        .json {
            return true
        }
        .protobuf {
            return true
        }
    }
}

// get_stats returns registry statistics
pub fn (mut r SchemaRegistry) get_stats() RegistryStats {
    r.lock.rlock()
    defer { r.lock.runlock() }
    
    return RegistryStats{
        total_schemas: r.schemas.len
        total_subjects: r.subjects.len
        next_id: r.next_id
    }
}

// RegistryStats holds registry statistics
pub struct RegistryStats {
pub:
    total_schemas   int
    total_subjects  int
    next_id         int
}
