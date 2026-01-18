// Interface Layer - Schema Registry REST API
// Kafka Schema Registry compatible REST endpoints
module rest

import domain
import service.schema
import json
import net.http

// SchemaAPI provides REST API handlers for Schema Registry
pub struct SchemaAPI {
mut:
    registry &schema.SchemaRegistry
}

// new_schema_api creates a new Schema REST API handler
pub fn new_schema_api(registry &schema.SchemaRegistry) &SchemaAPI {
    return &SchemaAPI{
        registry: registry
    }
}

// API Request/Response structures

// RegisterSchemaRequest for POST /subjects/{subject}/versions
struct RegisterSchemaRequest {
    schema      string @[json: 'schema']
    schema_type string @[json: 'schemaType']
    references  []ReferenceRequest @[json: 'references']
}

struct ReferenceRequest {
    name    string @[json: 'name']
    subject string @[json: 'subject']
    version int @[json: 'version']
}

// SchemaResponse for GET /schemas/ids/{id}
struct SchemaResponse {
    schema      string @[json: 'schema']
    schema_type string @[json: 'schemaType']
}

// RegisterResponse for POST /subjects/{subject}/versions
struct RegisterResponse {
    id int @[json: 'id']
}

// VersionResponse for GET /subjects/{subject}/versions/{version}
struct VersionResponse {
    subject     string @[json: 'subject']
    id          int @[json: 'id']
    version     int @[json: 'version']
    schema      string @[json: 'schema']
    schema_type string @[json: 'schemaType']
}

// CompatibilityRequest for PUT /config/{subject}
struct CompatibilityRequest {
    compatibility string @[json: 'compatibility']
}

// CompatibilityResponse for GET /config/{subject}
struct CompatibilityResponse {
    compatibility_level string @[json: 'compatibilityLevel']
}

// CompatibilityCheckResponse for POST /compatibility/subjects/{subject}/versions/{version}
struct CompatibilityCheckResponse {
    is_compatible bool @[json: 'is_compatible']
}

// ErrorResponse for error responses
struct ErrorResponse {
    error_code int @[json: 'error_code']
    message    string @[json: 'message']
}

// HTTP endpoint handlers

// handle_request routes requests to appropriate handlers
pub fn (mut api SchemaAPI) handle_request(method string, path string, body string) (int, string) {
    // Parse path components
    parts := path.trim_left('/').split('/')
    
    if parts.len == 0 {
        return api.error_response(404, 40401, 'Not found')
    }
    
    return match parts[0] {
        'subjects' { api.handle_subjects(method, parts[1..], body) }
        'schemas' { api.handle_schemas(method, parts[1..], body) }
        'config' { api.handle_config(method, parts[1..], body) }
        'compatibility' { api.handle_compatibility(method, parts[1..], body) }
        else { api.error_response(404, 40401, 'Not found') }
    }
}

// Subjects endpoints

fn (mut api SchemaAPI) handle_subjects(method string, parts []string, body string) (int, string) {
    // GET /subjects - List all subjects
    if parts.len == 0 {
        if method == 'GET' {
            return api.list_subjects()
        }
        return api.error_response(405, 40501, 'Method not allowed')
    }
    
    subject := parts[0]
    
    // /subjects/{subject}
    if parts.len == 1 {
        return match method {
            'GET' { api.get_subject(subject) }
            'DELETE' { api.delete_subject(subject) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    // /subjects/{subject}/versions
    if parts.len == 2 && parts[1] == 'versions' {
        return match method {
            'GET' { api.list_versions(subject) }
            'POST' { api.register_schema(subject, body) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    // /subjects/{subject}/versions/{version}
    if parts.len == 3 && parts[1] == 'versions' {
        version := parts[2].int()
        return match method {
            'GET' { api.get_schema_by_version(subject, version) }
            'DELETE' { api.delete_version(subject, version) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    // /subjects/{subject}/versions/{version}/schema
    if parts.len == 4 && parts[1] == 'versions' && parts[3] == 'schema' {
        version := parts[2].int()
        return match method {
            'GET' { api.get_raw_schema(subject, version) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    return api.error_response(404, 40401, 'Not found')
}

// Schemas endpoints

fn (mut api SchemaAPI) handle_schemas(method string, parts []string, body string) (int, string) {
    // /schemas/ids/{id}
    if parts.len == 2 && parts[0] == 'ids' {
        schema_id := parts[1].int()
        return match method {
            'GET' { api.get_schema_by_id(schema_id) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    // /schemas/ids/{id}/schema
    if parts.len == 3 && parts[0] == 'ids' && parts[2] == 'schema' {
        schema_id := parts[1].int()
        return match method {
            'GET' { api.get_raw_schema_by_id(schema_id) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    return api.error_response(404, 40401, 'Not found')
}

// Config endpoints

fn (mut api SchemaAPI) handle_config(method string, parts []string, body string) (int, string) {
    // GET/PUT /config - Global config
    if parts.len == 0 {
        return match method {
            'GET' { api.get_global_config() }
            'PUT' { api.set_global_config(body) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    // GET/PUT /config/{subject} - Subject config
    if parts.len == 1 {
        subject := parts[0]
        return match method {
            'GET' { api.get_subject_config(subject) }
            'PUT' { api.set_subject_config(subject, body) }
            else { api.error_response(405, 40501, 'Method not allowed') }
        }
    }
    
    return api.error_response(404, 40401, 'Not found')
}

// Compatibility endpoints

fn (mut api SchemaAPI) handle_compatibility(method string, parts []string, body string) (int, string) {
    // POST /compatibility/subjects/{subject}/versions/{version}
    if parts.len >= 4 && parts[0] == 'subjects' && parts[2] == 'versions' {
        subject := parts[1]
        version := parts[3].int()
        
        if method == 'POST' {
            return api.check_compatibility(subject, version, body)
        }
    }
    
    return api.error_response(404, 40401, 'Not found')
}

// Handler implementations

fn (mut api SchemaAPI) list_subjects() (int, string) {
    subjects := api.registry.list_subjects()
    return 200, json.encode(subjects)
}

fn (mut api SchemaAPI) get_subject(subject string) (int, string) {
    versions := api.registry.list_versions(subject) or {
        return api.error_response(404, 40401, 'Subject not found: ${subject}')
    }
    return 200, json.encode(versions)
}

fn (mut api SchemaAPI) delete_subject(subject string) (int, string) {
    deleted := api.registry.delete_subject(subject) or {
        return api.error_response(404, 40401, 'Subject not found: ${subject}')
    }
    return 200, json.encode(deleted)
}

fn (mut api SchemaAPI) list_versions(subject string) (int, string) {
    versions := api.registry.list_versions(subject) or {
        return api.error_response(404, 40401, 'Subject not found: ${subject}')
    }
    return 200, json.encode(versions)
}

fn (mut api SchemaAPI) register_schema(subject string, body string) (int, string) {
    req := json.decode(RegisterSchemaRequest, body) or {
        return api.error_response(400, 40001, 'Invalid request: ${err}')
    }
    
    schema_type := domain.schema_type_from_str(if req.schema_type.len > 0 { req.schema_type } else { 'AVRO' }) or {
        return api.error_response(400, 40001, 'Invalid schema type')
    }
    
    schema_id := api.registry.register(subject, req.schema, schema_type) or {
        return api.error_response(409, 40901, 'Schema registration failed: ${err}')
    }
    
    resp := RegisterResponse{ id: schema_id }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) get_schema_by_version(subject string, version int) (int, string) {
    schema := api.registry.get_schema_by_subject(subject, version) or {
        return api.error_response(404, 40402, 'Version not found: ${subject}/${version}')
    }
    
    // Get version info
    versions := api.registry.list_versions(subject) or { []int{} }
    actual_version := if version == -1 { versions.len } else { version }
    
    resp := VersionResponse{
        subject: subject
        id: schema.id
        version: actual_version
        schema: schema.schema_str
        schema_type: schema.schema_type.str()
    }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) delete_version(subject string, version int) (int, string) {
    schema_id := api.registry.delete_version(subject, version) or {
        return api.error_response(404, 40402, 'Version not found')
    }
    return 200, json.encode(schema_id)
}

fn (mut api SchemaAPI) get_raw_schema(subject string, version int) (int, string) {
    schema := api.registry.get_schema_by_subject(subject, version) or {
        return api.error_response(404, 40402, 'Version not found')
    }
    return 200, schema.schema_str
}

fn (mut api SchemaAPI) get_schema_by_id(schema_id int) (int, string) {
    schema := api.registry.get_schema(schema_id) or {
        return api.error_response(404, 40403, 'Schema not found')
    }
    
    resp := SchemaResponse{
        schema: schema.schema_str
        schema_type: schema.schema_type.str()
    }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) get_raw_schema_by_id(schema_id int) (int, string) {
    schema := api.registry.get_schema(schema_id) or {
        return api.error_response(404, 40403, 'Schema not found')
    }
    return 200, schema.schema_str
}

fn (mut api SchemaAPI) get_global_config() (int, string) {
    // Return global compatibility setting
    config := api.registry.get_global_config()
    resp := CompatibilityResponse{
        compatibility_level: config.compatibility.str()
    }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) set_global_config(body string) (int, string) {
    req := json.decode(CompatibilityRequest, body) or {
        return api.error_response(400, 40001, 'Invalid request: ${err}')
    }
    
    level := domain.compatibility_from_str(req.compatibility) or {
        return api.error_response(400, 40001, 'Invalid compatibility level: ${req.compatibility}')
    }
    
    // Update global config
    api.registry.set_global_config(domain.SubjectConfig{
        compatibility: level
    })
    
    resp := CompatibilityResponse{
        compatibility_level: level.str()
    }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) get_subject_config(subject string) (int, string) {
    compat := api.registry.get_compatibility(subject)
    resp := CompatibilityResponse{
        compatibility_level: compat.str()
    }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) set_subject_config(subject string, body string) (int, string) {
    req := json.decode(CompatibilityRequest, body) or {
        return api.error_response(400, 40001, 'Invalid request')
    }
    
    level := domain.compatibility_from_str(req.compatibility) or {
        return api.error_response(400, 40001, 'Invalid compatibility level')
    }
    
    api.registry.set_compatibility(subject, level)
    
    resp := CompatibilityResponse{
        compatibility_level: level.str()
    }
    return 200, json.encode(resp)
}

fn (mut api SchemaAPI) check_compatibility(subject string, version int, body string) (int, string) {
    req := json.decode(RegisterSchemaRequest, body) or {
        return api.error_response(400, 40001, 'Invalid request')
    }
    
    schema_type := domain.schema_type_from_str(if req.schema_type.len > 0 { req.schema_type } else { 'AVRO' }) or {
        return api.error_response(400, 40001, 'Invalid schema type')
    }
    
    is_compatible := api.registry.test_compatibility(subject, req.schema, schema_type) or {
        resp := CompatibilityCheckResponse{ is_compatible: false }
        return 200, json.encode(resp)
    }
    
    resp := CompatibilityCheckResponse{ is_compatible: is_compatible }
    return 200, json.encode(resp)
}

fn (api &SchemaAPI) error_response(status int, error_code int, message string) (int, string) {
    resp := ErrorResponse{
        error_code: error_code
        message: message
    }
    return status, json.encode(resp)
}
