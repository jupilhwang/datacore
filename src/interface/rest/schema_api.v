// Interface Layer - Schema Registry REST API
//
// Provides Kafka Schema Registry compatible REST endpoints.
// Supports registration, retrieval, and compatibility checking
// for Avro, JSON Schema, and Protobuf schemas.
//
// Key endpoints:
// - /subjects - subject management
// - /schemas - schema retrieval
// - /config - compatibility configuration
// - /compatibility - compatibility checking
module rest

import domain
import service.port
import json

/// SchemaAPI provides REST API handlers for the Schema Registry.
/// Depends on SchemaRegistryRestPort abstraction (DIP).
pub struct SchemaAPI {
mut:
	registry port.SchemaRegistryRestPort
}

/// new_schema_api creates a new Schema REST API handler.
/// Accepts any implementation satisfying SchemaRegistryRestPort.
pub fn new_schema_api(registry port.SchemaRegistryRestPort) &SchemaAPI {
	return &SchemaAPI{
		registry: registry
	}
}

// API request/response structs

// RegisterSchemaRequest is the request struct for POST /subjects/{subject}/versions.
struct RegisterSchemaRequest {
	schema      string             @[json: 'schema']
	schema_type string             @[json: 'schemaType']
	references  []ReferenceRequest @[json: 'references']
}

// ReferenceRequest is a struct holding schema reference information.
struct ReferenceRequest {
	name    string @[json: 'name']
	subject string @[json: 'subject']
	version int    @[json: 'version']
}

// SchemaResponse is the response struct for GET /schemas/ids/{id}.
struct SchemaResponse {
	schema      string @[json: 'schema']
	schema_type string @[json: 'schemaType']
}

// RegisterResponse is the response struct for POST /subjects/{subject}/versions.
struct RegisterResponse {
	id int @[json: 'id']
}

// VersionResponse is the response struct for GET /subjects/{subject}/versions/{version}.
struct VersionResponse {
	subject     string @[json: 'subject']
	id          int    @[json: 'id']
	version     int    @[json: 'version']
	schema      string @[json: 'schema']
	schema_type string @[json: 'schemaType']
}

// CompatibilityRequest is the request struct for PUT /config/{subject}.
struct CompatibilityRequest {
	compatibility string @[json: 'compatibility']
}

// CompatibilityResponse is the response struct for GET /config/{subject}.
struct CompatibilityResponse {
	compatibility_level string @[json: 'compatibilityLevel']
}

// CompatibilityCheckResponse is the response struct for POST /compatibility/subjects/{subject}/versions/{version}.
struct CompatibilityCheckResponse {
	is_compatible bool @[json: 'is_compatible']
}

// ErrorResponse is the error response struct.
struct ErrorResponse {
	error_code int    @[json: 'error_code']
	message    string @[json: 'message']
}

// HTTP endpoint handlers

/// handle_request routes the request to the appropriate handler.
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

// handle_subjects handles the /subjects endpoint.
fn (mut api SchemaAPI) handle_subjects(method string, parts []string, body string) (int, string) {
	// GET /subjects - list all subjects
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

// handle_schemas handles the /schemas endpoint.
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

// handle_config handles the /config endpoint.
fn (mut api SchemaAPI) handle_config(method string, parts []string, body string) (int, string) {
	// GET/PUT /config - global configuration
	if parts.len == 0 {
		return match method {
			'GET' { api.get_global_config() }
			'PUT' { api.set_global_config(body) }
			else { api.error_response(405, 40501, 'Method not allowed') }
		}
	}

	// GET/PUT /config/{subject} - subject configuration
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

// handle_compatibility handles the /compatibility endpoint.
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

// list_subjects returns a list of all subjects.
fn (mut api SchemaAPI) list_subjects() (int, string) {
	subjects := api.registry.list_subjects()
	return 200, json.encode(subjects)
}

// get_subject returns the version list for a subject.
fn (mut api SchemaAPI) get_subject(subject string) (int, string) {
	versions := api.registry.list_versions(subject) or {
		return api.error_response(404, 40401, 'Subject not found: ${subject}')
	}
	return 200, json.encode(versions)
}

// delete_subject deletes a subject.
fn (mut api SchemaAPI) delete_subject(subject string) (int, string) {
	deleted := api.registry.delete_subject(subject) or {
		return api.error_response(404, 40401, 'Subject not found: ${subject}')
	}
	return 200, json.encode(deleted)
}

// list_versions returns the version list for a subject.
fn (mut api SchemaAPI) list_versions(subject string) (int, string) {
	versions := api.registry.list_versions(subject) or {
		return api.error_response(404, 40401, 'Subject not found: ${subject}')
	}
	return 200, json.encode(versions)
}

// register_schema registers a new schema.
fn (mut api SchemaAPI) register_schema(subject string, body string) (int, string) {
	req := json.decode(RegisterSchemaRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request: ${err}')
	}

	schema_type := domain.schema_type_from_str(if req.schema_type.len > 0 {
		req.schema_type
	} else {
		'AVRO'
	}) or { return api.error_response(400, 40001, 'Invalid schema type') }

	schema_id := api.registry.register(subject, req.schema, schema_type) or {
		return api.error_response(409, 40901, 'Schema registration failed: ${err}')
	}

	resp := RegisterResponse{
		id: schema_id
	}
	return 200, json.encode(resp)
}

// get_schema_by_version retrieves a schema by version.
fn (mut api SchemaAPI) get_schema_by_version(subject string, version int) (int, string) {
	schema_data := api.registry.get_schema_by_subject(subject, version) or {
		return api.error_response(404, 40402, 'Version not found: ${subject}/${version}')
	}

	// Get version information
	versions := api.registry.list_versions(subject) or { []int{} }
	actual_version := if version == -1 { versions.len } else { version }

	resp := VersionResponse{
		subject:     subject
		id:          schema_data.id
		version:     actual_version
		schema:      schema_data.schema_str
		schema_type: schema_data.schema_type.str()
	}
	return 200, json.encode(resp)
}

// delete_version deletes a specific version.
fn (mut api SchemaAPI) delete_version(subject string, version int) (int, string) {
	schema_id := api.registry.delete_version(subject, version) or {
		return api.error_response(404, 40402, 'Version not found')
	}
	return 200, json.encode(schema_id)
}

// get_raw_schema returns the raw schema string.
fn (mut api SchemaAPI) get_raw_schema(subject string, version int) (int, string) {
	schema_data := api.registry.get_schema_by_subject(subject, version) or {
		return api.error_response(404, 40402, 'Version not found')
	}
	return 200, schema_data.schema_str
}

// get_schema_by_id retrieves a schema by ID.
fn (mut api SchemaAPI) get_schema_by_id(schema_id int) (int, string) {
	schema_data := api.registry.get_schema(schema_id) or {
		return api.error_response(404, 40403, 'Schema not found')
	}

	resp := SchemaResponse{
		schema:      schema_data.schema_str
		schema_type: schema_data.schema_type.str()
	}
	return 200, json.encode(resp)
}

// get_raw_schema_by_id returns the raw schema string by ID.
fn (mut api SchemaAPI) get_raw_schema_by_id(schema_id int) (int, string) {
	schema_data := api.registry.get_schema(schema_id) or {
		return api.error_response(404, 40403, 'Schema not found')
	}
	return 200, schema_data.schema_str
}

// get_global_config returns the global compatibility configuration.
fn (mut api SchemaAPI) get_global_config() (int, string) {
	// Return global compatibility configuration
	config := api.registry.get_global_config()
	resp := CompatibilityResponse{
		compatibility_level: config.compatibility.str()
	}
	return 200, json.encode(resp)
}

// set_global_config updates the global compatibility configuration.
fn (mut api SchemaAPI) set_global_config(body string) (int, string) {
	req := json.decode(CompatibilityRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request: ${err}')
	}

	level := domain.compatibility_from_str(req.compatibility) or {
		return api.error_response(400, 40001, 'Invalid compatibility level: ${req.compatibility}')
	}

	// Update global configuration
	api.registry.set_global_config(domain.SubjectConfig{
		compatibility: level
	})

	resp := CompatibilityResponse{
		compatibility_level: level.str()
	}
	return 200, json.encode(resp)
}

// get_subject_config returns the compatibility configuration for a subject.
fn (mut api SchemaAPI) get_subject_config(subject string) (int, string) {
	compat := api.registry.get_compatibility(subject)
	resp := CompatibilityResponse{
		compatibility_level: compat.str()
	}
	return 200, json.encode(resp)
}

// set_subject_config updates the compatibility configuration for a subject.
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

// check_compatibility checks schema compatibility.
fn (mut api SchemaAPI) check_compatibility(subject string, version int, body string) (int, string) {
	_ = version
	req := json.decode(RegisterSchemaRequest, body) or {
		return api.error_response(400, 40001, 'Invalid request')
	}

	schema_type := domain.schema_type_from_str(if req.schema_type.len > 0 {
		req.schema_type
	} else {
		'AVRO'
	}) or { return api.error_response(400, 40001, 'Invalid schema type') }

	is_compatible := api.registry.test_compatibility(subject, req.schema, schema_type) or {
		resp := CompatibilityCheckResponse{
			is_compatible: false
		}
		return 200, json.encode(resp)
	}

	resp := CompatibilityCheckResponse{
		is_compatible: is_compatible
	}
	return 200, json.encode(resp)
}

// error_response creates an error response.
fn (api &SchemaAPI) error_response(status int, error_code int, message string) (int, string) {
	resp := ErrorResponse{
		error_code: error_code
		message:    message
	}
	return status, json.encode(resp)
}
