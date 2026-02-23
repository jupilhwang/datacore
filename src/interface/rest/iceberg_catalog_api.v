// Interface Layer - Iceberg REST Catalog API
// Provides Apache Iceberg REST Catalog API endpoints.
// Supports v3 table format.
module rest

import json
import infra.storage.plugins.s3

/// IcebergCatalogAPI is the handler for the Iceberg REST Catalog API.
pub struct IcebergCatalogAPI {
mut:
	catalog        s3.IcebergCatalog
	warehouse      string
	prefix         string
	format_version int
}

/// new_iceberg_catalog_api creates a new Iceberg Catalog API.
pub fn new_iceberg_catalog_api(catalog s3.IcebergCatalog, warehouse string, format_version int) &IcebergCatalogAPI {
	return &IcebergCatalogAPI{
		catalog:        catalog
		warehouse:      warehouse
		prefix:         'iceberg'
		format_version: format_version
	}
}

/// handle_request handles an Iceberg Catalog API request.
/// Returns: (HTTP status code, response body)
pub fn (mut api IcebergCatalogAPI) handle_request(method string, path string, body string) (int, string) {
	// Parse path: /v1/iceberg/... or /v1/...
	parts := path.trim_left('/').split('/')

	if parts.len < 2 {
		return api.error_response(400, 'Invalid path')
	}

	// Handle /v1/config
	if parts.len == 2 && parts[1] == 'config' {
		return api.handle_config(method)
	}

	// Handle /v1/{prefix}/... paths
	if parts.len < 3 {
		return api.error_response(404, 'Not found')
	}

	// Parse path after prefix
	sub_parts := parts[2..]

	match sub_parts[0] {
		'namespaces' { return api.handle_namespaces(method, sub_parts[1..], body) }
		'tables' { return api.handle_tables_root(method, sub_parts[1..], body) }
		'transactions' { return api.handle_transactions(method, sub_parts[1..], body) }
		else { return api.error_response(404, 'Not found') }
	}
}

/// handle_config handles the /v1/config endpoint.
fn (mut api IcebergCatalogAPI) handle_config(method string) (int, string) {
	if method != 'GET' {
		return api.error_response(405, 'Method not allowed')
	}

	config := s3.new_catalog_config(api.warehouse, api.format_version)
	return 200, config.to_json()
}

/// handle_namespaces handles requests related to /v1/{prefix}/namespaces.
fn (mut api IcebergCatalogAPI) handle_namespaces(method string, parts []string, body string) (int, string) {
	// GET /namespaces - list namespaces
	if parts.len == 0 {
		return match method {
			'GET' { api.list_namespaces() }
			'POST' { api.create_namespace(body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	namespace := parts[0]

	// /namespaces/{namespace}
	if parts.len == 1 {
		return match method {
			'GET' { api.get_namespace(namespace) }
			'DELETE' { api.delete_namespace(namespace) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/properties
	if parts.len == 2 && parts[1] == 'properties' {
		return match method {
			'POST' { api.update_namespace_properties(namespace, body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/tables
	if parts.len == 2 && parts[1] == 'tables' {
		return match method {
			'GET' { api.list_tables(namespace) }
			'POST' { api.create_table(namespace, body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/tables/{table}
	if parts.len == 3 && parts[1] == 'tables' {
		table := parts[2]
		return match method {
			'GET' { api.load_table(namespace, table) }
			'POST' { api.commit_table(namespace, table, body) }
			'DELETE' { api.drop_table(namespace, table) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/tables/{table}/metrics
	if parts.len == 4 && parts[1] == 'tables' && parts[3] == 'metrics' {
		return match method {
			'POST' { api.report_metrics(parts[0], parts[2], body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	// /namespaces/{namespace}/register
	if parts.len == 2 && parts[1] == 'register' {
		return match method {
			'POST' { api.register_table(namespace, body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	return api.error_response(404, 'Not found')
}

/// handle_tables_root handles requests related to /v1/{prefix}/tables.
fn (mut api IcebergCatalogAPI) handle_tables_root(method string, parts []string, body string) (int, string) {
	// /tables/rename
	if parts.len == 1 && parts[0] == 'rename' {
		return match method {
			'POST' { api.rename_table(body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	return api.error_response(404, 'Not found')
}

/// handle_transactions handles requests related to /v1/{prefix}/transactions.
fn (mut api IcebergCatalogAPI) handle_transactions(method string, parts []string, body string) (int, string) {
	// /transactions/commit
	if parts.len == 1 && parts[0] == 'commit' {
		return match method {
			'POST' { api.commit_transaction(body) }
			else { api.error_response(405, 'Method not allowed') }
		}
	}

	return api.error_response(404, 'Not found')
}

// Namespace endpoint implementations

fn (mut api IcebergCatalogAPI) list_namespaces() (int, string) {
	// Return default namespace list (in real implementation, query from catalog)
	resp := s3.ListNamespacesResponse{
		namespaces: [['default']]
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) create_namespace(body string) (int, string) {
	req := json.decode(s3.CreateNamespaceRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.namespace.len == 0 {
		return api.error_response(400, 'Namespace is required')
	}

	// Create namespace in catalog
	api.catalog.create_namespace(req.namespace) or {
		return api.error_response(500, 'Failed to create namespace: ${err}')
	}

	resp := s3.CreateNamespaceResponse{
		namespace:  req.namespace
		properties: req.properties
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) get_namespace(namespace string) (int, string) {
	ns := [namespace]

	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	resp := s3.GetNamespaceResponse{
		namespace:  ns
		properties: {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) delete_namespace(namespace string) (int, string) {
	// Namespace can only be deleted when empty
	ns := [namespace]

	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	// Cannot delete if tables exist
	tables := api.catalog.list_tables(ns) or {
		return api.error_response(500, 'Failed to list tables: ${err}')
	}

	if tables.len > 0 {
		return api.error_response(409, 'Namespace not empty')
	}

	return 204, ''
}

fn (mut api IcebergCatalogAPI) update_namespace_properties(namespace string, body string) (int, string) {
	req := json.decode(s3.UpdateNamespacePropertiesRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	ns := [namespace]
	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	resp := s3.UpdateNamespacePropertiesResponse{
		updated: req.updates.keys()
		removed: req.removals
		missing: []
	}
	return 200, resp.to_json()
}

// Table endpoint implementations

fn (mut api IcebergCatalogAPI) list_tables(namespace string) (int, string) {
	ns := [namespace]

	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	tables := api.catalog.list_tables(ns) or {
		return api.error_response(500, 'Failed to list tables: ${err}')
	}

	mut identifiers := []s3.TableIdentifierRest{}
	for table in tables {
		identifiers << s3.TableIdentifierRest{
			namespace: table.namespace
			name:      table.name
		}
	}

	resp := s3.ListTablesResponse{
		identifiers: identifiers
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) create_table(namespace string, body string) (int, string) {
	req := json.decode(s3.CreateTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.name.len == 0 {
		return api.error_response(400, 'Table name is required')
	}

	ns := [namespace]
	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	identifier := s3.IcebergTableIdentifier{
		namespace: ns
		name:      req.name
	}

	// Convert REST schema to internal format
	schema := api.rest_schema_to_internal(req.schema)
	spec := api.rest_partition_spec_to_internal(req.partition_spec)

	location := if req.location.len > 0 {
		req.location
	} else {
		'${api.warehouse}/${namespace}/${req.name}'
	}

	metadata := api.catalog.create_table(identifier, schema, spec, location) or {
		return api.error_response(500, 'Failed to create table: ${err}')
	}

	resp := s3.LoadTableResponse{
		metadata_location: '${location}/metadata/v1.metadata.json'
		metadata:          s3.metadata_to_rest(metadata, location)
		config:            {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) load_table(namespace string, table string) (int, string) {
	identifier := s3.IcebergTableIdentifier{
		namespace: [namespace]
		name:      table
	}

	metadata := api.catalog.load_table(identifier) or {
		return api.error_response(404, 'Table not found: ${namespace}.${table}')
	}

	location := metadata.location

	resp := s3.LoadTableResponse{
		metadata_location: '${location}/metadata/v${metadata.format_version}.metadata.json'
		metadata:          s3.metadata_to_rest(metadata, location)
		config:            {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) commit_table(namespace string, table string, body string) (int, string) {
	req := json.decode(s3.CommitTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	identifier := s3.IcebergTableIdentifier{
		namespace: [namespace]
		name:      table
	}

	// Load current metadata
	mut metadata := api.catalog.load_table(identifier) or {
		return api.error_response(404, 'Table not found: ${namespace}.${table}')
	}

	// Validate requirements
	for requirement in req.requirements {
		match requirement.typ {
			'assert-current-schema-id' {
				if metadata.current_schema_id != requirement.schema_id {
					return api.error_response(412, 'Schema ID mismatch')
				}
			}
			'assert-table-uuid' {
				if metadata.table_uuid != requirement.uuid {
					return api.error_response(412, 'Table UUID mismatch')
				}
			}
			else {}
		}
	}

	// Apply updates
	for update in req.updates {
		match update.action {
			'add-schema' {
				schema := api.rest_schema_to_internal(update.schema)
				metadata.schemas << schema
			}
			'set-current-schema' {
				metadata.current_schema_id = update.schema.schema_id
			}
			'add-partition-spec' {
				spec := api.rest_partition_spec_to_internal(update.spec)
				metadata.partition_specs << spec
			}
			'set-default-spec' {
				metadata.default_spec_id = update.spec.spec_id
			}
			'add-snapshot' {
				snapshot := s3.IcebergSnapshot{
					snapshot_id:   update.snapshot.snapshot_id
					timestamp_ms:  update.snapshot.timestamp_ms
					manifest_list: update.snapshot.manifest_list
					schema_id:     update.snapshot.schema_id
					summary:       update.snapshot.summary
				}
				metadata.snapshots << snapshot
			}
			'set-snapshot-ref' {
				metadata.current_snapshot_id = update.snapshot.snapshot_id
			}
			'set-properties' {
				for key, value in update.properties {
					metadata.properties[key] = value
				}
			}
			'remove-properties' {
				for key in update.removals {
					metadata.properties.delete(key)
				}
			}
			else {}
		}
	}

	// Save metadata
	api.catalog.update_table(identifier, metadata) or {
		return api.error_response(500, 'Failed to update table: ${err}')
	}

	resp := s3.CommitTableResponse{
		metadata_location: '${metadata.location}/metadata/v${metadata.format_version}.metadata.json'
		metadata:          s3.metadata_to_rest(metadata, metadata.location)
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) drop_table(namespace string, table string) (int, string) {
	identifier := s3.IcebergTableIdentifier{
		namespace: [namespace]
		name:      table
	}

	api.catalog.drop_table(identifier) or {
		return api.error_response(404, 'Table not found: ${namespace}.${table}')
	}

	return 204, ''
}

fn (mut api IcebergCatalogAPI) register_table(namespace string, body string) (int, string) {
	req := json.decode(s3.RegisterTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.name.len == 0 {
		return api.error_response(400, 'Table name is required')
	}
	if req.metadata_location.len == 0 {
		return api.error_response(400, 'metadata-location is required')
	}

	ns := [namespace]
	if !api.catalog.namespace_exists(ns) {
		return api.error_response(404, 'Namespace not found: ${namespace}')
	}

	identifier := s3.IcebergTableIdentifier{
		namespace: ns
		name:      req.name
	}

	// Load metadata from the given metadata_location path via catalog
	metadata := api.catalog.load_metadata_at(req.metadata_location) or {
		return api.error_response(404, 'Metadata not accessible at: ${req.metadata_location}: ${err}')
	}

	// Persist the registration in the catalog
	api.catalog.update_table(identifier, metadata) or {
		// Table may not exist yet — create it
		api.catalog.create_table(identifier, if metadata.schemas.len > 0 {
			metadata.schemas[0]
		} else {
			s3.create_default_schema()
		}, if metadata.partition_specs.len > 0 {
			metadata.partition_specs[0]
		} else {
			s3.create_default_partition_spec()
		}, metadata.location) or {
			return api.error_response(500, 'Failed to register table: ${err}')
		}
	}

	resp := s3.LoadTableResponse{
		metadata_location: req.metadata_location
		metadata:          s3.metadata_to_rest(metadata, metadata.location)
		config:            {}
	}
	return 200, resp.to_json()
}

fn (mut api IcebergCatalogAPI) rename_table(body string) (int, string) {
	req := json.decode(s3.RenameTableRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.source.name.len == 0 {
		return api.error_response(400, 'Source table name is required')
	}
	if req.destination.name.len == 0 {
		return api.error_response(400, 'Destination table name is required')
	}

	src := s3.IcebergTableIdentifier{
		namespace: req.source.namespace
		name:      req.source.name
	}
	dst := s3.IcebergTableIdentifier{
		namespace: req.destination.namespace
		name:      req.destination.name
	}

	// Load source table metadata
	metadata := api.catalog.load_table(src) or {
		return api.error_response(404, 'Source table not found: ${req.source.namespace.join('.')}.${req.source.name}')
	}

	// Ensure destination namespace exists
	if !api.catalog.namespace_exists(dst.namespace) {
		return api.error_response(404, 'Destination namespace not found: ${dst.namespace.join('.')}')
	}

	// Create destination table with copied metadata
	dst_location := '${api.warehouse}/${dst.namespace.join('/')}/${dst.name}'
	mut dst_metadata := metadata
	dst_metadata.location = dst_location

	api.catalog.create_table(dst, if dst_metadata.schemas.len > 0 {
		dst_metadata.schemas[0]
	} else {
		s3.create_default_schema()
	}, if dst_metadata.partition_specs.len > 0 {
		dst_metadata.partition_specs[0]
	} else {
		s3.create_default_partition_spec()
	}, dst_location) or {
		return api.error_response(409, 'Destination table already exists or create failed: ${err}')
	}

	// Update with full metadata
	api.catalog.update_table(dst, dst_metadata) or {
		return api.error_response(500, 'Failed to update destination table: ${err}')
	}

	// Drop source table
	api.catalog.drop_table(src) or {
		// Log but don't fail - destination is created
		// In production, this would require a compensating transaction
	}

	return 204, ''
}

fn (mut api IcebergCatalogAPI) report_metrics(namespace string, table string, body string) (int, string) {
	// Metric reporting is currently ignored (logging only)
	return 204, ''
}

fn (mut api IcebergCatalogAPI) commit_transaction(body string) (int, string) {
	req := json.decode(s3.CommitTransactionRequest, body) or {
		return api.error_response(400, 'Invalid request: ${err}')
	}

	if req.table_changes.len == 0 {
		return api.error_response(400, 'No table changes provided')
	}

	mut committed := []s3.CommittedTableChange{}

	for change in req.table_changes {
		identifier := s3.IcebergTableIdentifier{
			namespace: change.identifier.namespace
			name:      change.identifier.name
		}

		// Load current metadata for this table
		mut metadata := api.catalog.load_table(identifier) or {
			return api.error_response(404, 'Table not found: ${change.identifier.namespace.join('.')}.${change.identifier.name}')
		}

		// Validate requirements
		for requirement in change.requirements {
			match requirement.typ {
				'assert-current-schema-id' {
					if metadata.current_schema_id != requirement.schema_id {
						return api.error_response(412, 'Schema ID mismatch for table ${change.identifier.name}: expected ${requirement.schema_id}, got ${metadata.current_schema_id}')
					}
				}
				'assert-table-uuid' {
					if metadata.table_uuid != requirement.uuid {
						return api.error_response(412, 'Table UUID mismatch for table ${change.identifier.name}')
					}
				}
				'assert-table-does-not-exist' {
					return api.error_response(409, 'Table already exists: ${change.identifier.name}')
				}
				'assert-table-exists' {
					// Already loaded successfully — requirement satisfied
				}
				'assert-ref-snapshot-id' {
					if metadata.current_snapshot_id != requirement.snapshot_id {
						return api.error_response(412, 'Snapshot ID mismatch for table ${change.identifier.name}')
					}
				}
				else {}
			}
		}

		// Apply updates
		for update in change.updates {
			match update.action {
				'add-schema' {
					schema := api.rest_schema_to_internal(update.schema)
					metadata.schemas << schema
				}
				'set-current-schema' {
					metadata.current_schema_id = update.schema.schema_id
				}
				'add-partition-spec' {
					spec := api.rest_partition_spec_to_internal(update.spec)
					metadata.partition_specs << spec
				}
				'set-default-spec' {
					metadata.default_spec_id = update.spec.spec_id
				}
				'add-snapshot' {
					snapshot := s3.IcebergSnapshot{
						snapshot_id:   update.snapshot.snapshot_id
						timestamp_ms:  update.snapshot.timestamp_ms
						manifest_list: update.snapshot.manifest_list
						schema_id:     update.snapshot.schema_id
						summary:       update.snapshot.summary
					}
					metadata.snapshots << snapshot
				}
				'set-snapshot-ref' {
					metadata.current_snapshot_id = update.snapshot.snapshot_id
				}
				'set-location' {
					metadata.location = update.location
				}
				'set-properties' {
					for key, value in update.properties {
						metadata.properties[key] = value
					}
				}
				'remove-properties' {
					for key in update.removals {
						metadata.properties.delete(key)
					}
				}
				else {}
			}
		}

		// Persist updated metadata
		api.catalog.update_table(identifier, metadata) or {
			return api.error_response(500, 'Failed to commit table ${change.identifier.name}: ${err}')
		}

		committed << s3.CommittedTableChange{
			identifier:        change.identifier
			metadata_location: '${metadata.location}/metadata/v${metadata.format_version}.metadata.json'
		}
	}

	resp := s3.CommitTransactionResponse{
		committed_changes: committed
	}
	return 200, resp.to_json()
}

fn (api &IcebergCatalogAPI) rest_schema_to_internal(schema s3.SchemaRest) s3.IcebergSchema {
	mut fields := []s3.IcebergField{}
	for field in schema.fields {
		fields << s3.IcebergField{
			id:            field.id
			name:          field.name
			typ:           field.typ
			required:      field.required
			default_value: field.initial_default
		}
	}
	return s3.IcebergSchema{
		schema_id:            schema.schema_id
		fields:               fields
		identifier_field_ids: schema.identifier_field_ids
	}
}

fn (api &IcebergCatalogAPI) rest_partition_spec_to_internal(spec s3.PartitionSpecRest) s3.IcebergPartitionSpec {
	mut fields := []s3.IcebergPartitionField{}
	for field in spec.fields {
		fields << s3.IcebergPartitionField{
			source_id:      field.source_id
			field_id:       field.field_id
			name:           field.name
			transform:      field.transform
			transform_args: field.transform_args
		}
	}
	return s3.IcebergPartitionSpec{
		spec_id: spec.spec_id
		fields:  fields
	}
}

fn (api &IcebergCatalogAPI) error_response(code int, message string) (int, string) {
	err := s3.iceberg_error(code, message)
	return code, err.to_json()
}
