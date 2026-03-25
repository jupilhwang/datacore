// GlueCatalog implements the IcebergCatalog interface using AWS Glue Data Catalog.
// Authenticates with AWS SigV4 and communicates via the Glue HTTP API.
module s3

import time
import strings
import net.http
import crypto.hmac
import crypto.sha256
import encoding.base64

/// GlueCatalog is an AWS Glue Data Catalog implementation.
/// Connects to AWS Glue via HTTP API with SigV4 authentication.
pub struct GlueCatalog {
pub mut:
	region        string
	warehouse     string
	adapter       &S3StorageAdapter
	access_key    string
	secret_key    string
	session_token string
	glue_endpoint string
}

/// new_glue_catalog creates a new Glue catalog.
pub fn new_glue_catalog(adapter &S3StorageAdapter, region string, warehouse string) &GlueCatalog {
	endpoint := 'https://glue.${region}.amazonaws.com'
	return &GlueCatalog{
		region:        region
		warehouse:     warehouse
		adapter:       adapter
		access_key:    ''
		secret_key:    ''
		session_token: ''
		glue_endpoint: endpoint
	}
}

/// create_table creates a new table in Glue.
pub fn (c &GlueCatalog) create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }
	table_uuid := generate_table_uuid(location)
	now := time.now()

	metadata := IcebergMetadata{
		format_version:      2
		table_uuid:          table_uuid
		location:            location
		last_updated_ms:     now.unix_milli()
		schemas:             [schema]
		current_schema_id:   0
		partition_specs:     [spec]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'created_by': 'DataCore GlueCatalog'
		}
	}

	// Encode metadata as Iceberg-compatible JSON
	metadata_json := encode_metadata(metadata)

	// Glue CreateTable request body
	request_body := c.build_create_table_request(db_name, identifier.name, location, metadata_json,
		schema, spec)

	c.call_glue_api('CreateTable', request_body) or {
		return error('Glue CreateTable failed for ${db_name}.${identifier.name}: ${err}')
	}

	return metadata
}

/// load_table loads a table from Glue.
pub fn (c &GlueCatalog) load_table(identifier IcebergTableIdentifier) !IcebergMetadata {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }

	request_body := '{"DatabaseName":"${db_name}","Name":"${identifier.name}"}'
	response := c.call_glue_api('GetTable', request_body) or {
		return error('Glue GetTable failed for ${db_name}.${identifier.name}: ${err}')
	}

	return c.parse_glue_table_response(response, identifier)
}

/// update_table updates a Glue table.
pub fn (c &GlueCatalog) update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }

	metadata_json := encode_metadata(metadata)

	request_body := c.build_update_table_request(db_name, identifier.name, metadata.location,
		metadata_json)

	c.call_glue_api('UpdateTable', request_body) or {
		return error('Glue UpdateTable failed for ${db_name}.${identifier.name}: ${err}')
	}
}

/// drop_table drops a Glue table.
pub fn (c &GlueCatalog) drop_table(identifier IcebergTableIdentifier) ! {
	db_name := if identifier.namespace.len > 0 { identifier.namespace[0] } else { 'default' }

	request_body := '{"DatabaseName":"${db_name}","Name":"${identifier.name}"}'
	c.call_glue_api('DeleteTable', request_body) or {
		return error('Glue DeleteTable failed for ${db_name}.${identifier.name}: ${err}')
	}
}

/// list_tables retrieves the list of tables from Glue.
pub fn (c &GlueCatalog) list_tables(namespace []string) ![]IcebergTableIdentifier {
	db_name := if namespace.len > 0 { namespace[0] } else { 'default' }

	request_body := '{"DatabaseName":"${db_name}"}'
	response := c.call_glue_api('GetTables', request_body) or {
		return error('Glue GetTables failed for ${db_name}: ${err}')
	}

	return c.parse_glue_tables_response(response, namespace)
}

/// namespace_exists checks whether the Glue database (namespace) exists.
pub fn (c &GlueCatalog) namespace_exists(namespace []string) bool {
	if namespace.len == 0 {
		return true
	}
	db_name := namespace[0]
	request_body := '{"Name":"${db_name}"}'
	c.call_glue_api('GetDatabase', request_body) or { return false }
	return true
}

/// create_namespace creates a new Glue database (namespace).
pub fn (c &GlueCatalog) create_namespace(namespace []string) ! {
	if namespace.len == 0 {
		return
	}
	db_name := namespace[0]
	request_body := '{"DatabaseInput":{"Name":"${db_name}","Description":"Created by DataCore GlueCatalog"}}'
	c.call_glue_api('CreateDatabase', request_body) or {
		return error('Glue CreateDatabase failed for ${db_name}: ${err}')
	}
}

/// load_metadata_at loads Iceberg metadata from a specific metadata file path via S3 adapter.
pub fn (mut c GlueCatalog) load_metadata_at(metadata_location string) !IcebergMetadata {
	data, _ := c.adapter.get_object(metadata_location, -1, -1) or {
		return error('Failed to read metadata at ${metadata_location}: ${err}')
	}
	mut tmp_catalog := new_hadoop_catalog(c.adapter, c.warehouse)
	return tmp_catalog.decode_metadata(data.bytestr())
}

// --- GlueCatalog <-> HadoopCatalog synchronization ---

/// export_table loads a single table from HadoopCatalog and upserts it into GlueCatalog.
/// Returns the metadata that was written to Glue.
fn (c &GlueCatalog) export_table(identifier IcebergTableIdentifier, mut hadoop_catalog HadoopCatalog) !IcebergMetadata {
	metadata := hadoop_catalog.load_table(identifier)!

	// Prefer metadata.location; fall back to computed warehouse path.
	location := if metadata.location.len > 0 {
		metadata.location
	} else {
		'${c.warehouse}/${identifier.namespace.join('/')}/${identifier.name}'
	}

	// Determine current schema and partition spec for create_table signature.
	schema := if metadata.schemas.len > 0 { metadata.schemas[0] } else { create_default_schema() }
	spec := if metadata.partition_specs.len > 0 {
		metadata.partition_specs[0]
	} else {
		create_default_partition_spec()
	}

	// Upsert: try create first, fall back to update when the table already exists in Glue.
	c.create_table(identifier, schema, spec, location) or {
		// Table already exists in Glue — sync metadata via update_table instead.
		c.update_table(identifier, metadata) or {
			return error('export_table failed: create_table failed, update_table also failed for ${identifier.name}: ${err}')
		}
		return metadata
	}
	return metadata
}

/// export_all exports every table in HadoopCatalog (under the given namespaces) to GlueCatalog.
/// Callers pass the list of namespaces to enumerate; use [[]] for the root namespace.
fn (c &GlueCatalog) export_all(mut hadoop_catalog HadoopCatalog, namespaces [][]string) ![]IcebergMetadata {
	mut results := []IcebergMetadata{}

	for ns in namespaces {
		tables := hadoop_catalog.list_tables(ns)!
		for tbl in tables {
			meta := c.export_table(tbl, mut hadoop_catalog)!
			results << meta
		}
	}

	return results
}

/// import_table loads a single table from GlueCatalog and commits its metadata to HadoopCatalog.
fn (c &GlueCatalog) import_table(identifier IcebergTableIdentifier, mut hadoop_catalog HadoopCatalog) ! {
	metadata := c.load_table(identifier)!
	hadoop_catalog.commit_metadata(identifier, metadata)!
}

/// import_all imports every Iceberg table listed in GlueCatalog into HadoopCatalog.
/// Callers pass the namespaces to enumerate; use [[]] for the root / "default" namespace.
fn (c &GlueCatalog) import_all(mut hadoop_catalog HadoopCatalog, namespaces [][]string) ![]IcebergTableIdentifier {
	mut imported := []IcebergTableIdentifier{}

	for ns in namespaces {
		tables := c.list_tables(ns)!
		for tbl in tables {
			c.import_table(tbl, mut hadoop_catalog)!
			imported << tbl
		}
	}

	return imported
}

// --- Glue HTTP API helpers ---

/// call_glue_api performs an authenticated HTTP POST to the Glue API.
fn (c &GlueCatalog) call_glue_api(action string, body string) !string {
	url := c.glue_endpoint
	now := time.now()
	// AWS SigV4 date formats: YYYYMMDDTHHmmssZ and YYYYMMDD
	amz_date := now.custom_format('YYYYMMDDTHHmmss') + 'Z'
	date_stamp := now.custom_format('YYYYMMDD')

	body_hash := sha256_hex(body.bytes())

	host := c.glue_endpoint.all_after('https://')

	mut req_headers := {
		'Content-Type':         'application/x-amz-json-1.1'
		'X-Amz-Target':         'AWSGlue.${action}'
		'X-Amz-Date':           amz_date
		'X-Amz-Content-Sha256': body_hash
		'Host':                 host
	}

	if c.session_token.len > 0 {
		req_headers['X-Amz-Security-Token'] = c.session_token
	}

	// Build authorization header if credentials are present
	if c.access_key.len > 0 && c.secret_key.len > 0 {
		auth := c.sigv4_authorization('POST', '/', '', req_headers, body, date_stamp,
			amz_date)
		req_headers['Authorization'] = auth
	}

	mut header := http.Header{}
	for key, value in req_headers {
		header.add_custom(key, value) or {}
	}

	config := http.FetchConfig{
		url:    url
		method: .post
		data:   body
		header: header
	}

	resp := http.fetch(config) or { return error('HTTP request failed: ${err}') }

	if resp.status_code >= 400 {
		return error('Glue API error ${resp.status_code}: ${resp.body}')
	}

	return resp.body
}

/// sigv4_authorization generates an AWS SigV4 Authorization header.
fn (c &GlueCatalog) sigv4_authorization(method string, uri string, query string, headers map[string]string, payload string, date_stamp string, amz_date string) string {
	service := 'glue'
	algorithm := 'AWS4-HMAC-SHA256'

	// Canonical headers (sorted)
	mut sorted_keys := headers.keys()
	sorted_keys.sort()
	mut canonical_headers := ''
	mut signed_headers_list := []string{}
	for key in sorted_keys {
		lk := key.to_lower()
		canonical_headers += '${lk}:${headers[key].trim_space()}\n'
		signed_headers_list << lk
	}
	signed_headers := signed_headers_list.join(';')

	payload_hash := sha256_hex(payload.bytes())
	canonical_request := '${method}\n${uri}\n${query}\n${canonical_headers}\n${signed_headers}\n${payload_hash}'

	credential_scope := '${date_stamp}/${c.region}/${service}/aws4_request'
	string_to_sign := '${algorithm}\n${amz_date}\n${credential_scope}\n${sha256_hex(canonical_request.bytes())}'

	signing_key := c.sigv4_signing_key(date_stamp, service)
	signature := hmac.new(signing_key, string_to_sign.bytes(), sha256.sum, sha256.block_size).hex()

	return '${algorithm} Credential=${c.access_key}/${credential_scope}, SignedHeaders=${signed_headers}, Signature=${signature}'
}

/// sigv4_signing_key derives the SigV4 signing key.
fn (c &GlueCatalog) sigv4_signing_key(date_stamp string, service string) []u8 {
	k_date := hmac.new(('AWS4' + c.secret_key).bytes(), date_stamp.bytes(), sha256.sum,
		sha256.block_size)
	k_region := hmac.new(k_date, c.region.bytes(), sha256.sum, sha256.block_size)
	k_service := hmac.new(k_region, service.bytes(), sha256.sum, sha256.block_size)
	k_signing := hmac.new(k_service, 'aws4_request'.bytes(), sha256.sum, sha256.block_size)
	return k_signing
}

/// build_create_table_request builds the Glue CreateTable JSON request body.
fn (c &GlueCatalog) build_create_table_request(db_name string, table_name string, location string, metadata_json string, schema IcebergSchema, spec IcebergPartitionSpec) string {
	// Encode metadata_json as base64 for Glue parameter storage
	metadata_b64 := base64.encode(metadata_json.bytes())

	mut col_defs := strings.new_builder(512)
	col_defs.write_string('[')
	for i, field in schema.fields {
		if i > 0 {
			col_defs.write_string(',')
		}
		glue_type := iceberg_type_to_glue(field.typ)
		col_defs.write_string('{"Name":"${field.name}","Type":"${glue_type}","Comment":"iceberg-field-id=${field.id}"}')
	}
	col_defs.write_string(']')

	return '{"DatabaseName":"${db_name}","TableInput":{"Name":"${table_name}",' +
		'"StorageDescriptor":{"Columns":${col_defs.str()},' + '"Location":"${location}",' +
		'"InputFormat":"org.apache.iceberg.mr.mapred.IcebergInputFormat",' +
		'"OutputFormat":"org.apache.iceberg.mr.mapred.IcebergOutputFormat",' +
		'"SerdeInfo":{"SerializationLibrary":"org.apache.iceberg.mr.hive.HiveIcebergSerDe"}},' +
		'"Parameters":{"table_type":"ICEBERG","metadata_location":"${location}/metadata/v1.metadata.json",' +
		'"iceberg_metadata":"${metadata_b64}"}}}'
}

/// build_update_table_request builds the Glue UpdateTable JSON request body.
fn (c &GlueCatalog) build_update_table_request(db_name string, table_name string, location string, metadata_json string) string {
	metadata_b64 := base64.encode(metadata_json.bytes())
	return '{"DatabaseName":"${db_name}","TableInput":{"Name":"${table_name}",' +
		'"Parameters":{"table_type":"ICEBERG",' +
		'"metadata_location":"${location}/metadata/latest.metadata.json",' +
		'"iceberg_metadata":"${metadata_b64}"}}}'
}

/// parse_glue_table_response parses the Glue GetTable response into IcebergMetadata.
fn (c &GlueCatalog) parse_glue_table_response(response string, identifier IcebergTableIdentifier) !IcebergMetadata {
	// Extract iceberg_metadata parameter (base64-encoded metadata JSON)
	if params_str := json_extract_object(response, 'Parameters') {
		if metadata_b64 := json_extract_string(params_str, 'iceberg_metadata') {
			metadata_json_bytes := base64.decode(metadata_b64)
			metadata_json := metadata_json_bytes.bytestr()
			mut catalog := new_hadoop_catalog(c.adapter, c.warehouse)
			return catalog.decode_metadata(metadata_json)
		}
	}

	// Fallback: reconstruct from Glue table structure
	location_str := if loc := json_extract_string(response, 'Location') {
		loc
	} else {
		'${c.warehouse}/${identifier.name}'
	}

	return IcebergMetadata{
		format_version:      2
		table_uuid:          generate_table_uuid(location_str)
		location:            location_str
		last_updated_ms:     time.now().unix_milli()
		schemas:             [create_default_schema()]
		current_schema_id:   0
		partition_specs:     [create_default_partition_spec()]
		default_spec_id:     0
		snapshots:           []
		current_snapshot_id: 0
		properties:          {
			'table_type': 'ICEBERG'
		}
	}
}

/// parse_glue_tables_response parses the Glue GetTables response into table identifiers.
fn (c &GlueCatalog) parse_glue_tables_response(response string, namespace []string) []IcebergTableIdentifier {
	mut identifiers := []IcebergTableIdentifier{}

	table_list_str := json_extract_array(response, 'TableList') or { return identifiers }
	items := json_split_array_items(table_list_str)

	for item in items {
		name := json_extract_string(item, 'Name') or { continue }
		// Only return Iceberg tables
		if params_str := json_extract_object(item, 'Parameters') {
			if table_type := json_extract_string(params_str, 'table_type') {
				if table_type == 'ICEBERG' {
					identifiers << IcebergTableIdentifier{
						namespace: namespace
						name:      name
					}
				}
			}
		}
	}
	return identifiers
}

/// sha256_hex computes SHA-256 and returns the lowercase hex string.
fn sha256_hex(data []u8) string {
	return sha256.sum(data).hex()
}

/// iceberg_type_to_glue converts an Iceberg type to a Glue/Hive column type.
fn iceberg_type_to_glue(iceberg_type string) string {
	return match iceberg_type {
		'int' { 'int' }
		'long' { 'bigint' }
		'float' { 'float' }
		'double' { 'double' }
		'boolean' { 'boolean' }
		'string' { 'string' }
		'binary' { 'binary' }
		'timestamp', 'timestamptz' { 'timestamp' }
		'date' { 'date' }
		'decimal' { 'decimal(38,10)' }
		else { 'string' }
	}
}
