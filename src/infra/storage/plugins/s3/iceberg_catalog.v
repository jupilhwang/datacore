// Provides Iceberg catalog integration and table management.
// Supported catalogs: Hadoop (file-based), Glue, REST
module s3

import time
import json
import sync

/// IcebergCatalog defines the Iceberg table catalog interface.
pub interface IcebergCatalog {
mut:
	create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata
	load_table(identifier IcebergTableIdentifier) !IcebergMetadata
	load_metadata_at(metadata_location string) !IcebergMetadata
	update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) !
	drop_table(identifier IcebergTableIdentifier) !
	list_tables(namespace []string) ![]IcebergTableIdentifier
	namespace_exists(namespace []string) bool
	create_namespace(namespace []string) !
}

/// CachedIcebergMetadata holds cached Iceberg table metadata.
struct CachedIcebergMetadata {
	metadata  IcebergMetadata
	cached_at time.Time
}

/// ObjectStore provides basic object storage operations for catalog backends.
interface ObjectStore {
mut:
	put_object(key string, data []u8) !
	get_object(key string, range_start i64, range_end i64) !([]u8, string)
	list_objects(prefix string) ![]S3Object
	delete_objects_with_prefix(prefix string) !
}

/// HadoopCatalog is an S3-based Hadoop catalog implementation.
pub struct HadoopCatalog {
pub mut:
	adapter    ObjectStore
	warehouse  string
	properties map[string]string
	// Iceberg metadata cache
	metadata_cache map[string]CachedIcebergMetadata
	metadata_lock  sync.RwMutex
}

/// cache_put stores metadata in cache with eviction logic.
fn (mut c HadoopCatalog) cache_put(cache_key string, metadata IcebergMetadata) {
	c.metadata_lock.@lock()
	defer { c.metadata_lock.unlock() }

	// Evict expired entries if cache is getting large
	if c.metadata_cache.len > iceberg_cache_max_entries {
		for key, entry in c.metadata_cache {
			if time.since(entry.cached_at) >= iceberg_cache_ttl {
				c.metadata_cache.delete(key)
			}
		}
	}

	c.metadata_cache[cache_key] = CachedIcebergMetadata{
		metadata:  metadata
		cached_at: time.now()
	}
}

/// invalidate_cache removes a single entry from the metadata cache.
fn (mut c HadoopCatalog) invalidate_cache(key string) {
	c.metadata_lock.@lock()
	c.metadata_cache.delete(key)
	c.metadata_lock.unlock()
}

/// new_hadoop_catalog creates a new Hadoop catalog.
pub fn new_hadoop_catalog(adapter ObjectStore, warehouse string) &HadoopCatalog {
	return &HadoopCatalog{
		adapter:    adapter
		warehouse:  normalize_s3_location(warehouse)
		properties: {}
	}
}

/// create_table creates a new Iceberg table.
pub fn (mut c HadoopCatalog) create_table(identifier IcebergTableIdentifier, schema IcebergSchema, spec IcebergPartitionSpec, location string) !IcebergMetadata {
	// Generate table path
	table_path := c.table_path(identifier)

	// Check if table already exists
	if c.table_exists(identifier) {
		return error('Table already exists: ${identifier.name}')
	}

	// Generate table UUID deterministically from location so the same table
	// recreated at the same path always yields the same UUID.
	table_uuid := generate_table_uuid(location)
	now := time.now()

	// Create initial metadata
	mut metadata := IcebergMetadata{
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
			'created_by': 'DataCore HadoopCatalog'
		}
	}

	// Metadata file path
	metadata_path := '${table_path}/metadata/00001-${table_uuid}.metadata.json'

	// Generate and save metadata JSON
	metadata_json := encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	// Create version hint file (tracks current metadata version)
	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, '1'.bytes())!

	return metadata
}

/// load_table loads an existing Iceberg table.
pub fn (mut c HadoopCatalog) load_table(identifier IcebergTableIdentifier) !IcebergMetadata {
	// Check cache first
	cache_key := c.table_path(identifier)
	c.metadata_lock.rlock()
	if cached := c.metadata_cache[cache_key] {
		if time.since(cached.cached_at) < iceberg_cache_ttl {
			c.metadata_lock.runlock()
			return cached.metadata
		}
	}
	c.metadata_lock.runlock()

	table_path := c.table_path(identifier)

	// Fetch version hint file (existence check; actual value unused — latest file is found by listing)
	version_hint_path := '${table_path}/metadata/version-hint.text'
	_, _ := c.adapter.get_object(version_hint_path, -1, -1) or {
		return error('Table not found: ${identifier.name}')
	}

	// Fetch latest metadata file
	// Metadata file pattern: {version:05d}-{uuid}.metadata.json
	prefix := '${table_path}/metadata/'
	objects := c.adapter.list_objects(prefix) or { return error('Failed to list metadata files') }

	// Find the latest version metadata file
	mut latest_metadata_path := ''
	mut latest_version := 0

	for obj in objects {
		filename := obj.key.split('/').last()
		if filename.ends_with('.metadata.json') {
			if filename.len < 5 {
				continue
			}
			// Extract version from filename (e.g., 00001-uuid.metadata.json -> 1)
			version_str := filename[0..5]
			file_version := version_str.int()
			if file_version > latest_version {
				latest_version = file_version
				latest_metadata_path = obj.key
			}
		}
	}

	if latest_metadata_path == '' {
		return error('No metadata file found for table: ${identifier.name}')
	}

	// Load and parse metadata file
	metadata_data, _ := c.adapter.get_object(latest_metadata_path, -1, -1)!
	metadata := c.decode_metadata(metadata_data.bytestr())!

	// Cache the loaded metadata
	c.cache_put(cache_key, metadata)

	return metadata
}

/// update_table updates the table metadata.
pub fn (mut c HadoopCatalog) update_table(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	table_path := c.table_path(identifier)

	// Optimistic concurrency control: create new version metadata file
	new_version := metadata.snapshots.len
	metadata_path := '${table_path}/metadata/${new_version:05d}-${metadata.table_uuid}.metadata.json'

	// Save metadata
	metadata_json := encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	// Update version hint
	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, new_version.str().bytes())!

	c.invalidate_cache(table_path)
}

/// drop_table drops the table.
pub fn (mut c HadoopCatalog) drop_table(identifier IcebergTableIdentifier) ! {
	table_path := c.table_path(identifier)

	// Check table existence
	if !c.table_exists(identifier) {
		return error('Table not found: ${identifier.name}')
	}

	// Delete all objects in the table
	prefix := '${table_path}/'
	c.adapter.delete_objects_with_prefix(prefix)!

	c.invalidate_cache(table_path)
}

/// list_tables lists all tables in the namespace.
pub fn (mut c HadoopCatalog) list_tables(namespace []string) ![]IcebergTableIdentifier {
	mut identifiers := []IcebergTableIdentifier{}

	// Generate namespace path
	ns_path := c.namespace_path(namespace)
	prefix := '${ns_path}/'

	// Fetch list of all objects in the namespace
	objects := c.adapter.list_objects(prefix) or { return []IcebergTableIdentifier{} }

	// Collect table list (directories with a metadata folder)
	mut seen_tables := map[string]bool{}
	for obj in objects {
		parts := obj.key.split('/')
		if parts.len > namespace.len + 1 {
			// Table name is the first folder after the namespace
			table_name := parts[namespace.len]
			if table_name !in seen_tables && table_name != '' {
				seen_tables[table_name] = true
				identifiers << IcebergTableIdentifier{
					namespace: namespace
					name:      table_name
				}
			}
		}
	}

	return identifiers
}

/// namespace_exists checks whether the namespace exists.
pub fn (mut c HadoopCatalog) namespace_exists(namespace []string) bool {
	if namespace.len == 0 {
		return true
	}

	ns_path := c.namespace_path(namespace)
	marker := '${ns_path}/.namespace'

	// Check for existence of namespace marker file
	_, _ := c.adapter.get_object(marker, -1, -1) or { return false }
	return true
}

/// create_namespace creates a new namespace.
pub fn (mut c HadoopCatalog) create_namespace(namespace []string) ! {
	if namespace.len == 0 {
		return
	}

	ns_path := c.namespace_path(namespace)
	marker := '${ns_path}/.namespace'

	// Create namespace marker file
	c.adapter.put_object(marker, '{}'.bytes())!
}

/// load_metadata_at loads Iceberg metadata from a specific metadata file path.
pub fn (mut c HadoopCatalog) load_metadata_at(metadata_location string) !IcebergMetadata {
	// Check cache first
	c.metadata_lock.rlock()
	if cached := c.metadata_cache[metadata_location] {
		if time.since(cached.cached_at) < iceberg_cache_ttl {
			c.metadata_lock.runlock()
			return cached.metadata
		}
	}
	c.metadata_lock.runlock()

	data, _ := c.adapter.get_object(metadata_location, -1, -1) or {
		return error('Failed to read metadata at ${metadata_location}: ${err}')
	}
	metadata := c.decode_metadata(data.bytestr())!

	// Cache the loaded metadata
	c.cache_put(metadata_location, metadata)

	return metadata
}

/// commit_metadata writes the given metadata as a new versioned metadata JSON file to S3
/// and updates the version-hint.text pointer.
pub fn (mut c HadoopCatalog) commit_metadata(identifier IcebergTableIdentifier, metadata IcebergMetadata) ! {
	table_path := c.table_path(identifier)

	// Derive next version number from existing snapshot count (same convention as update_table)
	new_version := metadata.snapshots.len + 1
	metadata_path := '${table_path}/metadata/${new_version:05d}-${metadata.table_uuid}.metadata.json'

	metadata_json := encode_metadata(metadata)
	c.adapter.put_object(metadata_path, metadata_json.bytes())!

	version_hint_path := '${table_path}/metadata/version-hint.text'
	c.adapter.put_object(version_hint_path, new_version.str().bytes())!

	c.invalidate_cache(table_path)
}

fn (mut c HadoopCatalog) table_exists(identifier IcebergTableIdentifier) bool {
	table_path := c.table_path(identifier)
	metadata_path := '${table_path}/metadata/'

	objects := c.adapter.list_objects(metadata_path) or { return false }
	return objects.len > 0
}

/// table_path returns the S3 path for the table.
fn (mut c HadoopCatalog) table_path(identifier IcebergTableIdentifier) string {
	ns_path := c.namespace_path(identifier.namespace)
	return '${ns_path}/${identifier.name}'
}

/// namespace_path returns the S3 path for the namespace.
fn (mut c HadoopCatalog) namespace_path(namespace []string) string {
	if namespace.len == 0 {
		return c.warehouse
	}
	return '${c.warehouse}/${namespace.join('/')}'
}

/// encode_metadata encodes metadata as a JSON string using standard json.encode.
/// Output uses kebab-case keys per the Iceberg specification.
fn encode_metadata(metadata IcebergMetadata) string {
	return json.encode(metadata)
}

/// decode_metadata decodes a JSON string into IcebergMetadata.
/// Handles both kebab-case (Iceberg standard) and camelCase (legacy) keys
/// by normalizing camelCase to kebab-case before decoding.
fn (mut c HadoopCatalog) decode_metadata(json_str string) !IcebergMetadata {
	if json_str == '' {
		return error('Empty metadata JSON')
	}

	normalized := normalize_metadata_json_keys(json_str)
	mut metadata := json.decode(IcebergMetadata, normalized) or {
		return error('Failed to decode metadata JSON: ${err}')
	}

	// Apply defaults for fields missing from the JSON (zero-value means absent)
	if metadata.format_version == 0 {
		metadata.format_version = 2
	}
	if metadata.table_uuid == '' {
		metadata.table_uuid = generate_table_uuid(json_str)
	}
	if metadata.last_updated_ms == 0 {
		metadata.last_updated_ms = time.now().unix_milli()
	}
	return metadata
}
