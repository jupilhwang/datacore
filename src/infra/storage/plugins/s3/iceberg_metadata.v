// Metadata JSON encoding, snapshot management, and file operations for Iceberg tables.
module s3

import json
import time

/// encode_metadata_json encodes table metadata as JSON using standard json.encode.
fn (w &IcebergWriter) encode_metadata_json() string {
	return json.encode(w.table_metadata)
}

/// write_metadata_file writes the table metadata file to S3.
fn (mut w IcebergWriter) write_metadata_file() !string {
	// Increment metadata version
	version := w.table_metadata.snapshots.len
	metadata_path := 'metadata/${version:05d}-${w.table_metadata.table_uuid}.metadata.json'

	// Encode metadata JSON
	metadata_json := w.encode_metadata_json()

	// Save metadata file to S3
	full_path := '${w.table_metadata.location}/${metadata_path}'
	w.adapter.put_object(full_path, metadata_json.bytes())!

	return metadata_path
}

/// create_snapshot creates a new snapshot with proper Iceberg v2 manifest-list structure.
/// Step 1: Write manifest file (data file entries).
/// Step 2: Write manifest-list file (references the manifest).
/// Step 3: Register snapshot pointing to the manifest-list.
fn (mut w IcebergWriter) create_snapshot(data_files []IcebergDataFile, topic string) !IcebergSnapshot {
	snapshot_id := generate_snapshot_id()
	now := time.now()

	// Step 1: Write manifest file
	manifest_path := w.generate_manifest_path(snapshot_id)
	manifest_content := w.encode_manifest(data_files)!
	w.adapter.put_object('${w.table_metadata.location}/${manifest_path}', manifest_content)!

	// Count added files and rows for summary
	mut added_files := 0
	mut added_records := i64(0)
	mut total_size := i64(0)
	for file in data_files {
		added_files++
		added_records += file.record_count
		total_size += file.file_size_in_bytes
	}

	// Step 2: Build manifest metadata and write manifest-list file
	manifest := IcebergManifest{
		manifest_path:   '${w.table_metadata.location}/${manifest_path}'
		manifest_length: i64(manifest_content.len)
		snapshot_id:     snapshot_id
		added_files:     added_files
		added_rows:      added_records
	}
	manifest_list_path := w.generate_manifest_list_path(snapshot_id)
	manifest_list_content := w.encode_manifest_list(manifest)!
	w.adapter.put_object('${w.table_metadata.location}/${manifest_list_path}', manifest_list_content)!

	summary := {
		'operation':               'append'
		'added-data-files':        added_files.str()
		'added-records':           added_records.str()
		'added-files-size':        total_size.str()
		'changed-partition-count': added_files.str()
		'topic':                   topic
	}

	// Step 3: Create snapshot pointing to the manifest-list
	snapshot := IcebergSnapshot{
		snapshot_id:   snapshot_id
		timestamp_ms:  now.unix_milli()
		manifest_list: manifest_list_path
		schema_id:     w.table_metadata.current_schema_id
		summary:       summary
	}

	w.table_metadata.snapshots << snapshot
	w.table_metadata.current_snapshot_id = snapshot_id
	w.table_metadata.last_updated_ms = now.unix_milli()

	return snapshot
}

/// generate_manifest_path generates a manifest file path.
fn (w &IcebergWriter) generate_manifest_path(snapshot_id i64) string {
	now := time.now()
	return 'metadata/snap-${snapshot_id}-${now.format_ss().replace(' ', '-').replace(':',
		'-')}.avro'
}

/// generate_manifest_list_path generates a manifest-list file path.
fn (w &IcebergWriter) generate_manifest_list_path(snapshot_id i64) string {
	now := time.now()
	return 'metadata/snap-${snapshot_id}-${now.format_ss().replace(' ', '-').replace(':',
		'-')}-manifest-list.avro'
}

/// time_travel time-travels to a specific snapshot.
fn (mut w IcebergWriter) time_travel(snapshot_id i64) bool {
	for snapshot in w.table_metadata.snapshots {
		if snapshot.snapshot_id == snapshot_id {
			w.table_metadata.current_snapshot_id = snapshot_id
			return true
		}
	}
	return false
}

/// list_snapshots returns all snapshots.
fn (w &IcebergWriter) list_snapshots() []IcebergSnapshot {
	return w.table_metadata.snapshots.clone()
}
