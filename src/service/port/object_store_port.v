// Object storage operation interfaces in the port (use-case) layer.
// Follows ISP: read operations and write operations are separated into
// ObjectStoreReaderPort and ObjectStoreWriterPort. The composite
// ObjectStorePort combines both for consumers that need full access.
// Follows DIP: high-level modules (service, iceberg catalog, transaction
// store) depend on these abstractions, not on concrete S3 implementations.
module port

import time

/// ObjectInfo holds metadata for a single stored object.
/// Abstracts infrastructure-specific types (e.g. S3 list results) into a
/// port-level value object usable across all storage backends.
pub struct ObjectInfo {
pub:
	key           string
	size          i64
	last_modified time.Time
	etag          string
}

/// ObjectStoreReaderPort defines read-only object storage operations.
pub interface ObjectStoreReaderPort {
mut:
	/// Retrieves object data and its ETag.
	/// Use range_start=-1, range_end=-1 to fetch the entire object.
	get_object(key string, range_start i64, range_end i64) !([]u8, string)
	/// Lists objects matching the given key prefix.
	list_objects(prefix string) ![]ObjectInfo
}

/// ObjectStoreWriterPort defines write/delete object storage operations.
pub interface ObjectStoreWriterPort {
mut:
	/// Stores an object under the given key.
	put_object(key string, data []u8) !
	/// Deletes a single object by key.
	delete_object(key string) !
	/// Deletes all objects whose keys start with the given prefix.
	delete_objects_with_prefix(prefix string) !
}

/// ObjectStorePort is the composite interface combining reader and writer.
pub interface ObjectStorePort {
	ObjectStoreReaderPort
	ObjectStoreWriterPort
}
