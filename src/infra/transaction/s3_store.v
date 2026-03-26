/// Infrastructure layer - S3-based transaction store
module transaction

import domain
import json
import service.port
import sync

// NOTE: Object storage operations are defined by port.ObjectStorePort
// (service/port/object_store_port.v). The former S3TransactionClient
// interface has been replaced with port.ObjectStorePort to follow DIP.

/// S3TransactionConfig holds the configuration for S3-based transaction storage.
pub struct S3TransactionConfig {
pub:
	prefix string = 'transactions'
}

/// S3TransactionStore implements TransactionStore using S3 object storage.
/// Transaction metadata is serialized as JSON and stored as S3 objects.
/// Key pattern: {prefix}/{transactional_id}/state.json
pub struct S3TransactionStore {
mut:
	client port.ObjectStorePort
	config S3TransactionConfig
	lock   sync.RwMutex
}

/// new_s3_transaction_store creates a new S3-based transaction store.
pub fn new_s3_transaction_store(client port.ObjectStorePort, config S3TransactionConfig) &S3TransactionStore {
	return &S3TransactionStore{
		client: client
		config: config
	}
}

/// Builds the S3 object key for a transaction.
fn (s &S3TransactionStore) build_key(transactional_id string) string {
	return '${s.config.prefix}/${transactional_id}/state.json'
}

/// get_transaction retrieves transaction metadata from S3 by transactional_id.
pub fn (mut s S3TransactionStore) get_transaction(transactional_id string) !domain.TransactionMetadata {
	s.lock.@lock()
	defer { s.lock.unlock() }

	key := s.build_key(transactional_id)
	data, _ := s.client.get_object(key, -1, -1) or {
		return error('transactional_id not found: ${transactional_id}')
	}

	return json.decode(domain.TransactionMetadata, data.bytestr()) or {
		return error('failed to decode transaction: ${err}')
	}
}

/// save_transaction saves transaction metadata to S3 as a JSON object.
pub fn (mut s S3TransactionStore) save_transaction(metadata domain.TransactionMetadata) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	key := s.build_key(metadata.transactional_id)
	data := json.encode(metadata)
	s.client.put_object(key, data.bytes())!
}

/// delete_transaction deletes transaction metadata from S3.
pub fn (mut s S3TransactionStore) delete_transaction(transactional_id string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	key := s.build_key(transactional_id)
	s.client.delete_object(key)!
}

/// list_transactions returns a list of all transactions stored in S3.
pub fn (mut s S3TransactionStore) list_transactions() ![]domain.TransactionMetadata {
	s.lock.@lock()
	defer { s.lock.unlock() }

	prefix := '${s.config.prefix}/'
	objects := s.client.list_objects(prefix) or {
		return error('failed to list transactions: ${err}')
	}

	mut result := []domain.TransactionMetadata{cap: objects.len}
	for obj in objects {
		data, _ := s.client.get_object(obj.key, -1, -1) or { continue }
		metadata := json.decode(domain.TransactionMetadata, data.bytestr()) or { continue }
		result << metadata
	}
	return result
}
