module transaction

import domain
import service.port

/// MockObjectStoreForTxn is an in-memory test double satisfying port.ObjectStorePort.
struct MockObjectStoreForTxn {
mut:
	objects map[string][]u8
}

fn (mut c MockObjectStoreForTxn) get_object(key string, range_start i64, range_end i64) !([]u8, string) {
	if key in c.objects {
		return c.objects[key].clone(), ''
	}
	return error('Object not found: ${key}')
}

fn (mut c MockObjectStoreForTxn) put_object(key string, data []u8) ! {
	c.objects[key] = data.clone()
}

fn (mut c MockObjectStoreForTxn) delete_object(key string) ! {
	if key !in c.objects {
		return error('Object not found: ${key}')
	}
	c.objects.delete(key)
}

fn (mut c MockObjectStoreForTxn) list_objects(prefix string) ![]port.ObjectInfo {
	mut result := []port.ObjectInfo{}
	for k, v in c.objects {
		if k.starts_with(prefix) {
			result << port.ObjectInfo{
				key:  k
				size: i64(v.len)
			}
		}
	}
	return result
}

fn (mut c MockObjectStoreForTxn) delete_objects_with_prefix(prefix string) ! {
	mut to_delete := []string{}
	for k, _ in c.objects {
		if k.starts_with(prefix) {
			to_delete << k
		}
	}
	for k in to_delete {
		c.objects.delete(k)
	}
}

fn create_test_s3_store() &S3TransactionStore {
	return new_s3_transaction_store(&MockObjectStoreForTxn{}, S3TransactionConfig{})
}

fn create_test_metadata(id string) domain.TransactionMetadata {
	return domain.TransactionMetadata{
		transactional_id:          id
		producer_id:               1000
		producer_epoch:            1
		txn_timeout_ms:            60000
		state:                     .ongoing
		topic_partitions:          [
			domain.TopicPartition{
				topic:     'test-topic'
				partition: 0
			},
		]
		txn_start_timestamp:       1000000
		txn_last_update_timestamp: 1000001
	}
}

fn test_s3_store_save_and_get_transaction() {
	mut store := create_test_s3_store()
	metadata := create_test_metadata('txn-1')

	store.save_transaction(metadata) or {
		assert false, 'save_transaction failed: ${err}'
		return
	}

	result := store.get_transaction('txn-1') or {
		assert false, 'get_transaction failed: ${err}'
		return
	}

	assert result.transactional_id == 'txn-1'
	assert result.producer_id == 1000
	assert result.producer_epoch == 1
	assert result.txn_timeout_ms == 60000
	assert result.state == .ongoing
	assert result.topic_partitions.len == 1
	assert result.topic_partitions[0].topic == 'test-topic'
	assert result.topic_partitions[0].partition == 0
	assert result.txn_start_timestamp == 1000000
	assert result.txn_last_update_timestamp == 1000001
}

fn test_s3_store_get_nonexistent_transaction() {
	mut store := create_test_s3_store()

	store.get_transaction('nonexistent') or {
		assert err.msg().contains('not found') || err.msg().contains('Object not found')
		return
	}
	assert false, 'expected error for nonexistent transaction'
}

fn test_s3_store_delete_transaction() {
	mut store := create_test_s3_store()
	metadata := create_test_metadata('txn-del')

	store.save_transaction(metadata) or {
		assert false, 'save failed: ${err}'
		return
	}

	store.delete_transaction('txn-del') or {
		assert false, 'delete failed: ${err}'
		return
	}

	store.get_transaction('txn-del') or {
		assert true
		return
	}
	assert false, 'expected error after delete'
}

fn test_s3_store_delete_nonexistent() {
	mut store := create_test_s3_store()

	store.delete_transaction('nonexistent') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'expected error for deleting nonexistent transaction'
}

fn test_s3_store_list_transactions() {
	mut store := create_test_s3_store()

	store.save_transaction(create_test_metadata('txn-a')) or {
		assert false, 'save txn-a failed: ${err}'
		return
	}
	store.save_transaction(create_test_metadata('txn-b')) or {
		assert false, 'save txn-b failed: ${err}'
		return
	}

	result := store.list_transactions() or {
		assert false, 'list failed: ${err}'
		return
	}

	assert result.len == 2
	ids := result.map(it.transactional_id)
	assert 'txn-a' in ids
	assert 'txn-b' in ids
}

fn test_s3_store_list_empty() {
	mut store := create_test_s3_store()

	result := store.list_transactions() or {
		assert false, 'list failed: ${err}'
		return
	}

	assert result.len == 0
}

fn test_s3_store_update_transaction_state() {
	mut store := create_test_s3_store()
	initial := create_test_metadata('txn-state')

	store.save_transaction(initial) or {
		assert false, 'save failed: ${err}'
		return
	}

	updated := domain.TransactionMetadata{
		transactional_id:          initial.transactional_id
		producer_id:               initial.producer_id
		producer_epoch:            initial.producer_epoch
		txn_timeout_ms:            initial.txn_timeout_ms
		state:                     .prepare_commit
		topic_partitions:          initial.topic_partitions
		txn_start_timestamp:       initial.txn_start_timestamp
		txn_last_update_timestamp: 2000000
	}
	store.save_transaction(updated) or {
		assert false, 'update failed: ${err}'
		return
	}

	result := store.get_transaction('txn-state') or {
		assert false, 'get failed: ${err}'
		return
	}

	assert result.state == .prepare_commit
	assert result.txn_last_update_timestamp == 2000000
}
