// Tests for ObjectStorePort interface hierarchy (ISP: Reader/Writer separation).
// Verifies that a mock struct satisfying all methods can be assigned to each
// sub-interface and the composite ObjectStorePort.
module port

import time

/// MockObjectStore is an in-memory test double for ObjectStorePort.
struct MockObjectStore {
mut:
	objects map[string][]u8
}

fn (mut m MockObjectStore) get_object(key string, range_start i64, range_end i64) !([]u8, string) {
	if key in m.objects {
		return m.objects[key].clone(), 'etag-mock'
	}
	return error('not found: ${key}')
}

fn (mut m MockObjectStore) list_objects(prefix string) ![]ObjectInfo {
	mut result := []ObjectInfo{}
	for k, v in m.objects {
		if k.starts_with(prefix) {
			result << ObjectInfo{
				key:  k
				size: i64(v.len)
			}
		}
	}
	return result
}

fn (mut m MockObjectStore) put_object(key string, data []u8) ! {
	m.objects[key] = data.clone()
}

fn (mut m MockObjectStore) delete_object(key string) ! {
	if key !in m.objects {
		return error('not found: ${key}')
	}
	m.objects.delete(key)
}

fn (mut m MockObjectStore) delete_objects_with_prefix(prefix string) ! {
	mut to_delete := []string{}
	for k, _ in m.objects {
		if k.starts_with(prefix) {
			to_delete << k
		}
	}
	for k in to_delete {
		m.objects.delete(k)
	}
}

fn test_reader_port_get_object() {
	mut store := &MockObjectStore{}
	store.put_object('doc/readme', 'hello world'.bytes()) or {
		assert false, 'put failed: ${err}'
		return
	}

	mut reader := ObjectStoreReaderPort(store)
	data, etag := reader.get_object('doc/readme', -1, -1) or {
		assert false, 'get failed: ${err}'
		return
	}
	assert data == 'hello world'.bytes()
	assert etag == 'etag-mock'
}

fn test_reader_port_list_objects() {
	mut store := &MockObjectStore{}
	store.put_object('ns/a.txt', 'aaa'.bytes()) or {
		assert false, 'put a failed: ${err}'
		return
	}
	store.put_object('ns/b.txt', 'bb'.bytes()) or {
		assert false, 'put b failed: ${err}'
		return
	}
	store.put_object('other/c.txt', 'c'.bytes()) or {
		assert false, 'put c failed: ${err}'
		return
	}

	mut reader := ObjectStoreReaderPort(store)
	objects := reader.list_objects('ns/') or {
		assert false, 'list failed: ${err}'
		return
	}

	assert objects.len == 2
	keys := objects.map(it.key)
	assert 'ns/a.txt' in keys
	assert 'ns/b.txt' in keys
}

fn test_writer_port_put_and_delete() {
	mut store := &MockObjectStore{}
	mut writer := ObjectStoreWriterPort(store)

	writer.put_object('k1', 'val'.bytes()) or {
		assert false, 'put failed: ${err}'
		return
	}

	writer.delete_object('k1') or {
		assert false, 'delete failed: ${err}'
		return
	}

	// verify deletion
	writer.delete_object('k1') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'expected error after double delete'
}

fn test_writer_port_delete_with_prefix() {
	mut store := &MockObjectStore{}
	store.put_object('pfx/a', [u8(1)]) or {
		assert false, 'put a failed: ${err}'
		return
	}
	store.put_object('pfx/b', [u8(2)]) or {
		assert false, 'put b failed: ${err}'
		return
	}
	store.put_object('other/c', [u8(3)]) or {
		assert false, 'put c failed: ${err}'
		return
	}

	mut writer := ObjectStoreWriterPort(store)
	writer.delete_objects_with_prefix('pfx/') or {
		assert false, 'delete prefix failed: ${err}'
		return
	}

	// only 'other/c' should remain
	mut reader := ObjectStoreReaderPort(store)
	remaining := reader.list_objects('') or {
		assert false, 'list failed: ${err}'
		return
	}
	assert remaining.len == 1
	assert remaining[0].key == 'other/c'
}

fn test_composite_port_round_trip() {
	mut store := &MockObjectStore{}
	mut obj_store := ObjectStorePort(store)

	obj_store.put_object('round/trip', 'payload'.bytes()) or {
		assert false, 'put failed: ${err}'
		return
	}

	data, _ := obj_store.get_object('round/trip', -1, -1) or {
		assert false, 'get failed: ${err}'
		return
	}
	assert data == 'payload'.bytes()

	objects := obj_store.list_objects('round/') or {
		assert false, 'list failed: ${err}'
		return
	}
	assert objects.len == 1
	assert objects[0].key == 'round/trip'
	assert objects[0].size == i64('payload'.len)

	obj_store.delete_object('round/trip') or {
		assert false, 'delete failed: ${err}'
		return
	}

	empty := obj_store.list_objects('round/') or {
		assert false, 'list after delete failed: ${err}'
		return
	}
	assert empty.len == 0
}

fn test_object_info_fields() {
	now := time.now()
	info := ObjectInfo{
		key:           'test/key.bin'
		size:          1024
		last_modified: now
		etag:          'abc123'
	}
	assert info.key == 'test/key.bin'
	assert info.size == 1024
	assert info.last_modified == now
	assert info.etag == 'abc123'
}
