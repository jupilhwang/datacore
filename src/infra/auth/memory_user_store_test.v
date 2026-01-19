// Unit Tests - Infra Layer: Memory User Store
module auth

import domain

fn test_memory_user_store_create_user() {
	mut store := new_memory_user_store()

	user := store.create_user('alice', 'secret123', .plain) or { panic(err) }

	assert user.username == 'alice'
	assert user.password_hash == 'secret123'
	assert user.mechanism == .plain
	assert user.created_at > 0
	assert user.updated_at > 0
}

fn test_memory_user_store_get_user() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	user := store.get_user('alice') or { panic(err) }

	assert user.username == 'alice'
	assert user.password_hash == 'secret123'
}

fn test_memory_user_store_get_user_not_found() {
	mut store := new_memory_user_store()

	store.get_user('nonexistent') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

fn test_memory_user_store_create_duplicate_user() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	// Try to create the same user again
	store.create_user('alice', 'different', .plain) or {
		assert err.msg().contains('already exists')
		return
	}
	assert false, 'should return error for duplicate user'
}

fn test_memory_user_store_update_password() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'oldpass', .plain) or { panic(err) }

	store.update_password('alice', 'newpass') or { panic(err) }

	user := store.get_user('alice') or { panic(err) }
	assert user.password_hash == 'newpass'
	assert user.updated_at >= user.created_at
}

fn test_memory_user_store_update_password_not_found() {
	mut store := new_memory_user_store()

	store.update_password('nonexistent', 'newpass') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

fn test_memory_user_store_delete_user() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	store.delete_user('alice') or { panic(err) }

	// Verify user is deleted
	store.get_user('alice') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'user should be deleted'
}

fn test_memory_user_store_delete_user_not_found() {
	mut store := new_memory_user_store()

	store.delete_user('nonexistent') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

fn test_memory_user_store_list_users() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'pass1', .plain) or { panic(err) }
	store.create_user('bob', 'pass2', .plain) or { panic(err) }
	store.create_user('charlie', 'pass3', .plain) or { panic(err) }

	users := store.list_users() or { panic(err) }

	assert users.len == 3

	mut usernames := []string{}
	for u in users {
		usernames << u.username
	}
	usernames.sort()

	assert usernames == ['alice', 'bob', 'charlie']
}

fn test_memory_user_store_list_users_empty() {
	mut store := new_memory_user_store()

	users := store.list_users() or { panic(err) }

	assert users.len == 0
}

fn test_memory_user_store_validate_password_success() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	valid := store.validate_password('alice', 'secret123') or { panic(err) }

	assert valid == true
}

fn test_memory_user_store_validate_password_wrong() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	valid := store.validate_password('alice', 'wrongpassword') or { panic(err) }

	assert valid == false
}

fn test_memory_user_store_validate_password_user_not_found() {
	mut store := new_memory_user_store()

	store.validate_password('nonexistent', 'anypass') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

fn test_memory_user_store_with_preloaded_users() {
	users := [
		domain.User{
			username:      'admin'
			password_hash: 'adminpass'
			mechanism:     .plain
			created_at:    1000
			updated_at:    1000
		},
		domain.User{
			username:      'user1'
			password_hash: 'user1pass'
			mechanism:     .plain
			created_at:    1000
			updated_at:    1000
		},
	]

	mut store := new_memory_user_store_with_users(users)

	admin := store.get_user('admin') or { panic(err) }
	assert admin.username == 'admin'
	assert admin.password_hash == 'adminpass'

	user1 := store.get_user('user1') or { panic(err) }
	assert user1.username == 'user1'
}
