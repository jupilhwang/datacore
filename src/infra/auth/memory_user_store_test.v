/// 단위 테스트 - 인프라 계층: 메모리 사용자 저장소
module auth

import domain

/// test_memory_user_store_create_user는 사용자 생성 기능을 테스트합니다.
fn test_memory_user_store_create_user() {
	mut store := new_memory_user_store()

	user := store.create_user('alice', 'secret123', .plain) or { panic(err) }

	assert user.username == 'alice'
	assert user.password_hash == 'secret123'
	assert user.mechanism == .plain
	assert user.created_at > 0
	assert user.updated_at > 0
}

/// test_memory_user_store_get_user는 사용자 조회 기능을 테스트합니다.
fn test_memory_user_store_get_user() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	user := store.get_user('alice') or { panic(err) }

	assert user.username == 'alice'
	assert user.password_hash == 'secret123'
}

/// test_memory_user_store_get_user_not_found는 존재하지 않는 사용자 조회 시 에러를 반환하는지 테스트합니다.
fn test_memory_user_store_get_user_not_found() {
	mut store := new_memory_user_store()

	store.get_user('nonexistent') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

/// test_memory_user_store_create_duplicate_user는 중복 사용자 생성 시 에러를 반환하는지 테스트합니다.
fn test_memory_user_store_create_duplicate_user() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	// 동일한 사용자를 다시 생성 시도
	store.create_user('alice', 'different', .plain) or {
		assert err.msg().contains('already exists')
		return
	}
	assert false, 'should return error for duplicate user'
}

/// test_memory_user_store_update_password는 비밀번호 업데이트 기능을 테스트합니다.
fn test_memory_user_store_update_password() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'oldpass', .plain) or { panic(err) }

	store.update_password('alice', 'newpass') or { panic(err) }

	user := store.get_user('alice') or { panic(err) }
	assert user.password_hash == 'newpass'
	assert user.updated_at >= user.created_at
}

/// test_memory_user_store_update_password_not_found는 존재하지 않는 사용자의 비밀번호 업데이트 시 에러를 반환하는지 테스트합니다.
fn test_memory_user_store_update_password_not_found() {
	mut store := new_memory_user_store()

	store.update_password('nonexistent', 'newpass') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

/// test_memory_user_store_delete_user는 사용자 삭제 기능을 테스트합니다.
fn test_memory_user_store_delete_user() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	store.delete_user('alice') or { panic(err) }

	// 사용자가 삭제되었는지 확인
	store.get_user('alice') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'user should be deleted'
}

/// test_memory_user_store_delete_user_not_found는 존재하지 않는 사용자 삭제 시 에러를 반환하는지 테스트합니다.
fn test_memory_user_store_delete_user_not_found() {
	mut store := new_memory_user_store()

	store.delete_user('nonexistent') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

/// test_memory_user_store_list_users는 모든 사용자 목록 조회 기능을 테스트합니다.
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

/// test_memory_user_store_list_users_empty는 빈 저장소에서 사용자 목록 조회 시 빈 배열을 반환하는지 테스트합니다.
fn test_memory_user_store_list_users_empty() {
	mut store := new_memory_user_store()

	users := store.list_users() or { panic(err) }

	assert users.len == 0
}

/// test_memory_user_store_validate_password_success는 올바른 비밀번호 검증이 성공하는지 테스트합니다.
fn test_memory_user_store_validate_password_success() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	valid := store.validate_password('alice', 'secret123') or { panic(err) }

	assert valid == true
}

/// test_memory_user_store_validate_password_wrong는 잘못된 비밀번호 검증이 실패하는지 테스트합니다.
fn test_memory_user_store_validate_password_wrong() {
	mut store := new_memory_user_store()
	store.create_user('alice', 'secret123', .plain) or { panic(err) }

	valid := store.validate_password('alice', 'wrongpassword') or { panic(err) }

	assert valid == false
}

/// test_memory_user_store_validate_password_user_not_found는 존재하지 않는 사용자의 비밀번호 검증 시 에러를 반환하는지 테스트합니다.
fn test_memory_user_store_validate_password_user_not_found() {
	mut store := new_memory_user_store()

	store.validate_password('nonexistent', 'anypass') or {
		assert err.msg().contains('not found')
		return
	}
	assert false, 'should return error for non-existent user'
}

/// test_memory_user_store_with_preloaded_users는 미리 로드된 사용자들로 저장소가 올바르게 초기화되는지 테스트합니다.
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
