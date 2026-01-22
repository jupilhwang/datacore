/// 인프라 계층 - 메모리 기반 사용자 저장소
module auth

import domain
import sync
import time

/// MemoryUserStore는 인메모리 저장소를 사용하여 UserStore 인터페이스를 구현합니다.
/// 사용자 생성, 조회, 수정, 삭제 및 비밀번호 검증 기능을 제공합니다.
pub struct MemoryUserStore {
mut:
	users map[string]domain.User
	lock  sync.RwMutex
}

/// new_memory_user_store는 새로운 인메모리 사용자 저장소를 생성합니다.
pub fn new_memory_user_store() &MemoryUserStore {
	return &MemoryUserStore{
		users: map[string]domain.User{}
	}
}

/// new_memory_user_store_with_users는 미리 구성된 사용자들로 저장소를 생성합니다.
pub fn new_memory_user_store_with_users(users []domain.User) &MemoryUserStore {
	mut store := new_memory_user_store()
	for user in users {
		store.users[user.username] = user
	}
	return store
}

/// get_user는 사용자명으로 사용자를 조회합니다.
pub fn (mut s MemoryUserStore) get_user(username string) !domain.User {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if user := s.users[username] {
		return user
	}
	return error('user not found: ${username}')
}

/// create_user는 새로운 사용자를 생성합니다.
pub fn (mut s MemoryUserStore) create_user(username string, password string, mechanism domain.SaslMechanism) !domain.User {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if username in s.users {
		return error('user already exists: ${username}')
	}

	now := time.now().unix()
	user := domain.User{
		username:      username
		password_hash: password // 프로덕션에서는 해시된 값이어야 합니다
		mechanism:     mechanism
		created_at:    now
		updated_at:    now
	}

	s.users[username] = user
	return user
}

/// update_password는 사용자의 비밀번호를 업데이트합니다.
pub fn (mut s MemoryUserStore) update_password(username string, new_password string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if user := s.users[username] {
		s.users[username] = domain.User{
			...user
			password_hash: new_password
			updated_at:    time.now().unix()
		}
		return
	}
	return error('user not found: ${username}')
}

/// delete_user는 사용자를 삭제합니다.
pub fn (mut s MemoryUserStore) delete_user(username string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if username !in s.users {
		return error('user not found: ${username}')
	}

	s.users.delete(username)
}

/// list_users는 모든 사용자를 반환합니다.
pub fn (mut s MemoryUserStore) list_users() ![]domain.User {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	mut result := []domain.User{}
	for _, user in s.users {
		result << user
	}
	return result
}

/// validate_password는 사용자의 비밀번호를 검증합니다.
pub fn (mut s MemoryUserStore) validate_password(username string, password string) !bool {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if user := s.users[username] {
		// 프로덕션에서는 해시된 비밀번호를 비교해야 합니다
		return user.password_hash == password
	}
	return error('user not found: ${username}')
}
