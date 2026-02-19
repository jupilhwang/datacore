/// Infrastructure layer - Memory-based user store
module auth

import domain
import sync
import time

/// MemoryUserStore implements the UserStore interface using an in-memory store.
/// Provides user creation, lookup, update, deletion, and password validation.
pub struct MemoryUserStore {
mut:
	users map[string]domain.User
	lock  sync.RwMutex
}

/// new_memory_user_store creates a new in-memory user store.
pub fn new_memory_user_store() &MemoryUserStore {
	return &MemoryUserStore{
		users: map[string]domain.User{}
	}
}

/// new_memory_user_store_with_users creates a store pre-populated with the given users.
pub fn new_memory_user_store_with_users(users []domain.User) &MemoryUserStore {
	mut store := new_memory_user_store()
	for user in users {
		store.users[user.username] = user
	}
	return store
}

/// get_user retrieves a user by username.
pub fn (mut s MemoryUserStore) get_user(username string) !domain.User {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if user := s.users[username] {
		return user
	}
	return error('user not found: ${username}')
}

/// create_user creates a new user.
pub fn (mut s MemoryUserStore) create_user(username string, password string, mechanism domain.SaslMechanism) !domain.User {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if username in s.users {
		return error('user already exists: ${username}')
	}

	now := time.now().unix()
	user := domain.User{
		username:      username
		password_hash: password
		mechanism:     mechanism
		created_at:    now
		updated_at:    now
	}

	s.users[username] = user
	return user
}

/// update_password updates the password of a user.
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

/// delete_user deletes a user.
pub fn (mut s MemoryUserStore) delete_user(username string) ! {
	s.lock.@lock()
	defer { s.lock.unlock() }

	if username !in s.users {
		return error('user not found: ${username}')
	}

	s.users.delete(username)
}

/// list_users returns all users.
pub fn (mut s MemoryUserStore) list_users() ![]domain.User {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	mut result := []domain.User{}
	for _, user in s.users {
		result << user
	}
	return result
}

/// validate_password validates a user's password.
pub fn (mut s MemoryUserStore) validate_password(username string, password string) !bool {
	s.lock.@rlock()
	defer { s.lock.runlock() }

	if user := s.users[username] {
		// In production, hashed passwords should be compared
		return user.password_hash == password
	}
	return error('user not found: ${username}')
}
