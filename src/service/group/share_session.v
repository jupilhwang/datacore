// Manages share fetch sessions for share group consumers.
// Sessions maintain state between consumers and brokers to enable efficient fetching.
module group

import domain
import sync
import time

/// ShareSessionManager manages share sessions.
/// Responsible for session creation, updates, and expiry handling.
pub struct ShareSessionManager {
mut:
	// Share sessions keyed by "group_id:member_id"
	sessions map[string]&domain.ShareSession
	// Thread safety
	lock sync.RwMutex
}

/// new_share_session_manager creates a new share session manager.
fn new_share_session_manager() &ShareSessionManager {
	return &ShareSessionManager{
		sessions: map[string]&domain.ShareSession{}
	}
}

/// get_or_create_session gets or creates a share session.
fn (mut m ShareSessionManager) get_or_create_session(group_id string, member_id string) &domain.ShareSession {
	m.lock.@lock()
	defer { m.lock.unlock() }

	return m.get_or_create_session_internal(group_id, member_id)
}

/// get_session returns a share session if it exists.
fn (mut m ShareSessionManager) get_session(group_id string, member_id string) ?&domain.ShareSession {
	m.lock.rlock()
	defer { m.lock.runlock() }

	key := '${group_id}:${member_id}'
	return m.sessions[key] or { return none }
}

/// update_session updates a share session.
/// Handles adding/removing partitions and incrementing the epoch.
fn (mut m ShareSessionManager) update_session(group_id string, member_id string, epoch i32, partitions_to_add []domain.ShareSessionPartition, partitions_to_remove []domain.ShareSessionPartition) !&domain.ShareSession {
	m.lock.@lock()
	defer { m.lock.unlock() }

	key := '${group_id}:${member_id}'
	mut session := m.sessions[key] or { return error('session not found') }

	// Verify epoch
	if epoch != session.session_epoch && epoch != -1 {
		return error('invalid session epoch')
	}

	now := time.now().unix_milli()
	session.last_used = now

	// Remove partitions
	for to_remove in partitions_to_remove {
		session.partitions = session.partitions.filter(fn [to_remove] (p domain.ShareSessionPartition) bool {
			return p.topic_name != to_remove.topic_name || p.partition != to_remove.partition
		})
	}

	// Add partitions
	for to_add in partitions_to_add {
		// Check if already exists
		mut exists := false
		for existing in session.partitions {
			if existing.topic_name == to_add.topic_name && existing.partition == to_add.partition {
				exists = true
				break
			}
		}
		if !exists {
			session.partitions << to_add
		}
	}

	// Increment epoch
	session.session_epoch += 1
	if session.session_epoch > 2147483647 {
		session.session_epoch = 1
	}

	return session
}

/// close_session closes a share session.
fn (mut m ShareSessionManager) close_session(group_id string, member_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	key := '${group_id}:${member_id}'
	m.sessions.delete(key)
}

/// delete_session deletes a session by key (internal use).
fn (mut m ShareSessionManager) delete_session(group_id string, member_id string) {
	key := '${group_id}:${member_id}'
	m.sessions.delete(key)
}

/// delete_sessions_for_group deletes all sessions for a group.
fn (mut m ShareSessionManager) delete_sessions_for_group(group_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut to_delete := []string{}
	for key, _ in m.sessions {
		if key.starts_with('${group_id}:') {
			to_delete << key
		}
	}
	for key in to_delete {
		m.sessions.delete(key)
	}
}

/// expire_sessions removes sessions that have not been used recently.
/// Returns a list of expired session keys.
fn (mut m ShareSessionManager) expire_sessions(timeout_ms i64) []string {
	m.lock.@lock()
	defer { m.lock.unlock() }

	now := time.now().unix_milli()
	mut expired := []string{}

	for key, session in m.sessions {
		if now - session.last_used > timeout_ms {
			expired << key
		}
	}

	for key in expired {
		m.sessions.delete(key)
	}

	return expired
}

/// get_or_create_session_internal gets or creates a session without acquiring a lock.
fn (mut m ShareSessionManager) get_or_create_session_internal(group_id string, member_id string) &domain.ShareSession {
	key := '${group_id}:${member_id}'

	if session := m.sessions[key] {
		return session
	}

	now := time.now().unix_milli()
	session := &domain.ShareSession{
		group_id:       group_id
		member_id:      member_id
		session_epoch:  1
		partitions:     []domain.ShareSessionPartition{}
		acquired_locks: map[string][]i64{}
		created_at:     now
		last_used:      now
	}
	m.sessions[key] = session
	return session
}
