// Share 그룹 컨슈머를 위한 share fetch 세션을 관리합니다.
// 세션은 컨슈머와 브로커 간의 상태를 유지하여 효율적인 fetch를 가능하게 합니다.
module group

import domain
import sync
import time

// Share 세션 매니저

/// ShareSessionManager는 share 세션을 관리합니다.
/// 세션 생성, 업데이트, 만료 처리를 담당합니다.
pub struct ShareSessionManager {
mut:
	// "group_id:member_id" 키로 관리되는 share 세션
	sessions map[string]&domain.ShareSession
	// 스레드 안전성
	lock sync.RwMutex
}

/// new_share_session_manager는 새로운 share 세션 매니저를 생성합니다.
pub fn new_share_session_manager() &ShareSessionManager {
	return &ShareSessionManager{
		sessions: map[string]&domain.ShareSession{}
	}
}

/// get_or_create_session은 share 세션을 가져오거나 생성합니다.
pub fn (mut m ShareSessionManager) get_or_create_session(group_id string, member_id string) &domain.ShareSession {
	m.lock.@lock()
	defer { m.lock.unlock() }

	return m.get_or_create_session_internal(group_id, member_id)
}

/// get_session은 존재하는 경우 share 세션을 반환합니다.
pub fn (mut m ShareSessionManager) get_session(group_id string, member_id string) ?&domain.ShareSession {
	m.lock.rlock()
	defer { m.lock.runlock() }

	key := '${group_id}:${member_id}'
	return m.sessions[key] or { return none }
}

/// update_session은 share 세션을 업데이트합니다.
/// 파티션 추가/제거 및 에포크 증가를 처리합니다.
pub fn (mut m ShareSessionManager) update_session(group_id string, member_id string, epoch i32, partitions_to_add []domain.ShareSessionPartition, partitions_to_remove []domain.ShareSessionPartition) !&domain.ShareSession {
	m.lock.@lock()
	defer { m.lock.unlock() }

	key := '${group_id}:${member_id}'
	mut session := m.sessions[key] or { return error('session not found') }

	// 에포크 확인
	if epoch != session.session_epoch && epoch != -1 {
		return error('invalid session epoch')
	}

	now := time.now().unix_milli()
	session.last_used = now

	// 파티션 제거
	for to_remove in partitions_to_remove {
		session.partitions = session.partitions.filter(fn [to_remove] (p domain.ShareSessionPartition) bool {
			return p.topic_name != to_remove.topic_name || p.partition != to_remove.partition
		})
	}

	// 파티션 추가
	for to_add in partitions_to_add {
		// 이미 존재하는지 확인
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

	// 에포크 증가
	session.session_epoch += 1
	if session.session_epoch > 2147483647 {
		session.session_epoch = 1 // 래핑
	}

	return session
}

/// close_session은 share 세션을 종료합니다.
pub fn (mut m ShareSessionManager) close_session(group_id string, member_id string) {
	m.lock.@lock()
	defer { m.lock.unlock() }

	key := '${group_id}:${member_id}'
	m.sessions.delete(key)
}

/// delete_session은 키로 세션을 삭제합니다 (내부 사용).
pub fn (mut m ShareSessionManager) delete_session(group_id string, member_id string) {
	key := '${group_id}:${member_id}'
	m.sessions.delete(key)
}

/// delete_sessions_for_group은 그룹의 모든 세션을 삭제합니다.
pub fn (mut m ShareSessionManager) delete_sessions_for_group(group_id string) {
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

/// expire_sessions는 최근에 사용되지 않은 세션을 제거합니다.
/// 반환값: 만료된 세션 키 목록
pub fn (mut m ShareSessionManager) expire_sessions(timeout_ms i64) []string {
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

/// get_or_create_session_internal은 잠금 없이 세션을 가져오거나 생성합니다.
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
