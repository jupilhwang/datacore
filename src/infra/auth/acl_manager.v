/// 인프라 계층 - 메모리 기반 ACL 관리자
module auth

import domain
import sync

/// MemoryAclManager는 인메모리 저장소를 사용하여 AclManager 인터페이스를 구현합니다.
/// ACL(액세스 제어 목록) 바인딩을 생성, 삭제, 조회하고 권한을 검증하는 기능을 제공합니다.
pub struct MemoryAclManager {
mut:
	acls []domain.AclBinding
	lock sync.RwMutex
}

/// new_memory_acl_manager는 새로운 인메모리 ACL 관리자를 생성합니다.
pub fn new_memory_acl_manager() &MemoryAclManager {
	return &MemoryAclManager{
		acls: []domain.AclBinding{}
	}
}

/// create_acls는 새로운 ACL 바인딩들을 생성합니다.
/// 이미 존재하는 ACL은 중복 생성하지 않습니다.
pub fn (mut m MemoryAclManager) create_acls(acls []domain.AclBinding) ![]domain.AclCreateResult {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut results := []domain.AclCreateResult{}

	for acl in acls {
		// ACL이 이미 존재하는지 확인
		mut exists := false
		for existing in m.acls {
			if existing.pattern.resource_type == acl.pattern.resource_type
				&& existing.pattern.name == acl.pattern.name
				&& existing.pattern.pattern_type == acl.pattern.pattern_type
				&& existing.entry.principal == acl.entry.principal
				&& existing.entry.host == acl.entry.host
				&& existing.entry.operation == acl.entry.operation
				&& existing.entry.permission_type == acl.entry.permission_type {
				exists = true
				break
			}
		}

		if !exists {
			m.acls << acl
		}

		results << domain.AclCreateResult{
			error_code: 0 // None
		}
	}

	return results
}

/// delete_acls는 필터와 일치하는 ACL 바인딩들을 삭제합니다.
pub fn (mut m MemoryAclManager) delete_acls(filters []domain.AclBindingFilter) ![]domain.AclDeleteResult {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut results := []domain.AclDeleteResult{}

	for filter in filters {
		mut deleted_acls := []domain.AclBinding{}
		mut remaining_acls := []domain.AclBinding{}

		for acl in m.acls {
			if matches_filter(acl, filter) {
				deleted_acls << acl
			} else {
				remaining_acls << acl
			}
		}

		m.acls = remaining_acls

		results << domain.AclDeleteResult{
			error_code:   0
			deleted_acls: deleted_acls
		}
	}

	return results
}

/// describe_acls는 필터와 일치하는 ACL 바인딩들을 반환합니다.
pub fn (mut m MemoryAclManager) describe_acls(filter domain.AclBindingFilter) ![]domain.AclBinding {
	m.lock.@rlock()
	defer { m.lock.runlock() }

	mut matched := []domain.AclBinding{}
	for acl in m.acls {
		if matches_filter(acl, filter) {
			matched << acl
		}
	}
	return matched
}

/// authorize는 해당 작업이 허용되는지 확인합니다.
/// Deny 권한이 Allow 권한보다 우선합니다.
pub fn (mut m MemoryAclManager) authorize(principal string, host string, operation domain.AclOperation, resource domain.ResourcePattern) !bool {
	m.lock.@rlock()
	defer { m.lock.runlock() }

	// 1. Deny 권한을 먼저 확인 (Deny가 Allow보다 우선)
	for acl in m.acls {
		if acl.entry.permission_type == .deny {
			if matches_resource(acl.pattern, resource)
				&& matches_principal(acl.entry.principal, principal)
				&& matches_host(acl.entry.host, host)
				&& matches_operation(acl.entry.operation, operation) {
				return false
			}
		}
	}

	// 2. Allow 권한 확인
	for acl in m.acls {
		if acl.entry.permission_type == .allow {
			if matches_resource(acl.pattern, resource)
				&& matches_principal(acl.entry.principal, principal)
				&& matches_host(acl.entry.host, host)
				&& matches_operation(acl.entry.operation, operation) {
				return true
			}
		}
	}

	// 일치하는 Allow ACL이 없으면 기본적으로 거부
	// 참고: Kafka에서는 리소스에 대한 ACL이 없으면 allow.everyone.if.no.acl.found 설정에 따라 결정됩니다.
	// 현재는 안전한 기본값으로 거부합니다.
	return false
}

/// 헬퍼 함수들

/// matches_filter는 ACL이 필터 조건과 일치하는지 확인합니다.
fn matches_filter(acl domain.AclBinding, filter domain.AclBindingFilter) bool {
	// 리소스 패턴 필터 확인
	if filter.pattern_filter.resource_type != .any
		&& filter.pattern_filter.resource_type != acl.pattern.resource_type {
		return false
	}
	if name := filter.pattern_filter.name {
		if name != acl.pattern.name {
			return false
		}
	}
	if filter.pattern_filter.pattern_type != .any && filter.pattern_filter.pattern_type != .match
		&& filter.pattern_filter.pattern_type != acl.pattern.pattern_type {
		return false
	}

	// 액세스 제어 항목 필터 확인
	if principal := filter.entry_filter.principal {
		if principal != acl.entry.principal {
			return false
		}
	}
	if host := filter.entry_filter.host {
		if host != acl.entry.host {
			return false
		}
	}
	if filter.entry_filter.operation != .any && filter.entry_filter.operation != acl.entry.operation {
		return false
	}
	if filter.entry_filter.permission_type != .any
		&& filter.entry_filter.permission_type != acl.entry.permission_type {
		return false
	}

	return true
}

/// matches_resource는 패턴이 리소스와 일치하는지 확인합니다.
fn matches_resource(pattern domain.ResourcePattern, resource domain.ResourcePattern) bool {
	if pattern.resource_type != resource.resource_type {
		return false
	}

	match pattern.pattern_type {
		.literal {
			return pattern.name == resource.name
		}
		.prefixed {
			return resource.name.starts_with(pattern.name)
		}
		else {
			return false
		}
	}
}

/// matches_principal은 ACL 주체가 요청 주체와 일치하는지 확인합니다.
/// 'User:*'는 모든 사용자와 일치합니다.
fn matches_principal(acl_principal string, request_principal string) bool {
	if acl_principal == 'User:*' {
		return true
	}
	return acl_principal == request_principal
}

/// matches_host는 ACL 호스트가 요청 호스트와 일치하는지 확인합니다.
/// '*'는 모든 호스트와 일치합니다.
fn matches_host(acl_host string, request_host string) bool {
	if acl_host == '*' {
		return true
	}
	return acl_host == request_host
}

/// matches_operation은 ACL 작업이 요청 작업과 일치하는지 확인합니다.
/// .all은 모든 작업과 일치합니다.
fn matches_operation(acl_operation domain.AclOperation, request_operation domain.AclOperation) bool {
	if acl_operation == .all {
		return true
	}
	return acl_operation == request_operation
}
