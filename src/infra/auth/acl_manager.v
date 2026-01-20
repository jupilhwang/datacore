// Infra Layer - Memory-based ACL Manager
module auth

import domain
import sync

// MemoryAclManager implements AclManager interface with in-memory storage
pub struct MemoryAclManager {
mut:
	acls []domain.AclBinding
	lock sync.RwMutex
}

// new_memory_acl_manager creates a new in-memory ACL manager
pub fn new_memory_acl_manager() &MemoryAclManager {
	return &MemoryAclManager{
		acls: []domain.AclBinding{}
	}
}

// create_acls creates new ACL bindings
pub fn (mut m MemoryAclManager) create_acls(acls []domain.AclBinding) ![]domain.AclCreateResult {
	m.lock.@lock()
	defer { m.lock.unlock() }

	mut results := []domain.AclCreateResult{}

	for acl in acls {
		// Check if ACL already exists
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

// delete_acls deletes ACL bindings matching the filters
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

// describe_acls returns ACL bindings matching the filter
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

// authorize checks if the operation is allowed
pub fn (mut m MemoryAclManager) authorize(principal string, host string, operation domain.AclOperation, resource domain.ResourcePattern) !bool {
	m.lock.@rlock()
	defer { m.lock.runlock() }

	// 1. Check for Deny permissions first (Deny overrides Allow)
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

	// 2. Check for Allow permissions
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

	// Default deny if no matching Allow ACL found
	// Note: In Kafka, if no ACLs exist for a resource, it depends on allow.everyone.if.no.acl.found config.
	// For now, we default to deny (safe default).
	return false
}

// Helper functions

fn matches_filter(acl domain.AclBinding, filter domain.AclBindingFilter) bool {
	// Resource Pattern Filter
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

	// Access Control Entry Filter
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

fn matches_principal(acl_principal string, request_principal string) bool {
	if acl_principal == 'User:*' {
		return true
	}
	return acl_principal == request_principal
}

fn matches_host(acl_host string, request_host string) bool {
	if acl_host == '*' {
		return true
	}
	return acl_host == request_host
}

fn matches_operation(acl_operation domain.AclOperation, request_operation domain.AclOperation) bool {
	if acl_operation == .all {
		return true
	}
	return acl_operation == request_operation
}
