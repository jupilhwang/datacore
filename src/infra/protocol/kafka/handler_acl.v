// Infra Layer - Kafka Protocol Handler - ACL Operations
// DescribeAcls, CreateAcls, DeleteAcls handlers
module kafka

import domain

// ============================================================================
// ACL Handlers
// ============================================================================

// DescribeAcls handler
fn (mut h Handler) handle_describe_acls(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_describe_acls_request(mut reader, version, is_flexible_version(.describe_acls,
		version))!

	// Check if ACL manager is configured
	if mut acl_mgr := h.acl_manager {
		filter := domain.AclBindingFilter{
			pattern_filter: domain.ResourcePatternFilter{
				resource_type: domain.resource_type_from_i8(req.resource_type)
				name:          req.resource_name
				pattern_type:  domain.pattern_type_from_i8(req.pattern_type)
			}
			entry_filter:   domain.AccessControlEntryFilter{
				principal:       req.principal
				host:            req.host
				operation:       domain.acl_operation_from_i8(req.operation)
				permission_type: domain.permission_type_from_i8(req.permission_type)
			}
		}

		acls := acl_mgr.describe_acls(filter) or {
			// Failed to describe ACLs
			resp := DescribeAclsResponse{
				throttle_time_ms: 0
				error_code:       i16(ErrorCode.unknown_server_error)
				error_message:    err.msg()
				resources:        []
			}
			return resp.encode(version)
		}

		// Group ACLs by resource for response
		mut resources := []DescribeAclsResource{}
		// Simple grouping: iterate and group manually or just return flat list if structure allows
		// Kafka response groups by resource.
		// Since we get flat list of AclBinding, we need to group them.

		mut resource_map := map[string][]DescribeAclsAcl{}
		mut resource_patterns := map[string]domain.ResourcePattern{}

		for acl in acls {
			key := '${acl.pattern.resource_type}:${acl.pattern.name}:${acl.pattern.pattern_type}'
			if key !in resource_map {
				resource_map[key] = []DescribeAclsAcl{}
				resource_patterns[key] = acl.pattern
			}
			resource_map[key] << DescribeAclsAcl{
				principal:       acl.entry.principal
				host:            acl.entry.host
				operation:       i8(acl.entry.operation)
				permission_type: i8(acl.entry.permission_type)
			}
		}

		for key, entries in resource_map {
			pattern := resource_patterns[key]
			resources << DescribeAclsResource{
				resource_type: i8(pattern.resource_type)
				resource_name: pattern.name
				pattern_type:  i8(pattern.pattern_type)
				acls:          entries
			}
		}

		resp := DescribeAclsResponse{
			throttle_time_ms: 0
			error_code:       0
			error_message:    none
			resources:        resources
		}
		return resp.encode(version)
	}

	// ACL not supported/configured
	resp := DescribeAclsResponse{
		throttle_time_ms: 0
		error_code:       i16(ErrorCode.security_disabled)
		error_message:    'ACLs are not enabled'
		resources:        []
	}
	return resp.encode(version)
}

// CreateAcls handler
fn (mut h Handler) handle_create_acls(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_create_acls_request(mut reader, version, is_flexible_version(.create_acls,
		version))!

	if mut acl_mgr := h.acl_manager {
		mut bindings := []domain.AclBinding{}
		for c in req.creations {
			bindings << domain.AclBinding{
				pattern: domain.ResourcePattern{
					resource_type: domain.resource_type_from_i8(c.resource_type)
					name:          c.resource_name
					pattern_type:  domain.pattern_type_from_i8(c.pattern_type)
				}
				entry:   domain.AccessControlEntry{
					principal:       c.principal
					host:            c.host
					operation:       domain.acl_operation_from_i8(c.operation)
					permission_type: domain.permission_type_from_i8(c.permission_type)
				}
			}
		}

		create_results := acl_mgr.create_acls(bindings) or {
			// Global failure
			mut results := []CreateAclsResult{}
			for _ in req.creations {
				results << CreateAclsResult{
					error_code:    i16(ErrorCode.unknown_server_error)
					error_message: err.msg()
				}
			}
			return CreateAclsResponse{
				throttle_time_ms: 0
				results:          results
			}.encode(version)
		}

		mut results := []CreateAclsResult{}
		for r in create_results {
			results << CreateAclsResult{
				error_code:    r.error_code
				error_message: r.error_message
			}
		}

		return CreateAclsResponse{
			throttle_time_ms: 0
			results:          results
		}.encode(version)
	}

	// ACL not supported
	mut results := []CreateAclsResult{}
	for _ in req.creations {
		results << CreateAclsResult{
			error_code:    i16(ErrorCode.security_disabled)
			error_message: 'ACLs are not enabled'
		}
	}
	return CreateAclsResponse{
		throttle_time_ms: 0
		results:          results
	}.encode(version)
}

// DeleteAcls handler
fn (mut h Handler) handle_delete_acls(body []u8, version i16) ![]u8 {
	mut reader := new_reader(body)
	req := parse_delete_acls_request(mut reader, version, is_flexible_version(.delete_acls,
		version))!

	if mut acl_mgr := h.acl_manager {
		mut filters := []domain.AclBindingFilter{}
		for f in req.filters {
			filters << domain.AclBindingFilter{
				pattern_filter: domain.ResourcePatternFilter{
					resource_type: domain.resource_type_from_i8(f.resource_type)
					name:          f.resource_name
					pattern_type:  domain.pattern_type_from_i8(f.pattern_type)
				}
				entry_filter:   domain.AccessControlEntryFilter{
					principal:       f.principal
					host:            f.host
					operation:       domain.acl_operation_from_i8(f.operation)
					permission_type: domain.permission_type_from_i8(f.permission_type)
				}
			}
		}

		delete_results := acl_mgr.delete_acls(filters) or {
			// Global failure
			mut results := []DeleteAclsResult{}
			for _ in req.filters {
				results << DeleteAclsResult{
					error_code:    i16(ErrorCode.unknown_server_error)
					error_message: err.msg()
					matching_acls: []
				}
			}
			return DeleteAclsResponse{
				throttle_time_ms: 0
				results:          results
			}.encode(version)
		}

		mut results := []DeleteAclsResult{}
		for r in delete_results {
			mut matching_acls := []DeleteAclsMatchingAcl{}
			for acl in r.deleted_acls {
				matching_acls << DeleteAclsMatchingAcl{
					error_code:      0
					error_message:   none
					resource_type:   i8(acl.pattern.resource_type)
					resource_name:   acl.pattern.name
					pattern_type:    i8(acl.pattern.pattern_type)
					principal:       acl.entry.principal
					host:            acl.entry.host
					operation:       i8(acl.entry.operation)
					permission_type: i8(acl.entry.permission_type)
				}
			}

			results << DeleteAclsResult{
				error_code:    r.error_code
				error_message: r.error_message
				matching_acls: matching_acls
			}
		}

		return DeleteAclsResponse{
			throttle_time_ms: 0
			results:          results
		}.encode(version)
	}

	// ACL not supported
	mut results := []DeleteAclsResult{}
	for _ in req.filters {
		results << DeleteAclsResult{
			error_code:    i16(ErrorCode.security_disabled)
			error_message: 'ACLs are not enabled'
			matching_acls: []
		}
	}
	return DeleteAclsResponse{
		throttle_time_ms: 0
		results:          results
	}.encode(version)
}
