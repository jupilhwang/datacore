// Kafka 프로토콜 - ACL 작업
// DescribeAcls, CreateAcls, DeleteAcls
// 요청/응답 타입, 파싱, 인코딩 및 핸들러
module kafka

import domain
import infra.observability
import time

// DescribeAcls (API Key 29)

/// DescribeAclsRequest은 DescribeAcls (API Key 29).
pub struct DescribeAclsRequest {
pub:
	resource_type   i8
	resource_name   ?string
	pattern_type    i8
	principal       ?string
	host            ?string
	operation       i8
	permission_type i8
}

/// DescribeAclsResponse는 관련 데이터를 담는 구조체입니다.
pub struct DescribeAclsResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    ?string
	resources        []DescribeAclsResource
}

/// DescribeAclsResource는 관련 데이터를 담는 구조체입니다.
pub struct DescribeAclsResource {
pub:
	resource_type i8
	resource_name string
	pattern_type  i8
	acls          []DescribeAclsAcl
}

/// DescribeAclsAcl는 관련 데이터를 담는 구조체입니다.
pub struct DescribeAclsAcl {
pub:
	principal       string
	host            string
	operation       i8
	permission_type i8
}

fn parse_describe_acls_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeAclsRequest {
	resource_type := reader.read_i8()!
	resource_name := reader.read_flex_nullable_string(is_flexible)!
	// v1+: pattern_type
	pattern_type := if version >= 1 { reader.read_i8()! } else { i8(3) } // Default to LITERAL (3)

	principal := reader.read_flex_nullable_string(is_flexible)!
	host := reader.read_flex_nullable_string(is_flexible)!
	operation := reader.read_i8()!
	permission_type := reader.read_i8()!

	reader.skip_flex_tagged_fields(is_flexible)!

	return DescribeAclsRequest{
		resource_type:   resource_type
		resource_name:   if resource_name.len > 0 { ?string(resource_name) } else { none }
		pattern_type:    pattern_type
		principal:       if principal.len > 0 { ?string(principal) } else { none }
		host:            if host.len > 0 { ?string(host) } else { none }
		operation:       operation
		permission_type: permission_type
	}
}

/// encode를 수행합니다.
pub fn (r DescribeAclsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)
	writer.write_i16(r.error_code)
	if is_flexible {
		writer.write_compact_nullable_string(r.error_message)
	} else {
		writer.write_nullable_string(r.error_message)
	}

	if is_flexible {
		writer.write_compact_array_len(r.resources.len)
	} else {
		writer.write_array_len(r.resources.len)
	}

	for res in r.resources {
		writer.write_i8(res.resource_type)
		if is_flexible {
			writer.write_compact_string(res.resource_name)
		} else {
			writer.write_string(res.resource_name)
		}
		// v1+: pattern_type
		if version >= 1 {
			writer.write_i8(res.pattern_type)
		}

		if is_flexible {
			writer.write_compact_array_len(res.acls.len)
		} else {
			writer.write_array_len(res.acls.len)
		}

		for acl in res.acls {
			if is_flexible {
				writer.write_compact_string(acl.principal)
				writer.write_compact_string(acl.host)
			} else {
				writer.write_string(acl.principal)
				writer.write_string(acl.host)
			}
			writer.write_i8(acl.operation)
			writer.write_i8(acl.permission_type)

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// CreateAcls (API Key 30)

/// CreateAclsRequest은 CreateAcls (API Key 30).
pub struct CreateAclsRequest {
pub:
	creations []CreateAclsCreation
}

/// CreateAclsCreation는 관련 데이터를 담는 구조체입니다.
pub struct CreateAclsCreation {
pub:
	resource_type   i8
	resource_name   string
	pattern_type    i8
	principal       string
	host            string
	operation       i8
	permission_type i8
}

/// CreateAclsResponse는 관련 데이터를 담는 구조체입니다.
pub struct CreateAclsResponse {
pub:
	throttle_time_ms i32
	results          []CreateAclsResult
}

/// CreateAclsResult는 관련 데이터를 담는 구조체입니다.
pub struct CreateAclsResult {
pub:
	error_code    i16
	error_message ?string
}

fn parse_create_acls_request(mut reader BinaryReader, version i16, is_flexible bool) !CreateAclsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut creations := []CreateAclsCreation{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := reader.read_flex_string(is_flexible)!
		// v1+: pattern_type
		pattern_type := if version >= 1 { reader.read_i8()! } else { i8(3) } // Default to LITERAL (3)

		principal := reader.read_flex_string(is_flexible)!
		host := reader.read_flex_string(is_flexible)!
		operation := reader.read_i8()!
		permission_type := reader.read_i8()!

		reader.skip_flex_tagged_fields(is_flexible)!

		creations << CreateAclsCreation{
			resource_type:   resource_type
			resource_name:   resource_name
			pattern_type:    pattern_type
			principal:       principal
			host:            host
			operation:       operation
			permission_type: permission_type
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return CreateAclsRequest{
		creations: creations
	}
}

/// encode를 수행합니다.
pub fn (r CreateAclsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		writer.write_i16(res.error_code)
		if is_flexible {
			writer.write_compact_nullable_string(res.error_message)
			writer.write_tagged_fields()
		} else {
			writer.write_nullable_string(res.error_message)
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// DeleteAcls (API Key 31)

/// DeleteAclsRequest은 DeleteAcls (API Key 31).
pub struct DeleteAclsRequest {
pub:
	filters []DeleteAclsFilter
}

/// DeleteAclsFilter는 관련 데이터를 담는 구조체입니다.
pub struct DeleteAclsFilter {
pub:
	resource_type   i8
	resource_name   ?string
	pattern_type    i8
	principal       ?string
	host            ?string
	operation       i8
	permission_type i8
}

/// DeleteAclsResponse는 관련 데이터를 담는 구조체입니다.
pub struct DeleteAclsResponse {
pub:
	throttle_time_ms i32
	results          []DeleteAclsResult
}

/// DeleteAclsResult는 관련 데이터를 담는 구조체입니다.
pub struct DeleteAclsResult {
pub:
	error_code    i16
	error_message ?string
	matching_acls []DeleteAclsMatchingAcl
}

/// DeleteAclsMatchingAcl는 관련 데이터를 담는 구조체입니다.
pub struct DeleteAclsMatchingAcl {
pub:
	error_code      i16
	error_message   ?string
	resource_type   i8
	resource_name   string
	pattern_type    i8
	principal       string
	host            string
	operation       i8
	permission_type i8
}

fn parse_delete_acls_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteAclsRequest {
	count := reader.read_flex_array_len(is_flexible)!
	mut filters := []DeleteAclsFilter{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := reader.read_flex_nullable_string(is_flexible)!
		// v1+: pattern_type
		pattern_type := if version >= 1 { reader.read_i8()! } else { i8(3) } // Default to LITERAL (3)

		principal := reader.read_flex_nullable_string(is_flexible)!
		host := reader.read_flex_nullable_string(is_flexible)!
		operation := reader.read_i8()!
		permission_type := reader.read_i8()!

		reader.skip_flex_tagged_fields(is_flexible)!

		filters << DeleteAclsFilter{
			resource_type:   resource_type
			resource_name:   if resource_name.len > 0 { ?string(resource_name) } else { none }
			pattern_type:    pattern_type
			principal:       if principal.len > 0 { ?string(principal) } else { none }
			host:            if host.len > 0 { ?string(host) } else { none }
			operation:       operation
			permission_type: permission_type
		}
	}

	reader.skip_flex_tagged_fields(is_flexible)!

	return DeleteAclsRequest{
		filters: filters
	}
}

/// encode를 수행합니다.
pub fn (r DeleteAclsResponse) encode(version i16) []u8 {
	is_flexible := version >= 2
	mut writer := new_writer()

	writer.write_i32(r.throttle_time_ms)

	if is_flexible {
		writer.write_compact_array_len(r.results.len)
	} else {
		writer.write_array_len(r.results.len)
	}

	for res in r.results {
		writer.write_i16(res.error_code)
		if is_flexible {
			writer.write_compact_nullable_string(res.error_message)
			writer.write_compact_array_len(res.matching_acls.len)
		} else {
			writer.write_nullable_string(res.error_message)
			writer.write_array_len(res.matching_acls.len)
		}

		for acl in res.matching_acls {
			writer.write_i16(acl.error_code)
			if is_flexible {
				writer.write_compact_nullable_string(acl.error_message)
			} else {
				writer.write_nullable_string(acl.error_message)
			}
			writer.write_i8(acl.resource_type)
			if is_flexible {
				writer.write_compact_string(acl.resource_name)
			} else {
				writer.write_string(acl.resource_name)
			}
			// v1+: pattern_type
			if version >= 1 {
				writer.write_i8(acl.pattern_type)
			}
			if is_flexible {
				writer.write_compact_string(acl.principal)
				writer.write_compact_string(acl.host)
			} else {
				writer.write_string(acl.principal)
				writer.write_string(acl.host)
			}
			writer.write_i8(acl.operation)
			writer.write_i8(acl.permission_type)

			if is_flexible {
				writer.write_tagged_fields()
			}
		}

		if is_flexible {
			writer.write_tagged_fields()
		}
	}

	if is_flexible {
		writer.write_tagged_fields()
	}

	return writer.bytes()
}

// ACL 핸들러

// DescribeAcls 핸들러
fn (mut h Handler) handle_describe_acls(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_describe_acls_request(mut reader, version, is_flexible_version(.describe_acls,
		version))!

	h.logger.debug('Processing describe ACLs', observability.field_int('resource_type',
		req.resource_type), observability.field_int('operation', req.operation))

	// ACL 매니저가 설정되어 있는지 확인
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
				throttle_time_ms: default_throttle_time_ms
				error_code:       i16(ErrorCode.unknown_server_error)
				error_message:    err.msg()
				resources:        []
			}
			return resp.encode(version)
		}

		// Group ACLs by resource for response
		mut resources := []DescribeAclsResource{}
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
			throttle_time_ms: default_throttle_time_ms
			error_code:       0
			error_message:    none
			resources:        resources
		}

		elapsed := time.since(start_time)
		h.logger.debug('Describe ACLs completed', observability.field_int('resources',
			resources.len), observability.field_duration('latency', elapsed))

		return resp.encode(version)
	}

	// ACL not supported/configured
	elapsed := time.since(start_time)
	h.logger.debug('Describe ACLs: security disabled', observability.field_duration('latency',
		elapsed))

	resp := DescribeAclsResponse{
		throttle_time_ms: default_throttle_time_ms
		error_code:       i16(ErrorCode.security_disabled)
		error_message:    'ACLs are not enabled'
		resources:        []
	}
	return resp.encode(version)
}

// CreateAcls 핸들러
fn (mut h Handler) handle_create_acls(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_create_acls_request(mut reader, version, is_flexible_version(.create_acls,
		version))!

	h.logger.debug('Processing create ACLs', observability.field_int('creations', req.creations.len))

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
				throttle_time_ms: default_throttle_time_ms
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
			throttle_time_ms: default_throttle_time_ms
			results:          results
		}.encode(version)
	}

	// ACL not supported
	elapsed := time.since(start_time)
	h.logger.debug('Create ACLs: security disabled', observability.field_duration('latency',
		elapsed))

	mut results := []CreateAclsResult{}
	for _ in req.creations {
		results << CreateAclsResult{
			error_code:    i16(ErrorCode.security_disabled)
			error_message: 'ACLs are not enabled'
		}
	}
	return CreateAclsResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}.encode(version)
}

// DeleteAcls 핸들러
fn (mut h Handler) handle_delete_acls(body []u8, version i16) ![]u8 {
	start_time := time.now()
	mut reader := new_reader(body)
	req := parse_delete_acls_request(mut reader, version, is_flexible_version(.delete_acls,
		version))!

	h.logger.debug('Processing delete ACLs', observability.field_int('filters', req.filters.len))

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
				throttle_time_ms: default_throttle_time_ms
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
			throttle_time_ms: default_throttle_time_ms
			results:          results
		}.encode(version)
	}

	// ACL not supported
	elapsed := time.since(start_time)
	h.logger.debug('Delete ACLs: security disabled', observability.field_duration('latency',
		elapsed))

	mut results := []DeleteAclsResult{}
	for _ in req.filters {
		results << DeleteAclsResult{
			error_code:    i16(ErrorCode.security_disabled)
			error_message: 'ACLs are not enabled'
			matching_acls: []
		}
	}
	return DeleteAclsResponse{
		throttle_time_ms: default_throttle_time_ms
		results:          results
	}.encode(version)
}
