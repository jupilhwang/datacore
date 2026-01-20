// Infra Layer - Kafka Request Parsing - ACL Operations
// DescribeAcls, CreateAcls, DeleteAcls request parsing
module kafka

// ============================================================================
// DescribeAcls Request (API Key 29)
// ============================================================================

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

fn parse_describe_acls_request(mut reader BinaryReader, version i16, is_flexible bool) !DescribeAclsRequest {
	resource_type := reader.read_i8()!
	resource_name := if is_flexible {
		reader.read_compact_nullable_string()!
	} else {
		reader.read_nullable_string()!
	}
	// v1+: pattern_type
	pattern_type := if version >= 1 { reader.read_i8()! } else { i8(3) } // Default to LITERAL (3)

	principal := if is_flexible {
		reader.read_compact_nullable_string()!
	} else {
		reader.read_nullable_string()!
	}
	host := if is_flexible {
		reader.read_compact_nullable_string()!
	} else {
		reader.read_nullable_string()!
	}
	operation := reader.read_i8()!
	permission_type := reader.read_i8()!

	if is_flexible {
		reader.skip_tagged_fields()!
	}

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

// ============================================================================
// CreateAcls Request (API Key 30)
// ============================================================================

pub struct CreateAclsRequest {
pub:
	creations []CreateAclsCreation
}

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

fn parse_create_acls_request(mut reader BinaryReader, version i16, is_flexible bool) !CreateAclsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut creations := []CreateAclsCreation{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
		// v1+: pattern_type
		pattern_type := if version >= 1 { reader.read_i8()! } else { i8(3) } // Default to LITERAL (3)

		principal := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
		host := if is_flexible {
			reader.read_compact_string()!
		} else {
			reader.read_string()!
		}
		operation := reader.read_i8()!
		permission_type := reader.read_i8()!

		if is_flexible {
			reader.skip_tagged_fields()!
		}

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

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return CreateAclsRequest{
		creations: creations
	}
}

// ============================================================================
// DeleteAcls Request (API Key 31)
// ============================================================================

pub struct DeleteAclsRequest {
pub:
	filters []DeleteAclsFilter
}

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

fn parse_delete_acls_request(mut reader BinaryReader, version i16, is_flexible bool) !DeleteAclsRequest {
	count := if is_flexible { reader.read_compact_array_len()! } else { reader.read_array_len()! }
	mut filters := []DeleteAclsFilter{}

	for _ in 0 .. count {
		resource_type := reader.read_i8()!
		resource_name := if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
		// v1+: pattern_type
		pattern_type := if version >= 1 { reader.read_i8()! } else { i8(3) } // Default to LITERAL (3)

		principal := if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
		host := if is_flexible {
			reader.read_compact_nullable_string()!
		} else {
			reader.read_nullable_string()!
		}
		operation := reader.read_i8()!
		permission_type := reader.read_i8()!

		if is_flexible {
			reader.skip_tagged_fields()!
		}

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

	if is_flexible {
		reader.skip_tagged_fields()!
	}

	return DeleteAclsRequest{
		filters: filters
	}
}
