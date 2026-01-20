// Adapter Layer - Kafka ACL Response Building
// DescribeAcls, CreateAcls, DeleteAcls responses
module kafka

// ============================================================================
// DescribeAcls Response (API Key 29)
// ============================================================================

pub struct DescribeAclsResponse {
pub:
	throttle_time_ms i32
	error_code       i16
	error_message    ?string
	resources        []DescribeAclsResource
}

pub struct DescribeAclsResource {
pub:
	resource_type i8
	resource_name string
	pattern_type  i8
	acls          []DescribeAclsAcl
}

pub struct DescribeAclsAcl {
pub:
	principal       string
	host            string
	operation       i8
	permission_type i8
}

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

// ============================================================================
// CreateAcls Response (API Key 30)
// ============================================================================

pub struct CreateAclsResponse {
pub:
	throttle_time_ms i32
	results          []CreateAclsResult
}

pub struct CreateAclsResult {
pub:
	error_code    i16
	error_message ?string
}

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

// ============================================================================
// DeleteAcls Response (API Key 31)
// ============================================================================

pub struct DeleteAclsResponse {
pub:
	throttle_time_ms i32
	results          []DeleteAclsResult
}

pub struct DeleteAclsResult {
pub:
	error_code    i16
	error_message ?string
	matching_acls []DeleteAclsMatchingAcl
}

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
