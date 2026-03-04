// Interface Layer - CLI ACL Commands
//
// Provides ACL management commands using the Kafka protocol.
// Supports creating, listing, and deleting Access Control List entries.
//
// Key features:
// - Create ACL bindings
// - List ACL bindings with optional filters
// - Delete ACL bindings
module cli

/// AclOptions holds ACL command options.
pub struct AclOptions {
pub:
	bootstrap_server string = 'localhost:9092'
	principal        string
	host             string = '*'
	operation        string = 'All'
	resource_type    string = 'Topic'
	resource_name    string
	pattern_type     string = 'Literal'
	permission       string = 'Allow'
}

/// parse_acl_options parses ACL command options.
pub fn parse_acl_options(args []string) AclOptions {
	mut opts := AclOptions{}

	mut i := 0
	for i < args.len {
		match args[i] {
			'--bootstrap-server', '-b' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						bootstrap_server: args[i + 1]
					}
					i += 1
				}
			}
			'--principal' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						principal: args[i + 1]
					}
					i += 1
				}
			}
			'--host' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						host: args[i + 1]
					}
					i += 1
				}
			}
			'--operation', '--op' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						operation: args[i + 1]
					}
					i += 1
				}
			}
			'--resource-type' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						resource_type: args[i + 1]
					}
					i += 1
				}
			}
			'--resource', '--resource-name' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						resource_name: args[i + 1]
					}
					i += 1
				}
			}
			'--pattern-type' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						pattern_type: args[i + 1]
					}
					i += 1
				}
			}
			'--permission' {
				if i + 1 < args.len {
					opts = AclOptions{
						...opts
						permission: args[i + 1]
					}
					i += 1
				}
			}
			else {}
		}
		i += 1
	}

	return opts
}

/// run_acl_create creates a new ACL binding.
pub fn run_acl_create(opts AclOptions) ! {
	if opts.principal == '' {
		return error('Principal is required. Use: --principal User:alice')
	}
	if opts.resource_name == '' {
		return error('Resource name is required. Use: --resource <name>')
	}

	resource_type_code := resource_type_to_code(opts.resource_type)
	operation_code := operation_to_code(opts.operation)
	permission_code := permission_to_code(opts.permission)
	pattern_type_code := pattern_type_to_code(opts.pattern_type)

	println('\x1b[90mCreating ACL...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	request := build_create_acls_request(opts.principal, opts.host, resource_type_code,
		opts.resource_name, pattern_type_code, operation_code, permission_code)
	send_kafka_request(mut conn, 30, 3, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 8 {
		return error('Failed to create ACL: invalid response')
	}

	println('\x1b[32m\u2713\x1b[0m ACL created successfully')
	println('  Principal:     ${opts.principal}')
	println('  Host:          ${opts.host}')
	println('  Resource Type: ${opts.resource_type}')
	println('  Resource:      ${opts.resource_name}')
	println('  Operation:     ${opts.operation}')
	println('  Permission:    ${opts.permission}')
}

/// run_acl_list lists ACL bindings with optional filters.
pub fn run_acl_list(opts AclOptions) ! {
	println('\x1b[90mListing ACLs...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	resource_type_code := if opts.resource_type == 'Topic' { u8(2) } else { u8(1) } // 1 = any
	operation_code := u8(1) // 1 = any
	permission_code := u8(1) // 1 = any
	pattern_type_code := u8(1) // 1 = any

	request := build_describe_acls_request(opts.principal, opts.host, resource_type_code,
		opts.resource_name, pattern_type_code, operation_code, permission_code)
	send_kafka_request(mut conn, 29, 3, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 8 {
		return error('Failed to list ACLs: invalid response')
	}

	acls := parse_describe_acls_response(response)

	if acls.len == 0 {
		println('\x1b[33mNo ACLs found\x1b[0m')
		return
	}

	println('')
	println('\x1b[33mACLs:\x1b[0m')
	println('  RESOURCE TYPE   RESOURCE NAME              PRINCIPAL')
	println('  OPERATION       PERMISSION')
	println('  ' + '-'.repeat(70))
	for acl in acls {
		println('  ${acl.resource_type}  /  ${acl.resource_name}')
		println('    Principal: ${acl.principal}  Host: ${acl.host}')
		println('    Operation: ${acl.operation}  Permission: ${acl.permission}')
		println('')
	}
}

/// run_acl_delete deletes an ACL binding.
pub fn run_acl_delete(opts AclOptions) ! {
	if opts.principal == '' {
		return error('Principal is required. Use: --principal User:alice')
	}
	if opts.resource_name == '' {
		return error('Resource name is required. Use: --resource <name>')
	}

	resource_type_code := resource_type_to_code(opts.resource_type)
	operation_code := operation_to_code(opts.operation)
	permission_code := permission_to_code(opts.permission)
	pattern_type_code := pattern_type_to_code(opts.pattern_type)

	println('\x1b[90mDeleting ACL...\x1b[0m')

	mut conn := connect_broker(opts.bootstrap_server)!
	defer { conn.close() or {} }

	request := build_delete_acls_request(opts.principal, opts.host, resource_type_code,
		opts.resource_name, pattern_type_code, operation_code, permission_code)
	send_kafka_request(mut conn, 31, 3, request)!
	response := read_kafka_response(mut conn)!

	if response.len < 8 {
		return error('Failed to delete ACL: invalid response')
	}

	println('\x1b[32m\u2713\x1b[0m ACL deleted successfully')
}

/// print_acl_help prints ACL command help.
pub fn print_acl_help() {
	println('\x1b[33mACL Commands:\x1b[0m')
	println('')
	println('Usage: datacore acl <command> [options]')
	println('')
	println('Commands:')
	println('  create     Create an ACL binding')
	println('  list       List ACL bindings')
	println('  delete     Delete an ACL binding')
	println('')
	println('Options:')
	println('  -b, --bootstrap-server  Broker address (default: localhost:9092)')
	println('      --principal         Principal (e.g. User:alice)')
	println('      --host              Host (* for all hosts, default: *)')
	println('      --operation         Operation: Read, Write, Create, Delete, Alter, Describe, All')
	println('      --resource-type     Resource type: Topic, Group, Cluster (default: Topic)')
	println('      --resource          Resource name')
	println('      --pattern-type      Pattern type: Literal, Prefixed (default: Literal)')
	println('      --permission        Permission: Allow, Deny (default: Allow)')
	println('')
	println('Examples:')
	println('  datacore acl create --principal User:alice --resource my-topic --operation Read')
	println('  datacore acl list --resource-type Topic')
	println('  datacore acl list --principal User:alice')
	println('  datacore acl delete --principal User:alice --resource my-topic --operation Read')
}

// AclEntry holds parsed ACL information
struct AclEntry {
	resource_type string
	resource_name string
	principal     string
	host          string
	operation     string
	permission    string
}

fn resource_type_to_code(resource_type string) u8 {
	return match resource_type.to_lower() {
		'topic' { 2 }
		'group' { 3 }
		'cluster' { 4 }
		'transactionalid' { 5 }
		'delegationtoken' { 6 }
		'user' { 7 }
		else { 2 } // default to topic
	}
}

fn operation_to_code(operation string) u8 {
	return match operation.to_lower() {
		'all' { 2 }
		'read' { 3 }
		'write' { 4 }
		'create' { 5 }
		'delete' { 6 }
		'alter' { 7 }
		'describe' { 8 }
		'clusteraction' { 9 }
		'describeconfigs' { 10 }
		'alterconfigs' { 11 }
		'idempotentwrite' { 12 }
		else { 2 } // default to all
	}
}

fn permission_to_code(permission string) u8 {
	return match permission.to_lower() {
		'allow' { 2 }
		'deny' { 3 }
		else { 2 } // default to allow
	}
}

fn pattern_type_to_code(pattern_type string) u8 {
	return match pattern_type.to_lower() {
		'literal' { 3 }
		'prefixed' { 4 }
		'any' { 1 }
		'match' { 2 }
		else { 3 } // default to literal
	}
}

fn resource_type_from_code(code u8) string {
	return match code {
		2 { 'Topic' }
		3 { 'Group' }
		4 { 'Cluster' }
		5 { 'TransactionalId' }
		6 { 'DelegationToken' }
		7 { 'User' }
		else { 'Unknown' }
	}
}

fn operation_from_code(code u8) string {
	return match code {
		2 { 'All' }
		3 { 'Read' }
		4 { 'Write' }
		5 { 'Create' }
		6 { 'Delete' }
		7 { 'Alter' }
		8 { 'Describe' }
		9 { 'ClusterAction' }
		10 { 'DescribeConfigs' }
		11 { 'AlterConfigs' }
		12 { 'IdempotentWrite' }
		else { 'Unknown' }
	}
}

fn permission_from_code(code u8) string {
	return match code {
		2 { 'Allow' }
		3 { 'Deny' }
		else { 'Unknown' }
	}
}

fn build_create_acls_request(principal string, host string, resource_type u8, resource_name string, pattern_type u8, operation u8, permission u8) []u8 {
	mut body := []u8{}

	// Creations array (compact array, 1 entry)
	body << u8(2)

	// ResourceType (1 byte)
	body << resource_type

	// Resource name (compact string)
	body << u8(resource_name.len + 1)
	body << resource_name.bytes()

	// PatternType (1 byte)
	body << pattern_type

	// Principal (compact string)
	body << u8(principal.len + 1)
	body << principal.bytes()

	// Host (compact string)
	body << u8(host.len + 1)
	body << host.bytes()

	// Operation (1 byte)
	body << operation

	// PermissionType (1 byte)
	body << permission

	// Tagged fields for creation entry
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

fn build_describe_acls_request(principal string, host string, resource_type u8, resource_name string, pattern_type u8, operation u8, permission u8) []u8 {
	mut body := []u8{}

	// ResourceTypeFilter (1 byte)
	body << resource_type

	// ResourceNameFilter (compact nullable string)
	if resource_name != '' {
		body << u8(resource_name.len + 1)
		body << resource_name.bytes()
	} else {
		body << u8(0) // null
	}

	// PatternTypeFilter (1 byte)
	body << pattern_type

	// PrincipalFilter (compact nullable string)
	if principal != '' {
		body << u8(principal.len + 1)
		body << principal.bytes()
	} else {
		body << u8(0) // null
	}

	// HostFilter (compact nullable string)
	if host != '' && host != '*' {
		body << u8(host.len + 1)
		body << host.bytes()
	} else {
		body << u8(0) // null
	}

	// OperationFilter (1 byte)
	body << operation

	// PermissionTypeFilter (1 byte)
	body << permission

	// Tagged fields
	body << u8(0)

	return body
}

fn build_delete_acls_request(principal string, host string, resource_type u8, resource_name string, pattern_type u8, operation u8, permission u8) []u8 {
	mut body := []u8{}

	// Filters array (compact array, 1 entry)
	body << u8(2)

	// ResourceTypeFilter (1 byte)
	body << resource_type

	// ResourceNameFilter (compact nullable string)
	if resource_name != '' {
		body << u8(resource_name.len + 1)
		body << resource_name.bytes()
	} else {
		body << u8(0)
	}

	// PatternTypeFilter (1 byte)
	body << pattern_type

	// PrincipalFilter (compact nullable string)
	if principal != '' {
		body << u8(principal.len + 1)
		body << principal.bytes()
	} else {
		body << u8(0)
	}

	// HostFilter (compact nullable string)
	if host != '' && host != '*' {
		body << u8(host.len + 1)
		body << host.bytes()
	} else {
		body << u8(0)
	}

	// OperationFilter (1 byte)
	body << operation

	// PermissionTypeFilter (1 byte)
	body << permission

	// Tagged fields
	body << u8(0)

	// Tagged fields for request
	body << u8(0)

	return body
}

fn parse_describe_acls_response(response []u8) []AclEntry {
	mut acls := []AclEntry{}

	if response.len < 12 {
		return acls
	}

	mut pos := 4 // skip correlation_id

	// Tagged fields
	if pos >= response.len {
		return acls
	}
	pos += 1

	// Throttle time (4 bytes)
	pos += 4

	// Error code (2 bytes)
	if pos + 2 > response.len {
		return acls
	}
	error_code := read_i16_be(response, pos)
	pos += 2

	if error_code != 0 {
		return acls
	}

	// Error message (compact nullable string)
	if pos >= response.len {
		return acls
	}
	msg_len := int(response[pos]) - 1
	pos += 1
	if msg_len > 0 && pos + msg_len <= response.len {
		pos += msg_len
	}

	// Resources array count
	if pos >= response.len {
		return acls
	}
	resources_count := int(response[pos]) - 1
	pos += 1

	for _ in 0 .. resources_count {
		if pos >= response.len {
			break
		}

		// ResourceType (1 byte)
		resource_type := resource_type_from_code(response[pos])
		pos += 1

		// ResourceName (compact string)
		if pos >= response.len {
			break
		}
		res_name_len := int(response[pos]) - 1
		pos += 1
		if pos + res_name_len > response.len {
			break
		}
		resource_name := response[pos..pos + res_name_len].bytestr()
		pos += res_name_len

		// PatternType (1 byte)
		if pos >= response.len {
			break
		}
		pos += 1

		// ACLs array count
		if pos >= response.len {
			break
		}
		acls_count := int(response[pos]) - 1
		pos += 1

		for _ in 0 .. acls_count {
			if pos >= response.len {
				break
			}

			// Principal (compact string)
			prin_len := int(response[pos]) - 1
			pos += 1
			if pos + prin_len > response.len {
				break
			}
			principal := response[pos..pos + prin_len].bytestr()
			pos += prin_len

			// Host (compact string)
			if pos >= response.len {
				break
			}
			host_len := int(response[pos]) - 1
			pos += 1
			if pos + host_len > response.len {
				break
			}
			host := response[pos..pos + host_len].bytestr()
			pos += host_len

			// Operation (1 byte)
			if pos >= response.len {
				break
			}
			operation := operation_from_code(response[pos])
			pos += 1

			// PermissionType (1 byte)
			if pos >= response.len {
				break
			}
			permission := permission_from_code(response[pos])
			pos += 1

			// Tagged fields
			if pos < response.len {
				pos += 1
			}

			acls << AclEntry{
				resource_type: resource_type
				resource_name: resource_name
				principal:     principal
				host:          host
				operation:     operation
				permission:    permission
			}
		}

		// Tagged fields for resource
		if pos < response.len {
			pos += 1
		}
	}

	return acls
}
