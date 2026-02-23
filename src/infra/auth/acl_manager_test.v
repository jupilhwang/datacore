/// Unit tests - Infrastructure layer: Memory ACL manager
module auth

import domain

/// test_memory_acl_manager tests the full functionality of MemoryAclManager.
/// Sequentially validates ACL creation, lookup, authorization, and deletion.
fn test_memory_acl_manager() {
	mut manager := new_memory_acl_manager()

	// 1. Create ACLs
	acls := [
		domain.AclBinding{
			pattern: domain.ResourcePattern{
				resource_type: .topic
				name:          'test-topic'
				pattern_type:  .literal
			}
			entry:   domain.AccessControlEntry{
				principal:       'User:alice'
				host:            '*'
				operation:       .read
				permission_type: .allow
			}
		},
		domain.AclBinding{
			pattern: domain.ResourcePattern{
				resource_type: .group
				name:          'test-group'
				pattern_type:  .prefixed
			}
			entry:   domain.AccessControlEntry{
				principal:       'User:bob'
				host:            '127.0.0.1'
				operation:       .write
				permission_type: .deny
			}
		},
	]

	create_results := manager.create_acls(acls) or { panic(err) }
	assert create_results.len == 2
	assert create_results[0].error_code == 0
	assert create_results[1].error_code == 0

	// 2. Lookup ACLs
	filter := domain.AclBindingFilter{
		pattern_filter: domain.ResourcePatternFilter{
			resource_type: .any
			name:          none
			pattern_type:  .any
		}
		entry_filter:   domain.AccessControlEntryFilter{
			principal:       none
			host:            none
			operation:       .any
			permission_type: .any
		}
	}

	found_acls := manager.describe_acls(filter) or { panic(err) }
	assert found_acls.len == 2

	// Filter by principal
	alice_filter := domain.AclBindingFilter{
		pattern_filter: domain.ResourcePatternFilter{
			resource_type: .any
			name:          none
			pattern_type:  .any
		}
		entry_filter:   domain.AccessControlEntryFilter{
			principal:       'User:alice'
			host:            none
			operation:       .any
			permission_type: .any
		}
	}
	alice_acls := manager.describe_acls(alice_filter) or { panic(err) }
	assert alice_acls.len == 1
	assert alice_acls[0].entry.principal == 'User:alice'

	// 3. Authorization check
	// Alice reads test-topic -> allowed
	allowed := manager.authorize('User:alice', '192.168.1.1', .read, domain.ResourcePattern{
		resource_type: .topic
		name:          'test-topic'
		pattern_type:  .literal
	}) or { false }
	assert allowed == true

	// Bob writes to test-group-1 (prefix match) -> denied
	denied := manager.authorize('User:bob', '127.0.0.1', .write, domain.ResourcePattern{
		resource_type: .group
		name:          'test-group-1'
		pattern_type:  .literal
	}) or { true }
	assert denied == false

	// 4. Delete ACLs
	delete_filter := domain.AclBindingFilter{
		pattern_filter: domain.ResourcePatternFilter{
			resource_type: .topic
			name:          'test-topic'
			pattern_type:  .literal
		}
		entry_filter:   domain.AccessControlEntryFilter{
			principal:       none
			host:            none
			operation:       .any
			permission_type: .any
		}
	}

	delete_results := manager.delete_acls([delete_filter]) or { panic(err) }
	assert delete_results.len == 1
	assert delete_results[0].deleted_acls.len == 1

	// Verify deletion
	remaining := manager.describe_acls(filter) or { panic(err) }
	assert remaining.len == 1
	assert remaining[0].pattern.resource_type == .group
}

/// test_deny_takes_precedence_over_allow verifies that DENY rules override ALLOW rules.
fn test_deny_takes_precedence_over_allow() {
	mut manager := new_memory_acl_manager()

	// Allow alice to read test-topic
	allow_acl := domain.AclBinding{
		pattern: domain.ResourcePattern{
			resource_type: .topic
			name:          'secured-topic'
			pattern_type:  .literal
		}
		entry:   domain.AccessControlEntry{
			principal:       'User:alice'
			host:            '*'
			operation:       .read
			permission_type: .allow
		}
	}

	// Deny alice from reading secured-topic (should take precedence)
	deny_acl := domain.AclBinding{
		pattern: domain.ResourcePattern{
			resource_type: .topic
			name:          'secured-topic'
			pattern_type:  .literal
		}
		entry:   domain.AccessControlEntry{
			principal:       'User:alice'
			host:            '*'
			operation:       .read
			permission_type: .deny
		}
	}

	_ = manager.create_acls([allow_acl, deny_acl]) or { panic(err) }

	// DENY should override ALLOW
	result := manager.authorize('User:alice', '10.0.0.1', .read, domain.ResourcePattern{
		resource_type: .topic
		name:          'secured-topic'
		pattern_type:  .literal
	}) or { panic(err) }
	assert result == false
}

/// test_wildcard_host_matches_any verifies '*' host matches any request host.
fn test_wildcard_host_matches_any() {
	mut manager := new_memory_acl_manager()

	acl := domain.AclBinding{
		pattern: domain.ResourcePattern{
			resource_type: .topic
			name:          'public-topic'
			pattern_type:  .literal
		}
		entry:   domain.AccessControlEntry{
			principal:       'User:carol'
			host:            '*'
			operation:       .write
			permission_type: .allow
		}
	}
	_ = manager.create_acls([acl]) or { panic(err) }

	// Any host should be allowed
	for host in ['192.168.1.1', '10.0.0.2', '172.16.0.1'] {
		allowed := manager.authorize('User:carol', host, .write, domain.ResourcePattern{
			resource_type: .topic
			name:          'public-topic'
			pattern_type:  .literal
		}) or { panic(err) }
		assert allowed == true
	}
}

/// test_user_wildcard_principal matches any user.
fn test_user_wildcard_principal() {
	mut manager := new_memory_acl_manager()

	// Allow any user to read shared-topic
	acl := domain.AclBinding{
		pattern: domain.ResourcePattern{
			resource_type: .topic
			name:          'shared-topic'
			pattern_type:  .literal
		}
		entry:   domain.AccessControlEntry{
			principal:       'User:*'
			host:            '*'
			operation:       .read
			permission_type: .allow
		}
	}
	_ = manager.create_acls([acl]) or { panic(err) }

	// Any principal should match
	for user in ['User:alice', 'User:bob', 'User:stranger'] {
		allowed := manager.authorize(user, '*', .read, domain.ResourcePattern{
			resource_type: .topic
			name:          'shared-topic'
			pattern_type:  .literal
		}) or { panic(err) }
		assert allowed == true
	}
}

/// test_prefixed_resource_pattern verifies prefix matching for resource names.
fn test_prefixed_resource_pattern() {
	mut manager := new_memory_acl_manager()

	// Allow read on all topics starting with 'dev-'
	acl := domain.AclBinding{
		pattern: domain.ResourcePattern{
			resource_type: .topic
			name:          'dev-'
			pattern_type:  .prefixed
		}
		entry:   domain.AccessControlEntry{
			principal:       'User:developer'
			host:            '*'
			operation:       .read
			permission_type: .allow
		}
	}
	_ = manager.create_acls([acl]) or { panic(err) }

	// Topics with 'dev-' prefix should be allowed
	for topic in ['dev-events', 'dev-logs', 'dev-metrics'] {
		allowed := manager.authorize('User:developer', '*', .read, domain.ResourcePattern{
			resource_type: .topic
			name:          topic
			pattern_type:  .literal
		}) or { panic(err) }
		assert allowed == true
	}

	// Topics without the prefix should not be allowed
	denied := manager.authorize('User:developer', '*', .read, domain.ResourcePattern{
		resource_type: .topic
		name:          'prod-events'
		pattern_type:  .literal
	}) or { panic(err) }
	assert denied == false
}

/// test_all_operation_matches_any_operation verifies .all operation allows any specific operation.
fn test_all_operation_matches_any_operation() {
	mut manager := new_memory_acl_manager()

	// Grant ALL operations on admin-topic
	acl := domain.AclBinding{
		pattern: domain.ResourcePattern{
			resource_type: .topic
			name:          'admin-topic'
			pattern_type:  .literal
		}
		entry:   domain.AccessControlEntry{
			principal:       'User:admin'
			host:            '*'
			operation:       .all
			permission_type: .allow
		}
	}
	_ = manager.create_acls([acl]) or { panic(err) }

	// All specific operations should be allowed
	for op in [domain.AclOperation.read, .write, .create, .delete, .alter, .describe] {
		allowed := manager.authorize('User:admin', '*', op, domain.ResourcePattern{
			resource_type: .topic
			name:          'admin-topic'
			pattern_type:  .literal
		}) or { panic(err) }
		assert allowed == true
	}
}

/// test_describe_acls_filter_by_resource_type verifies filter by resource type.
fn test_describe_acls_filter_by_resource_type() {
	mut manager := new_memory_acl_manager()

	acls := [
		domain.AclBinding{
			pattern: domain.ResourcePattern{
				resource_type: .topic
				name:          'my-topic'
				pattern_type:  .literal
			}
			entry:   domain.AccessControlEntry{
				principal:       'User:user1'
				host:            '*'
				operation:       .read
				permission_type: .allow
			}
		},
		domain.AclBinding{
			pattern: domain.ResourcePattern{
				resource_type: .group
				name:          'my-group'
				pattern_type:  .literal
			}
			entry:   domain.AccessControlEntry{
				principal:       'User:user2'
				host:            '*'
				operation:       .read
				permission_type: .allow
			}
		},
	]
	_ = manager.create_acls(acls) or { panic(err) }

	// Filter by topic only
	topic_filter := domain.AclBindingFilter{
		pattern_filter: domain.ResourcePatternFilter{
			resource_type: .topic
			name:          none
			pattern_type:  .any
		}
		entry_filter:   domain.AccessControlEntryFilter{
			principal:       none
			host:            none
			operation:       .any
			permission_type: .any
		}
	}
	topic_acls := manager.describe_acls(topic_filter) or { panic(err) }
	assert topic_acls.len == 1
	assert topic_acls[0].pattern.resource_type == .topic

	// Filter by group only
	group_filter := domain.AclBindingFilter{
		pattern_filter: domain.ResourcePatternFilter{
			resource_type: .group
			name:          none
			pattern_type:  .any
		}
		entry_filter:   domain.AccessControlEntryFilter{
			principal:       none
			host:            none
			operation:       .any
			permission_type: .any
		}
	}
	group_acls := manager.describe_acls(group_filter) or { panic(err) }
	assert group_acls.len == 1
	assert group_acls[0].pattern.resource_type == .group
}

/// test_no_acl_defaults_to_deny verifies that no matching ACL defaults to deny.
fn test_no_acl_defaults_to_deny() {
	mut manager := new_memory_acl_manager()

	// No ACLs exist
	result := manager.authorize('User:nobody', '*', .read, domain.ResourcePattern{
		resource_type: .topic
		name:          'any-topic'
		pattern_type:  .literal
	}) or { panic(err) }
	assert result == false
}
