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
