/// 단위 테스트 - 인프라 계층: 메모리 ACL 관리자
module auth

import domain

/// test_memory_acl_manager는 MemoryAclManager의 전체 기능을 테스트합니다.
/// ACL 생성, 조회, 권한 검증, 삭제 기능을 순차적으로 검증합니다.
fn test_memory_acl_manager() {
	mut manager := new_memory_acl_manager()

	// 1. ACL 생성
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

	// 2. ACL 조회
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

	// 주체(principal)로 필터링
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

	// 3. 권한 검증
	// Alice가 test-topic 읽기 -> 허용
	allowed := manager.authorize('User:alice', '192.168.1.1', .read, domain.ResourcePattern{
		resource_type: .topic
		name:          'test-topic'
		pattern_type:  .literal
	}) or { false }
	assert allowed == true

	// Bob이 test-group-1에 쓰기 (접두사 매칭) -> 거부
	denied := manager.authorize('User:bob', '127.0.0.1', .write, domain.ResourcePattern{
		resource_type: .group
		name:          'test-group-1'
		pattern_type:  .literal // 리소스는 literal, 패턴은 접두사 매칭
	}) or { true }
	assert denied == false

	// 4. ACL 삭제
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

	// 삭제 확인
	remaining := manager.describe_acls(filter) or { panic(err) }
	assert remaining.len == 1
	assert remaining[0].pattern.resource_type == .group
}
