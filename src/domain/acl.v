// 도메인 레이어 - ACL(접근 제어 목록) 도메인 모델
// Kafka 리소스에 대한 권한 관리를 정의합니다.
module domain

/// ResourceType은 접근하려는 리소스의 유형을 나타냅니다.
/// unknown: 알 수 없음
/// any: 모든 리소스
/// topic: 토픽
/// group: 컨슈머 그룹
/// cluster: 클러스터
/// transactional_id: 트랜잭션 ID
/// delegation_token: 위임 토큰
/// user: 사용자
pub enum ResourceType {
	unknown          = 0
	any              = 1
	topic            = 2
	group            = 3
	cluster          = 4
	transactional_id = 5
	delegation_token = 6
	user             = 7
}

/// PatternType은 리소스 이름 패턴 매칭 유형을 나타냅니다.
/// unknown: 알 수 없음
/// any: 모든 패턴
/// match: 매칭
/// literal: 정확히 일치
/// prefixed: 접두사 매칭
pub enum PatternType {
	unknown  = 0
	any      = 1
	match    = 2 // 리터럴 매칭
	literal  = 3
	prefixed = 4
}

/// AclOperation은 수행하려는 작업을 나타냅니다.
/// read: 읽기
/// write: 쓰기
/// create: 생성
/// delete: 삭제
/// alter: 변경
/// describe: 조회
pub enum AclOperation {
	unknown          = 0
	any              = 1
	all              = 2
	read             = 3
	write            = 4
	create           = 5
	delete           = 6
	alter            = 7
	describe         = 8
	cluster_action   = 9
	describe_configs = 10
	alter_configs    = 11
	idempotent_write = 12
}

/// PermissionType은 허용 또는 거부 권한을 나타냅니다.
/// allow: 허용
/// deny: 거부
pub enum PermissionType {
	unknown = 0
	any     = 1
	allow   = 2
	deny    = 3
}

/// AccessControlEntry(ACE)는 누가 무엇을 할 수 있는지 정의합니다.
/// principal: 주체 (예: "User:alice")
/// host: 호스트 (예: "*" 또는 특정 IP)
/// operation: 작업
/// permission_type: 권한 유형
pub struct AccessControlEntry {
pub:
	principal       string
	host            string
	operation       AclOperation
	permission_type PermissionType
}

/// ResourcePattern은 ACL을 위한 리소스 패턴을 정의합니다.
/// resource_type: 리소스 유형
/// name: 리소스 이름
/// pattern_type: 패턴 유형
pub struct ResourcePattern {
pub:
	resource_type ResourceType
	name          string
	pattern_type  PatternType
}

/// AclBinding은 리소스 패턴과 접근 제어 항목을 결합합니다.
/// pattern: 리소스 패턴
/// entry: 접근 제어 항목
pub struct AclBinding {
pub:
	pattern ResourcePattern
	entry   AccessControlEntry
}

/// AclBindingFilter는 ACL 필터링에 사용됩니다.
pub struct AclBindingFilter {
pub:
	pattern_filter ResourcePatternFilter
	entry_filter   AccessControlEntryFilter
}

/// ResourcePatternFilter는 리소스 패턴 필터입니다.
pub struct ResourcePatternFilter {
pub:
	resource_type ResourceType
	name          ?string
	pattern_type  PatternType
}

/// AccessControlEntryFilter는 접근 제어 항목 필터입니다.
pub struct AccessControlEntryFilter {
pub:
	principal       ?string
	host            ?string
	operation       AclOperation
	permission_type PermissionType
}

/// AclCreateResult는 ACL 생성 결과입니다.
pub struct AclCreateResult {
pub:
	error_code    i16
	error_message ?string
}

/// AclDeleteResult는 ACL 삭제 결과입니다.
pub struct AclDeleteResult {
pub:
	error_code    i16
	error_message ?string
	deleted_acls  []AclBinding
}

// 헬퍼 메서드

/// str은 ResourceType을 문자열로 변환합니다.
pub fn (t ResourceType) str() string {
	return match t {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.topic { 'Topic' }
		.group { 'Group' }
		.cluster { 'Cluster' }
		.transactional_id { 'TransactionalId' }
		.delegation_token { 'DelegationToken' }
		.user { 'User' }
	}
}

/// resource_type_from_i8은 i8 값을 ResourceType으로 변환합니다.
pub fn resource_type_from_i8(val i8) ResourceType {
	return unsafe { ResourceType(val) }
}

/// str은 PatternType을 문자열로 변환합니다.
pub fn (t PatternType) str() string {
	return match t {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.match { 'Match' }
		.literal { 'Literal' }
		.prefixed { 'Prefixed' }
	}
}

/// pattern_type_from_i8은 i8 값을 PatternType으로 변환합니다.
pub fn pattern_type_from_i8(val i8) PatternType {
	return unsafe { PatternType(val) }
}

/// str은 AclOperation을 문자열로 변환합니다.
pub fn (o AclOperation) str() string {
	return match o {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.all { 'All' }
		.read { 'Read' }
		.write { 'Write' }
		.create { 'Create' }
		.delete { 'Delete' }
		.alter { 'Alter' }
		.describe { 'Describe' }
		.cluster_action { 'ClusterAction' }
		.describe_configs { 'DescribeConfigs' }
		.alter_configs { 'AlterConfigs' }
		.idempotent_write { 'IdempotentWrite' }
	}
}

/// acl_operation_from_i8은 i8 값을 AclOperation으로 변환합니다.
pub fn acl_operation_from_i8(val i8) AclOperation {
	return unsafe { AclOperation(val) }
}

/// str은 PermissionType을 문자열로 변환합니다.
pub fn (p PermissionType) str() string {
	return match p {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.allow { 'Allow' }
		.deny { 'Deny' }
	}
}

/// permission_type_from_i8은 i8 값을 PermissionType으로 변환합니다.
pub fn permission_type_from_i8(val i8) PermissionType {
	return unsafe { PermissionType(val) }
}
