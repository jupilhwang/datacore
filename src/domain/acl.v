module domain

// ResourceType represents the type of resource being accessed
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

// PatternType represents the pattern matching type for resource names
pub enum PatternType {
	unknown  = 0
	any      = 1
	match    = 2 // Literal match (renamed from 'match' to avoid keyword conflict if needed, but 'match' is keyword in V)
	literal  = 3
	prefixed = 4
}

// Operation represents the operation being performed
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

// PermissionType represents allow or deny permission
pub enum PermissionType {
	unknown = 0
	any     = 1
	allow   = 2
	deny    = 3
}

// AccessControlEntry (ACE) defines who can do what
pub struct AccessControlEntry {
pub:
	principal       string
	host            string
	operation       AclOperation
	permission_type PermissionType
}

// ResourcePattern defines a resource pattern for ACLs
pub struct ResourcePattern {
pub:
	resource_type ResourceType
	name          string
	pattern_type  PatternType
}

// AclBinding combines a resource pattern with an access control entry
pub struct AclBinding {
pub:
	pattern ResourcePattern
	entry   AccessControlEntry
}

// AclBindingFilter is used for filtering ACLs
pub struct AclBindingFilter {
pub:
	pattern_filter ResourcePatternFilter
	entry_filter   AccessControlEntryFilter
}

pub struct ResourcePatternFilter {
pub:
	resource_type ResourceType
	name          ?string
	pattern_type  PatternType
}

pub struct AccessControlEntryFilter {
pub:
	principal       ?string
	host            ?string
	operation       AclOperation
	permission_type PermissionType
}

pub struct AclCreateResult {
pub:
	error_code    i16
	error_message ?string
}

pub struct AclDeleteResult {
pub:
	error_code    i16
	error_message ?string
	deleted_acls  []AclBinding
}

// Helper methods for enums

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

pub fn resource_type_from_i8(val i8) ResourceType {
	return unsafe { ResourceType(val) }
}

pub fn (t PatternType) str() string {
	return match t {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.match { 'Match' }
		.literal { 'Literal' }
		.prefixed { 'Prefixed' }
	}
}

pub fn pattern_type_from_i8(val i8) PatternType {
	return unsafe { PatternType(val) }
}

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

pub fn acl_operation_from_i8(val i8) AclOperation {
	return unsafe { AclOperation(val) }
}

pub fn (p PermissionType) str() string {
	return match p {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.allow { 'Allow' }
		.deny { 'Deny' }
	}
}

pub fn permission_type_from_i8(val i8) PermissionType {
	return unsafe { PermissionType(val) }
}
