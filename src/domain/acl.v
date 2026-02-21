module domain

/// ResourceType represents the type of resource being accessed.
/// unknown: unknown
/// any: any resource
/// topic: topic
/// group: consumer group
/// cluster: cluster
/// transactional_id: transaction ID
/// delegation_token: delegation token
/// user: user
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

/// PatternType represents the resource name pattern matching type.
/// unknown: unknown
/// any: any pattern
/// match: matching
/// literal: exact match
/// prefixed: prefix matching
pub enum PatternType {
	unknown  = 0
	any      = 1
	match    = 2
	literal  = 3
	prefixed = 4
}

/// AclOperation represents the operation being performed.
/// read: read
/// write: write
/// create: create
/// delete: delete
/// alter: alter
/// describe: describe/query
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

/// PermissionType represents allow or deny permissions.
/// allow: allow
/// deny: deny
pub enum PermissionType {
	unknown = 0
	any     = 1
	allow   = 2
	deny    = 3
}

/// AccessControlEntry (ACE) defines who can do what.
/// principal: principal (e.g. "User:alice")
/// host: host (e.g. "*" or a specific IP)
/// operation: operation
/// permission_type: permission type
pub struct AccessControlEntry {
pub:
	principal       string
	host            string
	operation       AclOperation
	permission_type PermissionType
}

/// ResourcePattern defines a resource pattern for ACLs.
/// resource_type: resource type
/// name: resource name
/// pattern_type: pattern type
pub struct ResourcePattern {
pub:
	resource_type ResourceType
	name          string
	pattern_type  PatternType
}

/// AclBinding combines a resource pattern with an access control entry.
/// pattern: resource pattern
/// entry: access control entry
pub struct AclBinding {
pub:
	pattern ResourcePattern
	entry   AccessControlEntry
}

/// AclBindingFilter is used for ACL filtering.
pub struct AclBindingFilter {
pub:
	pattern_filter ResourcePatternFilter
	entry_filter   AccessControlEntryFilter
}

/// ResourcePatternFilter is a resource pattern filter.
pub struct ResourcePatternFilter {
pub:
	resource_type ResourceType
	name          ?string
	pattern_type  PatternType
}

/// AccessControlEntryFilter is an access control entry filter.
pub struct AccessControlEntryFilter {
pub:
	principal       ?string
	host            ?string
	operation       AclOperation
	permission_type PermissionType
}

/// AclCreateResult is the result of creating an ACL.
pub struct AclCreateResult {
pub:
	error_code    i16
	error_message ?string
}

/// AclDeleteResult is the result of deleting an ACL.
pub struct AclDeleteResult {
pub:
	error_code    i16
	error_message ?string
	deleted_acls  []AclBinding
}

// Helper methods

/// str converts ResourceType to a string.
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

/// resource_type_from_i8 converts an i8 value to a ResourceType with validation.
/// Returns .unknown for unrecognized values.
pub fn resource_type_from_i8(val i8) ResourceType {
	return match val {
		0 { ResourceType.unknown }
		1 { ResourceType.any }
		2 { ResourceType.topic }
		3 { ResourceType.group }
		4 { ResourceType.cluster }
		5 { ResourceType.transactional_id }
		6 { ResourceType.delegation_token }
		7 { ResourceType.user }
		else { ResourceType.unknown }
	}
}

/// str converts PatternType to a string.
pub fn (t PatternType) str() string {
	return match t {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.match { 'Match' }
		.literal { 'Literal' }
		.prefixed { 'Prefixed' }
	}
}

/// pattern_type_from_i8 converts an i8 value to a PatternType with validation.
/// Returns .unknown for unrecognized values.
pub fn pattern_type_from_i8(val i8) PatternType {
	return match val {
		0 { PatternType.unknown }
		1 { PatternType.any }
		2 { PatternType.match }
		3 { PatternType.literal }
		4 { PatternType.prefixed }
		else { PatternType.unknown }
	}
}

/// str converts AclOperation to a string.
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

/// acl_operation_from_i8 converts an i8 value to an AclOperation with validation.
/// Returns .unknown for unrecognized values.
pub fn acl_operation_from_i8(val i8) AclOperation {
	return match val {
		0 { AclOperation.unknown }
		1 { AclOperation.any }
		2 { AclOperation.all }
		3 { AclOperation.read }
		4 { AclOperation.write }
		5 { AclOperation.create }
		6 { AclOperation.delete }
		7 { AclOperation.alter }
		8 { AclOperation.describe }
		9 { AclOperation.cluster_action }
		10 { AclOperation.describe_configs }
		11 { AclOperation.alter_configs }
		12 { AclOperation.idempotent_write }
		else { AclOperation.unknown }
	}
}

/// str converts PermissionType to a string.
pub fn (p PermissionType) str() string {
	return match p {
		.unknown { 'Unknown' }
		.any { 'Any' }
		.allow { 'Allow' }
		.deny { 'Deny' }
	}
}

/// permission_type_from_i8 converts an i8 value to a PermissionType with validation.
/// Returns .unknown for unrecognized values.
pub fn permission_type_from_i8(val i8) PermissionType {
	return match val {
		0 { PermissionType.unknown }
		1 { PermissionType.any }
		2 { PermissionType.allow }
		3 { PermissionType.deny }
		else { PermissionType.unknown }
	}
}
