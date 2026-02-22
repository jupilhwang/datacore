// Tests for CLI command option parsing
module cli

fn test_parse_topic_options_basic() {
	args := ['my-topic', '--partitions', '3', '--replication-factor', '2']
	opts := parse_topic_options(args)
	assert opts.topic == 'my-topic'
	assert opts.partitions == 3
	assert opts.replication == 2
}

fn test_parse_topic_options_defaults() {
	args := ['test-topic']
	opts := parse_topic_options(args)
	assert opts.topic == 'test-topic'
	assert opts.partitions == 1
	assert opts.replication == 1
	assert opts.bootstrap_server == 'localhost:9092'
}

fn test_parse_topic_options_bootstrap() {
	args := ['--bootstrap-server', 'broker1:9092', '-t', 'my-topic']
	opts := parse_topic_options(args)
	assert opts.bootstrap_server == 'broker1:9092'
	assert opts.topic == 'my-topic'
}

fn test_parse_topic_options_alter() {
	args := ['my-topic', '--new-partitions', '6']
	opts := parse_topic_options(args)
	assert opts.topic == 'my-topic'
	assert opts.new_partitions == 6
}

fn test_parse_group_options_basic() {
	args := ['my-group']
	opts := parse_group_options(args)
	assert opts.group_id == 'my-group'
	assert opts.bootstrap_server == 'localhost:9092'
}

fn test_parse_group_options_with_topic() {
	args := ['my-group', '--topic', 'my-topic', '--partition', '2']
	opts := parse_group_options(args)
	assert opts.group_id == 'my-group'
	assert opts.topic == 'my-topic'
	assert opts.partition == 2
}

fn test_parse_group_options_to_earliest() {
	args := ['--group', 'g1', '--topic', 't1', '--to-earliest']
	opts := parse_group_options(args)
	assert opts.group_id == 'g1'
	assert opts.topic == 't1'
	assert opts.to_earliest == true
	assert opts.to_latest == false
}

fn test_parse_group_options_to_latest() {
	args := ['g1', '--topic', 't1', '--to-latest']
	opts := parse_group_options(args)
	assert opts.to_latest == true
	assert opts.to_earliest == false
}

fn test_parse_share_group_options_basic() {
	args := ['my-share-group']
	opts := parse_share_group_options(args)
	assert opts.group_id == 'my-share-group'
	assert opts.bootstrap_server == 'localhost:9092'
}

fn test_parse_share_group_options_with_group_flag() {
	args := ['--group', 'sg1', '-b', 'broker:9092']
	opts := parse_share_group_options(args)
	assert opts.group_id == 'sg1'
	assert opts.bootstrap_server == 'broker:9092'
}

fn test_parse_acl_options_defaults() {
	args := []string{}
	opts := parse_acl_options(args)
	assert opts.host == '*'
	assert opts.operation == 'All'
	assert opts.resource_type == 'Topic'
	assert opts.pattern_type == 'Literal'
	assert opts.permission == 'Allow'
}

fn test_parse_acl_options_full() {
	args := ['--principal', 'User:alice', '--operation', 'Read', '--resource', 'my-topic',
		'--permission', 'Allow', '--resource-type', 'Topic', '--host', '192.168.1.1']
	opts := parse_acl_options(args)
	assert opts.principal == 'User:alice'
	assert opts.operation == 'Read'
	assert opts.resource_name == 'my-topic'
	assert opts.permission == 'Allow'
	assert opts.resource_type == 'Topic'
	assert opts.host == '192.168.1.1'
}

fn test_parse_cluster_options_default() {
	args := []string{}
	opts := parse_cluster_options(args)
	assert opts.bootstrap_server == 'localhost:9092'
}

fn test_parse_cluster_options_with_bootstrap() {
	args := ['-b', 'my-broker:9093']
	opts := parse_cluster_options(args)
	assert opts.bootstrap_server == 'my-broker:9093'
}

fn test_parse_offset_options_get() {
	args := ['my-group', '--topic', 'my-topic', '--partition', '1']
	opts := parse_offset_options(args)
	assert opts.group_id == 'my-group'
	assert opts.topic == 'my-topic'
	assert opts.partition == 1
}

fn test_parse_offset_options_set_with_value() {
	args := ['grp1', '--topic', 'topic1', '--partition', '0', '--offset', '500']
	opts := parse_offset_options(args)
	assert opts.group_id == 'grp1'
	assert opts.topic == 'topic1'
	assert opts.offset == 500
}

fn test_parse_offset_options_to_earliest() {
	args := ['grp1', '--topic', 'topic1', '--to-earliest']
	opts := parse_offset_options(args)
	assert opts.to_earliest == true
	assert opts.to_latest == false
}

fn test_parse_offset_options_to_latest() {
	args := ['grp1', '--topic', 'topic1', '--to-latest']
	opts := parse_offset_options(args)
	assert opts.to_latest == true
	assert opts.to_earliest == false
}

fn test_parse_health_options_default() {
	args := []string{}
	opts := parse_health_options(args)
	assert opts.bootstrap_server == 'localhost:9092'
	assert opts.rest_endpoint == 'http://localhost:8080'
	assert opts.pid_path == '/tmp/datacore.pid'
	assert opts.timeout_sec == 5
}

fn test_parse_health_options_custom() {
	args := ['--bootstrap-server', 'broker:9092', '--endpoint', 'http://broker:8080', '--timeout',
		'10']
	opts := parse_health_options(args)
	assert opts.bootstrap_server == 'broker:9092'
	assert opts.rest_endpoint == 'http://broker:8080'
	assert opts.timeout_sec == 10
}

fn test_acl_resource_type_to_code() {
	assert resource_type_to_code('topic') == 2
	assert resource_type_to_code('Topic') == 2
	assert resource_type_to_code('group') == 3
	assert resource_type_to_code('Group') == 3
	assert resource_type_to_code('cluster') == 4
	assert resource_type_to_code('unknown_type') == 2 // default to topic
}

fn test_acl_operation_to_code() {
	assert operation_to_code('read') == 3
	assert operation_to_code('Read') == 3
	assert operation_to_code('write') == 4
	assert operation_to_code('Write') == 4
	assert operation_to_code('all') == 2
	assert operation_to_code('All') == 2
	assert operation_to_code('create') == 5
	assert operation_to_code('delete') == 6
}

fn test_acl_permission_to_code() {
	assert permission_to_code('allow') == 2
	assert permission_to_code('Allow') == 2
	assert permission_to_code('deny') == 3
	assert permission_to_code('Deny') == 3
	assert permission_to_code('unknown') == 2 // default to allow
}

fn test_acl_pattern_type_to_code() {
	assert pattern_type_to_code('literal') == 3
	assert pattern_type_to_code('Literal') == 3
	assert pattern_type_to_code('prefixed') == 4
	assert pattern_type_to_code('Prefixed') == 4
	assert pattern_type_to_code('any') == 1
}

fn test_acl_resource_type_from_code() {
	assert resource_type_from_code(2) == 'Topic'
	assert resource_type_from_code(3) == 'Group'
	assert resource_type_from_code(4) == 'Cluster'
	assert resource_type_from_code(99) == 'Unknown'
}

fn test_acl_operation_from_code() {
	assert operation_from_code(3) == 'Read'
	assert operation_from_code(4) == 'Write'
	assert operation_from_code(2) == 'All'
	assert operation_from_code(99) == 'Unknown'
}

fn test_acl_permission_from_code() {
	assert permission_from_code(2) == 'Allow'
	assert permission_from_code(3) == 'Deny'
	assert permission_from_code(99) == 'Unknown'
}

fn test_build_create_acls_request_not_empty() {
	req := build_create_acls_request('User:alice', '*', u8(2), 'my-topic', u8(3), u8(3),
		u8(2))
	assert req.len > 0
}

fn test_build_describe_acls_request_not_empty() {
	req := build_describe_acls_request('', '', u8(1), '', u8(1), u8(1), u8(1))
	assert req.len > 0
}

fn test_build_delete_acls_request_not_empty() {
	req := build_delete_acls_request('User:alice', '*', u8(2), 'my-topic', u8(3), u8(3),
		u8(2))
	assert req.len > 0
}

fn test_build_create_partitions_request_not_empty() {
	req := build_create_partitions_request('my-topic', 6, 30000)
	assert req.len > 0
}

fn test_build_list_share_groups_request_not_empty() {
	req := build_list_share_groups_request()
	assert req.len > 0
}

fn test_build_delete_groups_request_not_empty() {
	req := build_delete_groups_request('my-share-group')
	assert req.len > 0
}

fn test_build_offset_fetch_request_not_empty() {
	req := build_offset_fetch_request('my-group', 'my-topic', 0)
	assert req.len > 0
}

fn test_build_offset_commit_request_not_empty() {
	req := build_offset_commit_request('my-group', 'my-topic', 0, i64(100))
	assert req.len > 0
}

fn test_build_group_offset_commit_request_not_empty() {
	req := build_group_offset_commit_request('my-group', 'my-topic', 0, i64(100))
	assert req.len > 0
}

fn test_build_describe_cluster_configs_request_not_empty() {
	req := build_describe_cluster_configs_request()
	assert req.len > 0
}
