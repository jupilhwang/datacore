module domain

/// DomainError는 도메인 수준 에러를 나타냅니다.
/// code: Kafka 에러 코드
/// message: 에러 메시지
pub struct DomainError {
pub:
	code    ErrorCode
	message string
}

/// ErrorCode는 Kafka 호환 에러 코드를 나타냅니다.
/// Kafka 프로토콜 스펙에 정의된 에러 코드와 동일합니다.
pub enum ErrorCode {
	none                             = 0
	unknown_server_error             = -1
	offset_out_of_range              = 1
	corrupt_message                  = 2
	unknown_topic_or_partition       = 3
	invalid_fetch_size               = 4
	leader_not_available             = 5
	not_leader_or_follower           = 6
	request_timed_out                = 7
	broker_not_available             = 8
	replica_not_available            = 9
	message_too_large                = 10
	stale_controller_epoch           = 11
	offset_metadata_too_large        = 12
	network_exception                = 13
	coordinator_load_in_progress     = 14
	coordinator_not_available        = 15
	not_coordinator                  = 16
	invalid_topic_exception          = 17
	record_list_too_large            = 18
	not_enough_replicas              = 19
	not_enough_replicas_after_append = 20
	invalid_required_acks            = 21
	illegal_generation               = 22
	inconsistent_group_protocol      = 23
	invalid_group_id                 = 24
	unknown_member_id                = 25
	invalid_session_timeout          = 26
	rebalance_in_progress            = 27
	invalid_commit_offset_size       = 28
	topic_authorization_failed       = 29
	group_authorization_failed       = 30
	cluster_authorization_failed     = 31
	invalid_timestamp                = 32
	unsupported_sasl_mechanism       = 33
	illegal_sasl_state               = 34
	unsupported_version              = 35
	topic_already_exists             = 36
	invalid_partitions               = 37
	invalid_replication_factor       = 38
	invalid_replica_assignment       = 39
	invalid_config                   = 40
	not_controller                   = 41
	invalid_request                  = 42
	sasl_authentication_failed       = 58
	group_id_not_found               = 69
	unknown_topic_id                 = 100
}

/// message는 에러 코드에 해당하는 메시지를 반환합니다.
pub fn (e ErrorCode) message() string {
	return match e {
		.none { 'Success' }
		.unknown_server_error { 'Unknown server error' }
		.offset_out_of_range { 'Offset out of range' }
		.corrupt_message { 'Corrupt message' }
		.unknown_topic_or_partition { 'Unknown topic or partition' }
		.invalid_fetch_size { 'Invalid fetch size' }
		.leader_not_available { 'Leader not available' }
		.not_leader_or_follower { 'Not leader or follower' }
		.request_timed_out { 'Request timed out' }
		.broker_not_available { 'Broker not available' }
		.topic_already_exists { 'Topic already exists' }
		.invalid_partitions { 'Invalid partitions' }
		.invalid_replication_factor { 'Invalid replication factor' }
		.group_id_not_found { 'Group ID not found' }
		.unknown_topic_id { 'Unknown topic ID' }
		else { 'Error code: ${int(e)}' }
	}
}
