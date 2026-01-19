// Adapter Layer - Kafka Protocol Types
module kafka

// API Keys - Kafka Protocol
pub enum ApiKey {
	produce                         = 0
	fetch                           = 1
	list_offsets                    = 2
	metadata                        = 3
	leader_and_isr                  = 4
	stop_replica                    = 5
	update_metadata                 = 6
	controlled_shutdown             = 7
	offset_commit                   = 8
	offset_fetch                    = 9
	find_coordinator                = 10
	join_group                      = 11
	heartbeat                       = 12
	leave_group                     = 13
	sync_group                      = 14
	describe_groups                 = 15
	list_groups                     = 16
	sasl_handshake                  = 17
	api_versions                    = 18
	create_topics                   = 19
	delete_topics                   = 20
	delete_records                  = 21
	init_producer_id                = 22
	offset_for_leader_epoch         = 23
	add_partitions_to_txn           = 24
	add_offsets_to_txn              = 25
	end_txn                         = 26
	write_txn_markers               = 27
	txn_offset_commit               = 28
	describe_acls                   = 29
	create_acls                     = 30
	delete_acls                     = 31
	describe_configs                = 32
	alter_configs                   = 33
	alter_replica_log_dirs          = 34
	describe_log_dirs               = 35
	sasl_authenticate               = 36
	create_partitions               = 37
	create_delegation_token         = 38
	renew_delegation_token          = 39
	expire_delegation_token         = 40
	describe_delegation_token       = 41
	delete_groups                   = 42
	elect_leaders                   = 43
	incremental_alter_configs       = 44
	alter_partition_reassignments   = 45
	list_partition_reassignments    = 46
	offset_delete                   = 47
	describe_client_quotas          = 48
	alter_client_quotas             = 49
	describe_user_scram_credentials = 50
	alter_user_scram_credentials    = 51
	describe_quorum                 = 55
	alter_partition                 = 56
	update_features                 = 57
	envelope                        = 58
	describe_cluster                = 60
	describe_producers              = 61
	unregister_broker               = 64
	describe_transactions           = 65
	list_transactions               = 66
	allocate_producer_ids           = 67
	consumer_group_heartbeat        = 68
	consumer_group_describe         = 69
	get_telemetry_subscriptions     = 71
	push_telemetry                  = 72
	list_client_metrics_resources   = 74
	describe_topic_partitions       = 75
	share_group_heartbeat           = 76
	share_group_describe            = 77
	share_fetch                     = 78
	share_acknowledge               = 79
	initialize_share_group_state    = 83
	read_share_group_state          = 84
	write_share_group_state         = 85
	delete_share_group_state        = 86
}

// Error Codes - Kafka Protocol
pub enum ErrorCode {
	none                                  = 0
	unknown_server_error                  = -1
	offset_out_of_range                   = 1
	corrupt_message                       = 2
	unknown_topic_or_partition            = 3
	invalid_fetch_size                    = 4
	leader_not_available                  = 5
	not_leader_or_follower                = 6
	request_timed_out                     = 7
	broker_not_available                  = 8
	replica_not_available                 = 9
	message_too_large                     = 10
	stale_controller_epoch                = 11
	offset_metadata_too_large             = 12
	network_exception                     = 13
	coordinator_load_in_progress          = 14
	coordinator_not_available             = 15
	not_coordinator                       = 16
	invalid_topic_exception               = 17
	record_list_too_large                 = 18
	not_enough_replicas                   = 19
	not_enough_replicas_after_append      = 20
	invalid_required_acks                 = 21
	illegal_generation                    = 22
	inconsistent_group_protocol           = 23
	invalid_group_id                      = 24
	unknown_member_id                     = 25
	invalid_session_timeout               = 26
	rebalance_in_progress                 = 27
	invalid_commit_offset_size            = 28
	topic_authorization_failed            = 29
	group_authorization_failed            = 30
	cluster_authorization_failed          = 31
	invalid_timestamp                     = 32
	unsupported_sasl_mechanism            = 33
	illegal_sasl_state                    = 34
	unsupported_version                   = 35
	topic_already_exists                  = 36
	invalid_partitions                    = 37
	invalid_replication_factor            = 38
	invalid_replica_assignment            = 39
	invalid_config                        = 40
	not_controller                        = 41
	invalid_request                       = 42
	unsupported_for_message_format        = 43
	policy_violation                      = 44
	out_of_order_sequence_number          = 45
	duplicate_sequence_number             = 46
	invalid_producer_epoch                = 47
	invalid_txn_state                     = 48
	invalid_producer_id_mapping           = 49
	invalid_transaction_timeout           = 50
	concurrent_transactions               = 51
	transaction_coordinator_fenced        = 52
	transactional_id_authorization_failed = 53
	security_disabled                     = 54
	operation_not_attempted               = 55
	kafka_storage_error                   = 56
	log_dir_not_found                     = 57
	sasl_authentication_failed            = 58
	unknown_producer_id                   = 59
	reassignment_in_progress              = 60
	delegation_token_auth_disabled        = 61
	delegation_token_not_found            = 62
	delegation_token_owner_mismatch       = 63
	delegation_token_request_not_allowed  = 64
	delegation_token_authorization_failed = 65
	delegation_token_expired              = 66
	invalid_principal_type                = 67
	non_empty_group                       = 68
	group_id_not_found                    = 69
	fetch_session_id_not_found            = 70
	invalid_fetch_session_epoch           = 71
	listener_not_found                    = 72
	topic_deletion_disabled               = 73
	fenced_leader_epoch                   = 74
	unknown_leader_epoch                  = 75
	unsupported_compression_type          = 76
	stale_broker_epoch                    = 77
	offset_not_available                  = 78
	member_id_required                    = 79
	preferred_leader_not_available        = 80
	group_max_size_reached                = 81
	fenced_instance_id                    = 82
	eligible_leaders_not_available        = 83
	election_not_needed                   = 84
	no_reassignment_in_progress           = 85
	group_subscribed_to_topic             = 86
	invalid_record                        = 87
	unstable_offset_commit                = 88
	throttling_quota_exceeded             = 89
	producer_fenced                       = 90
	resource_not_found                    = 91
	duplicate_resource                    = 92
	unacceptable_credential               = 93
	inconsistent_voter_set                = 94
	invalid_update_version                = 95
	feature_update_failed                 = 96
	principal_deserialization_failure     = 97
	snapshot_not_found                    = 98
	position_out_of_range                 = 99
	unknown_topic_id                      = 100
	duplicate_broker_registration         = 101
	broker_id_not_registered              = 102
	inconsistent_topic_id                 = 103
	inconsistent_cluster_id               = 104
	transactional_id_not_found            = 105
	fetch_session_topic_id_error          = 106
	ineligible_replica                    = 107
	new_leader_elected                    = 108
}

// API Version Range
pub struct ApiVersionRange {
pub:
	api_key     ApiKey
	min_version i16
	max_version i16
}

// Get supported API versions
pub fn get_supported_api_versions() []ApiVersionRange {
	return [
		ApiVersionRange{.produce, 0, 13}, // v9+ flexible, v13+ TopicId support
		ApiVersionRange{.fetch, 0, 13}, // v12+ flexible, v13 TopicId support
		ApiVersionRange{.list_offsets, 0, 8}, // v6+ flexible, v7-8 identical structure
		ApiVersionRange{.metadata, 0, 12}, // v9+ flexible, v10+ topic_id, v12 name is nullable
		ApiVersionRange{.offset_commit, 0, 9}, // v8+ flexible
		ApiVersionRange{.offset_fetch, 0, 10}, // v6+ flexible, v8+ groups format, v9+ member_id/epoch
		ApiVersionRange{.find_coordinator, 0, 3}, // v3 is flexible but simple. v4+ adds batches which might be complex. Downgrade to v3 for safety.
		ApiVersionRange{.join_group, 0, 9}, // v6+ flexible
		ApiVersionRange{.heartbeat, 0, 4}, // v4+ flexible
		ApiVersionRange{.leave_group, 0, 5}, // v4+ flexible
		ApiVersionRange{.sync_group, 0, 5}, // v4+ flexible
		ApiVersionRange{.describe_groups, 0, 5}, // v5+ flexible
		ApiVersionRange{.list_groups, 0, 5}, // v3+ flexible, v4+ states_filter, v5+ types_filter
		ApiVersionRange{.sasl_handshake, 0, 1}, // SASL mechanism negotiation
		ApiVersionRange{.api_versions, 0, 2}, // v0-v2 non-flexible. v3+ requires valid tagged fields support
		ApiVersionRange{.create_topics, 0, 7},
		ApiVersionRange{.delete_topics, 0, 6},
		ApiVersionRange{.delete_records, 0, 2},
		ApiVersionRange{.init_producer_id, 0, 4},
		ApiVersionRange{.describe_configs, 0, 4},
		ApiVersionRange{.alter_configs, 0, 2},
		ApiVersionRange{.create_partitions, 0, 3},
		ApiVersionRange{.sasl_authenticate, 0, 2}, // SASL authentication (v2+ flexible)
		ApiVersionRange{.delete_groups, 0, 2},
		ApiVersionRange{.describe_cluster, 0, 1},
		ApiVersionRange{.consumer_group_heartbeat, 0, 0},
		ApiVersionRange{.consumer_group_describe, 0, 0},
		ApiVersionRange{.share_group_heartbeat, 0, 0},
		ApiVersionRange{.share_group_describe, 0, 0},
		ApiVersionRange{.share_fetch, 0, 0},
		ApiVersionRange{.share_acknowledge, 0, 0},
	]
}
