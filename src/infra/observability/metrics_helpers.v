module observability

/// new_datacore_metrics creates and registers all DataCore metrics.
pub fn new_datacore_metrics() DataCoreMetrics {
	mut reg := get_registry()

	return DataCoreMetrics{
		// Legacy metrics
		messages_produced_total: reg.register('datacore_messages_produced_total', 'Total number of messages produced',
			.counter)
		messages_consumed_total: reg.register('datacore_messages_consumed_total', 'Total number of messages consumed',
			.counter)
		bytes_produced_total:    reg.register('datacore_bytes_produced_total', 'Total bytes produced',
			.counter)
		bytes_consumed_total:    reg.register('datacore_bytes_consumed_total', 'Total bytes consumed',
			.counter)
		active_connections:      reg.register('datacore_active_connections', 'Number of active client connections',
			.gauge)
		request_latency_seconds: reg.register('datacore_request_latency_seconds', 'Request latency in seconds',
			.histogram)
		topic_count:             reg.register('datacore_topics_total', 'Total number of topics',
			.gauge)
		partition_count:         reg.register('datacore_partitions_total', 'Total number of partitions',
			.gauge)
		consumer_group_count:    reg.register('datacore_consumer_groups_total', 'Total number of consumer groups',
			.gauge)
		produce_requests_total:  reg.register('datacore_produce_requests_total', 'Total produce requests',
			.counter)
		fetch_requests_total:    reg.register('datacore_fetch_requests_total', 'Total fetch requests',
			.counter)
		metadata_requests_total: reg.register('datacore_metadata_requests_total', 'Total metadata requests',
			.counter)
		errors_total:            reg.register('datacore_errors_total', 'Total errors',
			.counter)

		// Kafka-compatible: BrokerTopicMetrics
		broker_topic: BrokerTopicMetrics{
			messages_in_per_sec:                  reg.register('kafka_server_broker_topic_metrics_messages_in_per_sec',
				'Incoming message rate', .gauge)
			bytes_in_per_sec:                     reg.register('kafka_server_broker_topic_metrics_bytes_in_per_sec',
				'Byte in rate from clients', .gauge)
			bytes_out_per_sec:                    reg.register('kafka_server_broker_topic_metrics_bytes_out_per_sec',
				'Byte out rate to clients', .gauge)
			bytes_rejected_per_sec:               reg.register('kafka_server_broker_topic_metrics_bytes_rejected_per_sec',
				'Rejected byte rate', .gauge)
			total_produce_requests_per_sec:       reg.register('kafka_server_broker_topic_metrics_total_produce_requests_per_sec',
				'Produce request rate', .gauge)
			total_fetch_requests_per_sec:         reg.register('kafka_server_broker_topic_metrics_total_fetch_requests_per_sec',
				'Fetch request rate', .gauge)
			failed_produce_requests_per_sec:      reg.register('kafka_server_broker_topic_metrics_failed_produce_requests_per_sec',
				'Failed produce request rate', .gauge)
			failed_fetch_requests_per_sec:        reg.register('kafka_server_broker_topic_metrics_failed_fetch_requests_per_sec',
				'Failed fetch request rate', .gauge)
			invalid_magic_number_records_per_sec: reg.register('kafka_server_broker_topic_metrics_invalid_magic_number_per_sec',
				'Invalid magic number records rate', .gauge)
			invalid_message_crc_records_per_sec:  reg.register('kafka_server_broker_topic_metrics_invalid_message_crc_per_sec',
				'Invalid CRC records rate', .gauge)
			invalid_offset_or_sequence_per_sec:   reg.register('kafka_server_broker_topic_metrics_invalid_offset_or_sequence_per_sec',
				'Invalid offset/sequence records rate', .gauge)
		}

		// Kafka-compatible: RequestMetrics
		request: RequestMetrics{
			requests_per_sec:       reg.register('kafka_network_request_metrics_requests_per_sec',
				'Request rate', .gauge)
			errors_per_sec:         reg.register('kafka_network_request_metrics_errors_per_sec',
				'Error rate', .gauge)
			request_bytes:          reg.register('kafka_network_request_metrics_request_bytes',
				'Request size in bytes', .histogram)
			response_bytes:         reg.register('kafka_network_request_metrics_response_bytes',
				'Response size in bytes', .histogram)
			total_time_ms:          reg.register('kafka_network_request_metrics_total_time_ms',
				'Total request time in ms', .histogram)
			request_queue_time_ms:  reg.register('kafka_network_request_metrics_request_queue_time_ms',
				'Time in request queue in ms', .histogram)
			local_time_ms:          reg.register('kafka_network_request_metrics_local_time_ms',
				'Local processing time in ms', .histogram)
			response_queue_time_ms: reg.register('kafka_network_request_metrics_response_queue_time_ms',
				'Time in response queue in ms', .histogram)
			response_send_time_ms:  reg.register('kafka_network_request_metrics_response_send_time_ms',
				'Response send time in ms', .histogram)
			request_queue_size:     reg.register('kafka_network_request_channel_request_queue_size',
				'Size of request queue', .gauge)
		}

		// Kafka-compatible: SocketServerMetrics
		socket_server: SocketServerMetrics{
			connections_total:                  reg.register('kafka_network_socket_server_connections_total',
				'Total connections created', .counter)
			connections_active:                 reg.register('kafka_network_socket_server_connections_active',
				'Current active connections', .gauge)
			connections_creation_rate:          reg.register('kafka_network_socket_server_connection_creation_rate',
				'Connection creation rate per sec', .gauge)
			connections_close_rate:             reg.register('kafka_network_socket_server_connection_close_rate',
				'Connection close rate per sec', .gauge)
			connections_rejected:               reg.register('kafka_network_socket_server_connections_rejected_total',
				'Rejected connections', .counter)
			network_processor_avg_idle_percent: reg.register('kafka_network_socket_server_network_processor_avg_idle_percent',
				'Avg idle percent of network processors', .gauge)
			expired_connections_killed_count:   reg.register('kafka_network_socket_server_expired_connections_killed_count',
				'Expired connections killed', .counter)
			bytes_received_total:               reg.register('kafka_network_socket_server_bytes_received_total',
				'Total bytes received', .counter)
			bytes_sent_total:                   reg.register('kafka_network_socket_server_bytes_sent_total',
				'Total bytes sent', .counter)
		}

		// Kafka-compatible: GroupCoordinatorMetrics
		group_coordinator: GroupCoordinatorMetrics{
			num_partitions_loading:                   reg.register('kafka_server_group_coordinator_num_partitions_loading',
				'Number of loading partitions', .gauge)
			num_partitions_active:                    reg.register('kafka_server_group_coordinator_num_partitions_active',
				'Number of active partitions', .gauge)
			num_partitions_failed:                    reg.register('kafka_server_group_coordinator_num_partitions_failed',
				'Number of failed partitions', .gauge)
			partition_load_time_max:                  reg.register('kafka_server_group_coordinator_partition_load_time_max',
				'Max partition load time in ms', .gauge)
			partition_load_time_avg:                  reg.register('kafka_server_group_coordinator_partition_load_time_avg',
				'Avg partition load time in ms', .gauge)
			event_queue_size:                         reg.register('kafka_server_group_coordinator_event_queue_size',
				'Event queue size', .gauge)
			event_queue_time_ms:                      reg.register('kafka_server_group_coordinator_event_queue_time_ms',
				'Event queue time in ms', .histogram)
			event_processing_time_ms:                 reg.register('kafka_server_group_coordinator_event_processing_time_ms',
				'Event processing time in ms', .histogram)
			group_count_consumer:                     reg.register('kafka_server_group_coordinator_group_count_consumer',
				'Consumer protocol groups', .gauge)
			group_count_classic:                      reg.register('kafka_server_group_coordinator_group_count_classic',
				'Classic protocol groups', .gauge)
			consumer_group_count_empty:               reg.register('kafka_server_group_coordinator_consumer_group_count_empty',
				'Empty consumer groups', .gauge)
			consumer_group_count_assigning:           reg.register('kafka_server_group_coordinator_consumer_group_count_assigning',
				'Assigning consumer groups', .gauge)
			consumer_group_count_reconciling:         reg.register('kafka_server_group_coordinator_consumer_group_count_reconciling',
				'Reconciling consumer groups', .gauge)
			consumer_group_count_stable:              reg.register('kafka_server_group_coordinator_consumer_group_count_stable',
				'Stable consumer groups', .gauge)
			consumer_group_count_dead:                reg.register('kafka_server_group_coordinator_consumer_group_count_dead',
				'Dead consumer groups', .gauge)
			classic_group_count_preparing_rebalance:  reg.register('kafka_server_group_metadata_manager_num_groups_preparing_rebalance',
				'Classic groups preparing rebalance', .gauge)
			classic_group_count_completing_rebalance: reg.register('kafka_server_group_metadata_manager_num_groups_completing_rebalance',
				'Classic groups completing rebalance', .gauge)
			classic_group_count_stable:               reg.register('kafka_server_group_metadata_manager_num_groups_stable',
				'Stable classic groups', .gauge)
			classic_group_count_dead:                 reg.register('kafka_server_group_metadata_manager_num_groups_dead',
				'Dead classic groups', .gauge)
			classic_group_count_empty:                reg.register('kafka_server_group_metadata_manager_num_groups_empty',
				'Empty classic groups', .gauge)
			consumer_group_rebalance_rate:            reg.register('kafka_server_group_coordinator_consumer_group_rebalance_rate',
				'Consumer group rebalance rate', .gauge)
			consumer_group_rebalance_count:           reg.register('kafka_server_group_coordinator_consumer_group_rebalance_count',
				'Consumer group rebalance count', .counter)
			classic_group_rebalance_rate:             reg.register('kafka_server_group_coordinator_classic_group_rebalance_rate',
				'Classic group rebalance rate', .gauge)
			classic_group_rebalance_count:            reg.register('kafka_server_group_coordinator_classic_group_rebalance_count',
				'Classic group rebalance count', .counter)
			num_offsets:                              reg.register('kafka_server_group_metadata_manager_num_offsets',
				'Total committed offsets', .gauge)
			offset_commit_rate:                       reg.register('kafka_server_group_coordinator_offset_commit_rate',
				'Offset commit rate', .gauge)
			offset_commit_count:                      reg.register('kafka_server_group_coordinator_offset_commit_count',
				'Offset commit count', .counter)
			offset_expiration_rate:                   reg.register('kafka_server_group_coordinator_offset_expiration_rate',
				'Offset expiration rate', .gauge)
			offset_expiration_count:                  reg.register('kafka_server_group_coordinator_offset_expiration_count',
				'Offset expiration count', .counter)
		}

		// Kafka-compatible: LogMetrics
		log: LogMetrics{
			log_start_offset:            reg.register('kafka_log_log_start_offset', 'First offset in partition',
				.gauge)
			log_end_offset:              reg.register('kafka_log_log_end_offset', 'Last offset in partition',
				.gauge)
			size_bytes:                  reg.register('kafka_log_size_bytes', 'Size of partition in bytes',
				.gauge)
			num_log_segments:            reg.register('kafka_log_num_log_segments', 'Number of log segments',
				.gauge)
			log_flush_rate_and_time_ms:  reg.register('kafka_log_log_flush_rate_and_time_ms',
				'Log flush rate and time', .histogram)
			offline_log_directory_count: reg.register('kafka_log_log_manager_offline_log_directory_count',
				'Offline log directory count', .gauge)
		}

		// Kafka-compatible: AuthenticationMetrics
		auth: AuthenticationMetrics{
			successful_authentication_total:   reg.register('kafka_server_socket_server_successful_authentication_total',
				'Successful authentications', .counter)
			successful_authentication_rate:    reg.register('kafka_server_socket_server_successful_authentication_rate',
				'Successful authentication rate', .gauge)
			failed_authentication_total:       reg.register('kafka_server_socket_server_failed_authentication_total',
				'Failed authentications', .counter)
			failed_authentication_rate:        reg.register('kafka_server_socket_server_failed_authentication_rate',
				'Failed authentication rate', .gauge)
			successful_reauthentication_total: reg.register('kafka_server_socket_server_successful_reauthentication_total',
				'Successful reauthentications', .counter)
			failed_reauthentication_total:     reg.register('kafka_server_socket_server_failed_reauthentication_total',
				'Failed reauthentications', .counter)
			reauthentication_latency_avg:      reg.register('kafka_server_socket_server_reauthentication_latency_avg',
				'Avg reauthentication latency', .gauge)
			reauthentication_latency_max:      reg.register('kafka_server_socket_server_reauthentication_latency_max',
				'Max reauthentication latency', .gauge)
		}

		// DataCore only: StorageMetrics (for S3/SQLite/Memory plugins)
		storage: StorageMetrics{
			storage_append_total:           reg.register('datacore_storage_append_total',
				'Total storage append operations', .counter)
			storage_append_latency:         reg.register('datacore_storage_append_latency_seconds',
				'Storage append latency', .histogram)
			storage_fetch_total:            reg.register('datacore_storage_fetch_total',
				'Total storage fetch operations', .counter)
			storage_fetch_latency:          reg.register('datacore_storage_fetch_latency_seconds',
				'Storage fetch latency', .histogram)
			storage_delete_total:           reg.register('datacore_storage_delete_total',
				'Total storage delete operations', .counter)
			storage_bytes_total:            reg.register('datacore_storage_bytes_total',
				'Total storage bytes', .gauge)
			storage_records_total:          reg.register('datacore_storage_records_total',
				'Total storage records', .gauge)
			remote_fetch_bytes_per_sec:     reg.register('kafka_server_broker_topic_metrics_remote_fetch_bytes_per_sec',
				'Remote fetch bytes rate (S3)', .gauge)
			remote_fetch_requests_per_sec:  reg.register('kafka_server_broker_topic_metrics_remote_fetch_requests_per_sec',
				'Remote fetch requests rate (S3)', .gauge)
			remote_fetch_errors_per_sec:    reg.register('kafka_server_broker_topic_metrics_remote_fetch_errors_per_sec',
				'Remote fetch errors rate (S3)', .gauge)
			remote_copy_bytes_per_sec:      reg.register('kafka_server_broker_topic_metrics_remote_copy_bytes_per_sec',
				'Remote copy bytes rate (S3)', .gauge)
			remote_copy_requests_per_sec:   reg.register('kafka_server_broker_topic_metrics_remote_copy_requests_per_sec',
				'Remote copy requests rate (S3)', .gauge)
			remote_copy_errors_per_sec:     reg.register('kafka_server_broker_topic_metrics_remote_copy_errors_per_sec',
				'Remote copy errors rate (S3)', .gauge)
			remote_copy_lag_bytes:          reg.register('kafka_server_broker_topic_metrics_remote_copy_lag_bytes',
				'Remote copy lag bytes (S3)', .gauge)
			remote_delete_requests_per_sec: reg.register('kafka_server_broker_topic_metrics_remote_delete_requests_per_sec',
				'Remote delete requests rate (S3)', .gauge)
			remote_delete_errors_per_sec:   reg.register('kafka_server_broker_topic_metrics_remote_delete_errors_per_sec',
				'Remote delete errors rate (S3)', .gauge)
		}

		// DataCore only: SchemaRegistryMetrics
		schema_registry: SchemaRegistryMetrics{
			schemas_total:               reg.register('datacore_schema_registry_schemas_total',
				'Total schemas', .gauge)
			subjects_total:              reg.register('datacore_schema_registry_subjects_total',
				'Total subjects', .gauge)
			schema_versions_total:       reg.register('datacore_schema_registry_versions_total',
				'Total schema versions', .gauge)
			schema_compatibility_checks: reg.register('datacore_schema_registry_compatibility_checks_total',
				'Schema compatibility checks', .counter)
			schema_validation_errors:    reg.register('datacore_schema_registry_validation_errors_total',
				'Schema validation errors', .counter)
		}

		// Task #15: ShareGroupMetrics (KIP-932)
		share_group: ShareGroupMetrics{
			acquired:        reg.register('datacore_share_group_acquired', 'Total records acquired by share groups',
				.counter)
			acked:           reg.register('datacore_share_group_acked', 'Total records acknowledged by share groups',
				.counter)
			released:        reg.register('datacore_share_group_released', 'Total records released by share groups',
				.counter)
			rejected:        reg.register('datacore_share_group_rejected', 'Total records rejected by share groups',
				.counter)
			active_sessions: reg.register('datacore_share_group_active_sessions', 'Active share group sessions',
				.gauge)
		}

		// Task #15: GrpcGatewayMetrics
		grpc_gateway: GrpcGatewayMetrics{
			requests_total:     reg.register('datacore_grpc_requests_total', 'Total gRPC gateway requests',
				.counter)
			latency_seconds:    reg.register('datacore_grpc_latency_seconds', 'gRPC gateway response latency in seconds',
				.histogram)
			errors_total:       reg.register('datacore_grpc_errors_total', 'Total gRPC gateway errors',
				.counter)
			active_connections: reg.register('datacore_grpc_active_connections', 'Active gRPC connections',
				.gauge)
		}

		// Task #15: PartitionDetailMetrics
		partition_detail: PartitionDetailMetrics{
			log_size:       reg.register('datacore_partition_log_size', 'Partition log size in bytes',
				.gauge)
			current_offset: reg.register('datacore_partition_offset', 'Current high-water mark offset',
				.gauge)
			lag:            reg.register('datacore_partition_lag', 'Consumer lag per partition',
				.gauge)
		}

		// Task #15: ConsumerGroupDetailMetrics
		consumer_group_detail: ConsumerGroupDetailMetrics{
			members: reg.register('datacore_consumer_group_members', 'Number of members in consumer group',
				.gauge)
			lag:     reg.register('datacore_consumer_group_lag', 'Total consumer group lag across partitions',
				.gauge)
		}
	}
}

// Helper functions for recording metrics

/// record_produce records metrics for a produce request.
pub fn (mut m DataCoreMetrics) record_produce(topic string, bytes i64, records int, success bool, latency_ms f64) {
	m.messages_produced_total.inc_by(records)
	m.bytes_produced_total.inc_by(bytes)
	m.produce_requests_total.inc()

	m.broker_topic.messages_in_per_sec.set(f64(records))
	m.broker_topic.bytes_in_per_sec.set(f64(bytes))
	m.broker_topic.total_produce_requests_per_sec.inc()

	if !success {
		m.errors_total.inc()
		m.broker_topic.failed_produce_requests_per_sec.inc()
	}

	m.request.total_time_ms.observe(latency_ms)
	m.request.local_time_ms.observe(latency_ms)
}

/// record_fetch records metrics for a fetch request.
pub fn (mut m DataCoreMetrics) record_fetch(topic string, bytes i64, records int, success bool, latency_ms f64) {
	m.messages_consumed_total.inc_by(records)
	m.bytes_consumed_total.inc_by(bytes)
	m.fetch_requests_total.inc()

	m.broker_topic.bytes_out_per_sec.set(f64(bytes))
	m.broker_topic.total_fetch_requests_per_sec.inc()

	if !success {
		m.errors_total.inc()
		m.broker_topic.failed_fetch_requests_per_sec.inc()
	}

	m.request.total_time_ms.observe(latency_ms)
}

/// record_connection_open records a connection open event.
pub fn (mut m DataCoreMetrics) record_connection_open() {
	m.active_connections.inc()
	m.socket_server.connections_active.inc()
	m.socket_server.connections_total.inc()
	m.socket_server.connections_creation_rate.inc()
}

/// record_connection_close records a connection close event.
pub fn (mut m DataCoreMetrics) record_connection_close() {
	m.active_connections.dec()
	m.socket_server.connections_active.dec()
	m.socket_server.connections_close_rate.inc()
}

/// record_connection_rejected records a connection rejected event.
pub fn (mut m DataCoreMetrics) record_connection_rejected() {
	m.socket_server.connections_rejected.inc()
}

/// record_auth_success records an authentication success event.
pub fn (mut m DataCoreMetrics) record_auth_success() {
	m.auth.successful_authentication_total.inc()
	m.auth.successful_authentication_rate.inc()
}

/// record_auth_failure records an authentication failure event.
pub fn (mut m DataCoreMetrics) record_auth_failure() {
	m.auth.failed_authentication_total.inc()
	m.auth.failed_authentication_rate.inc()
}

/// record_group_state records a consumer group state change.
pub fn (mut m DataCoreMetrics) record_group_state(protocol string, state string, delta int) {
	if protocol == 'consumer' {
		match state {
			'empty' { m.group_coordinator.consumer_group_count_empty.inc_by(delta) }
			'assigning' { m.group_coordinator.consumer_group_count_assigning.inc_by(delta) }
			'reconciling' { m.group_coordinator.consumer_group_count_reconciling.inc_by(delta) }
			'stable' { m.group_coordinator.consumer_group_count_stable.inc_by(delta) }
			'dead' { m.group_coordinator.consumer_group_count_dead.inc_by(delta) }
			else {}
		}
	} else {
		match state {
			'preparing_rebalance' { m.group_coordinator.classic_group_count_preparing_rebalance.inc_by(delta) }
			'completing_rebalance' { m.group_coordinator.classic_group_count_completing_rebalance.inc_by(delta) }
			'stable' { m.group_coordinator.classic_group_count_stable.inc_by(delta) }
			'dead' { m.group_coordinator.classic_group_count_dead.inc_by(delta) }
			'empty' { m.group_coordinator.classic_group_count_empty.inc_by(delta) }
			else {}
		}
	}
}

/// record_offset_commit records an offset commit.
pub fn (mut m DataCoreMetrics) record_offset_commit() {
	m.group_coordinator.offset_commit_count.inc()
	m.group_coordinator.offset_commit_rate.inc()
}

/// record_storage_append records a storage engine append operation.
pub fn (mut m DataCoreMetrics) record_storage_append(bytes i64, latency_seconds f64) {
	m.storage.storage_append_total.inc()
	m.storage.storage_append_latency.observe(latency_seconds)
}

/// record_storage_fetch records a storage engine fetch operation.
pub fn (mut m DataCoreMetrics) record_storage_fetch(bytes i64, latency_seconds f64) {
	m.storage.storage_fetch_total.inc()
	m.storage.storage_fetch_latency.observe(latency_seconds)
}

/// record_request_timing records detailed request timing.
pub fn (mut m DataCoreMetrics) record_request_timing(queue_time_ms f64, local_time_ms f64, response_queue_time_ms f64, send_time_ms f64) {
	m.request.request_queue_time_ms.observe(queue_time_ms)
	m.request.local_time_ms.observe(local_time_ms)
	m.request.response_queue_time_ms.observe(response_queue_time_ms)
	m.request.response_send_time_ms.observe(send_time_ms)
	total := queue_time_ms + local_time_ms + response_queue_time_ms + send_time_ms
	m.request.total_time_ms.observe(total)
}

/// update_gauges updates gauge metrics (called periodically).
pub fn (mut m DataCoreMetrics) update_gauges(topics int, partitions int, groups int, storage_bytes i64) {
	m.topic_count.set(topics)
	m.partition_count.set(partitions)
	m.consumer_group_count.set(groups)
	m.storage.storage_bytes_total.set(f64(storage_bytes))
}

// Task #15: Share Group metric helpers

/// record_share_group_acquire records a share group record acquisition.
pub fn (mut m DataCoreMetrics) record_share_group_acquire(count int) {
	m.share_group.acquired.inc_by(count)
}

/// record_share_group_ack records a share group record acknowledgement.
pub fn (mut m DataCoreMetrics) record_share_group_ack(count int) {
	m.share_group.acked.inc_by(count)
}

/// record_share_group_release records a share group record release.
pub fn (mut m DataCoreMetrics) record_share_group_release(count int) {
	m.share_group.released.inc_by(count)
}

/// record_share_group_reject records a share group record rejection.
pub fn (mut m DataCoreMetrics) record_share_group_reject(count int) {
	m.share_group.rejected.inc_by(count)
}

/// update_share_group_sessions updates the active share group session count.
pub fn (mut m DataCoreMetrics) update_share_group_sessions(count int) {
	m.share_group.active_sessions.set(f64(count))
}

// Task #15: gRPC Gateway metric helpers

/// record_grpc_request records a gRPC gateway request.
pub fn (mut m DataCoreMetrics) record_grpc_request(success bool, latency_seconds f64) {
	m.grpc_gateway.requests_total.inc()
	m.grpc_gateway.latency_seconds.observe(latency_seconds)
	if !success {
		m.grpc_gateway.errors_total.inc()
	}
}

/// update_grpc_connections updates the active gRPC connection count.
pub fn (mut m DataCoreMetrics) update_grpc_connections(count int) {
	m.grpc_gateway.active_connections.set(f64(count))
}

// Task #15: Partition detail metric helpers

/// update_partition_metrics updates partition-level detail metrics.
pub fn (mut m DataCoreMetrics) update_partition_metrics(log_size i64, current_offset i64, lag i64) {
	m.partition_detail.log_size.set(f64(log_size))
	m.partition_detail.current_offset.set(f64(current_offset))
	m.partition_detail.lag.set(f64(lag))
}

// Task #15: Consumer group detail metric helpers

/// update_consumer_group_metrics updates consumer group detail metrics.
pub fn (mut m DataCoreMetrics) update_consumer_group_metrics(members int, lag i64) {
	m.consumer_group_detail.members.set(f64(members))
	m.consumer_group_detail.lag.set(f64(lag))
}
