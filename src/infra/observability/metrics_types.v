module observability

// Kafka-compatible metrics for DataCore
// Reference: https://kafka.apache.org/41/operations/monitoring/

/// BrokerTopicMetrics - per-topic broker metrics (kafka.server:type=BrokerTopicMetrics)
pub struct BrokerTopicMetrics {
pub mut:
	// Message rates
	messages_in_per_sec    &Metric
	bytes_in_per_sec       &Metric
	bytes_out_per_sec      &Metric
	bytes_rejected_per_sec &Metric

	// Request rates
	total_produce_requests_per_sec  &Metric
	total_fetch_requests_per_sec    &Metric
	failed_produce_requests_per_sec &Metric
	failed_fetch_requests_per_sec   &Metric

	// Validation failures
	invalid_magic_number_records_per_sec &Metric
	invalid_message_crc_records_per_sec  &Metric
	invalid_offset_or_sequence_per_sec   &Metric
}

/// RequestMetrics - request processing metrics (kafka.network:type=RequestMetrics)
pub struct RequestMetrics {
pub mut:
	// Request rate by type
	requests_per_sec &Metric
	errors_per_sec   &Metric

	// Request sizes
	request_bytes  &Metric
	response_bytes &Metric

	// Request timing (all in seconds)
	total_time_ms          &Metric
	request_queue_time_ms  &Metric
	local_time_ms          &Metric
	response_queue_time_ms &Metric
	response_send_time_ms  &Metric

	// Queue
	request_queue_size &Metric
}

/// SocketServerMetrics - network/socket metrics (kafka.network:type=SocketServer)
pub struct SocketServerMetrics {
pub mut:
	// Connection metrics
	connections_total         &Metric
	connections_active        &Metric
	connections_creation_rate &Metric
	connections_close_rate    &Metric
	connections_rejected      &Metric

	// I/O metrics
	network_processor_avg_idle_percent &Metric
	expired_connections_killed_count   &Metric

	// Traffic
	bytes_received_total &Metric
	bytes_sent_total     &Metric
}

/// GroupCoordinatorMetrics - consumer group coordinator metrics
pub struct GroupCoordinatorMetrics {
pub mut:
	// Partition state counts
	num_partitions_loading &Metric
	num_partitions_active  &Metric
	num_partitions_failed  &Metric

	// Load times
	partition_load_time_max &Metric
	partition_load_time_avg &Metric

	// Event processing
	event_queue_size         &Metric
	event_queue_time_ms      &Metric
	event_processing_time_ms &Metric

	// Group counts
	group_count_consumer &Metric
	group_count_classic  &Metric

	// Consumer group states (KIP-848)
	consumer_group_count_empty       &Metric
	consumer_group_count_assigning   &Metric
	consumer_group_count_reconciling &Metric
	consumer_group_count_stable      &Metric
	consumer_group_count_dead        &Metric

	// Classic group states
	classic_group_count_preparing_rebalance  &Metric
	classic_group_count_completing_rebalance &Metric
	classic_group_count_stable               &Metric
	classic_group_count_dead                 &Metric
	classic_group_count_empty                &Metric

	// Rebalance metrics
	consumer_group_rebalance_rate  &Metric
	consumer_group_rebalance_count &Metric
	classic_group_rebalance_rate   &Metric
	classic_group_rebalance_count  &Metric

	// Offset metrics
	num_offsets             &Metric
	offset_commit_rate      &Metric
	offset_commit_count     &Metric
	offset_expiration_rate  &Metric
	offset_expiration_count &Metric
}

/// LogMetrics - log/partition metrics (kafka.log:type=Log)
pub struct LogMetrics {
pub mut:
	// Per-partition
	log_start_offset &Metric
	log_end_offset   &Metric
	size_bytes       &Metric
	num_log_segments &Metric

	// Log manager
	log_flush_rate_and_time_ms  &Metric
	offline_log_directory_count &Metric
}

/// AuthenticationMetrics - SASL/authentication metrics
pub struct AuthenticationMetrics {
pub mut:
	successful_authentication_total   &Metric
	successful_authentication_rate    &Metric
	failed_authentication_total       &Metric
	failed_authentication_rate        &Metric
	successful_reauthentication_total &Metric
	failed_reauthentication_total     &Metric
	reauthentication_latency_avg      &Metric
	reauthentication_latency_max      &Metric
}

/// StorageMetrics - storage engine specific metrics (DataCore only)
pub struct StorageMetrics {
pub mut:
	// Storage operations
	storage_append_total   &Metric
	storage_append_latency &Metric
	storage_fetch_total    &Metric
	storage_fetch_latency  &Metric
	storage_delete_total   &Metric

	// Storage sizes
	storage_bytes_total   &Metric
	storage_records_total &Metric

	// For S3 storage plugin
	remote_fetch_bytes_per_sec     &Metric
	remote_fetch_requests_per_sec  &Metric
	remote_fetch_errors_per_sec    &Metric
	remote_copy_bytes_per_sec      &Metric
	remote_copy_requests_per_sec   &Metric
	remote_copy_errors_per_sec     &Metric
	remote_copy_lag_bytes          &Metric
	remote_delete_requests_per_sec &Metric
	remote_delete_errors_per_sec   &Metric
}

/// SchemaRegistryMetrics - schema registry metrics (DataCore only)
pub struct SchemaRegistryMetrics {
pub mut:
	schemas_total               &Metric
	subjects_total              &Metric
	schema_versions_total       &Metric
	schema_compatibility_checks &Metric
	schema_validation_errors    &Metric
}

/// ShareGroupMetrics - Share Group metrics (KIP-932)
pub struct ShareGroupMetrics {
pub mut:
	// Records acquired (ShareFetch)
	acquired &Metric
	// Records acknowledged
	acked &Metric
	// Records released (returned without ack)
	released &Metric
	// Records rejected (permanent failure)
	rejected &Metric
	// Active share group sessions
	active_sessions &Metric
}

/// GrpcGatewayMetrics - gRPC gateway metrics
pub struct GrpcGatewayMetrics {
pub mut:
	// Total gRPC requests
	requests_total &Metric
	// gRPC response latency histogram (seconds)
	latency_seconds &Metric
	// gRPC errors
	errors_total &Metric
	// Active gRPC connections
	active_connections &Metric
}

/// PartitionDetailMetrics - per-partition detail metrics (Task #15)
pub struct PartitionDetailMetrics {
pub mut:
	// Log size in bytes
	log_size &Metric
	// Current high-water mark offset
	current_offset &Metric
	// Consumer lag (distance between producer and consumer offset)
	lag &Metric
}

/// ConsumerGroupDetailMetrics - consumer group detail metrics (Task #15)
pub struct ConsumerGroupDetailMetrics {
pub mut:
	// Number of members in the group
	members &Metric
	// Total consumer group lag across all partitions
	lag &Metric
}

/// DataCoreMetrics - complete metrics collection
pub struct DataCoreMetrics {
pub mut:
	// Legacy metrics (backward compatibility)
	messages_produced_total &Metric
	messages_consumed_total &Metric
	bytes_produced_total    &Metric
	bytes_consumed_total    &Metric
	active_connections      &Metric
	request_latency_seconds &Metric
	topic_count             &Metric
	partition_count         &Metric
	consumer_group_count    &Metric
	produce_requests_total  &Metric
	fetch_requests_total    &Metric
	metadata_requests_total &Metric
	errors_total            &Metric

	// Kafka-compatible metrics
	broker_topic      BrokerTopicMetrics
	request           RequestMetrics
	socket_server     SocketServerMetrics
	group_coordinator GroupCoordinatorMetrics
	log               LogMetrics
	auth              AuthenticationMetrics
	storage           StorageMetrics
	schema_registry   SchemaRegistryMetrics

	// Task #15: Extended metrics
	share_group           ShareGroupMetrics
	grpc_gateway          GrpcGatewayMetrics
	partition_detail      PartitionDetailMetrics
	consumer_group_detail ConsumerGroupDetailMetrics
}
