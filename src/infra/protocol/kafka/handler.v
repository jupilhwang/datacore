// Main handler struct and request routing
//
// This module implements the core Kafka protocol handler.
// It parses client requests, routes them to the appropriate API handler,
// and produces responses.
module kafka

import infra.compression
import infra.observability
import service.cluster
import service.group
import service.offset
import service.port
import service.schema
import service.transaction
import time
import domain
import common

/// PartitionAssignerPtr is a pointer type for the partition assignment service,
/// referencing PartitionAssigner from the service.cluster module.
pub type PartitionAssignerPtr = &cluster.PartitionAssigner

/// Handler is the core protocol handler that processes Kafka requests and produces responses.
///
/// It handles all Kafka API requests for the broker and integrates with
/// storage, authentication, ACL, transaction coordinator, and other components.
pub struct Handler {
	broker_id   i32
	host        string
	broker_port i32
	cluster_id  string
mut:
	storage                 port.StoragePort
	offset_manager          &offset.OffsetManager
	broker_registry         ?&cluster.BrokerRegistry
	partition_assigner      ?PartitionAssignerPtr
	auth_manager            ?port.AuthManager
	acl_manager             ?port.AclManager
	txn_coordinator         ?transaction.TransactionCoordinator
	share_group_coordinator ?&group.ShareGroupCoordinator
	schema_registry         ?&schema.SchemaRegistry
	logger                  &observability.Logger
	metrics                 &observability.ProtocolMetrics
	compression_service     &compression.CompressionService
	// negotiated_mechanism stores the mechanism agreed upon during SaslHandshake
	// so that handle_sasl_authenticate can use it instead of guessing from bytes
	negotiated_mechanism ?domain.SaslMechanism
}

/// HandlerConfig holds all configuration for creating a Kafka protocol handler.
/// Use new_handler_from_config to construct a Handler from this struct.
pub struct HandlerConfig {
pub:
	broker_id           i32
	host                string
	broker_port         i32
	cluster_id          string
	storage             port.StoragePort
	auth_manager        ?port.AuthManager
	acl_manager         ?port.AclManager
	txn_coordinator     ?transaction.TransactionCoordinator
	share_coordinator   ?&group.ShareGroupCoordinator
	compression_service &compression.CompressionService = unsafe { nil }
}

// StorageSubPorts wraps a StoragePort in a concrete struct, enabling V to satisfy
// narrower sub-interfaces (TopicStoragePort, OffsetStoragePort, etc.).
// V does not support narrowing one interface type to another directly; this struct
// delegates all sub-interface methods to the underlying StoragePort.
struct StorageSubPorts {
mut:
	s port.StoragePort
}

fn (mut w StorageSubPorts) create_topic(name string, partitions int, config domain.TopicConfig) !domain.TopicMetadata {
	return w.s.create_topic(name, partitions, config)
}

fn (mut w StorageSubPorts) delete_topic(name string) ! {
	return w.s.delete_topic(name)
}

fn (mut w StorageSubPorts) list_topics() ![]domain.TopicMetadata {
	return w.s.list_topics()
}

fn (mut w StorageSubPorts) get_topic(name string) !domain.TopicMetadata {
	return w.s.get_topic(name)
}

fn (mut w StorageSubPorts) get_topic_by_id(topic_id []u8) !domain.TopicMetadata {
	return w.s.get_topic_by_id(topic_id)
}

fn (mut w StorageSubPorts) add_partitions(name string, new_count int) ! {
	return w.s.add_partitions(name, new_count)
}

fn (mut w StorageSubPorts) commit_offsets(group_id string, offsets []domain.PartitionOffset) ! {
	return w.s.commit_offsets(group_id, offsets)
}

fn (mut w StorageSubPorts) fetch_offsets(group_id string, partitions []domain.TopicPartition) ![]domain.OffsetFetchResult {
	return w.s.fetch_offsets(group_id, partitions)
}

/// new_handler_from_config creates a Handler from a HandlerConfig.
/// This is the single initialization path that all constructor helpers delegate to.
pub fn new_handler_from_config(cfg HandlerConfig) Handler {
	logger := observability.get_named_logger('kafka.handler')
	mut sub := StorageSubPorts{
		s: cfg.storage
	}
	offset_mgr := offset.new_offset_manager(sub, sub, logger)
	metrics := observability.new_protocol_metrics()

	return Handler{
		broker_id:               cfg.broker_id
		host:                    cfg.host
		broker_port:             cfg.broker_port
		cluster_id:              cfg.cluster_id
		storage:                 cfg.storage
		offset_manager:          offset_mgr
		broker_registry:         none
		partition_assigner:      none
		auth_manager:            cfg.auth_manager
		acl_manager:             cfg.acl_manager
		txn_coordinator:         cfg.txn_coordinator
		share_group_coordinator: cfg.share_coordinator
		logger:                  logger
		metrics:                 metrics
		compression_service:     cfg.compression_service
	}
}

/// new_handler creates a new Kafka protocol handler with storage only.
///
/// This is the basic handler with no authentication or ACL.
/// Suitable for development and testing environments.
pub fn new_handler(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, compression_service &compression.CompressionService) Handler {
	return new_handler_from_config(HandlerConfig{
		broker_id:           broker_id
		host:                host
		broker_port:         broker_port
		cluster_id:          cluster_id
		storage:             storage
		compression_service: compression_service
	})
}

/// new_handler_with_auth creates a new Kafka protocol handler with storage and an auth manager.
///
/// Use this in environments that require SASL authentication.
pub fn new_handler_with_auth(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager port.AuthManager, compression_service &compression.CompressionService) Handler {
	return new_handler_from_config(HandlerConfig{
		broker_id:           broker_id
		host:                host
		broker_port:         broker_port
		cluster_id:          cluster_id
		storage:             storage
		auth_manager:        auth_manager
		compression_service: compression_service
	})
}

/// new_handler_full creates a fully configured Kafka protocol handler with all components.
///
/// Use this in production when authentication, ACL, and transactions are all required.
pub fn new_handler_full(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?transaction.TransactionCoordinator, compression_service &compression.CompressionService) Handler {
	return new_handler_from_config(HandlerConfig{
		broker_id:           broker_id
		host:                host
		broker_port:         broker_port
		cluster_id:          cluster_id
		storage:             storage
		auth_manager:        auth_manager
		acl_manager:         acl_manager
		txn_coordinator:     txn_coordinator
		compression_service: compression_service
	})
}

/// new_handler_with_share_groups creates a Kafka protocol handler with Share Group support (KIP-932).
///
/// Supports queue-based message consumption patterns.
pub fn new_handler_with_share_groups(broker_id i32, host string, broker_port i32, cluster_id string, storage port.StoragePort, auth_manager ?port.AuthManager, acl_manager ?port.AclManager, txn_coordinator ?transaction.TransactionCoordinator, share_coordinator &group.ShareGroupCoordinator, compression_service &compression.CompressionService) Handler {
	return new_handler_from_config(HandlerConfig{
		broker_id:           broker_id
		host:                host
		broker_port:         broker_port
		cluster_id:          cluster_id
		storage:             storage
		auth_manager:        auth_manager
		acl_manager:         acl_manager
		txn_coordinator:     txn_coordinator
		share_coordinator:   share_coordinator
		compression_service: compression_service
	})
}

/// set_broker_registry sets the broker registry for multi-broker mode.
///
/// Used for broker-to-broker coordination in a cluster environment.
pub fn (mut h Handler) set_broker_registry(registry &cluster.BrokerRegistry) {
	h.broker_registry = registry
}

/// set_share_group_coordinator sets the Share Group coordinator on the handler.
pub fn (mut h Handler) set_share_group_coordinator(coordinator &group.ShareGroupCoordinator) {
	h.share_group_coordinator = coordinator
}

/// set_schema_registry sets the schema registry on the handler.
pub fn (mut h Handler) set_schema_registry(registry &schema.SchemaRegistry) {
	h.schema_registry = registry
}

/// get_topic_schema retrieves the schema configuration for a topic.
fn (mut h Handler) get_topic_schema(topic_name string) ?domain.Schema {
	if mut registry := h.schema_registry {
		// Look up schema configuration from topic metadata
		if topic_meta := h.storage.get_topic(topic_name) {
			schema_subject := topic_meta.config['schema.subject'] or { return none }
			schema_version := common.parse_config_int(topic_meta.config, 'schema.version',
				1)

			// Retrieve schema (version -1 means latest)
			return registry.get_schema_by_subject(schema_subject, schema_version) or { return none }
		}
	}
	return none
}

/// encode_record_with_schema encodes a record according to a schema.
/// Currently only validates schema configuration and logs.
// TODO(jira#XXX): implement actual schema encoding after encoder module update
fn (mut h Handler) encode_record_with_schema(record &domain.Record, schema_obj &domain.Schema) ![]u8 {
	h.logger.debug('Schema encoding configured', observability.field_string('schema_type',
		schema_obj.schema_type.str()), observability.field_int('schema_id', schema_obj.id))

	return record.value
}

/// decode_record_with_schema decodes a record according to a schema.
/// Currently only validates schema configuration and logs.
// TODO(jira#XXX): implement actual schema decoding after encoder module update
fn (mut h Handler) decode_record_with_schema(record_data []u8, schema_obj &domain.Schema) ![]u8 {
	h.logger.debug('Schema decoding configured', observability.field_string('schema_type',
		schema_obj.schema_type.str()), observability.field_int('schema_id', schema_obj.id))

	return record_data
}

/// set_partition_assigner sets the partition assignment service on the handler.
/// Used for dynamic partition assignment.
pub fn (mut h Handler) set_partition_assigner(assigner PartitionAssignerPtr) {
	h.partition_assigner = assigner
}

/// handle_request processes a received request and returns response bytes.
///
/// Accepts raw byte data, parses it, routes it to the appropriate API handler,
/// and produces a response.
/// Requires connection context for authentication verification.
pub fn (mut h Handler) handle_request(data []u8, mut conn ?&domain.AuthConnection) ![]u8 {
	start_time := time.now()

	req := parse_request(data) or {
		h.logger.error('Failed to parse request', observability.field_err_str(err.str()),
			observability.field_bytes('request_size', data.len))
		return err
	}

	api_key := api_key_from_i16(req.header.api_key) or {
		return error('Unsupported API key ${req.header.api_key}: ${err}')
	}
	version := req.header.api_version
	correlation_id := req.header.correlation_id

	h.logger.debug('Processing request', observability.field_string('api', api_key.str()),
		observability.field_int('version', version), observability.field_int('correlation_id',
		correlation_id), observability.field_bytes('request_size', data.len))

	mut is_authenticated := false
	if c := conn {
		is_authenticated = c.is_authenticated()
	}
	h.authorize_request(api_key, is_authenticated, correlation_id)!

	// sasl_authenticate is handled separately to update conn state directly in this function.
	// This avoids V interface mutation issues when passing conn through nested function calls.
	if api_key == .sasl_authenticate {
		sasl_result := h.handle_sasl_authenticate(req.body, version) or {
			elapsed := time.since(start_time)
			h.metrics.record_request(api_key.str(), elapsed.milliseconds(), false, data.len,
				0)
			return err
		}
		if principal := sasl_result.principal {
			if mut c := conn {
				c.set_authenticated(principal)
			}
		}
		elapsed := time.since(start_time)
		h.metrics.record_request(api_key.str(), elapsed.milliseconds(), true, data.len,
			sasl_result.response_bytes.len)
		h.logger.debug('Request completed', observability.field_string('api', api_key.str()),
			observability.field_int('correlation_id', correlation_id), observability.field_bytes('response_size',
			sasl_result.response_bytes.len), observability.field_duration('latency', elapsed))
		return h.finalize_response(api_key, version, correlation_id, sasl_result.response_bytes)
	}

	response_body := h.dispatch_request(api_key, req.body, version) or {
		elapsed := time.since(start_time)
		h.metrics.record_request(api_key.str(), elapsed.milliseconds(), false, data.len,
			0)
		return err
	}

	elapsed := time.since(start_time)
	h.metrics.record_request(api_key.str(), elapsed.milliseconds(), true, data.len, response_body.len)

	h.logger.debug('Request completed', observability.field_string('api', api_key.str()),
		observability.field_int('correlation_id', correlation_id), observability.field_bytes('response_size',
		response_body.len), observability.field_duration('latency', elapsed))

	return h.finalize_response(api_key, version, correlation_id, response_body)
}

// authorize_request enforces authentication for protected APIs.
// Only enforced when auth_manager is configured; public APIs are always allowed.
fn (mut h Handler) authorize_request(api_key ApiKey, is_authenticated bool, correlation_id i32) ! {
	is_public_api := api_key in [.api_versions, .sasl_handshake, .sasl_authenticate]
	if h.auth_manager == none || is_public_api {
		return
	}
	if !is_authenticated {
		h.logger.warn('Unauthorized request', observability.field_string('api', api_key.str()),
			observability.field_int('correlation_id', correlation_id))
		return error('${int(domain.ErrorCode.sasl_authentication_failed)}: Authentication required for ${api_key.str()}')
	}
}

// dispatch_request routes the request body to the appropriate API handler and returns the response body.
fn (mut h Handler) dispatch_request(api_key ApiKey, body []u8, version i16) ![]u8 {
	return match api_key {
		.api_versions {
			h.handle_api_versions(version)
		}
		.metadata {
			h.handle_metadata(body, version)!
		}
		.find_coordinator {
			h.handle_find_coordinator(body, version)!
		}
		.produce {
			h.handle_produce(body, version)!
		}
		.fetch {
			h.handle_fetch(body, version)!
		}
		.list_offsets {
			h.handle_list_offsets(body, version)!
		}
		.offset_commit {
			h.handle_offset_commit(body, version)!
		}
		.offset_fetch {
			h.handle_offset_fetch(body, version)!
		}
		.join_group {
			h.handle_join_group(body, version)!
		}
		.sync_group {
			h.handle_sync_group(body, version)!
		}
		.heartbeat {
			h.handle_heartbeat(body, version)!
		}
		.leave_group {
			h.handle_leave_group(body, version)!
		}
		.list_groups {
			h.handle_list_groups(body, version)!
		}
		.describe_groups {
			h.handle_describe_groups(body, version)!
		}
		.delete_groups {
			h.handle_delete_groups(body, version)!
		}
		.create_topics {
			h.handle_create_topics(body, version)!
		}
		.delete_topics {
			h.handle_delete_topics(body, version)!
		}
		.init_producer_id {
			h.handle_init_producer_id(body, version)!
		}
		.add_partitions_to_txn {
			h.handle_add_partitions_to_txn(body, version)!
		}
		.add_offsets_to_txn {
			h.handle_add_offsets_to_txn(body, version)!
		}
		.end_txn {
			h.handle_end_txn(body, version)!
		}
		.write_txn_markers {
			h.handle_write_txn_markers(body, version)!
		}
		.txn_offset_commit {
			h.handle_txn_offset_commit(body, version)!
		}
		.consumer_group_heartbeat {
			h.handle_consumer_group_heartbeat(body, version)!
		}
		.consumer_group_describe {
			h.handle_consumer_group_describe(body, version)!
		}
		.sasl_handshake {
			h.handle_sasl_handshake(body, version)!
		}
		.describe_cluster {
			h.handle_describe_cluster(body, version)!
		}
		.describe_configs {
			h.handle_describe_configs(body, version)!
		}
		.describe_acls {
			h.handle_describe_acls(body, version)!
		}
		.create_acls {
			h.handle_create_acls(body, version)!
		}
		.delete_acls {
			h.handle_delete_acls(body, version)!
		}
		.alter_configs {
			h.handle_alter_configs(body, version)!
		}
		.create_partitions {
			h.handle_create_partitions(body, version)!
		}
		.delete_records {
			h.handle_delete_records(body, version)!
		}
		.alter_replica_log_dirs {
			h.handle_alter_replica_log_dirs(body, version)!
		}
		.describe_log_dirs {
			h.handle_describe_log_dirs(body, version)!
		}
		.incremental_alter_configs {
			h.handle_incremental_alter_configs(body, version)!
		}
		.describe_topic_partitions {
			h.handle_describe_topic_partitions(body, version)!
		}
		.share_group_heartbeat {
			h.handle_share_group_heartbeat(body, version)!
		}
		.share_fetch {
			h.handle_share_fetch(body, version)!
		}
		.share_acknowledge {
			h.handle_share_acknowledge(body, version)!
		}
		.initialize_share_group_state {
			h.handle_initialize_share_group_state(body, version)!
		}
		.read_share_group_state {
			h.handle_read_share_group_state(body, version)!
		}
		.write_share_group_state {
			h.handle_write_share_group_state(body, version)!
		}
		.delete_share_group_state {
			h.handle_delete_share_group_state(body, version)!
		}
		else {
			h.logger.warn('Unsupported API key', observability.field_int('api_key', int(api_key)))
			return error('unsupported API key: ${int(api_key)}')
		}
	}
}

// finalize_response builds the final response bytes with the appropriate header format.
// Note: ApiVersions always uses a non-flexible header, even for v3+,
// because the client does not yet know the server capabilities.
fn (h Handler) finalize_response(api_key ApiKey, version i16, correlation_id i32, response_body []u8) []u8 {
	if api_key == .api_versions {
		return build_response(correlation_id, response_body)
	}
	if is_flexible_version(api_key, version) {
		return build_flexible_response(correlation_id, response_body)
	}
	return build_response(correlation_id, response_body)
}

/// handle_frame processes a received Frame and returns a response Frame.
///
/// This is the preferred way to handle requests.
/// Frame-based processing provides better type safety and structured handling.
pub fn (mut h Handler) handle_frame(frame Frame) !Frame {
	// Extract request header information
	req_header := match frame.header {
		FrameRequestHeader { frame.header as FrameRequestHeader }
		else { return error('expected request header') }
	}

	api_key := req_header.api_key
	version := req_header.api_version
	correlation_id := req_header.correlation_id

	// Process body by type and return the response body
	response_body := h.process_body(frame.body, api_key, version)!

	// Build response frame
	return frame_response(correlation_id, api_key, version, response_body)
}

// process_body dispatches the body to the appropriate handler and returns the response body.
fn (mut h Handler) process_body(body Body, api_key ApiKey, version i16) !Body {
	return match body {
		ApiVersionsRequest {
			Body(new_api_versions_response())
		}
		MetadataRequest {
			Body(h.process_metadata(body, version)!)
		}
		ProduceRequest {
			Body(h.process_produce(body, version)!)
		}
		FetchRequest {
			Body(h.process_fetch(body, version)!)
		}
		FindCoordinatorRequest {
			Body(h.process_find_coordinator(body, version)!)
		}
		JoinGroupRequest {
			Body(h.process_join_group(body, version)!)
		}
		SyncGroupRequest {
			Body(h.process_sync_group(body, version)!)
		}
		HeartbeatRequest {
			Body(h.process_heartbeat(body, version)!)
		}
		LeaveGroupRequest {
			Body(h.process_leave_group(body, version)!)
		}
		OffsetCommitRequest {
			Body(h.process_offset_commit(body, version)!)
		}
		OffsetFetchRequest {
			Body(h.process_offset_fetch(body, version)!)
		}
		ListOffsetsRequest {
			Body(h.process_list_offsets(body, version)!)
		}
		CreateTopicsRequest {
			Body(h.process_create_topics(body, version)!)
		}
		DeleteTopicsRequest {
			Body(h.process_delete_topics(body, version)!)
		}
		ListGroupsRequest {
			Body(h.process_list_groups(body, version)!)
		}
		DescribeGroupsRequest {
			Body(h.process_describe_groups(body, version)!)
		}
		InitProducerIdRequest {
			Body(h.process_init_producer_id(body, version)!)
		}
		ConsumerGroupHeartbeatRequest {
			Body(h.process_consumer_group_heartbeat(body, version)!)
		}
		DescribeClusterRequest {
			Body(h.process_describe_cluster(body, version)!)
		}
		DescribeConfigsRequest {
			Body(h.process_describe_configs(body, version)!)
		}
		else {
			return error('unsupported request type')
		}
	}
}

// empty_consumer_assignment builds a minimal (empty) consumer group assignment payload.
fn empty_consumer_assignment() []u8 {
	mut writer := new_writer()
	// Consumer protocol version
	writer.write_i16(0)
	// Assignment array length (0 topics)
	writer.write_i32(0)
	// User data length (0)
	writer.write_i32(0)
	return writer.bytes()
}

// parse_config_i64, parse_config_int are now in common/config_utils.v. generate_uuid remains in core/utils.v.

/// get_metrics_summary returns a summary of protocol metrics.
pub fn (mut h Handler) get_metrics_summary() string {
	return h.metrics.get_summary()
}

/// get_metrics returns the protocol metrics struct.
pub fn (mut h Handler) get_metrics() &observability.ProtocolMetrics {
	return h.metrics
}

/// reset_metrics resets all protocol metrics.
pub fn (mut h Handler) reset_metrics() {
	h.metrics.reset()
}
