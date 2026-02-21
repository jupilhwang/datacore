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

/// new_handler_from_config creates a Handler from a HandlerConfig.
/// This is the single initialization path that all constructor helpers delegate to.
pub fn new_handler_from_config(cfg HandlerConfig) Handler {
	logger := observability.get_named_logger('kafka.handler')
	offset_mgr := offset.new_offset_manager(cfg.storage, logger)
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
/// Actual encoding will be activated after the encoder module is updated.
fn (mut h Handler) encode_record_with_schema(record &domain.Record, schema_obj &domain.Schema) ![]u8 {
	// Log schema info; encoding will be implemented after encoder module update
	h.logger.debug('Schema encoding configured', observability.field_string('schema_type',
		schema_obj.schema_type.str()), observability.field_int('schema_id', schema_obj.id))

	// Return original value for now; real encoding will be added after encoder module update
	return record.value
}

/// decode_record_with_schema decodes a record according to a schema.
/// Currently only validates schema configuration and logs.
/// Actual decoding will be activated after the encoder module is updated.
fn (mut h Handler) decode_record_with_schema(record_data []u8, schema_obj &domain.Schema) ![]u8 {
	// Log schema info; decoding will be implemented after encoder module update
	h.logger.debug('Schema decoding configured', observability.field_string('schema_type',
		schema_obj.schema_type.str()), observability.field_int('schema_id', schema_obj.id))

	// Return original data for now; real decoding will be added after encoder module update
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

	// Parse request
	req := parse_request(data) or {
		h.logger.error('Failed to parse request', observability.field_err_str(err.str()),
			observability.field_bytes('request_size', data.len))
		return err
	}

	api_key := unsafe { ApiKey(req.header.api_key) }
	version := req.header.api_version
	correlation_id := req.header.correlation_id

	h.logger.debug('Processing request', observability.field_string('api', api_key.str()),
		observability.field_int('version', version), observability.field_int('correlation_id',
		correlation_id), observability.field_bytes('request_size', data.len))

	// Security: Check authentication for protected APIs
	// Only enforce authentication if auth_manager is configured
	// If auth_manager is none, authentication is disabled (optional)
	is_public_api := api_key in [.api_versions, .sasl_handshake, .sasl_authenticate]
	mut is_authenticated := false
	if c := conn {
		is_authenticated = c.is_authenticated()
	}
	if h.auth_manager != none && !is_public_api && !is_authenticated {
		h.logger.warn('Unauthorized request', observability.field_string('api', api_key.str()),
			observability.field_int('correlation_id', correlation_id))
		return error('${int(domain.ErrorCode.sasl_authentication_failed)}: Authentication required for ${api_key.str()}')
	}

	// Dispatch based on API key
	mut response_body := []u8{}
	mut success := true

	response_body = match api_key {
		.api_versions {
			h.handle_api_versions(version)
		}
		.metadata {
			h.handle_metadata(req.body, version) or {
				success = false
				return err
			}
		}
		.find_coordinator {
			h.handle_find_coordinator(req.body, version) or {
				success = false
				return err
			}
		}
		.produce {
			h.handle_produce(req.body, version) or {
				success = false
				return err
			}
		}
		.fetch {
			h.handle_fetch(req.body, version) or {
				success = false
				return err
			}
		}
		.list_offsets {
			h.handle_list_offsets(req.body, version) or {
				success = false
				return err
			}
		}
		.offset_commit {
			h.handle_offset_commit(req.body, version) or {
				success = false
				return err
			}
		}
		.offset_fetch {
			h.handle_offset_fetch(req.body, version) or {
				success = false
				return err
			}
		}
		.join_group {
			h.handle_join_group(req.body, version) or {
				success = false
				return err
			}
		}
		.sync_group {
			h.handle_sync_group(req.body, version) or {
				success = false
				return err
			}
		}
		.heartbeat {
			h.handle_heartbeat(req.body, version) or {
				success = false
				return err
			}
		}
		.leave_group {
			h.handle_leave_group(req.body, version) or {
				success = false
				return err
			}
		}
		.list_groups {
			h.handle_list_groups(req.body, version) or {
				success = false
				return err
			}
		}
		.describe_groups {
			h.handle_describe_groups(req.body, version) or {
				success = false
				return err
			}
		}
		.delete_groups {
			h.handle_delete_groups(req.body, version) or {
				success = false
				return err
			}
		}
		.create_topics {
			h.handle_create_topics(req.body, version) or {
				success = false
				return err
			}
		}
		.delete_topics {
			h.handle_delete_topics(req.body, version) or {
				success = false
				return err
			}
		}
		.init_producer_id {
			h.handle_init_producer_id(req.body, version) or {
				success = false
				return err
			}
		}
		.add_partitions_to_txn {
			h.handle_add_partitions_to_txn(req.body, version) or {
				success = false
				return err
			}
		}
		.add_offsets_to_txn {
			h.handle_add_offsets_to_txn(req.body, version) or {
				success = false
				return err
			}
		}
		.end_txn {
			h.handle_end_txn(req.body, version) or {
				success = false
				return err
			}
		}
		.write_txn_markers {
			h.handle_write_txn_markers(req.body, version) or {
				success = false
				return err
			}
		}
		.txn_offset_commit {
			h.handle_txn_offset_commit(req.body, version) or {
				success = false
				return err
			}
		}
		.consumer_group_heartbeat {
			h.handle_consumer_group_heartbeat(req.body, version) or {
				success = false
				return err
			}
		}
		.consumer_group_describe {
			h.handle_consumer_group_describe(req.body, version) or {
				success = false
				return err
			}
		}
		.sasl_handshake {
			h.handle_sasl_handshake(req.body, version) or {
				success = false
				return err
			}
		}
		.sasl_authenticate {
			h.handle_sasl_authenticate(mut conn, req.body, version) or {
				success = false
				return err
			}
		}
		.describe_cluster {
			h.handle_describe_cluster(req.body, version) or {
				success = false
				return err
			}
		}
		.describe_configs {
			h.handle_describe_configs(req.body, version)!
		}
		.describe_acls {
			h.handle_describe_acls(req.body, version)!
		}
		.create_acls {
			h.handle_create_acls(req.body, version)!
		}
		.delete_acls {
			h.handle_delete_acls(req.body, version)!
		}
		.alter_configs {
			h.handle_alter_configs(req.body, version)!
		}
		.create_partitions {
			h.handle_create_partitions(req.body, version)!
		}
		.delete_records {
			h.handle_delete_records(req.body, version)!
		}
		.share_group_heartbeat {
			h.handle_share_group_heartbeat(req.body, version)!
		}
		.share_fetch {
			h.handle_share_fetch(req.body, version)!
		}
		.share_acknowledge {
			h.handle_share_acknowledge(req.body, version)!
		}
		else {
			h.logger.warn('Unsupported API key', observability.field_int('api_key', int(api_key)))
			success = false
			return error('unsupported API key: ${int(api_key)}')
		}
	}

	elapsed := time.since(start_time)
	latency_ms := elapsed.milliseconds()

	// Record metrics
	h.metrics.record_request(api_key.str(), latency_ms, success, data.len, response_body.len)

	h.logger.debug('Request completed', observability.field_string('api', api_key.str()),
		observability.field_int('correlation_id', correlation_id), observability.field_bytes('response_size',
		response_body.len), observability.field_duration('latency', elapsed))

	// Build response with header.
	// Note: ApiVersions always uses a non-flexible header, even for v3+,
	// because the client does not yet know the server capabilities.
	if api_key == .api_versions {
		return build_response(correlation_id, response_body)
	}

	is_flexible := is_flexible_version(api_key, version)
	if is_flexible {
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
